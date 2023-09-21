package warehouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/samber/lo"
	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/alerta"
	"github.com/rudderlabs/rudder-server/services/notifier"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/identity"
	integrationsconfig "github.com/rudderlabs/rudder-server/warehouse/integrations/config"
	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	"github.com/rudderlabs/rudder-server/warehouse/schema"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

const (
	GeneratingStagingFileFailedState = "generating_staging_file_failed"
	GeneratedStagingFileState        = "generated_staging_file"
	FetchingRemoteSchemaFailed       = "fetching_remote_schema_failed"
	InternalProcessingFailed         = "internal_processing_failed"
)

const (
	cloudSourceCategory          = "cloud"
	singerProtocolSourceCategory = "singer-protocol"
)

var stateTransitions map[string]*uploadState

type uploadState struct {
	inProgress string
	failed     string
	completed  string
	nextState  *uploadState
}

type tableNameT string

type UploadJobFactory struct {
	app                  app.App
	dbHandle             *sqlquerywrapper.DB
	destinationValidator validations.DestinationValidator
	loadFile             *loadfiles.LoadFileGenerator
	recovery             *service.Recovery
	notifier             *notifier.Notifier
	conf                 *config.Config
	logger               logger.Logger
	statsFactory         stats.Stats
	encodingFactory      *encoding.Factory
}

type UploadJob struct {
	app                  app.App
	ctx                  context.Context
	dbHandle             *sqlmiddleware.DB
	destinationValidator validations.DestinationValidator
	loadfile             *loadfiles.LoadFileGenerator
	tableUploadsRepo     *repo.TableUploads
	recovery             *service.Recovery
	whManager            manager.Manager
	notifier             *notifier.Notifier
	schemaHandle         *schema.Schema // @TODO local interface
	conf                 *config.Config
	logger               logger.Logger
	statsFactory         stats.Stats
	loadFileGenStartTime time.Time

	upload         model.Upload
	warehouse      model.Warehouse
	stagingFiles   []*model.StagingFile
	stagingFileIDs []int64
	alertSender    alerta.AlertSender
	now            func() time.Time

	pendingTableUploads      []model.PendingTableUpload
	pendingTableUploadsRepo  pendingTableUploadsRepo
	pendingTableUploadsOnce  sync.Once
	pendingTableUploadsError error

	config struct {
		refreshPartitionBatchSize                        int
		retryTimeWindow                                  time.Duration
		minRetryAttempts                                 int
		disableAlter                                     bool
		minUploadBackoff                                 time.Duration
		maxUploadBackoff                                 time.Duration
		alwaysRegenerateAllLoadFiles                     bool
		reportingEnabled                                 bool
		generateTableLoadCountMetrics                    bool
		disableGenerateTableLoadCountMetricsWorkspaceIDs []string
		maxParallelLoadsWorkspaceIDs                     map[string]interface{}
		columnsBatchSize                                 int
		longRunningUploadStatThresholdInMin              time.Duration
		tableCountQueryTimeout                           time.Duration
	}

	errorHandler    ErrorHandler
	encodingFactory *encoding.Factory

	stats struct {
		uploadTime                         stats.Measurement
		userTablesLoadTime                 stats.Measurement
		identityTablesLoadTime             stats.Measurement
		otherTablesLoadTime                stats.Measurement
		loadFileGenerationTime             stats.Measurement
		tablesAdded                        stats.Measurement
		columnsAdded                       stats.Measurement
		uploadFailed                       stats.Measurement
		totalRowsSynced                    stats.Measurement
		numStagedEvents                    stats.Measurement
		uploadSuccess                      stats.Measurement
		stagingLoadFileEventsCountMismatch stats.Measurement
	}
}

type UploadColumn struct {
	Column string
	Value  interface{}
}

type pendingTableUploadsRepo interface {
	PendingTableUploads(ctx context.Context, namespace string, uploadID int64, destID string) ([]model.PendingTableUpload, error)
}

const (
	UploadStatusField          = "status"
	UploadStartLoadFileIDField = "start_load_file_id"
	UploadEndLoadFileIDField   = "end_load_file_id"
	UploadUpdatedAtField       = "updated_at"
	UploadTimingsField         = "timings"
	UploadSchemaField          = "schema"
	UploadLastExecAtField      = "last_exec_at"
	UploadInProgress           = "in_progress"
)

var (
	alwaysMarkExported                               = []string{whutils.DiscardsTable}
	warehousesToAlwaysRegenerateAllLoadFilesOnResume = []string{whutils.SNOWFLAKE, whutils.BQ}
	mergeSourceCategoryMap                           = map[string]struct{}{
		"cloud":           {},
		"singer-protocol": {},
	}
)

func init() {
	initializeStateMachine()
}

func (f *UploadJobFactory) NewUploadJob(ctx context.Context, dto *model.UploadJob, whManager manager.Manager) *UploadJob {
	ujCtx := whutils.CtxWithUploadID(ctx, dto.Upload.ID)

	uj := &UploadJob{
		ctx:                  ujCtx,
		app:                  f.app,
		dbHandle:             f.dbHandle,
		loadfile:             f.loadFile,
		recovery:             f.recovery,
		notifier:             f.notifier,
		whManager:            whManager,
		destinationValidator: f.destinationValidator,
		conf:                 f.conf,
		logger:               f.logger,
		statsFactory:         f.statsFactory,
		tableUploadsRepo:     repo.NewTableUploads(f.dbHandle),
		schemaHandle: schema.New(
			f.dbHandle,
			dto.Warehouse,
			config.Default,
			f.logger.Child("warehouse"),
		),

		upload:         dto.Upload,
		warehouse:      dto.Warehouse,
		stagingFiles:   dto.StagingFiles,
		stagingFileIDs: repo.StagingFileIDs(dto.StagingFiles),

		pendingTableUploadsRepo: repo.NewUploads(f.dbHandle),
		pendingTableUploads:     []model.PendingTableUpload{},

		alertSender: alerta.NewClient(
			f.conf.GetString("ALERTA_URL", "https://alerta.rudderstack.com/api/"),
		),
		now: timeutil.Now,

		errorHandler:    ErrorHandler{whManager},
		encodingFactory: f.encodingFactory,
	}

	uj.config.refreshPartitionBatchSize = f.conf.GetInt("Warehouse.refreshPartitionBatchSize", 100)
	uj.config.minRetryAttempts = f.conf.GetInt("Warehouse.minRetryAttempts", 3)
	uj.config.disableAlter = f.conf.GetBool("Warehouse.disableAlter", false)
	uj.config.alwaysRegenerateAllLoadFiles = f.conf.GetBool("Warehouse.alwaysRegenerateAllLoadFiles", true)
	uj.config.reportingEnabled = f.conf.GetBool("Reporting.enabled", types.DefaultReportingEnabled)
	uj.config.generateTableLoadCountMetrics = f.conf.GetBool("Warehouse.generateTableLoadCountMetrics", true)
	uj.config.disableGenerateTableLoadCountMetricsWorkspaceIDs = f.conf.GetStringSlice("Warehouse.disableGenerateTableLoadCountMetricsWorkspaceIDs", nil)
	uj.config.columnsBatchSize = f.conf.GetInt(fmt.Sprintf("Warehouse.%s.columnsBatchSize", whutils.WHDestNameMap[uj.upload.DestinationType]), 100)
	uj.config.maxParallelLoadsWorkspaceIDs = f.conf.GetStringMap(fmt.Sprintf("Warehouse.%s.maxParallelLoadsWorkspaceIDs", whutils.WHDestNameMap[uj.upload.DestinationType]), nil)

	if f.conf.IsSet("Warehouse.tableCountQueryTimeout") {
		uj.config.tableCountQueryTimeout = f.conf.GetDuration("Warehouse.tableCountQueryTimeout", 30, time.Second)
	} else {
		uj.config.tableCountQueryTimeout = f.conf.GetDuration("Warehouse.tableCountQueryTimeoutInS", 30, time.Second)
	}
	if f.conf.IsSet("Warehouse.longRunningUploadStatThreshold") {
		uj.config.longRunningUploadStatThresholdInMin = f.conf.GetDuration("Warehouse.longRunningUploadStatThreshold", 120, time.Minute)
	} else {
		uj.config.longRunningUploadStatThresholdInMin = f.conf.GetDuration("Warehouse.longRunningUploadStatThresholdInMin", 120, time.Minute)
	}
	if f.conf.IsSet("Warehouse.minUploadBackoff") {
		uj.config.minUploadBackoff = f.conf.GetDuration("Warehouse.minUploadBackoff", 60, time.Second)
	} else {
		uj.config.minUploadBackoff = f.conf.GetDuration("Warehouse.minUploadBackoffInS", 60, time.Second)
	}
	if f.conf.IsSet("Warehouse.maxUploadBackoff") {
		uj.config.maxUploadBackoff = f.conf.GetDuration("Warehouse.maxUploadBackoff", 1800, time.Second)
	} else {
		uj.config.maxUploadBackoff = f.conf.GetDuration("Warehouse.maxUploadBackoffInS", 1800, time.Second)
	}
	if f.conf.IsSet("Warehouse.retryTimeWindow") {
		uj.config.retryTimeWindow = f.conf.GetDuration("Warehouse.retryTimeWindow", 180, time.Minute)
	} else {
		uj.config.retryTimeWindow = f.conf.GetDuration("Warehouse.retryTimeWindowInMins", 180, time.Minute)
	}

	uj.stats.uploadTime = uj.timerStat("upload_time")
	uj.stats.userTablesLoadTime = uj.timerStat("user_tables_load_time")
	uj.stats.identityTablesLoadTime = uj.timerStat("identity_tables_load_time")
	uj.stats.otherTablesLoadTime = uj.timerStat("other_tables_load_time")
	uj.stats.loadFileGenerationTime = uj.timerStat("load_file_generation_time")
	uj.stats.tablesAdded = uj.counterStat("tables_added")
	uj.stats.columnsAdded = uj.counterStat("columns_added")
	uj.stats.uploadFailed = uj.counterStat("warehouse_failed_uploads")
	uj.stats.totalRowsSynced = uj.counterStat("total_rows_synced")
	uj.stats.numStagedEvents = uj.counterStat("num_staged_events")
	uj.stats.uploadSuccess = uj.counterStat("upload_success")
	uj.stats.stagingLoadFileEventsCountMismatch = uj.guageStat("warehouse_staging_load_file_events_count_mismatched")

	return uj
}

func (job *UploadJob) identifiesTableName() string {
	return whutils.ToProviderCase(job.warehouse.Type, whutils.IdentifiesTable)
}

func (job *UploadJob) usersTableName() string {
	return whutils.ToProviderCase(job.warehouse.Type, whutils.UsersTable)
}

func (job *UploadJob) identityMergeRulesTableName() string {
	return whutils.ToProviderCase(job.warehouse.Type, whutils.IdentityMergeRulesTable)
}

func (job *UploadJob) identityMappingsTableName() string {
	return whutils.ToProviderCase(job.warehouse.Type, whutils.IdentityMappingsTable)
}

func (job *UploadJob) trackLongRunningUpload() chan struct{} {
	ch := make(chan struct{}, 1)
	rruntime.GoForWarehouse(func() {
		select {
		case <-ch:
			// do nothing
		case <-time.After(job.config.longRunningUploadStatThresholdInMin):
			job.logger.Infof("[WH]: Registering stat for long running upload: %d, dest: %s", job.upload.ID, job.warehouse.Identifier)

			job.statsFactory.NewTaggedStat(
				"warehouse.long_running_upload",
				stats.CountType,
				stats.Tags{
					"workspaceId": job.warehouse.WorkspaceID,
					"destID":      job.warehouse.Destination.ID,
				},
			).Count(1)
		}
	})
	return ch
}

func (job *UploadJob) generateUploadSchema() error {
	uploadSchema, err := job.schemaHandle.PrepareUploadSchema(job.ctx, job.stagingFiles)
	if err != nil {
		return fmt.Errorf("consolidate staging files schema using warehouse schema: %w", err)
	}

	marshalledSchema, err := json.Marshal(uploadSchema)
	if err != nil {
		panic(err)
	}

	err = job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumn{
		{Column: UploadSchemaField, Value: marshalledSchema},
	}})
	if err != nil {
		return fmt.Errorf("set upload schema: %w", err)
	}

	job.upload.UploadSchema = uploadSchema

	return nil
}

func (job *UploadJob) initTableUploads() error {
	schemaForUpload := job.upload.UploadSchema
	destType := job.warehouse.Type
	tables := make([]string, 0, len(schemaForUpload))
	for t := range schemaForUpload {
		tables = append(tables, t)
		// also track upload to rudder_identity_mappings if the upload has records for rudder_identity_merge_rules
		if slices.Contains(whutils.IdentityEnabledWarehouses, destType) && t == whutils.ToProviderCase(destType, whutils.IdentityMergeRulesTable) {
			if _, ok := schemaForUpload[whutils.ToProviderCase(destType, whutils.IdentityMappingsTable)]; !ok {
				tables = append(tables, whutils.ToProviderCase(destType, whutils.IdentityMappingsTable))
			}
		}
	}

	return job.tableUploadsRepo.Insert(
		job.ctx,
		job.upload.ID,
		tables,
	)
}

func (job *UploadJob) getTotalRowsInLoadFiles(ctx context.Context) int64 {
	var total sql.NullInt64

	sqlStatement := fmt.Sprintf(`
		WITH row_numbered_load_files as (
		  SELECT
			total_events,
			table_name,
			row_number() OVER (
			  PARTITION BY staging_file_id,
			  table_name
			  ORDER BY
				id DESC
			) AS row_number
		  FROM
			%[1]s
		  WHERE
			staging_file_id IN (%[2]v)
		)
		SELECT
		  SUM(total_events)
		FROM
		  row_numbered_load_files WHERE
		  row_number = 1
		  AND table_name != '%[3]s';
	`,
		whutils.WarehouseLoadFilesTable,
		misc.IntArrayToString(job.stagingFileIDs, ","),
		whutils.ToProviderCase(job.warehouse.Type, whutils.DiscardsTable),
	)
	if err := job.dbHandle.QueryRowContext(ctx, sqlStatement).Scan(&total); err != nil {
		job.logger.Errorf(`Error in getTotalRowsInLoadFiles: %v`, err)
	}
	return total.Int64
}

func (job *UploadJob) matchRowsInStagingAndLoadFiles(ctx context.Context) error {
	rowsInStagingFiles, err := repo.NewStagingFiles(job.dbHandle).TotalEventsForUpload(ctx, job.upload)
	if err != nil {
		return fmt.Errorf("total rows: %w", err)
	}
	rowsInLoadFiles := job.getTotalRowsInLoadFiles(ctx)
	if (rowsInStagingFiles != rowsInLoadFiles) || rowsInStagingFiles == 0 || rowsInLoadFiles == 0 {
		job.logger.Errorf(`Error: Rows count mismatch between staging and load files for upload:%d. rowsInStagingFiles: %d, rowsInLoadFiles: %d`, job.upload.ID, rowsInStagingFiles, rowsInLoadFiles)
		job.stats.stagingLoadFileEventsCountMismatch.Gauge(rowsInStagingFiles - rowsInLoadFiles)
	}
	return nil
}

func (job *UploadJob) run() (err error) {
	start := job.now()
	ch := job.trackLongRunningUpload()
	defer func() {
		_ = job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumn{{Column: UploadInProgress, Value: false}}})

		job.stats.uploadTime.Since(start)
		ch <- struct{}{}
	}()

	_ = job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumn{{Column: UploadLastExecAtField, Value: job.now()}, {Column: UploadInProgress, Value: true}}})

	if len(job.stagingFiles) == 0 {
		err := fmt.Errorf("no staging files found")
		_, _ = job.setUploadError(err, InternalProcessingFailed)
		return err
	}

	whManager := job.whManager
	whManager.SetConnectionTimeout(whutils.GetConnectionTimeout(
		job.warehouse.Type, job.warehouse.Destination.ID,
	))
	err = whManager.Setup(job.ctx, job.warehouse, job)
	if err != nil {
		_, _ = job.setUploadError(err, InternalProcessingFailed)
		return err
	}
	defer whManager.Cleanup(job.ctx)

	if err = job.recovery.Recover(job.ctx, whManager, job.warehouse); err != nil {
		_, _ = job.setUploadError(err, InternalProcessingFailed)
		return err
	}

	hasSchemaChanged, err := job.schemaHandle.SyncRemoteSchema(job.ctx, whManager, job.upload.ID)
	if err != nil {
		_, _ = job.setUploadError(err, FetchingRemoteSchemaFailed)
		return err
	}
	if hasSchemaChanged {
		job.logger.Infof("[WH] Remote schema changed for Warehouse: %s", job.warehouse.Identifier)
	}

	userTables := []string{job.identifiesTableName(), job.usersTableName()}
	identityTables := []string{job.identityMergeRulesTableName(), job.identityMappingsTableName()}

	var (
		newStatus       string
		nextUploadState *uploadState
	)

	// do not set nextUploadState if hasSchemaChanged to make it start from 1st step again
	if !hasSchemaChanged {
		nextUploadState = getNextUploadState(job.upload.Status)
	}
	if nextUploadState == nil {
		nextUploadState = stateTransitions[model.GeneratedUploadSchema]
	}

	for {
		stateStartTime := job.now()
		err = nil

		_ = job.setUploadStatus(UploadStatusOpts{Status: nextUploadState.inProgress})
		job.logger.Debugf("[WH] Upload: %d, Current state: %s", job.upload.ID, nextUploadState.inProgress)

		targetStatus := nextUploadState.completed

		switch targetStatus {
		case model.GeneratedUploadSchema:
			newStatus = nextUploadState.failed
			err = job.generateUploadSchema()
			if err != nil {
				break
			}
			newStatus = nextUploadState.completed

		case model.CreatedTableUploads:
			newStatus = nextUploadState.failed
			err = job.initTableUploads()
			if err != nil {
				break
			}
			newStatus = nextUploadState.completed

		case model.GeneratedLoadFiles:
			newStatus = nextUploadState.failed
			// generate load files for all staging files(including succeeded) if hasSchemaChanged or if its snowflake(to have all load files in same folder in bucket) or set via toml/env
			generateAll := hasSchemaChanged || slices.Contains(warehousesToAlwaysRegenerateAllLoadFilesOnResume, job.warehouse.Type) || job.config.alwaysRegenerateAllLoadFiles
			var startLoadFileID, endLoadFileID int64
			if generateAll {
				startLoadFileID, endLoadFileID, err = job.loadfile.ForceCreateLoadFiles(job.ctx, job.DTO())
			} else {
				startLoadFileID, endLoadFileID, err = job.loadfile.CreateLoadFiles(job.ctx, job.DTO())
			}
			if err != nil {
				break
			}

			if err = job.setLoadFileIDs(startLoadFileID, endLoadFileID); err != nil {
				break
			}

			if err = job.matchRowsInStagingAndLoadFiles(job.ctx); err != nil {
				break
			}

			_ = job.recordLoadFileGenerationTimeStat(startLoadFileID, endLoadFileID)

			newStatus = nextUploadState.completed

		case model.UpdatedTableUploadsCounts:
			newStatus = nextUploadState.failed
			for tableName := range job.upload.UploadSchema {
				if err = job.tableUploadsRepo.PopulateTotalEventsFromStagingFileIDs(
					job.ctx,
					job.upload.ID,
					tableName,
					job.stagingFileIDs,
				); err != nil {
					err = fmt.Errorf("populate table uploads total events from staging file: %w", err)
					break
				}
			}
			if err != nil {
				break
			}
			newStatus = nextUploadState.completed

		case model.CreatedRemoteSchema:
			newStatus = nextUploadState.failed
			if job.schemaHandle.IsWarehouseSchemaEmpty() {
				if err = whManager.CreateSchema(job.ctx); err != nil {
					break
				}
			}
			newStatus = nextUploadState.completed

		case model.ExportedData:
			newStatus = nextUploadState.failed

			var currentJobSucceededTables map[string]model.PendingTableUpload

			if _, currentJobSucceededTables, err = job.TablesToSkip(); err != nil {
				err = fmt.Errorf("tables to skip: %w", err)
				break
			}

			var (
				loadErrors        []error
				loadErrorLock     sync.Mutex
				loadFilesTableMap map[tableNameT]bool
			)

			loadFilesTableMap, err = job.getLoadFilesTableMap()
			if err != nil {
				err = fmt.Errorf("unable to get load files table map: %w", err)
				break
			}

			var wg sync.WaitGroup
			wg.Add(3)

			rruntime.GoForWarehouse(func() {
				defer wg.Done()

				var succeededUserTableCount int
				for _, userTable := range userTables {
					if _, ok := currentJobSucceededTables[userTable]; ok {
						succeededUserTableCount++
					}
				}
				if succeededUserTableCount >= len(userTables) {
					return
				}
				err := job.exportUserTables(loadFilesTableMap)
				if err != nil {
					loadErrorLock.Lock()
					loadErrors = append(loadErrors, err)
					loadErrorLock.Unlock()
				}
			})

			rruntime.GoForWarehouse(func() {
				defer wg.Done()

				var succeededIdentityTableCount int
				for _, identityTable := range identityTables {
					if _, ok := currentJobSucceededTables[identityTable]; ok {
						succeededIdentityTableCount++
					}
				}
				if succeededIdentityTableCount >= len(identityTables) {
					return
				}
				err := job.exportIdentities()
				if err != nil {
					loadErrorLock.Lock()
					loadErrors = append(loadErrors, err)
					loadErrorLock.Unlock()
				}
			})

			rruntime.GoForWarehouse(func() {
				defer wg.Done()

				specialTables := make([]string, 0, len(userTables)+len(identityTables))
				specialTables = append(specialTables, userTables...)
				specialTables = append(specialTables, identityTables...)

				err := job.exportRegularTables(specialTables, loadFilesTableMap)
				if err != nil {
					loadErrorLock.Lock()
					loadErrors = append(loadErrors, err)
					loadErrorLock.Unlock()
				}
			})

			wg.Wait()

			if err := job.RefreshPartitions(job.upload.LoadFileStartID, job.upload.LoadFileEndID); err != nil {
				loadErrors = append(loadErrors, fmt.Errorf("refresh partitions: %w", err))
			}

			if len(loadErrors) > 0 {
				err = misc.ConcatErrors(loadErrors)
				break
			}
			job.generateUploadSuccessMetrics()

			newStatus = nextUploadState.completed

		default:
			// If unknown state, start again
			newStatus = model.Waiting
		}

		if err != nil {
			state, err := job.setUploadError(err, newStatus)
			if err == nil && state == model.Aborted {
				job.generateUploadAbortedMetrics()
			}
			break
		}

		job.logger.Debugf("[WH] Upload: %d, Next state: %s", job.upload.ID, newStatus)

		uploadStatusOpts := UploadStatusOpts{Status: newStatus}
		if newStatus == model.ExportedData {

			rowCount, _ := repo.NewStagingFiles(job.dbHandle).TotalEventsForUpload(job.ctx, job.upload)

			reportingMetric := types.PUReportedMetric{
				ConnectionDetails: types.ConnectionDetails{
					SourceID:        job.upload.SourceID,
					DestinationID:   job.upload.DestinationID,
					SourceTaskRunID: job.upload.SourceTaskRunID,
					SourceJobID:     job.upload.SourceJobID,
					SourceJobRunID:  job.upload.SourceJobRunID,
				},
				PUDetails: types.PUDetails{
					InPU:       types.BATCH_ROUTER,
					PU:         types.WAREHOUSE,
					TerminalPU: true,
				},
				StatusDetail: &types.StatusDetail{
					Status:      jobsdb.Succeeded.State,
					StatusCode:  200,
					Count:       rowCount,
					SampleEvent: []byte("{}"),
				},
			}
			uploadStatusOpts.ReportingMetric = reportingMetric
		}
		_ = job.setUploadStatus(uploadStatusOpts)

		// record metric for time taken by the current state
		job.timerStat(nextUploadState.inProgress).SendTiming(time.Since(stateStartTime))

		if newStatus == model.ExportedData {
			break
		}

		nextUploadState = getNextUploadState(newStatus)
	}

	if newStatus != model.ExportedData {
		return fmt.Errorf("upload Job failed: %w", err)
	}

	return nil
}

func (job *UploadJob) exportUserTables(loadFilesTableMap map[tableNameT]bool) (err error) {
	uploadSchema := job.upload.UploadSchema
	if _, ok := uploadSchema[job.identifiesTableName()]; ok {

		defer job.stats.userTablesLoadTime.RecordDuration()()
		var loadErrors []error
		loadErrors, err = job.loadUserTables(loadFilesTableMap)
		if err != nil {
			return
		}

		if len(loadErrors) > 0 {
			err = misc.ConcatErrors(loadErrors)
			return
		}
	}
	return
}

func (job *UploadJob) exportIdentities() (err error) {
	// Load Identities if enabled
	uploadSchema := job.upload.UploadSchema
	if whutils.IDResolutionEnabled() && slices.Contains(whutils.IdentityEnabledWarehouses, job.warehouse.Type) {
		if _, ok := uploadSchema[job.identityMergeRulesTableName()]; ok {
			defer job.stats.identityTablesLoadTime.RecordDuration()()

			var loadErrors []error
			loadErrors, err = job.loadIdentityTables(false)
			if err != nil {
				return
			}

			if len(loadErrors) > 0 {
				err = misc.ConcatErrors(loadErrors)
				return
			}
		}
	}
	return
}

func (job *UploadJob) exportRegularTables(specialTables []string, loadFilesTableMap map[tableNameT]bool) (err error) {
	//[]string{job.identifiesTableName(), job.usersTableName(), job.identityMergeRulesTableName(), job.identityMappingsTableName()}
	// Export all other tables
	defer job.stats.otherTablesLoadTime.RecordDuration()()

	loadErrors := job.loadAllTablesExcept(specialTables, loadFilesTableMap)

	if len(loadErrors) > 0 {
		err = misc.ConcatErrors(loadErrors)
		return
	}

	return
}

// CanAppend returns true if:
// * the source is not an ETL source
// * the source is not a replay source
// * the source category is not in "mergeSourceCategoryMap"
func (job *UploadJob) CanAppend() bool {
	if isSourceETL := job.upload.SourceJobRunID != ""; isSourceETL {
		return false
	}
	if job.warehouse.Source.IsReplaySource() {
		return false
	}
	if _, isMergeCategory := mergeSourceCategoryMap[job.warehouse.Source.SourceDefinition.Category]; isMergeCategory {
		return false
	}
	return true
}

func (job *UploadJob) TablesToSkip() (map[string]model.PendingTableUpload, map[string]model.PendingTableUpload, error) {
	job.pendingTableUploadsOnce.Do(func() {
		job.pendingTableUploads, job.pendingTableUploadsError = job.pendingTableUploadsRepo.PendingTableUploads(
			job.ctx,
			job.upload.Namespace,
			job.upload.ID,
			job.upload.DestinationID,
		)
	})

	if job.pendingTableUploadsError != nil {
		return nil, nil, fmt.Errorf("pending table uploads: %w", job.pendingTableUploadsError)
	}

	var (
		previouslyFailedTableMap   = make(map[string]model.PendingTableUpload)
		currentlySucceededTableMap = make(map[string]model.PendingTableUpload)
	)

	for _, pendingTableUpload := range job.pendingTableUploads {
		if pendingTableUpload.UploadID < job.upload.ID && pendingTableUpload.Status == model.TableUploadExportingFailed {
			previouslyFailedTableMap[pendingTableUpload.TableName] = pendingTableUpload
		}
		if pendingTableUpload.UploadID == job.upload.ID && pendingTableUpload.Status == model.TableUploadExported { // Current upload and table upload succeeded
			currentlySucceededTableMap[pendingTableUpload.TableName] = pendingTableUpload
		}
	}
	return previouslyFailedTableMap, currentlySucceededTableMap, nil
}

func (job *UploadJob) resolveIdentities(populateHistoricIdentities bool) (err error) {
	idr := identity.New(
		job.warehouse,
		job.dbHandle,
		job,
		job.upload.ID,
		job.whManager,
		downloader.NewDownloader(&job.warehouse, job, 8),
		job.encodingFactory,
	)
	if populateHistoricIdentities {
		return idr.ResolveHistoricIdentities(job.ctx)
	}
	return idr.Resolve(job.ctx)
}

func (job *UploadJob) UpdateTableSchema(tName string, tableSchemaDiff whutils.TableSchemaDiff) (err error) {
	job.logger.Infof(`[WH]: Starting schema update for table %s in namespace %s of destination %s:%s`, tName, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)
	if tableSchemaDiff.TableToBeCreated {
		err = job.whManager.CreateTable(job.ctx, tName, tableSchemaDiff.ColumnMap)
		if err != nil {
			job.logger.Errorf("Error creating table %s on namespace: %s, error: %v", tName, job.warehouse.Namespace, err)
			return err
		}
		job.stats.tablesAdded.Increment()
		return nil
	}

	if err = job.addColumnsToWarehouse(job.ctx, tName, tableSchemaDiff.ColumnMap); err != nil {
		return fmt.Errorf("adding columns to warehouse: %w", err)
	}

	if err = job.alterColumnsToWarehouse(job.ctx, tName, tableSchemaDiff.AlteredColumnMap); err != nil {
		return fmt.Errorf("altering columns to warehouse: %w", err)
	}

	return nil
}

func (job *UploadJob) alterColumnsToWarehouse(ctx context.Context, tName string, columnsMap model.TableSchema) error {
	if job.config.disableAlter {
		job.logger.Debugw("skipping alter columns to warehouse",
			logfield.SourceID, job.warehouse.Source.ID,
			logfield.SourceType, job.warehouse.Source.SourceDefinition.Name,
			logfield.DestinationID, job.warehouse.Destination.ID,
			logfield.DestinationType, job.warehouse.Destination.DestinationDefinition.Name,
			logfield.WorkspaceID, job.warehouse.WorkspaceID,
			logfield.TableName, tName,
			"columns", columnsMap,
		)
		return nil
	}
	var responseToAlerta []model.AlterTableResponse
	var errs []error

	for columnName, columnType := range columnsMap {
		res, err := job.whManager.AlterColumn(ctx, tName, columnName, columnType)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if res.IsDependent {
			responseToAlerta = append(responseToAlerta, res)
			continue
		}

		job.logger.Infof(`
			[WH]: Altered column %s of type %s in table %s in namespace %s of destination %s:%s
		`,
			columnName,
			columnType,
			tName,
			job.warehouse.Namespace,
			job.warehouse.Type,
			job.warehouse.Destination.ID,
		)
	}

	if len(responseToAlerta) > 0 {
		queries := make([]string, len(responseToAlerta))
		for i, res := range responseToAlerta {
			queries[i] = res.Query
		}

		query := strings.Join(queries, "\n")
		job.logger.Infof("altering dependent columns: %s", query)

		err := job.alertSender.SendAlert(ctx, "warehouse-column-changes",
			alerta.SendAlertOpts{
				Severity:    alerta.SeverityCritical,
				Priority:    alerta.PriorityP1,
				Environment: alerta.PROXYMODE,
				Tags: alerta.Tags{
					"destID":      job.upload.DestinationID,
					"destType":    job.upload.DestinationType,
					"workspaceID": job.upload.WorkspaceID,
					"query":       query,
				},
			},
		)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return misc.ConcatErrors(errs)
	}

	return nil
}

func (job *UploadJob) addColumnsToWarehouse(ctx context.Context, tName string, columnsMap model.TableSchema) (err error) {
	job.logger.Infof(`[WH]: Adding columns for table %s in namespace %s of destination %s:%s`, tName, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)

	var columnsToAdd []whutils.ColumnInfo
	for columnName, columnType := range columnsMap {
		// columns present in unrecognized schema should be skipped
		if job.schemaHandle.IsColumnInUnrecognizedSchema(tName, columnName) {
			continue
		}

		columnsToAdd = append(columnsToAdd, whutils.ColumnInfo{Name: columnName, Type: columnType})
	}

	chunks := lo.Chunk(columnsToAdd, job.config.columnsBatchSize)
	for _, chunk := range chunks {
		err = job.whManager.AddColumns(ctx, tName, chunk)
		if err != nil {
			err = fmt.Errorf("failed to add columns for table %s in namespace %s of destination %s:%s with error: %w", tName, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID, err)
			break
		}

		job.stats.columnsAdded.Count(len(chunk))
	}
	return err
}

func (job *UploadJob) loadAllTablesExcept(skipLoadForTables []string, loadFilesTableMap map[tableNameT]bool) []error {
	maxParallelLoadsMap := integrationsconfig.MaxParallelLoadsMap(job.conf)

	uploadSchema := job.upload.UploadSchema
	var parallelLoads int
	var ok bool
	if parallelLoads, ok = maxParallelLoadsMap[job.warehouse.Type]; !ok {
		parallelLoads = 1
	}

	if k, ok := job.config.maxParallelLoadsWorkspaceIDs[strings.ToLower(job.warehouse.WorkspaceID)]; ok {
		if load, ok := k.(float64); ok {
			parallelLoads = int(load)
		}
	}

	job.logger.Infof(`[WH]: Running %d parallel loads in namespace %s of destination %s:%s`, parallelLoads, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)

	var loadErrors []error
	var loadErrorLock sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(uploadSchema))

	var alteredSchemaInAtLeastOneTable atomic.Bool
	concurrencyGuard := make(chan struct{}, parallelLoads)

	var (
		err                       error
		previouslyFailedTables    map[string]model.PendingTableUpload
		currentJobSucceededTables map[string]model.PendingTableUpload
	)
	if previouslyFailedTables, currentJobSucceededTables, err = job.TablesToSkip(); err != nil {
		return []error{fmt.Errorf("tables to skip: %w", err)}
	}

	for tableName := range uploadSchema {
		if slices.Contains(skipLoadForTables, tableName) {
			wg.Done()
			continue
		}
		if _, ok := currentJobSucceededTables[tableName]; ok {
			wg.Done()
			continue
		}
		if prevJobStatus, ok := previouslyFailedTables[tableName]; ok {
			skipError := fmt.Errorf("skipping table %s because it previously failed to load in an earlier job: %d with error: %s", tableName, prevJobStatus.UploadID, prevJobStatus.Error)
			loadErrors = append(loadErrors, skipError)
			wg.Done()
			continue
		}
		hasLoadFiles := loadFilesTableMap[tableNameT(tableName)]
		if !hasLoadFiles {
			if slices.Contains(alwaysMarkExported, strings.ToLower(tableName)) {
				status := model.TableUploadExported
				_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tableName, repo.TableUploadSetOptions{
					Status: &status,
				})
			}
			wg.Done()
			continue
		}
		tableName := tableName
		concurrencyGuard <- struct{}{}
		rruntime.GoForWarehouse(func() {
			alteredSchema, err := job.loadTable(tableName)
			if alteredSchema {
				alteredSchemaInAtLeastOneTable.Store(true)
			}
			if err != nil {
				loadErrorLock.Lock()
				loadErrors = append(loadErrors, err)
				loadErrorLock.Unlock()
			}

			<-concurrencyGuard
			wg.Done()
		})
	}
	wg.Wait()

	if alteredSchemaInAtLeastOneTable.Load() {
		job.logger.Infof("loadAllTablesExcept: schema changed - updating local schema for %s", job.warehouse.Identifier)
		_ = job.schemaHandle.UpdateLocalSchemaWithWarehouse(job.ctx, job.upload.ID) // TODO check error
	}

	return loadErrors
}

func (job *UploadJob) updateSchema(tName string) (alteredSchema bool, err error) {
	tableSchemaDiff := job.schemaHandle.TableSchemaDiff(tName, job.GetTableSchemaInUpload(tName))
	if tableSchemaDiff.Exists {
		err = job.UpdateTableSchema(tName, tableSchemaDiff)
		if err != nil {
			return
		}

		job.schemaHandle.UpdateWarehouseTableSchema(tName, tableSchemaDiff.UpdatedSchema)
		alteredSchema = true
	}
	return
}

func (job *UploadJob) getTotalCount(tName string) (int64, error) {
	var (
		total    int64
		countErr error
	)

	operation := func() error {
		ctx, cancel := context.WithTimeout(job.ctx, job.config.tableCountQueryTimeout)
		defer cancel()

		total, countErr = job.whManager.GetTotalCountInTable(ctx, tName)
		return countErr
	}

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 5 * time.Second
	expBackoff.RandomizationFactor = 0
	expBackoff.Reset()

	backoffWithMaxRetry := backoff.WithMaxRetries(expBackoff, 5)
	err := backoff.Retry(operation, backoffWithMaxRetry)
	return total, err
}

func (job *UploadJob) loadTable(tName string) (bool, error) {
	alteredSchema, err := job.updateSchema(tName)
	if err != nil {
		status := model.TableUploadUpdatingSchemaFailed
		errorsString := misc.QuoteLiteral(err.Error())
		_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tName, repo.TableUploadSetOptions{
			Status: &status,
			Error:  &errorsString,
		})
		return alteredSchema, fmt.Errorf("update schema: %w", err)
	}

	job.logger.Infow("starting load for table",
		logfield.UploadJobID, job.upload.ID,
		logfield.SourceID, job.warehouse.Source.ID,
		logfield.DestinationID, job.warehouse.Destination.ID,
		logfield.SourceType, job.warehouse.Source.SourceDefinition.Name,
		logfield.DestinationType, job.warehouse.Destination.DestinationDefinition.Name,
		logfield.WorkspaceID, job.warehouse.WorkspaceID,
		logfield.Namespace, job.warehouse.Namespace,
		logfield.TableName, tName,
	)

	status := model.TableUploadExecuting
	lastExecTime := job.now()
	_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tName, repo.TableUploadSetOptions{
		Status:       &status,
		LastExecTime: &lastExecTime,
	})

	generateTableLoadCountVerificationsMetrics := job.config.generateTableLoadCountMetrics
	if slices.Contains(job.config.disableGenerateTableLoadCountMetricsWorkspaceIDs, job.upload.WorkspaceID) {
		generateTableLoadCountVerificationsMetrics = false
	}

	var totalBeforeLoad, totalAfterLoad int64
	if generateTableLoadCountVerificationsMetrics {
		var errTotalCount error
		totalBeforeLoad, errTotalCount = job.getTotalCount(tName)
		if errTotalCount != nil {
			job.logger.Warnw("total count in table before loading",
				logfield.SourceID, job.upload.SourceID,
				logfield.DestinationID, job.upload.DestinationID,
				logfield.DestinationType, job.upload.DestinationType,
				logfield.WorkspaceID, job.upload.WorkspaceID,
				logfield.Error, errTotalCount,
				logfield.TableName, tName,
			)
		}
	}

	err = job.whManager.LoadTable(job.ctx, tName)
	if err != nil {
		status := model.TableUploadExportingFailed
		errorsString := misc.QuoteLiteral(err.Error())
		_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tName, repo.TableUploadSetOptions{
			Status: &status,
			Error:  &errorsString,
		})
		return alteredSchema, fmt.Errorf("load table: %w", err)
	}

	func() {
		if !generateTableLoadCountVerificationsMetrics {
			return
		}
		var errTotalCount error
		totalAfterLoad, errTotalCount = job.getTotalCount(tName)
		if errTotalCount != nil {
			job.logger.Warnw("total count in table after loading",
				logfield.SourceID, job.upload.SourceID,
				logfield.DestinationID, job.upload.DestinationID,
				logfield.DestinationType, job.upload.DestinationType,
				logfield.WorkspaceID, job.upload.WorkspaceID,
				logfield.Error, errTotalCount,
				logfield.TableName, tName,
			)
			return
		}
		tableUpload, errEventCount := job.tableUploadsRepo.GetByUploadIDAndTableName(job.ctx, job.upload.ID, tName)
		if errEventCount != nil {
			return
		}

		// TODO : Perform the comparison here in the codebase
		job.guageStat(`pre_load_table_rows`, whutils.Tag{Name: "tableName", Value: strings.ToLower(tName)}).Gauge(int(totalBeforeLoad))
		job.guageStat(`post_load_table_rows_estimate`, whutils.Tag{Name: "tableName", Value: strings.ToLower(tName)}).Gauge(int(totalBeforeLoad + tableUpload.TotalEvents))
		job.guageStat(`post_load_table_rows`, whutils.Tag{Name: "tableName", Value: strings.ToLower(tName)}).Gauge(int(totalAfterLoad))
	}()

	status = model.TableUploadExported
	_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tName, repo.TableUploadSetOptions{
		Status: &status,
	})
	tableUpload, queryErr := job.tableUploadsRepo.GetByUploadIDAndTableName(job.ctx, job.upload.ID, tName)
	if queryErr == nil {
		job.recordTableLoad(tName, tableUpload.TotalEvents)
	}

	job.columnCountStat(tName)

	return alteredSchema, nil
}

// columnCountStat sent the column count for a table to statsd
// skip sending for S3_DATALAKE, GCS_DATALAKE, AZURE_DATALAKE
func (job *UploadJob) columnCountStat(tableName string) {
	var (
		columnCountLimit int
		ok               bool
	)

	switch job.warehouse.Type {
	case whutils.S3Datalake, whutils.GCSDatalake, whutils.AzureDatalake:
		return
	}

	columnCountLimitMap := integrationsconfig.ColumnCountLimitMap(job.conf)

	if columnCountLimit, ok = columnCountLimitMap[job.warehouse.Type]; !ok {
		return
	}

	tags := []whutils.Tag{
		{Name: "tableName", Value: strings.ToLower(tableName)},
	}
	currentColumnsCount := job.schemaHandle.GetColumnsCountInWarehouseSchema(tableName)

	job.counterStat(`warehouse_load_table_column_count`, tags...).Count(currentColumnsCount)
	job.counterStat(`warehouse_load_table_column_limit`, tags...).Count(columnCountLimit)
}

func (job *UploadJob) loadUserTables(loadFilesTableMap map[tableNameT]bool) ([]error, error) {
	var hasLoadFiles bool
	userTables := []string{job.identifiesTableName(), job.usersTableName()}

	var (
		err                       error
		previouslyFailedTables    map[string]model.PendingTableUpload
		currentJobSucceededTables map[string]model.PendingTableUpload
	)
	if previouslyFailedTables, currentJobSucceededTables, err = job.TablesToSkip(); err != nil {
		return []error{}, fmt.Errorf("tables to skip: %w", err)
	}

	for _, tName := range userTables {
		if prevJobStatus, ok := previouslyFailedTables[tName]; ok {
			skipError := fmt.Errorf("skipping table %s because it previously failed to load in an earlier job: %d with error: %s", tName, prevJobStatus.UploadID, prevJobStatus.Error)
			return []error{skipError}, nil
		}
	}
	for _, tName := range userTables {
		if _, ok := currentJobSucceededTables[tName]; ok {
			continue
		}
		hasLoadFiles = loadFilesTableMap[tableNameT(tName)]
		if hasLoadFiles {
			// There is at least one table to load
			break
		}
	}

	if !hasLoadFiles {
		return []error{}, nil
	}

	// Load all user tables
	status := model.TableUploadExecuting
	lastExecTime := job.now()
	_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, job.identifiesTableName(), repo.TableUploadSetOptions{
		Status:       &status,
		LastExecTime: &lastExecTime,
	})

	alteredIdentitySchema, err := job.updateSchema(job.identifiesTableName())
	if err != nil {
		status := model.TableUploadUpdatingSchemaFailed
		errorsString := misc.QuoteLiteral(err.Error())
		_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, job.identifiesTableName(), repo.TableUploadSetOptions{
			Status: &status,
			Error:  &errorsString,
		})
		return job.processLoadTableResponse(map[string]error{job.identifiesTableName(): err})
	}
	var alteredUserSchema bool
	if _, ok := job.upload.UploadSchema[job.usersTableName()]; ok {
		status := model.TableUploadExecuting
		lastExecTime := job.now()
		_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, job.usersTableName(), repo.TableUploadSetOptions{
			Status:       &status,
			LastExecTime: &lastExecTime,
		})
		alteredUserSchema, err = job.updateSchema(job.usersTableName())
		if err != nil {
			status = model.TableUploadUpdatingSchemaFailed
			errorsString := misc.QuoteLiteral(err.Error())
			_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, job.usersTableName(), repo.TableUploadSetOptions{
				Status: &status,
				Error:  &errorsString,
			})
			return job.processLoadTableResponse(map[string]error{job.usersTableName(): err})
		}
	}

	// Skip loading user tables if identifies table schema is not present
	if identifiesSchema := job.GetTableSchemaInUpload(job.identifiesTableName()); len(identifiesSchema) == 0 {
		return []error{}, nil
	}

	errorMap := job.whManager.LoadUserTables(job.ctx)

	if alteredIdentitySchema || alteredUserSchema {
		job.logger.Infof("loadUserTables: schema changed - updating local schema for %s", job.warehouse.Identifier)
		_ = job.schemaHandle.UpdateLocalSchemaWithWarehouse(job.ctx, job.upload.ID)
	}
	return job.processLoadTableResponse(errorMap)
}

func (job *UploadJob) loadIdentityTables(populateHistoricIdentities bool) (loadErrors []error, tableUploadErr error) {
	job.logger.Infof(`[WH]: Starting load for identity tables in namespace %s of destination %s:%s`, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)
	identityTables := []string{job.identityMergeRulesTableName(), job.identityMappingsTableName()}

	var (
		err                       error
		previouslyFailedTables    map[string]model.PendingTableUpload
		currentJobSucceededTables map[string]model.PendingTableUpload
	)
	if previouslyFailedTables, currentJobSucceededTables, err = job.TablesToSkip(); err != nil {
		return []error{}, fmt.Errorf("tables to skip: %w", err)
	}

	for _, tableName := range identityTables {
		if prevJobStatus, ok := previouslyFailedTables[tableName]; ok {
			skipError := fmt.Errorf("skipping table %s because it previously failed to load in an earlier job: %d with error: %s", tableName, prevJobStatus.UploadID, prevJobStatus.Error)
			return []error{skipError}, nil
		}
	}

	errorMap := make(map[string]error)
	// var generated bool
	if generated, _ := job.areIdentityTablesLoadFilesGenerated(job.ctx); !generated {
		if err := job.resolveIdentities(populateHistoricIdentities); err != nil {
			job.logger.Errorf(` ID Resolution operation failed: %v`, err)
			errorMap[job.identityMergeRulesTableName()] = err
			return job.processLoadTableResponse(errorMap)
		}
	}

	var alteredSchema bool
	for _, tableName := range identityTables {
		if _, loaded := currentJobSucceededTables[tableName]; loaded {
			continue
		}

		errorMap[tableName] = nil

		tableSchemaDiff := job.schemaHandle.TableSchemaDiff(tableName, job.GetTableSchemaInUpload(tableName))
		if tableSchemaDiff.Exists {
			err := job.UpdateTableSchema(tableName, tableSchemaDiff)
			if err != nil {
				status := model.TableUploadUpdatingSchemaFailed
				errorsString := misc.QuoteLiteral(err.Error())
				_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tableName, repo.TableUploadSetOptions{
					Status: &status,
					Error:  &errorsString,
				})
				errorMap := map[string]error{tableName: err}
				return job.processLoadTableResponse(errorMap)
			}
			job.schemaHandle.UpdateWarehouseTableSchema(tableName, tableSchemaDiff.UpdatedSchema)

			status := model.TableUploadUpdatedSchema
			_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tableName, repo.TableUploadSetOptions{
				Status: &status,
			})
			alteredSchema = true
		}

		status := model.TableUploadExecuting
		lastExecTime := job.now()
		err = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tableName, repo.TableUploadSetOptions{
			Status:       &status,
			LastExecTime: &lastExecTime,
		})
		if err != nil {
			errorMap[tableName] = err
			break
		}

		switch tableName {
		case job.identityMergeRulesTableName():
			err = job.whManager.LoadIdentityMergeRulesTable(job.ctx)
		case job.identityMappingsTableName():
			err = job.whManager.LoadIdentityMappingsTable(job.ctx)
		}

		if err != nil {
			errorMap[tableName] = err
			break
		}
	}

	if alteredSchema {
		job.logger.Infof("loadIdentityTables: schema changed - updating local schema for %s", job.warehouse.Identifier)
		_ = job.schemaHandle.UpdateLocalSchemaWithWarehouse(job.ctx, job.upload.ID) // TODO check error
	}

	return job.processLoadTableResponse(errorMap)
}

func (job *UploadJob) processLoadTableResponse(errorMap map[string]error) (errors []error, tableUploadErr error) {
	for tName, loadErr := range errorMap {
		// TODO: set last_exec_time
		if loadErr != nil {
			errors = append(errors, loadErr)
			errorsString := misc.QuoteLiteral(loadErr.Error())
			status := model.TableUploadExportingFailed
			tableUploadErr = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tName, repo.TableUploadSetOptions{
				Status: &status,
				Error:  &errorsString,
			})
		} else {
			status := model.TableUploadExported
			tableUploadErr = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tName, repo.TableUploadSetOptions{
				Status: &status,
			})
			if tableUploadErr == nil {
				// Since load is successful, we assume all events in load files are uploaded
				tableUpload, queryErr := job.tableUploadsRepo.GetByUploadIDAndTableName(job.ctx, job.upload.ID, tName)
				if queryErr == nil {
					job.recordTableLoad(tName, tableUpload.TotalEvents)
				}
			}
		}

		if tableUploadErr != nil {
			break
		}

	}
	return errors, tableUploadErr
}

// getNewTimings appends current status with current time to timings column
// e.g. status: exported_data, timings: [{exporting_data: 2020-04-21 15:16:19.687716}] -> [{exporting_data: 2020-04-21 15:16:19.687716, exported_data: 2020-04-21 15:26:34.344356}]
func (job *UploadJob) getNewTimings(status string) ([]byte, model.Timings) {
	timings, err := repo.NewUploads(job.dbHandle).UploadTimings(job.ctx, job.upload.ID)
	if err != nil {
		job.logger.Error("error getting timing, scrapping them", err)
	}
	timing := map[string]time.Time{status: job.now()}
	timings = append(timings, timing)
	marshalledTimings, err := json.Marshal(timings)
	if err != nil {
		panic(err)
	}
	return marshalledTimings, timings
}

func (job *UploadJob) getUploadFirstAttemptTime() (timing time.Time) {
	var firstTiming sql.NullString
	sqlStatement := fmt.Sprintf(`
		SELECT
		  timings -> 0 as firstTimingObj
		FROM
		  %s
		WHERE
		  id = %d;
`,
		whutils.WarehouseUploadsTable,
		job.upload.ID,
	)
	err := job.dbHandle.QueryRowContext(job.ctx, sqlStatement).Scan(&firstTiming)
	if err != nil {
		return
	}
	_, timing = whutils.TimingFromJSONString(firstTiming)
	return timing
}

type UploadStatusOpts struct {
	Status           string
	AdditionalFields []UploadColumn
	ReportingMetric  types.PUReportedMetric
}

func (job *UploadJob) setUploadStatus(statusOpts UploadStatusOpts) (err error) {
	job.logger.Debugf("[WH]: Setting status of %s for wh_upload:%v", statusOpts.Status, job.upload.ID)
	defer func() {
		if err != nil {
			job.logger.Warnw("error setting upload status", logfield.Error, err.Error())
		}
	}()

	// TODO: fetch upload model instead of just timings
	marshalledTimings, timings := job.getNewTimings(statusOpts.Status)
	opts := []UploadColumn{
		{Column: UploadStatusField, Value: statusOpts.Status},
		{Column: UploadTimingsField, Value: marshalledTimings},
		{Column: UploadUpdatedAtField, Value: job.now()},
	}

	job.upload.Status = statusOpts.Status
	job.upload.Timings = timings

	additionalFields := make([]UploadColumn, 0, len(statusOpts.AdditionalFields)+len(opts))
	additionalFields = append(additionalFields, statusOpts.AdditionalFields...)
	additionalFields = append(additionalFields, opts...)

	uploadColumnOpts := UploadColumnsOpts{Fields: additionalFields}

	if statusOpts.ReportingMetric != (types.PUReportedMetric{}) {
		var txn *sqlquerywrapper.Tx
		txn, err = job.dbHandle.BeginTx(job.ctx, &sql.TxOptions{})
		if err != nil {
			return
		}
		uploadColumnOpts.Txn = txn
		err = job.setUploadColumns(uploadColumnOpts)
		if err != nil {
			return
		}
		if job.config.reportingEnabled {
			job.app.Features().Reporting.GetReportingInstance().Report(
				[]*types.PUReportedMetric{&statusOpts.ReportingMetric},
				txn.GetTx(),
			)
		}
		err = txn.Commit()
		return
	}
	err = job.setUploadColumns(uploadColumnOpts)
	return
}

// Set LoadFileIDs
func (job *UploadJob) setLoadFileIDs(startLoadFileID, endLoadFileID int64) error {
	if startLoadFileID > endLoadFileID {
		return fmt.Errorf("end id less than start id: %d > %d", startLoadFileID, endLoadFileID)
	}

	job.upload.LoadFileStartID = startLoadFileID
	job.upload.LoadFileEndID = endLoadFileID

	return job.setUploadColumns(UploadColumnsOpts{
		Fields: []UploadColumn{
			{Column: UploadStartLoadFileIDField, Value: startLoadFileID},
			{Column: UploadEndLoadFileIDField, Value: endLoadFileID},
		},
	})
}

type UploadColumnsOpts struct {
	Fields []UploadColumn
	Txn    *sqlmiddleware.Tx
}

// SetUploadColumns sets any column values passed as args in UploadColumn format for WarehouseUploadsTable
func (job *UploadJob) setUploadColumns(opts UploadColumnsOpts) error {
	var columns string
	values := []interface{}{job.upload.ID}
	// setting values using syntax $n since Exec can correctly format time.Time strings
	for idx, f := range opts.Fields {
		// start with $2 as $1 is upload.ID
		columns += fmt.Sprintf(`%s=$%d`, f.Column, idx+2)
		if idx < len(opts.Fields)-1 {
			columns += ","
		}
		values = append(values, f.Value)
	}
	sqlStatement := fmt.Sprintf(`
		UPDATE
		  %s
		SET
		  %s
		WHERE
		  id = $1;`,
		whutils.WarehouseUploadsTable,
		columns,
	)

	var querier interface {
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	}
	if opts.Txn != nil {
		querier = opts.Txn
	} else {
		querier = job.dbHandle
	}
	_, err := querier.ExecContext(job.ctx, sqlStatement, values...)
	return err
}

// extractAndUpdateUploadErrorsByState extracts and augment errors in format
// { "internal_processing_failed": { "errors": ["account-locked", "account-locked"] }}
// from a particular upload.
func extractAndUpdateUploadErrorsByState(message json.RawMessage, state string, statusError error) (map[string]map[string]interface{}, error) {
	var uploadErrors map[string]map[string]interface{}
	err := json.Unmarshal(message, &uploadErrors)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal error into upload errors: %v", err)
	}

	if uploadErrors == nil {
		uploadErrors = make(map[string]map[string]interface{})
	}

	if _, ok := uploadErrors[state]; !ok {
		uploadErrors[state] = make(map[string]interface{})
	}
	errorByState := uploadErrors[state]

	// increment attempts for errored stage
	if attempt, ok := errorByState["attempt"]; ok {
		errorByState["attempt"] = int(attempt.(float64)) + 1
	} else {
		errorByState["attempt"] = 1
	}

	// append errors for errored stage
	if errList, ok := errorByState["errors"]; ok {
		errorByState["errors"] = append(errList.([]interface{}), statusError.Error())
	} else {
		errorByState["errors"] = []string{statusError.Error()}
	}

	return uploadErrors, nil
}

// Aborted returns true if the job has been aborted
func (job *UploadJob) Aborted(attempts int, startTime time.Time) bool {
	// Defensive check to prevent garbage startTime
	if startTime.IsZero() {
		return false
	}

	return attempts > job.config.minRetryAttempts && job.now().Sub(startTime) > job.config.retryTimeWindow
}

func (job *UploadJob) setUploadError(statusError error, state string) (string, error) {
	var (
		errorTags                  = job.errorHandler.MatchErrorMappings(statusError)
		destCredentialsValidations *bool
	)

	defer func() {
		job.logger.Warnw("upload error",
			logfield.UploadJobID, job.upload.ID,
			logfield.UploadStatus, state,
			logfield.SourceID, job.upload.SourceID,
			logfield.DestinationID, job.upload.DestinationID,
			logfield.DestinationType, job.upload.DestinationType,
			logfield.WorkspaceID, job.upload.WorkspaceID,
			logfield.Namespace, job.upload.Namespace,
			logfield.Error, statusError,
			logfield.UseRudderStorage, job.upload.UseRudderStorage,
			logfield.Priority, job.upload.Priority,
			logfield.Retried, job.upload.Retried,
			logfield.Attempt, job.upload.Attempts,
			logfield.LoadFileType, job.upload.LoadFileType,
			logfield.ErrorMapping, errorTags.Value,
			logfield.DestinationCredsValid, destCredentialsValidations,
		)
	}()

	job.counterStat(fmt.Sprintf("error_%s", state)).Count(1)
	upload := job.upload

	err := job.setUploadStatus(UploadStatusOpts{Status: state})
	if err != nil {
		return "", fmt.Errorf("unable to set upload's job: %d status: %w", job.upload.ID, err)
	}

	uploadErrors, err := extractAndUpdateUploadErrorsByState(job.upload.Error, state, statusError)
	if err != nil {
		return "", fmt.Errorf("unable to handle upload errors in job: %d by state: %s, err: %v",
			job.upload.ID,
			state,
			err)
	}

	// Reset the state as aborted if max retries
	// exceeded.
	uploadErrorAttempts := uploadErrors[state]["attempt"].(int)

	if job.Aborted(uploadErrorAttempts, job.getUploadFirstAttemptTime()) {
		state = model.Aborted
	}

	metadata := repo.ExtractUploadMetadata(job.upload)

	metadata.NextRetryTime = job.now().Add(job.durationBeforeNextAttempt(upload.Attempts + 1))
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		metadataJSON = []byte("{}")
	}

	serializedErr, _ := json.Marshal(&uploadErrors)
	serializedErr = whutils.SanitizeJSON(serializedErr)

	uploadColumns := []UploadColumn{
		{Column: "status", Value: state},
		{Column: "metadata", Value: metadataJSON},
		{Column: "error", Value: serializedErr},
		{Column: "updated_at", Value: job.now()},
	}

	txn, err := job.dbHandle.BeginTx(job.ctx, &sql.TxOptions{})
	if err != nil {
		return "", fmt.Errorf("unable to start transaction: %w", err)
	}
	if err = job.setUploadColumns(UploadColumnsOpts{Fields: uploadColumns, Txn: txn}); err != nil {
		return "", fmt.Errorf("unable to change upload columns: %w", err)
	}
	inputCount, _ := repo.NewStagingFiles(job.dbHandle).TotalEventsForUpload(job.ctx, upload)
	outputCount, _ := job.tableUploadsRepo.TotalExportedEvents(job.ctx, job.upload.ID, []string{
		whutils.ToProviderCase(job.warehouse.Type, whutils.DiscardsTable),
	})

	failCount := inputCount - outputCount
	reportingStatus := jobsdb.Failed.State
	if state == model.Aborted {
		reportingStatus = jobsdb.Aborted.State
	}
	reportingMetrics := []*types.PUReportedMetric{{
		ConnectionDetails: types.ConnectionDetails{
			SourceID:        job.upload.SourceID,
			DestinationID:   job.upload.DestinationID,
			SourceTaskRunID: job.upload.SourceTaskRunID,
			SourceJobID:     job.upload.SourceJobID,
			SourceJobRunID:  job.upload.SourceJobRunID,
		},
		PUDetails: types.PUDetails{
			InPU:       types.BATCH_ROUTER,
			PU:         types.WAREHOUSE,
			TerminalPU: true,
		},
		StatusDetail: &types.StatusDetail{
			Status:         reportingStatus,
			StatusCode:     400, // TODO: Change this to error specific code
			Count:          failCount,
			SampleEvent:    []byte("{}"),
			SampleResponse: string(serializedErr),
		},
	}}
	if outputCount > 0 {
		reportingMetrics = append(reportingMetrics, &types.PUReportedMetric{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:        job.upload.SourceID,
				DestinationID:   job.upload.DestinationID,
				SourceTaskRunID: job.upload.SourceTaskRunID,
				SourceJobID:     job.upload.SourceJobID,
				SourceJobRunID:  job.upload.SourceJobRunID,
			},
			PUDetails: types.PUDetails{
				InPU:       types.BATCH_ROUTER,
				PU:         types.WAREHOUSE,
				TerminalPU: true,
			},
			StatusDetail: &types.StatusDetail{
				Status:         jobsdb.Succeeded.State,
				StatusCode:     400, // TODO: Change this to error specific code
				Count:          failCount,
				SampleEvent:    []byte("{}"),
				SampleResponse: string(serializedErr),
			},
		})
	}
	if job.config.reportingEnabled {
		job.app.Features().Reporting.GetReportingInstance().Report(reportingMetrics, txn.GetTx())
	}
	err = txn.Commit()

	job.upload.Status = state
	job.upload.Error = serializedErr

	job.stats.uploadFailed.Count(1)

	// On aborted state, validate credentials to allow
	// us to differentiate between user caused abort vs platform issue.
	if state == model.Aborted {
		// base tag to be sent as stat

		tags := []whutils.Tag{errorTags}

		valid, err := job.validateDestinationCredentials()
		if err == nil {
			tags = append(tags, whutils.Tag{Name: "destination_creds_valid", Value: strconv.FormatBool(valid)})
			destCredentialsValidations = &valid
		}

		job.counterStat("upload_aborted", tags...).Count(1)
	}

	return state, err
}

func (job *UploadJob) durationBeforeNextAttempt(attempt int64) time.Duration { // Add state(retryable/non-retryable) as an argument to decide backoff etc.
	var d time.Duration
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = job.config.minUploadBackoff
	b.MaxInterval = job.config.maxUploadBackoff
	b.MaxElapsedTime = 0
	b.Multiplier = 2
	b.RandomizationFactor = 0
	b.Reset()
	for index := int64(0); index < attempt; index++ {
		d = b.NextBackOff()
	}
	return d
}

func (job *UploadJob) validateDestinationCredentials() (bool, error) {
	if job.destinationValidator == nil {
		return false, errors.New("failed to validate as destinationValidator is not set")
	}
	response := job.destinationValidator.Validate(job.ctx, &job.warehouse.Destination)
	return response.Success, nil
}

func (job *UploadJob) getLoadFilesTableMap() (loadFilesMap map[tableNameT]bool, err error) {
	loadFilesMap = make(map[tableNameT]bool)

	sourceID := job.warehouse.Source.ID
	destID := job.warehouse.Destination.ID

	sqlStatement := fmt.Sprintf(`
		SELECT
		  distinct table_name
		FROM
		  %s
		WHERE
		  (
			source_id = $1
			AND destination_id = $2
			AND id >= $3
			AND id <= $4
		  );
`,
		whutils.WarehouseLoadFilesTable,
	) /**/
	sqlStatementArgs := []interface{}{
		sourceID,
		destID,
		job.upload.LoadFileStartID,
		job.upload.LoadFileEndID,
	}
	rows, err := job.dbHandle.QueryContext(job.ctx, sqlStatement, sqlStatementArgs...)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
		return
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		err = fmt.Errorf("error occurred while executing distinct table name query for jobId: %d, sourceId: %s, destinationId: %s, err: %w", job.upload.ID, job.warehouse.Source.ID, job.warehouse.Destination.ID, err)
		return
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			err = fmt.Errorf("error occurred while processing distinct table name query for jobId: %d, sourceId: %s, destinationId: %s, err: %w", job.upload.ID, job.warehouse.Source.ID, job.warehouse.Destination.ID, err)
			return
		}
		loadFilesMap[tableNameT(tableName)] = true
	}
	if err = rows.Err(); err != nil {
		err = fmt.Errorf("interate distinct table name query for jobId: %d, sourceId: %s, destinationId: %s, err: %w", job.upload.ID, job.warehouse.Source.ID, job.warehouse.Destination.ID, err)
	}
	return
}

func (job *UploadJob) areIdentityTablesLoadFilesGenerated(ctx context.Context) (bool, error) {
	var (
		mergeRulesTable = whutils.ToProviderCase(job.warehouse.Type, whutils.IdentityMergeRulesTable)
		mappingsTable   = whutils.ToProviderCase(job.warehouse.Type, whutils.IdentityMappingsTable)
		tu              model.TableUpload
		err             error
	)

	if tu, err = job.tableUploadsRepo.GetByUploadIDAndTableName(ctx, job.upload.ID, mergeRulesTable); err != nil {
		return false, fmt.Errorf("table upload not found for merge rules table: %w", err)
	}
	if tu.Location == "" {
		return false, fmt.Errorf("merge rules location not found: %w", err)
	}
	if tu, err = job.tableUploadsRepo.GetByUploadIDAndTableName(ctx, job.upload.ID, mappingsTable); err != nil {
		return false, fmt.Errorf("table upload not found for mappings table: %w", err)
	}
	if tu.Location == "" {
		return false, fmt.Errorf("mappings location not found: %w", err)
	}
	return true, nil
}

func (job *UploadJob) GetLoadFilesMetadata(ctx context.Context, options whutils.GetLoadFilesOptions) (loadFiles []whutils.LoadFile) {
	var tableFilterSQL string
	if options.Table != "" {
		tableFilterSQL = fmt.Sprintf(` AND table_name='%s'`, options.Table)
	}

	var limitSQL string
	if options.Limit != 0 {
		limitSQL = fmt.Sprintf(`LIMIT %d`, options.Limit)
	}

	sqlStatement := fmt.Sprintf(`
		WITH row_numbered_load_files as (
		  SELECT
			location,
			metadata,
			row_number() OVER (
			  PARTITION BY staging_file_id,
			  table_name
			  ORDER BY
				id DESC
			) AS row_number
		  FROM
			%[1]s
		  WHERE
			staging_file_id IN (%[2]v) %[3]s
		)
		SELECT
		  location,
		  metadata
		FROM
		  row_numbered_load_files
		WHERE
		  row_number = 1
		%[4]s;
`,
		whutils.WarehouseLoadFilesTable,
		misc.IntArrayToString(job.stagingFileIDs, ","),
		tableFilterSQL,
		limitSQL,
	)

	job.logger.Debugf(`Fetching loadFileLocations: %v`, sqlStatement)
	rows, err := job.dbHandle.QueryContext(ctx, sqlStatement)
	if err != nil {
		panic(fmt.Errorf("query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var location string
		var metadata json.RawMessage
		err := rows.Scan(&location, &metadata)
		if err != nil {
			panic(fmt.Errorf("failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		loadFiles = append(loadFiles, whutils.LoadFile{
			Location: location,
			Metadata: metadata,
		})
	}
	if err = rows.Err(); err != nil {
		panic(fmt.Errorf("iterate query results: %s\nwith Error : %w", sqlStatement, err))
	}
	return
}

func (job *UploadJob) GetSampleLoadFileLocation(ctx context.Context, tableName string) (location string, err error) {
	locations := job.GetLoadFilesMetadata(ctx, whutils.GetLoadFilesOptions{Table: tableName, Limit: 1})
	if len(locations) == 0 {
		return "", fmt.Errorf(`no load file found for table:%s`, tableName)
	}
	return locations[0].Location, nil
}

// GetSchemaInWarehouse TODO check usage of this function
func (job *UploadJob) GetSchemaInWarehouse() (schema model.Schema) {
	if job.schemaHandle == nil {
		return
	}
	return job.schemaHandle.GetSchemaInWarehouse()
}

// GetTableSchemaInWarehouse TODO check usage of this function
func (job *UploadJob) GetTableSchemaInWarehouse(tableName string) model.TableSchema {
	return job.schemaHandle.GetTableSchemaInWarehouse(tableName)
}

func (job *UploadJob) GetTableSchemaInUpload(tableName string) model.TableSchema {
	return job.upload.UploadSchema[tableName]
}

func (job *UploadJob) GetSingleLoadFile(ctx context.Context, tableName string) (whutils.LoadFile, error) {
	var (
		tableUpload model.TableUpload
		err         error
	)

	if tableUpload, err = job.tableUploadsRepo.GetByUploadIDAndTableName(ctx, job.upload.ID, tableName); err != nil {
		return whutils.LoadFile{}, fmt.Errorf("get single load file: %w", err)
	}

	return whutils.LoadFile{Location: tableUpload.Location}, err
}

func (job *UploadJob) ShouldOnDedupUseNewRecord() bool {
	category := job.warehouse.Source.SourceDefinition.Category
	return category == singerProtocolSourceCategory || category == cloudSourceCategory
}

func (job *UploadJob) UseRudderStorage() bool {
	return job.upload.UseRudderStorage
}

func (job *UploadJob) GetLoadFileGenStartTIme() time.Time {
	if !job.loadFileGenStartTime.IsZero() {
		return job.loadFileGenStartTime
	}
	return model.GetLoadFileGenTime(job.upload.Timings)
}

func (job *UploadJob) GetLoadFileType() string {
	return job.upload.LoadFileType
}

func (job *UploadJob) GetFirstLastEvent() (time.Time, time.Time) {
	return job.upload.FirstEventAt, job.upload.LastEventAt
}

func (job *UploadJob) DTO() *model.UploadJob {
	return &model.UploadJob{
		Warehouse:    job.warehouse,
		Upload:       job.upload,
		StagingFiles: job.stagingFiles,
	}
}

/*
 * State Machine for upload job lifecycle
 */

func getNextUploadState(dbStatus string) *uploadState {
	for _, uploadState := range stateTransitions {
		if dbStatus == uploadState.inProgress || dbStatus == uploadState.failed {
			return uploadState
		}
		if dbStatus == uploadState.completed {
			return uploadState.nextState
		}
	}
	return nil
}

func getInProgressState(state string) string {
	uploadState, ok := stateTransitions[state]
	if !ok {
		panic(fmt.Errorf("invalid Upload state: %s", state))
	}
	return uploadState.inProgress
}

func initializeStateMachine() {
	stateTransitions = make(map[string]*uploadState)

	waitingState := &uploadState{
		completed: model.Waiting,
	}
	stateTransitions[model.Waiting] = waitingState

	generateUploadSchemaState := &uploadState{
		inProgress: "generating_upload_schema",
		failed:     "generating_upload_schema_failed",
		completed:  model.GeneratedUploadSchema,
	}
	stateTransitions[model.GeneratedUploadSchema] = generateUploadSchemaState

	createTableUploadsState := &uploadState{
		inProgress: "creating_table_uploads",
		failed:     "creating_table_uploads_failed",
		completed:  model.CreatedTableUploads,
	}
	stateTransitions[model.CreatedTableUploads] = createTableUploadsState

	generateLoadFilesState := &uploadState{
		inProgress: "generating_load_files",
		failed:     "generating_load_files_failed",
		completed:  model.GeneratedLoadFiles,
	}
	stateTransitions[model.GeneratedLoadFiles] = generateLoadFilesState

	updateTableUploadCountsState := &uploadState{
		inProgress: "updating_table_uploads_counts",
		failed:     "updating_table_uploads_counts_failed",
		completed:  model.UpdatedTableUploadsCounts,
	}
	stateTransitions[model.UpdatedTableUploadsCounts] = updateTableUploadCountsState

	createRemoteSchemaState := &uploadState{
		inProgress: "creating_remote_schema",
		failed:     "creating_remote_schema_failed",
		completed:  model.CreatedRemoteSchema,
	}
	stateTransitions[model.CreatedRemoteSchema] = createRemoteSchemaState

	exportDataState := &uploadState{
		inProgress: "exporting_data",
		failed:     "exporting_data_failed",
		completed:  model.ExportedData,
	}
	stateTransitions[model.ExportedData] = exportDataState

	abortState := &uploadState{
		completed: model.Aborted,
	}
	stateTransitions[model.Aborted] = abortState

	waitingState.nextState = generateUploadSchemaState
	generateUploadSchemaState.nextState = createTableUploadsState
	createTableUploadsState.nextState = generateLoadFilesState
	generateLoadFilesState.nextState = updateTableUploadCountsState
	updateTableUploadCountsState.nextState = createRemoteSchemaState
	createRemoteSchemaState.nextState = exportDataState
	exportDataState.nextState = nil
	abortState.nextState = nil
}

// GetLocalSchema TODO check usage of this function
func (job *UploadJob) GetLocalSchema(ctx context.Context) (model.Schema, error) {
	return job.schemaHandle.GetLocalSchema(ctx)
}

// UpdateLocalSchema TODO check usage of this function
func (job *UploadJob) UpdateLocalSchema(ctx context.Context, schema model.Schema) error {
	return job.schemaHandle.UpdateLocalSchema(ctx, job.upload.ID, schema)
}

func (job *UploadJob) RefreshPartitions(loadFileStartID, loadFileEndID int64) error {
	if !slices.Contains(whutils.TimeWindowDestinations, job.upload.DestinationType) {
		return nil
	}

	var (
		repository schemarepository.SchemaRepository
		err        error
	)

	if repository, err = schemarepository.NewSchemaRepository(job.warehouse, job, job.logger); err != nil {
		return fmt.Errorf("create schema repository: %w", err)
	}

	// Refresh partitions if exists
	for tableName := range job.upload.UploadSchema {
		loadFiles := job.GetLoadFilesMetadata(job.ctx, whutils.GetLoadFilesOptions{
			Table:   tableName,
			StartID: loadFileStartID,
			EndID:   loadFileEndID,
		})
		batches := lo.Chunk(loadFiles, job.config.refreshPartitionBatchSize)
		for _, batch := range batches {
			if err = repository.RefreshPartitions(job.ctx, tableName, batch); err != nil {
				return fmt.Errorf("refresh partitions: %w", err)
			}
		}
	}
	return nil
}
