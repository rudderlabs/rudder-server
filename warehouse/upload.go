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
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"

	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/services/alerta"

	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"

	"golang.org/x/exp/slices"

	"github.com/cenkalti/backoff/v4"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/identity"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

const (
	GeneratingStagingFileFailedState = "generating_staging_file_failed"
	GeneratedStagingFileState        = "generated_staging_file"
	FetchingRemoteSchemaFailed       = "fetching_remote_schema_failed"
	InternalProcessingFailed         = "internal_processing_failed"
)

const (
	CloudSourceCategory          = "cloud"
	SingerProtocolSourceCategory = "singer-protocol"
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
	dbHandle             *sql.DB
	destinationValidator validations.DestinationValidator
	loadFile             *loadfiles.LoadFileGenerator
	recovery             *service.Recovery
	pgNotifier           *pgnotifier.PGNotifier
	stats                stats.Stats
}

type UploadJob struct {
	dbHandle             *sql.DB
	destinationValidator validations.DestinationValidator
	loadfile             *loadfiles.LoadFileGenerator
	tableUploadsRepo     *repo.TableUploads
	recovery             *service.Recovery
	whManager            manager.Manager
	pgNotifier           *pgnotifier.PGNotifier
	schemaHandle         *SchemaHandle
	stats                stats.Stats
	LoadFileGenStartTime time.Time

	upload         model.Upload
	warehouse      model.Warehouse
	stagingFiles   []*model.StagingFile
	stagingFileIDs []int64
	schemaLock     sync.Mutex
	uploadLock     sync.Mutex
	AlertSender    alerta.AlertSender
	Now            func() time.Time

	pendingTableUploads       []model.PendingTableUpload
	pendingTableUploadsRepo   pendingTableUploadsRepo
	pendingTableUploadsOnce   sync.Once
	pendingTableUploadsError  error
	RefreshPartitionBatchSize int
	RetryTimeWindow           time.Duration
	MinRetryAttempts          int

	ErrorHandler ErrorHandler
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
	MergedSchemaField          = "mergedschema"
	UploadLastExecAtField      = "last_exec_at"
	UploadInProgress           = "in_progress"
)

var (
	alwaysMarkExported                               = []string{warehouseutils.DiscardsTable}
	warehousesToAlwaysRegenerateAllLoadFilesOnResume = []string{warehouseutils.SNOWFLAKE, warehouseutils.BQ}
)

var (
	maxParallelLoads    map[string]int
	columnCountLimitMap map[string]int
)

func Init() {
	setMaxParallelLoads()
}

func init() {
	initializeStateMachine()
}

func setMaxParallelLoads() {
	maxParallelLoads = map[string]int{
		warehouseutils.BQ:             config.GetInt("Warehouse.bigquery.maxParallelLoads", 20),
		warehouseutils.RS:             config.GetInt("Warehouse.redshift.maxParallelLoads", 8),
		warehouseutils.POSTGRES:       config.GetInt("Warehouse.postgres.maxParallelLoads", 8),
		warehouseutils.MSSQL:          config.GetInt("Warehouse.mssql.maxParallelLoads", 8),
		warehouseutils.SNOWFLAKE:      config.GetInt("Warehouse.snowflake.maxParallelLoads", 8),
		warehouseutils.CLICKHOUSE:     config.GetInt("Warehouse.clickhouse.maxParallelLoads", 8),
		warehouseutils.DELTALAKE:      config.GetInt("Warehouse.deltalake.maxParallelLoads", 8),
		warehouseutils.S3_DATALAKE:    config.GetInt("Warehouse.s3_datalake.maxParallelLoads", 8),
		warehouseutils.GCS_DATALAKE:   config.GetInt("Warehouse.gcs_datalake.maxParallelLoads", 8),
		warehouseutils.AZURE_DATALAKE: config.GetInt("Warehouse.azure_datalake.maxParallelLoads", 8),
	}
	columnCountLimitMap = map[string]int{
		warehouseutils.AZURE_SYNAPSE: config.GetInt("Warehouse.azure_synapse.columnCountLimit", 1024),
		warehouseutils.BQ:            config.GetInt("Warehouse.bigquery.columnCountLimit", 10000),
		warehouseutils.CLICKHOUSE:    config.GetInt("Warehouse.clickhouse.columnCountLimit", 1000),
		warehouseutils.MSSQL:         config.GetInt("Warehouse.mssql.columnCountLimit", 1024),
		warehouseutils.POSTGRES:      config.GetInt("Warehouse.postgres.columnCountLimit", 1600),
		warehouseutils.RS:            config.GetInt("Warehouse.redshift.columnCountLimit", 1600),
		warehouseutils.S3_DATALAKE:   config.GetInt("Warehouse.s3_datalake.columnCountLimit", 10000),
	}
}

func (f *UploadJobFactory) NewUploadJob(dto *model.UploadJob, whManager manager.Manager) *UploadJob {
	return &UploadJob{
		dbHandle:             f.dbHandle,
		loadfile:             f.loadFile,
		recovery:             f.recovery,
		pgNotifier:           f.pgNotifier,
		whManager:            whManager,
		destinationValidator: f.destinationValidator,
		stats:                f.stats,
		tableUploadsRepo:     repo.NewTableUploads(f.dbHandle),

		upload:         dto.Upload,
		warehouse:      dto.Warehouse,
		stagingFiles:   dto.StagingFiles,
		stagingFileIDs: repo.StagingFileIDs(dto.StagingFiles),

		pendingTableUploadsRepo: repo.NewUploads(f.dbHandle),
		pendingTableUploads:     []model.PendingTableUpload{},

		RefreshPartitionBatchSize: config.GetInt("Warehouse.refreshPartitionBatchSize", 100),
		RetryTimeWindow:           retryTimeWindow,
		MinRetryAttempts:          minRetryAttempts,

		AlertSender: alerta.NewClient(
			config.GetString("ALERTA_URL", "https://alerta.rudderstack.com/api/"),
		),
		Now: timeutil.Now,

		ErrorHandler: ErrorHandler{whManager},
	}
}

func (job *UploadJob) identifiesTableName() string {
	return warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentifiesTable)
}

func (job *UploadJob) usersTableName() string {
	return warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.UsersTable)
}

func (job *UploadJob) identityMergeRulesTableName() string {
	return warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentityMergeRulesTable)
}

func (job *UploadJob) identityMappingsTableName() string {
	return warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentityMappingsTable)
}

func (job *UploadJob) trackLongRunningUpload() chan struct{} {
	ch := make(chan struct{}, 1)
	rruntime.GoForWarehouse(func() {
		select {
		case <-ch:
			// do nothing
		case <-time.After(longRunningUploadStatThresholdInMin):
			pkgLogger.Infof("[WH]: Registering stat for long running upload: %d, dest: %s", job.upload.ID, job.warehouse.Identifier)

			job.stats.NewTaggedStat(
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

func (job *UploadJob) generateUploadSchema(schemaHandle *SchemaHandle) error {
	schemaHandle.uploadSchema = schemaHandle.consolidateStagingFilesSchemaUsingWarehouseSchema()
	// set upload schema
	_ = job.setMergedSchema(schemaHandle.uploadSchema)
	err := job.setUploadSchema(schemaHandle.uploadSchema)
	return err
}

func (job *UploadJob) initTableUploads() error {
	schemaForUpload := job.upload.UploadSchema
	destType := job.warehouse.Type
	tables := make([]string, 0, len(schemaForUpload))
	for t := range schemaForUpload {
		tables = append(tables, t)
		// also track upload to rudder_identity_mappings if the upload has records for rudder_identity_merge_rules
		if misc.Contains(warehouseutils.IdentityEnabledWarehouses, destType) && t == warehouseutils.ToProviderCase(destType, warehouseutils.IdentityMergeRulesTable) {
			if _, ok := schemaForUpload[warehouseutils.ToProviderCase(destType, warehouseutils.IdentityMappingsTable)]; !ok {
				tables = append(tables, warehouseutils.ToProviderCase(destType, warehouseutils.IdentityMappingsTable))
			}
		}
	}

	return job.tableUploadsRepo.Insert(
		context.TODO(),
		job.upload.ID,
		tables,
	)
}

func (job *UploadJob) syncRemoteSchema() (schemaChanged bool, err error) {
	schemaHandle := SchemaHandle{
		warehouse:    job.warehouse,
		stagingFiles: job.stagingFiles,
		dbHandle:     job.dbHandle,
	}
	job.schemaHandle = &schemaHandle
	schemaHandle.localSchema = schemaHandle.getLocalSchema()
	schemaHandle.schemaInWarehouse, schemaHandle.unrecognizedSchemaInWarehouse, err = schemaHandle.fetchSchemaFromWarehouse(job.whManager)
	if err != nil {
		return false, err
	}

	schemaChanged = hasSchemaChanged(schemaHandle.localSchema, schemaHandle.schemaInWarehouse)
	if schemaChanged {
		pkgLogger.Infof("syncRemoteSchema: schema changed - updating local schema for %s", job.warehouse.Identifier)
		err = schemaHandle.updateLocalSchema(schemaHandle.schemaInWarehouse)
		if err != nil {
			return false, err
		}
		schemaHandle.localSchema = schemaHandle.schemaInWarehouse
	}

	return schemaChanged, nil
}

func (job *UploadJob) getTotalRowsInLoadFiles() int64 {
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
		warehouseutils.WarehouseLoadFilesTable,
		misc.IntArrayToString(job.stagingFileIDs, ","),
		warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.DiscardsTable),
	)
	err := dbHandle.QueryRow(sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`Error in getTotalRowsInLoadFiles: %v`, err)
	}
	return total.Int64
}

func (job *UploadJob) matchRowsInStagingAndLoadFiles(ctx context.Context) error {
	rowsInStagingFiles, err := repo.NewStagingFiles(dbHandle).TotalEventsForUpload(ctx, job.upload)
	if err != nil {
		return fmt.Errorf("total rows: %w", err)
	}
	rowsInLoadFiles := job.getTotalRowsInLoadFiles()
	if (rowsInStagingFiles != rowsInLoadFiles) || rowsInStagingFiles == 0 || rowsInLoadFiles == 0 {
		pkgLogger.Errorf(`Error: Rows count mismatch between staging and load files for upload:%d. rowsInStagingFiles: %d, rowsInLoadFiles: %d`, job.upload.ID, rowsInStagingFiles, rowsInLoadFiles)
		job.guageStat("warehouse_staging_load_file_events_count_mismatched").Gauge(rowsInStagingFiles - rowsInLoadFiles)
	}
	return nil
}

func (job *UploadJob) run() (err error) {
	timerStat := job.timerStat("upload_time")
	start := job.Now()
	ch := job.trackLongRunningUpload()
	defer func() {
		_ = job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumn{{Column: UploadInProgress, Value: false}}})

		timerStat.Since(start)
		ch <- struct{}{}
	}()

	job.uploadLock.Lock()
	defer job.uploadLock.Unlock()
	_ = job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumn{{Column: UploadLastExecAtField, Value: job.Now()}, {Column: UploadInProgress, Value: true}}})

	if len(job.stagingFiles) == 0 {
		err := fmt.Errorf("no staging files found")
		_, _ = job.setUploadError(err, InternalProcessingFailed)
		return err
	}

	whManager := job.whManager
	err = whManager.Setup(job.warehouse, job)
	if err != nil {
		_, _ = job.setUploadError(err, InternalProcessingFailed)
		return err
	}
	defer whManager.Cleanup()

	err = job.recovery.Recover(context.TODO(), whManager, job.warehouse)
	if err != nil {
		_, _ = job.setUploadError(err, InternalProcessingFailed)
		return err
	}

	hasSchemaChanged, err := job.syncRemoteSchema()
	if err != nil {
		_, _ = job.setUploadError(err, FetchingRemoteSchemaFailed)
		return err
	}
	if hasSchemaChanged {
		pkgLogger.Infof("[WH] Remote schema changed for Warehouse: %s", job.warehouse.Identifier)
	}
	schemaHandle := job.schemaHandle
	schemaHandle.uploadSchema = job.upload.UploadSchema

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
		stateStartTime := job.Now()
		err = nil

		_ = job.setUploadStatus(UploadStatusOpts{Status: nextUploadState.inProgress})
		pkgLogger.Debugf("[WH] Upload: %d, Current state: %s", job.upload.ID, nextUploadState.inProgress)

		targetStatus := nextUploadState.completed

		switch targetStatus {
		case model.GeneratedUploadSchema:
			newStatus = nextUploadState.failed
			err = job.generateUploadSchema(schemaHandle)
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
			generateAll := hasSchemaChanged || misc.Contains(warehousesToAlwaysRegenerateAllLoadFilesOnResume, job.warehouse.Type) || config.GetBool("Warehouse.alwaysRegenerateAllLoadFiles", true)
			var startLoadFileID, endLoadFileID int64
			if generateAll {
				startLoadFileID, endLoadFileID, err = job.loadfile.ForceCreateLoadFiles(context.TODO(), job.DTO())
			} else {
				startLoadFileID, endLoadFileID, err = job.loadfile.CreateLoadFiles(context.TODO(), job.DTO())
			}
			if err != nil {
				break
			}

			err = job.setLoadFileIDs(startLoadFileID, endLoadFileID)
			if err != nil {
				break
			}

			err = job.matchRowsInStagingAndLoadFiles(context.TODO())
			if err != nil {
				break
			}

			_ = job.recordLoadFileGenerationTimeStat(startLoadFileID, endLoadFileID)

			newStatus = nextUploadState.completed

		case model.UpdatedTableUploadsCounts:
			newStatus = nextUploadState.failed
			for tableName := range job.upload.UploadSchema {
				err = job.tableUploadsRepo.PopulateTotalEventsFromStagingFileIDs(
					context.TODO(),
					job.upload.ID,
					tableName,
					job.stagingFileIDs,
				)
				if err != nil {
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
			if len(schemaHandle.schemaInWarehouse) == 0 {
				err = whManager.CreateSchema()
				if err != nil {
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
				var succeededUserTableCount int
				for _, userTable := range userTables {
					if _, ok := currentJobSucceededTables[userTable]; ok {
						succeededUserTableCount++
					}
				}
				if succeededUserTableCount >= len(userTables) {
					wg.Done()
					return
				}
				err = job.exportUserTables(loadFilesTableMap)
				if err != nil {
					loadErrorLock.Lock()
					loadErrors = append(loadErrors, err)
					loadErrorLock.Unlock()
				}
				wg.Done()
			})

			rruntime.GoForWarehouse(func() {
				var succeededIdentityTableCount int
				for _, identityTable := range identityTables {
					if _, ok := currentJobSucceededTables[identityTable]; ok {
						succeededIdentityTableCount++
					}
				}
				if succeededIdentityTableCount >= len(identityTables) {
					wg.Done()
					return
				}
				err = job.exportIdentities()
				if err != nil {
					loadErrorLock.Lock()
					loadErrors = append(loadErrors, err)
					loadErrorLock.Unlock()
				}
				wg.Done()
			})

			rruntime.GoForWarehouse(func() {
				specialTables := append(userTables, identityTables...)
				err = job.exportRegularTables(specialTables, loadFilesTableMap)
				if err != nil {
					loadErrorLock.Lock()
					loadErrors = append(loadErrors, err)
					loadErrorLock.Unlock()
				}
				wg.Done()
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

		pkgLogger.Debugf("[WH] Upload: %d, Next state: %s", job.upload.ID, newStatus)

		uploadStatusOpts := UploadStatusOpts{Status: newStatus}
		if newStatus == model.ExportedData {

			rowCount, _ := repo.NewStagingFiles(dbHandle).TotalEventsForUpload(context.TODO(), job.upload)

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

		loadTimeStat := job.timerStat("user_tables_load_time")
		defer loadTimeStat.RecordDuration()()
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
	if warehouseutils.IDResolutionEnabled() && misc.Contains(warehouseutils.IdentityEnabledWarehouses, job.warehouse.Type) {
		if _, ok := uploadSchema[job.identityMergeRulesTableName()]; ok {
			loadTimeStat := job.timerStat("identity_tables_load_time")
			defer loadTimeStat.RecordDuration()()

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
	loadTimeStat := job.timerStat("other_tables_load_time")
	defer loadTimeStat.RecordDuration()()

	loadErrors := job.loadAllTablesExcept(specialTables, loadFilesTableMap)

	if len(loadErrors) > 0 {
		err = misc.ConcatErrors(loadErrors)
		return
	}

	return
}

func (job *UploadJob) TablesToSkip() (map[string]model.PendingTableUpload, map[string]model.PendingTableUpload, error) {
	job.pendingTableUploadsOnce.Do(func() {
		job.pendingTableUploads, job.pendingTableUploadsError = job.pendingTableUploadsRepo.PendingTableUploads(
			context.TODO(),
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
	idr := identity.HandleT{
		Warehouse:          job.warehouse,
		DB:                 job.dbHandle,
		UploadID:           job.upload.ID,
		Uploader:           job,
		WarehouseManager:   job.whManager,
		LoadFileDownloader: downloader.NewDownloader(&job.warehouse, job, 8),
	}
	if populateHistoricIdentities {
		return idr.ResolveHistoricIdentities()
	}
	return idr.Resolve()
}

func (job *UploadJob) UpdateTableSchema(tName string, tableSchemaDiff warehouseutils.TableSchemaDiff) (err error) {
	pkgLogger.Infof(`[WH]: Starting schema update for table %s in namespace %s of destination %s:%s`, tName, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)

	if tableSchemaDiff.TableToBeCreated {
		err = job.whManager.CreateTable(tName, tableSchemaDiff.ColumnMap)
		if err != nil {
			pkgLogger.Errorf("Error creating table %s on namespace: %s, error: %v", tName, job.warehouse.Namespace, err)
			return err
		}
		job.counterStat("tables_added").Increment()
		return nil
	}

	if err = job.addColumnsToWarehouse(tName, tableSchemaDiff.ColumnMap); err != nil {
		return fmt.Errorf("adding columns to warehouse: %w", err)
	}

	if err = job.alterColumnsToWarehouse(tName, tableSchemaDiff.AlteredColumnMap); err != nil {
		return fmt.Errorf("altering columns to warehouse: %w", err)
	}

	return nil
}

func (job *UploadJob) alterColumnsToWarehouse(tName string, columnsMap model.TableSchema) error {
	var responseToAlerta []model.AlterTableResponse
	var errs []error

	for columnName, columnType := range columnsMap {
		res, err := job.whManager.AlterColumn(tName, columnName, columnType)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if res.IsDependent {
			responseToAlerta = append(responseToAlerta, res)
			continue
		}

		pkgLogger.Infof(`
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
		pkgLogger.Infof("altering dependent columns: %s", query)

		err := job.AlertSender.SendAlert(context.TODO(), "warehouse-column-changes",
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

func (job *UploadJob) addColumnsToWarehouse(tName string, columnsMap model.TableSchema) (err error) {
	pkgLogger.Infof(`[WH]: Adding columns for table %s in namespace %s of destination %s:%s`, tName, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)

	destType := job.upload.DestinationType
	columnsBatchSize := config.GetInt(fmt.Sprintf("Warehouse.%s.columnsBatchSize", warehouseutils.WHDestNameMap[destType]), 100)

	var columnsToAdd []warehouseutils.ColumnInfo
	for columnName, columnType := range columnsMap {
		// columns present in unrecognized schema should be skipped
		if unrecognizedSchema, ok := job.schemaHandle.unrecognizedSchemaInWarehouse[tName]; ok {
			if _, ok := unrecognizedSchema[columnName]; ok {
				continue
			}
		}

		columnsToAdd = append(columnsToAdd, warehouseutils.ColumnInfo{Name: columnName, Type: columnType})
	}

	chunks := lo.Chunk(columnsToAdd, columnsBatchSize)
	for _, chunk := range chunks {
		err = job.whManager.AddColumns(tName, chunk)
		if err != nil {
			err = fmt.Errorf("failed to add columns for table %s in namespace %s of destination %s:%s with error: %w", tName, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID, err)
			break
		}

		job.counterStat("columns_added").Count(len(chunk))
	}
	return err
}

func (job *UploadJob) loadAllTablesExcept(skipLoadForTables []string, loadFilesTableMap map[tableNameT]bool) []error {
	uploadSchema := job.upload.UploadSchema
	var parallelLoads int
	var ok bool
	if parallelLoads, ok = maxParallelLoads[job.warehouse.Type]; !ok {
		parallelLoads = 1
	}

	configKey := fmt.Sprintf("Warehouse.%s.maxParallelLoadsWorkspaceIDs", warehouseutils.WHDestNameMap[job.upload.DestinationType])
	if k, ok := config.GetStringMap(configKey, nil)[job.warehouse.WorkspaceID]; ok {
		if load, ok := k.(float64); ok {
			parallelLoads = int(load)
		}
	}

	pkgLogger.Infof(`[WH]: Running %d parallel loads in namespace %s of destination %s:%s`, parallelLoads, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)

	var loadErrors []error
	var loadErrorLock sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(uploadSchema))

	var alteredSchemaInAtLeastOneTable bool
	loadChan := make(chan struct{}, parallelLoads)

	var (
		err                       error
		previouslyFailedTables    map[string]model.PendingTableUpload
		currentJobSucceededTables map[string]model.PendingTableUpload
	)
	if previouslyFailedTables, currentJobSucceededTables, err = job.TablesToSkip(); err != nil {
		return []error{fmt.Errorf("tables to skip: %w", err)}
	}

	for tableName := range uploadSchema {
		if misc.Contains(skipLoadForTables, tableName) {
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
			wg.Done()
			if misc.Contains(alwaysMarkExported, strings.ToLower(tableName)) {
				status := model.TableUploadExported
				_ = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, tableName, repo.TableUploadSetOptions{
					Status: &status,
				})
			}
			continue
		}
		tName := tableName
		loadChan <- struct{}{}
		rruntime.GoForWarehouse(func() {
			alteredSchema, err := job.loadTable(tName)
			if alteredSchema {
				alteredSchemaInAtLeastOneTable = true
			}

			if err != nil {
				loadErrorLock.Lock()
				loadErrors = append(loadErrors, err)
				loadErrorLock.Unlock()
			}
			wg.Done()
			<-loadChan
		})
	}
	wg.Wait()

	if alteredSchemaInAtLeastOneTable {
		pkgLogger.Infof("loadAllTablesExcept: schema changed - updating local schema for %s", job.warehouse.Identifier)
		_ = job.schemaHandle.updateLocalSchema(job.schemaHandle.schemaInWarehouse)
	}

	return loadErrors
}

func (job *UploadJob) updateSchema(tName string) (alteredSchema bool, err error) {
	tableSchemaDiff := getTableSchemaDiff(tName, job.schemaHandle.schemaInWarehouse, job.upload.UploadSchema)
	if tableSchemaDiff.Exists {
		err = job.UpdateTableSchema(tName, tableSchemaDiff)
		if err != nil {
			return
		}

		job.setUpdatedTableSchema(tName, tableSchemaDiff.UpdatedSchema)
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
		ctx, cancel := context.WithTimeout(context.TODO(), tableCountQueryTimeout)
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
		_ = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, tName, repo.TableUploadSetOptions{
			Status: &status,
		})
		return alteredSchema, fmt.Errorf("update schema: %w", err)
	}

	pkgLogger.Infow("stating load for table",
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
	lastExecTime := job.Now()
	_ = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, tName, repo.TableUploadSetOptions{
		Status:       &status,
		LastExecTime: &lastExecTime,
	})

	generateTableLoadCountVerificationsMetrics := config.GetBool("Warehouse.generateTableLoadCountMetrics", true)

	disableGenerateMetricsWorkspaceIDs := config.GetStringSlice("Warehouse.disableGenerateTableLoadCountMetricsWorkspaceIDs", nil)
	if slices.Contains(disableGenerateMetricsWorkspaceIDs, job.upload.WorkspaceID) {
		generateTableLoadCountVerificationsMetrics = false
	}

	var totalBeforeLoad, totalAfterLoad int64
	if generateTableLoadCountVerificationsMetrics {
		var errTotalCount error
		totalBeforeLoad, errTotalCount = job.getTotalCount(tName)
		if errTotalCount != nil {
			pkgLogger.Warnw("total count in table before loading",
				logfield.SourceID, job.upload.SourceID,
				logfield.DestinationID, job.upload.DestinationID,
				logfield.DestinationType, job.upload.DestinationType,
				logfield.WorkspaceID, job.upload.WorkspaceID,
				logfield.Error, errTotalCount,
				logfield.TableName, tName,
			)
		}
	}

	err = job.whManager.LoadTable(tName)
	if err != nil {
		status := model.TableUploadExportingFailed
		_ = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, tName, repo.TableUploadSetOptions{
			Status: &status,
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
			pkgLogger.Warnw("total count in table after loading",
				logfield.SourceID, job.upload.SourceID,
				logfield.DestinationID, job.upload.DestinationID,
				logfield.DestinationType, job.upload.DestinationType,
				logfield.WorkspaceID, job.upload.WorkspaceID,
				logfield.Error, errTotalCount,
				logfield.TableName, tName,
			)
			return
		}
		tableUpload, errEventCount := job.tableUploadsRepo.GetByUploadIDAndTableName(context.TODO(), job.upload.ID, tName)
		if errEventCount != nil {
			return
		}

		// TODO : Perform the comparison here in the codebase
		job.guageStat(`pre_load_table_rows`, Tag{Name: "tableName", Value: strings.ToLower(tName)}).Gauge(int(totalBeforeLoad))
		job.guageStat(`post_load_table_rows_estimate`, Tag{Name: "tableName", Value: strings.ToLower(tName)}).Gauge(int(totalBeforeLoad + tableUpload.TotalEvents))
		job.guageStat(`post_load_table_rows`, Tag{Name: "tableName", Value: strings.ToLower(tName)}).Gauge(int(totalAfterLoad))
	}()

	status = model.TableUploadExported
	_ = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, tName, repo.TableUploadSetOptions{
		Status: &status,
	})
	tableUpload, queryErr := job.tableUploadsRepo.GetByUploadIDAndTableName(context.TODO(), job.upload.ID, tName)
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
	case warehouseutils.S3_DATALAKE, warehouseutils.GCS_DATALAKE, warehouseutils.AZURE_DATALAKE:
		return
	}

	if columnCountLimit, ok = columnCountLimitMap[job.warehouse.Type]; !ok {
		return
	}

	tags := []Tag{
		{Name: "tableName", Value: strings.ToLower(tableName)},
	}
	currentColumnsCount := len(job.schemaHandle.schemaInWarehouse[tableName])

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

	defer job.timerStat("user_tables_load_time").RecordDuration()()

	// Load all user tables
	status := model.TableUploadExecuting
	lastExecTime := job.Now()
	_ = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, job.identifiesTableName(), repo.TableUploadSetOptions{
		Status:       &status,
		LastExecTime: &lastExecTime,
	})

	alteredIdentitySchema, err := job.updateSchema(job.identifiesTableName())
	if err != nil {
		status := model.TableUploadUpdatingSchemaFailed
		errorsString := misc.QuoteLiteral(err.Error())
		_ = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, job.identifiesTableName(), repo.TableUploadSetOptions{
			Status: &status,
			Error:  &errorsString,
		})
		return job.processLoadTableResponse(map[string]error{job.identifiesTableName(): err})
	}
	var alteredUserSchema bool
	if _, ok := job.upload.UploadSchema[job.usersTableName()]; ok {
		status := model.TableUploadExecuting
		lastExecTime := job.Now()
		_ = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, job.usersTableName(), repo.TableUploadSetOptions{
			Status:       &status,
			LastExecTime: &lastExecTime,
		})
		alteredUserSchema, err = job.updateSchema(job.usersTableName())
		if err != nil {
			status = model.TableUploadUpdatingSchemaFailed
			errorsString := misc.QuoteLiteral(err.Error())
			_ = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, job.usersTableName(), repo.TableUploadSetOptions{
				Status: &status,
				Error:  &errorsString,
			})
			return job.processLoadTableResponse(map[string]error{job.usersTableName(): err})
		}
	}

	// Skip loading user tables if identifies table schema is not present
	if identifiesSchema := job.GetTableSchemaInUpload(warehouseutils.IdentifiesTable); len(identifiesSchema) == 0 {
		return []error{}, nil
	}

	errorMap := job.whManager.LoadUserTables()

	if alteredIdentitySchema || alteredUserSchema {
		pkgLogger.Infof("loadUserTables: schema changed - updating local schema for %s", job.warehouse.Identifier)
		_ = job.schemaHandle.updateLocalSchema(job.schemaHandle.schemaInWarehouse)
	}
	return job.processLoadTableResponse(errorMap)
}

func (job *UploadJob) loadIdentityTables(populateHistoricIdentities bool) (loadErrors []error, tableUploadErr error) {
	pkgLogger.Infof(`[WH]: Starting load for identity tables in namespace %s of destination %s:%s`, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)
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
	if generated, _ := job.areIdentityTablesLoadFilesGenerated(); !generated {
		err := job.resolveIdentities(populateHistoricIdentities)
		if err != nil {
			pkgLogger.Errorf(` ID Resolution operation failed: %v`, err)
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

		tableSchemaDiff := getTableSchemaDiff(tableName, job.schemaHandle.schemaInWarehouse, job.upload.UploadSchema)
		if tableSchemaDiff.Exists {
			err := job.UpdateTableSchema(tableName, tableSchemaDiff)
			if err != nil {
				status := model.TableUploadUpdatingSchemaFailed
				errorsString := misc.QuoteLiteral(err.Error())
				_ = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, tableName, repo.TableUploadSetOptions{
					Status: &status,
					Error:  &errorsString,
				})
				errorMap := map[string]error{tableName: err}
				return job.processLoadTableResponse(errorMap)
			}
			job.setUpdatedTableSchema(tableName, tableSchemaDiff.UpdatedSchema)

			status := model.TableUploadUpdatedSchema
			_ = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, tableName, repo.TableUploadSetOptions{
				Status: &status,
			})
			alteredSchema = true
		}

		status := model.TableUploadExecuting
		lastExecTime := job.Now()
		err = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, tableName, repo.TableUploadSetOptions{
			Status:       &status,
			LastExecTime: &lastExecTime,
		})
		if err != nil {
			errorMap[tableName] = err
			break
		}

		switch tableName {
		case job.identityMergeRulesTableName():
			err = job.whManager.LoadIdentityMergeRulesTable()
		case job.identityMappingsTableName():
			err = job.whManager.LoadIdentityMappingsTable()
		}

		if err != nil {
			errorMap[tableName] = err
			break
		}
	}

	if alteredSchema {
		pkgLogger.Infof("loadIdentityTables: schema changed - updating local schema for %s", job.warehouse.Identifier)
		_ = job.schemaHandle.updateLocalSchema(job.schemaHandle.schemaInWarehouse)
	}

	return job.processLoadTableResponse(errorMap)
}

func (job *UploadJob) setUpdatedTableSchema(tableName string, updatedSchema model.TableSchema) {
	job.schemaLock.Lock()
	job.schemaHandle.schemaInWarehouse[tableName] = updatedSchema
	job.schemaLock.Unlock()
}

func (job *UploadJob) processLoadTableResponse(errorMap map[string]error) (errors []error, tableUploadErr error) {
	for tName, loadErr := range errorMap {
		// TODO: set last_exec_time
		if loadErr != nil {
			errors = append(errors, loadErr)
			errorsString := misc.QuoteLiteral(loadErr.Error())
			status := model.TableUploadExportingFailed
			tableUploadErr = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, tName, repo.TableUploadSetOptions{
				Status: &status,
				Error:  &errorsString,
			})
		} else {
			status := model.TableUploadExported
			tableUploadErr = job.tableUploadsRepo.Set(context.TODO(), job.upload.ID, tName, repo.TableUploadSetOptions{
				Status: &status,
			})
			if tableUploadErr == nil {
				// Since load is successful, we assume all events in load files are uploaded
				tableUpload, queryErr := job.tableUploadsRepo.GetByUploadIDAndTableName(context.TODO(), job.upload.ID, tName)
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
// e.g. status: exported_data, timings: [{exporting_data: 2020-04-21 15:16:19.687716] -> [{exporting_data: 2020-04-21 15:16:19.687716, exported_data: 2020-04-21 15:26:34.344356}]
func (job *UploadJob) getNewTimings(status string) ([]byte, model.Timings) {
	timings, err := repo.NewUploads(job.dbHandle).UploadTimings(context.TODO(), job.upload.ID)
	if err != nil {
		pkgLogger.Error("error getting timing, scrapping them", err)
	}
	timing := map[string]time.Time{status: job.Now()}
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
		warehouseutils.WarehouseUploadsTable,
		job.upload.ID,
	)
	err := job.dbHandle.QueryRow(sqlStatement).Scan(&firstTiming)
	if err != nil {
		return
	}
	_, timing = warehouseutils.TimingFromJSONString(firstTiming)
	return timing
}

type UploadStatusOpts struct {
	Status           string
	AdditionalFields []UploadColumn
	ReportingMetric  types.PUReportedMetric
}

func (job *UploadJob) setUploadStatus(statusOpts UploadStatusOpts) (err error) {
	pkgLogger.Debugf("[WH]: Setting status of %s for wh_upload:%v", statusOpts.Status, job.upload.ID)
	// TODO: fetch upload model instead of just timings
	marshalledTimings, timings := job.getNewTimings(statusOpts.Status)
	opts := []UploadColumn{
		{Column: UploadStatusField, Value: statusOpts.Status},
		{Column: UploadTimingsField, Value: marshalledTimings},
		{Column: UploadUpdatedAtField, Value: job.Now()},
	}

	job.upload.Status = statusOpts.Status
	job.upload.Timings = timings
	additionalFields := append(statusOpts.AdditionalFields, opts...)

	uploadColumnOpts := UploadColumnsOpts{Fields: additionalFields}

	if statusOpts.ReportingMetric != (types.PUReportedMetric{}) {
		txn, err := dbHandle.Begin()
		if err != nil {
			return err
		}
		uploadColumnOpts.Txn = txn
		err = job.setUploadColumns(uploadColumnOpts)
		if err != nil {
			return err
		}

		if config.GetBool("Reporting.enabled", types.DefaultReportingEnabled) {
			application.Features().Reporting.GetReportingInstance().Report([]*types.PUReportedMetric{&statusOpts.ReportingMetric}, txn)
		}
		err = txn.Commit()
		return err
	}
	return job.setUploadColumns(uploadColumnOpts)
}

// SetUploadSchema
func (job *UploadJob) setUploadSchema(consolidatedSchema model.Schema) error {
	marshalledSchema, err := json.Marshal(consolidatedSchema)
	if err != nil {
		panic(err)
	}
	job.upload.UploadSchema = consolidatedSchema
	return job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumn{{Column: UploadSchemaField, Value: marshalledSchema}}})
}

func (job *UploadJob) setMergedSchema(mergedSchema model.Schema) error {
	marshalledSchema, err := json.Marshal(mergedSchema)
	if err != nil {
		panic(err)
	}
	job.upload.MergedSchema = mergedSchema
	return job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumn{{Column: MergedSchemaField, Value: marshalledSchema}}})
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
	Txn    *sql.Tx
}

// SetUploadColumns sets any column values passed as args in UploadColumn format for WarehouseUploadsTable
func (job *UploadJob) setUploadColumns(opts UploadColumnsOpts) (err error) {
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
		  id = $1;
`,
		warehouseutils.WarehouseUploadsTable,
		columns,
	)
	if opts.Txn != nil {
		_, err = opts.Txn.Exec(sqlStatement, values...)
	} else {
		_, err = dbHandle.Exec(sqlStatement, values...)
	}

	return err
}

func (job *UploadJob) triggerUploadNow() (err error) {
	job.uploadLock.Lock()
	defer job.uploadLock.Unlock()
	newJobState := model.Waiting

	metadata := repo.ExtractUploadMetadata(job.upload)

	metadata.NextRetryTime = job.Now().Add(-time.Hour * 1)
	metadata.Retried = true
	metadata.Priority = 50

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	uploadColumns := []UploadColumn{
		{Column: "status", Value: newJobState},
		{Column: "metadata", Value: metadataJSON},
		{Column: "updated_at", Value: job.Now()},
	}

	txn, err := job.dbHandle.Begin()
	if err != nil {
		panic(err)
	}
	err = job.setUploadColumns(UploadColumnsOpts{Fields: uploadColumns, Txn: txn})
	if err != nil {
		panic(err)
	}
	err = txn.Commit()

	job.upload.Status = newJobState
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

	return attempts > job.MinRetryAttempts && job.Now().Sub(startTime) > job.RetryTimeWindow
}

func (job *UploadJob) setUploadError(statusError error, state string) (string, error) {
	var (
		errorTags                  = job.ErrorHandler.MatchErrorMappings(statusError)
		destCredentialsValidations *bool
	)

	defer func() {
		pkgLogger.Warnw("upload error",
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

	metadata.NextRetryTime = job.Now().Add(DurationBeforeNextAttempt(upload.Attempts + 1))
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		metadataJSON = []byte("{}")
	}

	serializedErr, _ := json.Marshal(&uploadErrors)

	uploadColumns := []UploadColumn{
		{Column: "status", Value: state},
		{Column: "metadata", Value: metadataJSON},
		{Column: "error", Value: serializedErr},
		{Column: "updated_at", Value: job.Now()},
	}

	txn, err := job.dbHandle.Begin()
	if err != nil {
		return "", fmt.Errorf("unable to start transaction: %w", err)
	}

	err = job.setUploadColumns(UploadColumnsOpts{Fields: uploadColumns, Txn: txn})
	if err != nil {
		return "", fmt.Errorf("unable to change upload columns: %w", err)
	}

	inputCount, _ := repo.NewStagingFiles(dbHandle).TotalEventsForUpload(context.TODO(), upload)
	outputCount, _ := job.tableUploadsRepo.TotalExportedEvents(context.TODO(), job.upload.ID, []string{
		warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.DiscardsTable),
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
	if config.GetBool("Reporting.enabled", types.DefaultReportingEnabled) {
		application.Features().Reporting.GetReportingInstance().Report(reportingMetrics, txn)
	}
	err = txn.Commit()

	job.upload.Status = state
	job.upload.Error = serializedErr

	attempts := job.getAttemptNumber()
	job.counterStat("warehouse_failed_uploads", Tag{Name: "attempt_number", Value: strconv.Itoa(attempts)}).Count(1)

	// On aborted state, validate credentials to allow
	// us to differentiate between user caused abort vs platform issue.
	if state == model.Aborted {
		// base tag to be sent as stat

		tags := []Tag{{Name: "attempt_number", Value: strconv.Itoa(attempts)}}
		tags = append(tags, errorTags)

		valid, err := job.validateDestinationCredentials()
		if err == nil {
			tags = append(tags, Tag{Name: "destination_creds_valid", Value: strconv.FormatBool(valid)})
			destCredentialsValidations = &valid
		}

		job.counterStat("upload_aborted", tags...).Count(1)
	}

	return state, err
}

func (job *UploadJob) validateDestinationCredentials() (bool, error) {
	if job.destinationValidator == nil {
		return false, errors.New("failed to validate as destinationValidator is not set")
	}
	response := job.destinationValidator.Validate(&job.warehouse.Destination)
	return response.Success, nil
}

func (job *UploadJob) getAttemptNumber() int {
	uploadError := job.upload.Error
	var attempts int32
	if string(uploadError) == "" {
		return 0
	}

	gjson.Parse(string(uploadError)).ForEach(func(key, value gjson.Result) bool {
		attempts += int32(gjson.Get(value.String(), "attempt").Int())
		return true
	})
	return int(attempts)
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
		warehouseutils.WarehouseLoadFilesTable,
	)
	sqlStatementArgs := []interface{}{
		sourceID,
		destID,
		job.upload.LoadFileStartID,
		job.upload.LoadFileEndID,
	}
	rows, err := dbHandle.Query(sqlStatement, sqlStatementArgs...)
	if err == sql.ErrNoRows {
		err = nil
		return
	}
	if err != nil && err != sql.ErrNoRows {
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
	return
}

func (job *UploadJob) areIdentityTablesLoadFilesGenerated() (bool, error) {
	var (
		mergeRulesTable = warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentityMergeRulesTable)
		mappingsTable   = warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentityMappingsTable)
	)

	if tu, err := job.tableUploadsRepo.GetByUploadIDAndTableName(context.TODO(), job.upload.ID, mergeRulesTable); err != nil && tu.Location == "" {
		return false, fmt.Errorf("merge rules location not found: %w", err)
	}
	if tu, err := job.tableUploadsRepo.GetByUploadIDAndTableName(context.TODO(), job.upload.ID, mappingsTable); err != nil && tu.Location == "" {
		return false, fmt.Errorf("mappings location not found: %w", err)
	}
	return true, nil
}

func (job *UploadJob) GetLoadFilesMetadata(options warehouseutils.GetLoadFilesOptions) (loadFiles []warehouseutils.LoadFile) {
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
		warehouseutils.WarehouseLoadFilesTable,
		misc.IntArrayToString(job.stagingFileIDs, ","),
		tableFilterSQL,
		limitSQL,
	)

	pkgLogger.Debugf(`Fetching loadFileLocations: %v`, sqlStatement)
	rows, err := dbHandle.Query(sqlStatement)
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var location string
		var metadata json.RawMessage
		err := rows.Scan(&location, &metadata)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		loadFiles = append(loadFiles, warehouseutils.LoadFile{
			Location: location,
			Metadata: metadata,
		})
	}
	return
}

func (job *UploadJob) GetSampleLoadFileLocation(tableName string) (location string, err error) {
	locations := job.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptions{Table: tableName, Limit: 1})
	if len(locations) == 0 {
		return "", fmt.Errorf(`no load file found for table:%s`, tableName)
	}
	return locations[0].Location, nil
}

func (job *UploadJob) GetSchemaInWarehouse() (schema model.Schema) {
	if job.schemaHandle == nil {
		return
	}
	return job.schemaHandle.schemaInWarehouse
}

func (job *UploadJob) GetTableSchemaInWarehouse(tableName string) model.TableSchema {
	return job.schemaHandle.schemaInWarehouse[tableName]
}

func (job *UploadJob) GetTableSchemaInUpload(tableName string) model.TableSchema {
	return job.schemaHandle.uploadSchema[tableName]
}

func (job *UploadJob) GetSingleLoadFile(tableName string) (warehouseutils.LoadFile, error) {
	var (
		tableUpload model.TableUpload
		err         error
	)

	if tableUpload, err = job.tableUploadsRepo.GetByUploadIDAndTableName(context.TODO(), job.upload.ID, tableName); err != nil {
		return warehouseutils.LoadFile{}, fmt.Errorf("get single load file: %w", err)
	}

	return warehouseutils.LoadFile{Location: tableUpload.Location}, err
}

func (job *UploadJob) ShouldOnDedupUseNewRecord() bool {
	category := job.warehouse.Source.SourceDefinition.Category
	return category == SingerProtocolSourceCategory || category == CloudSourceCategory
}

func (job *UploadJob) UseRudderStorage() bool {
	return job.upload.UseRudderStorage
}

func (job *UploadJob) GetLoadFileGenStartTIme() time.Time {
	if !job.LoadFileGenStartTime.IsZero() {
		return job.LoadFileGenStartTime
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

func (job *UploadJob) GetLocalSchema() model.Schema {
	return job.schemaHandle.getLocalSchema()
}

func (job *UploadJob) UpdateLocalSchema(schema model.Schema) error {
	return job.schemaHandle.updateLocalSchema(schema)
}

func (job *UploadJob) RefreshPartitions(loadFileStartID, loadFileEndID int64) error {
	if !slices.Contains(warehouseutils.TimeWindowDestinations, job.upload.DestinationType) {
		return nil
	}

	var (
		repository schemarepository.SchemaRepository
		err        error
	)

	if repository, err = schemarepository.NewSchemaRepository(job.warehouse, job); err != nil {
		return fmt.Errorf("create schema repository: %w", err)
	}

	// Refresh partitions if exists
	for tableName := range job.upload.UploadSchema {
		loadFiles := job.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptions{
			Table:   tableName,
			StartID: loadFileStartID,
			EndID:   loadFileEndID,
		})
		batches := schemarepository.LoadFileBatching(loadFiles, job.RefreshPartitionBatchSize)

		for _, batch := range batches {
			if err = repository.RefreshPartitions(tableName, batch); err != nil {
				return fmt.Errorf("refresh partitions: %w", err)
			}
		}
	}

	return nil
}
