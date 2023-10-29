package router

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/alerta"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service"
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
	reporting            types.Reporting
	db                   *sqlquerywrapper.DB
	destinationValidator validations.DestinationValidator
	loadFile             *loadfiles.LoadFileGenerator
	recovery             *service.Recovery
	conf                 *config.Config
	logger               logger.Logger
	statsFactory         stats.Stats
	encodingFactory      *encoding.Factory
}

type UploadJob struct {
	ctx                  context.Context
	db                   *sqlquerywrapper.DB
	reporting            types.Reporting
	destinationValidator validations.DestinationValidator
	loadfile             *loadfiles.LoadFileGenerator
	tableUploadsRepo     *repo.TableUploads
	recovery             *service.Recovery
	whManager            manager.Manager
	schemaHandle         *schema.Schema
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
		refreshPartitionBatchSize           int
		retryTimeWindow                     time.Duration
		minRetryAttempts                    int
		disableAlter                        bool
		minUploadBackoff                    time.Duration
		maxUploadBackoff                    time.Duration
		alwaysRegenerateAllLoadFiles        bool
		reportingEnabled                    bool
		maxParallelLoadsWorkspaceIDs        map[string]interface{}
		columnsBatchSize                    int
		longRunningUploadStatThresholdInMin time.Duration
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
		reporting:            f.reporting,
		db:                   f.db,
		loadfile:             f.loadFile,
		recovery:             f.recovery,
		whManager:            whManager,
		destinationValidator: f.destinationValidator,
		conf:                 f.conf,
		logger:               f.logger,
		statsFactory:         f.statsFactory,
		tableUploadsRepo:     repo.NewTableUploads(f.db),
		schemaHandle: schema.New(
			f.db,
			dto.Warehouse,
			f.conf,
			f.logger.Child("warehouse"),
		),

		upload:         dto.Upload,
		warehouse:      dto.Warehouse,
		stagingFiles:   dto.StagingFiles,
		stagingFileIDs: repo.StagingFileIDs(dto.StagingFiles),

		pendingTableUploadsRepo: repo.NewUploads(f.db),
		pendingTableUploads:     []model.PendingTableUpload{},

		alertSender: alerta.NewClient(
			f.conf.GetString("ALERTA_URL", "https://alerta.rudderstack.com/api/"),
		),
		now: timeutil.Now,

		errorHandler:    ErrorHandler{Mapper: whManager},
		encodingFactory: f.encodingFactory,
	}

	uj.config.refreshPartitionBatchSize = f.conf.GetInt("Warehouse.refreshPartitionBatchSize", 100)
	uj.config.minRetryAttempts = f.conf.GetInt("Warehouse.minRetryAttempts", 3)
	uj.config.disableAlter = f.conf.GetBool("Warehouse.disableAlter", false)
	uj.config.alwaysRegenerateAllLoadFiles = f.conf.GetBool("Warehouse.alwaysRegenerateAllLoadFiles", true)
	uj.config.reportingEnabled = f.conf.GetBool("Reporting.enabled", types.DefaultReportingEnabled)
	uj.config.columnsBatchSize = f.conf.GetInt(fmt.Sprintf("Warehouse.%s.columnsBatchSize", whutils.WHDestNameMap[uj.upload.DestinationType]), 100)
	uj.config.maxParallelLoadsWorkspaceIDs = f.conf.GetStringMap(fmt.Sprintf("Warehouse.%s.maxParallelLoadsWorkspaceIDs", whutils.WHDestNameMap[uj.upload.DestinationType]), nil)
	uj.config.longRunningUploadStatThresholdInMin = f.conf.GetDurationVar(120, time.Minute, "Warehouse.longRunningUploadStatThreshold", "Warehouse.longRunningUploadStatThresholdInMin")
	uj.config.minUploadBackoff = f.conf.GetDurationVar(60, time.Second, "Warehouse.minUploadBackoff", "Warehouse.minUploadBackoffInS")
	uj.config.maxUploadBackoff = f.conf.GetDurationVar(1800, time.Second, "Warehouse.maxUploadBackoff", "Warehouse.maxUploadBackoffInS")
	uj.config.retryTimeWindow = f.conf.GetDurationVar(180, time.Minute, "Warehouse.retryTimeWindow", "Warehouse.retryTimeWindowInMins")

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
			if err = job.generateUploadSchema(); err != nil {
				break
			}
			newStatus = nextUploadState.completed

		case model.CreatedTableUploads:
			newStatus = nextUploadState.failed
			if err = job.createTableUploads(); err != nil {
				break
			}
			newStatus = nextUploadState.completed

		case model.GeneratedLoadFiles:
			newStatus = nextUploadState.failed
			if err = job.generateLoadFiles(hasSchemaChanged); err != nil {
				break
			}
			newStatus = nextUploadState.completed

		case model.UpdatedTableUploadsCounts:
			newStatus = nextUploadState.failed
			if err = job.updateTableUploadsCounts(); err != nil {
				break
			}
			newStatus = nextUploadState.completed

		case model.CreatedRemoteSchema:
			newStatus = nextUploadState.failed
			if err = job.createRemoteSchema(whManager); err != nil {
				break
			}
			newStatus = nextUploadState.completed

		case model.ExportedData:
			newStatus = nextUploadState.failed
			if err = job.exportData(); err != nil {
				break
			}
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

			rowCount, _ := repo.NewStagingFiles(job.db).TotalEventsForUpload(job.ctx, job.upload)

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

// getNewTimings appends current status with current time to timings column
// e.g. status: exported_data, timings: [{exporting_data: 2020-04-21 15:16:19.687716}] -> [{exporting_data: 2020-04-21 15:16:19.687716, exported_data: 2020-04-21 15:26:34.344356}]
func (job *UploadJob) getNewTimings(status string) ([]byte, model.Timings, error) {
	timings, err := repo.NewUploads(job.db).UploadTimings(job.ctx, job.upload.ID)
	if err != nil {
		return nil, nil, err
	}
	timing := map[string]time.Time{status: job.now()}
	timings = append(timings, timing)
	marshalledTimings, err := json.Marshal(timings)
	if err != nil {
		return nil, nil, err
	}
	return marshalledTimings, timings, nil
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
	err := job.db.QueryRowContext(job.ctx, sqlStatement).Scan(&firstTiming)
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
	marshalledTimings, timings, err := job.getNewTimings(statusOpts.Status)
	if err != nil {
		return
	}
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
		txn, err = job.db.BeginTx(job.ctx, &sql.TxOptions{})
		if err != nil {
			return
		}
		defer func() {
			if err != nil {
				_ = txn.Rollback()
			}
		}()

		uploadColumnOpts.Txn = txn
		err = job.setUploadColumns(uploadColumnOpts)
		if err != nil {
			return
		}
		if job.config.reportingEnabled {
			err = job.reporting.Report(
				[]*types.PUReportedMetric{&statusOpts.ReportingMetric},
				txn.Tx,
			)
			if err != nil {
				return
			}
		}
		err = txn.Commit()
		return
	}
	err = job.setUploadColumns(uploadColumnOpts)
	return
}

type UploadColumnsOpts struct {
	Fields []UploadColumn
	Txn    *sqlquerywrapper.Tx
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
		querier = job.db
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
		jobErrorType               = job.errorHandler.MatchUploadJobErrorType(statusError)
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
			logfield.ErrorMapping, jobErrorType,
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
		{Column: "error_category", Value: model.GetUserFriendlyJobErrorCategory(jobErrorType)},
	}

	txn, err := job.db.BeginTx(job.ctx, &sql.TxOptions{})
	if err != nil {
		return "", fmt.Errorf("starting transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = txn.Rollback()
		}
	}()

	if err = job.setUploadColumns(UploadColumnsOpts{Fields: uploadColumns, Txn: txn}); err != nil {
		return "", fmt.Errorf("changing upload columns: %w", err)
	}
	inputCount, _ := repo.NewStagingFiles(job.db).TotalEventsForUpload(job.ctx, upload)
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
		if err = job.reporting.Report(reportingMetrics, txn.Tx); err != nil {
			return "", fmt.Errorf("reporting metrics: %w", err)
		}
	}
	if err = txn.Commit(); err != nil {
		return "", fmt.Errorf("committing transaction: %w", err)
	}

	job.upload.Status = state
	job.upload.Error = serializedErr

	job.stats.uploadFailed.Count(1)

	// On aborted state, validate credentials to allow
	// us to differentiate between user caused abort vs platform issue.
	if state == model.Aborted {
		// base tag to be sent as stat

		tags := []whutils.Tag{{Name: "error_mapping", Value: jobErrorType}}

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

func (job *UploadJob) GetLoadFilesMetadata(ctx context.Context, options whutils.GetLoadFilesOptions) (loadFiles []whutils.LoadFile, err error) {
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
	rows, err := job.db.QueryContext(ctx, sqlStatement)
	if err != nil {
		return nil, fmt.Errorf("query: %s\nfailed with Error : %w", sqlStatement, err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var location string
		var metadata json.RawMessage
		err := rows.Scan(&location, &metadata)
		if err != nil {
			return nil, fmt.Errorf("failed to scan result from query: %s\nwith Error : %w", sqlStatement, err)
		}
		loadFiles = append(loadFiles, whutils.LoadFile{
			Location: location,
			Metadata: metadata,
		})
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate query results: %s\nwith Error : %w", sqlStatement, err)
	}
	return
}

func (job *UploadJob) GetSampleLoadFileLocation(ctx context.Context, tableName string) (location string, err error) {
	locations, err := job.GetLoadFilesMetadata(ctx, whutils.GetLoadFilesOptions{Table: tableName, Limit: 1})
	if err != nil {
		return "", fmt.Errorf("get load file metadata: %w", err)
	}
	if len(locations) == 0 {
		return "", fmt.Errorf(`no load file found for table:%s`, tableName)
	}
	return locations[0].Location, nil
}

func (job *UploadJob) IsWarehouseSchemaEmpty() bool {
	return job.schemaHandle.IsWarehouseSchemaEmpty()
}

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

func (job *UploadJob) GetLocalSchema(ctx context.Context) (model.Schema, error) {
	return job.schemaHandle.GetLocalSchema(ctx)
}

func (job *UploadJob) UpdateLocalSchema(ctx context.Context, schema model.Schema) error {
	return job.schemaHandle.UpdateLocalSchema(ctx, job.upload.ID, schema)
}
