package warehouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gofrs/uuid"
	"github.com/lib/pq"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/configuration_testing"
	"github.com/rudderlabs/rudder-server/warehouse/identity"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// Upload Status
const (
	Waiting                   = "waiting"
	GeneratedUploadSchema     = "generated_upload_schema"
	CreatedTableUploads       = "created_table_uploads"
	GeneratedLoadFiles        = "generated_load_files"
	UpdatedTableUploadsCounts = "updated_table_uploads_counts"
	CreatedRemoteSchema       = "created_remote_schema"
	ExportedUserTables        = "exported_user_tables"
	ExportedData              = "exported_data"
	ExportedIdentities        = "exported_identities"
	Aborted                   = "aborted"
)

const (
	GeneratingStagingFileFailedState        = "generating_staging_file_failed"
	GeneratedStagingFileState               = "generated_staging_file"
	PopulatingHistoricIdentitiesState       = "populating_historic_identities"
	PopulatingHistoricIdentitiesStateFailed = "populating_historic_identities_failed"
	FetchingRemoteSchemaFailed              = "fetching_remote_schema_failed"
	InternalProcessingFailed                = "internal_processing_failed"
)

// Table Upload status
const (
	TableUploadExecuting               = "executing"
	TableUploadUpdatingSchema          = "updating_schema"
	TableUploadUpdatingSchemaFailed    = "updating_schema_failed"
	TableUploadUpdatedSchema           = "updated_schema"
	TableUploadExporting               = "exporting_data"
	TableUploadExportingFailed         = "exporting_data_failed"
	UserTableUploadExportingFailed     = "exporting_user_tables_failed"
	IdentityTableUploadExportingFailed = "exporting_identities_failed"
	TableUploadExported                = "exported_data"
)

const (
	CloudSourceCateogry = "cloud"
)

var stateTransitions map[string]*uploadStateT

type uploadStateT struct {
	inProgress string
	failed     string
	completed  string
	nextState  *uploadStateT
}

type UploadT struct {
	ID                   int64
	Namespace            string
	SourceID             string
	SourceType           string
	SourceCategory       string
	DestinationID        string
	DestinationType      string
	StartStagingFileID   int64
	EndStagingFileID     int64
	StartLoadFileID      int64
	EndLoadFileID        int64
	Status               string
	UploadSchema         warehouseutils.SchemaT
	MergedSchema         warehouseutils.SchemaT
	Error                json.RawMessage
	Timings              []map[string]string
	FirstAttemptAt       time.Time
	LastAttemptAt        time.Time
	Attempts             int64
	Metadata             json.RawMessage
	FirstEventAt         time.Time
	LastEventAt          time.Time
	UseRudderStorage     bool
	LoadFileGenStartTime time.Time
	TimingsObj           sql.NullString
	Priority             int
	// cloud sources specific info
	SourceBatchID   string
	SourceTaskID    string
	SourceTaskRunID string
	SourceJobID     string
	SourceJobRunID  string
	LoadFileType    string
}

type tableNameT string

type UploadJobT struct {
	upload               *UploadT
	dbHandle             *sql.DB
	warehouse            warehouseutils.WarehouseT
	whManager            manager.ManagerI
	stagingFiles         []*StagingFileT
	stagingFileIDs       []int64
	pgNotifier           *pgnotifier.PgNotifierT
	schemaHandle         *SchemaHandleT
	schemaLock           sync.Mutex
	uploadLock           sync.Mutex
	hasAllTablesSkipped  bool
	tableUploadStatuses  []*TableUploadStatusT
	destinationValidator configuration_testing.DestinationValidator
}

type UploadColumnT struct {
	Column string
	Value  interface{}
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
	warehousesToVerifyLoadFilesFolder                = []string{warehouseutils.SNOWFLAKE}
)

var (
	maxParallelLoads      map[string]int
	columnCountThresholds map[string]int
)

func init() {
	initializeStateMachine()
}

func setMaxParallelLoads() {
	maxParallelLoads = map[string]int{
		warehouseutils.BQ:         config.GetInt("Warehouse.bigquery.maxParallelLoads", 20),
		warehouseutils.RS:         config.GetInt("Warehouse.redshift.maxParallelLoads", 3),
		warehouseutils.POSTGRES:   config.GetInt("Warehouse.postgres.maxParallelLoads", 3),
		warehouseutils.MSSQL:      config.GetInt("Warehouse.mssql.maxParallelLoads", 3),
		warehouseutils.SNOWFLAKE:  config.GetInt("Warehouse.snowflake.maxParallelLoads", 3),
		warehouseutils.CLICKHOUSE: config.GetInt("Warehouse.clickhouse.maxParallelLoads", 3),
		warehouseutils.DELTALAKE:  config.GetInt("Warehouse.deltalake.maxParallelLoads", 3),
	}
	columnCountThresholds = map[string]int{
		warehouseutils.AZURE_SYNAPSE: config.GetInt("Warehouse.azure_synapse.columnCountThreshold", 800),
		warehouseutils.BQ:            config.GetInt("Warehouse.bigquery.columnCountThreshold", 8000),
		warehouseutils.CLICKHOUSE:    config.GetInt("Warehouse.clickhouse.columnCountThreshold", 800),
		warehouseutils.MSSQL:         config.GetInt("Warehouse.mssql.columnCountThreshold", 800),
		warehouseutils.POSTGRES:      config.GetInt("Warehouse.postgres.columnCountThreshold", 1200),
		warehouseutils.RS:            config.GetInt("Warehouse.redshift.columnCountThreshold", 1200),
		warehouseutils.SNOWFLAKE:     config.GetInt("Warehouse.snowflake.columnCountThreshold", 1600),
	}
}

func (job *UploadJobT) identifiesTableName() string {
	return warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentifiesTable)
}

func (job *UploadJobT) usersTableName() string {
	return warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.UsersTable)
}

func (job *UploadJobT) identityMergeRulesTableName() string {
	return warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentityMergeRulesTable)
}

func (job *UploadJobT) identityMappingsTableName() string {
	return warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentityMappingsTable)
}

func (job *UploadJobT) trackLongRunningUpload() chan struct{} {
	ch := make(chan struct{}, 1)
	rruntime.GoForWarehouse(func() {
		select {
		case <-ch:
			// do nothing
		case <-time.After(longRunningUploadStatThresholdInMin):
			pkgLogger.Infof("[WH]: Registering stat for long running upload: %d, dest: %s", job.upload.ID, job.warehouse.Identifier)
			warehouseutils.DestStat(stats.CountType, "long_running_upload", job.warehouse.Destination.ID).Count(1)
		}
	})
	return ch
}

func (job *UploadJobT) generateUploadSchema(schemaHandle *SchemaHandleT) error {
	schemaHandle.uploadSchema = schemaHandle.consolidateStagingFilesSchemaUsingWarehouseSchema()
	if job.upload.LoadFileType == warehouseutils.LOAD_FILE_TYPE_PARQUET {
		// set merged schema if the loadFileType is parquet
		mergedSchema := mergeUploadAndLocalSchemas(schemaHandle.uploadSchema, schemaHandle.localSchema)
		err := job.setMergedSchema(mergedSchema)
		if err != nil {
			return err
		}
	}
	// set upload schema
	err := job.setUploadSchema(schemaHandle.uploadSchema)
	return err
}

func (job *UploadJobT) initTableUploads() error {
	schemaForUpload := job.upload.UploadSchema
	destType := job.warehouse.Type
	tables := make([]string, 0, len(schemaForUpload))
	for t := range schemaForUpload {
		tables = append(tables, t)
		// also track upload to rudder_identity_mappings if the upload has records for rudder_identity_merge_rules
		if misc.ContainsString(warehouseutils.IdentityEnabledWarehouses, destType) && t == warehouseutils.ToProviderCase(destType, warehouseutils.IdentityMergeRulesTable) {
			if _, ok := schemaForUpload[warehouseutils.ToProviderCase(destType, warehouseutils.IdentityMappingsTable)]; !ok {
				tables = append(tables, warehouseutils.ToProviderCase(destType, warehouseutils.IdentityMappingsTable))
			}
		}
	}

	return createTableUploads(job.upload.ID, tables)
}

func (job *UploadJobT) syncRemoteSchema() (schemaChanged bool, err error) {
	schemaHandle := SchemaHandleT{
		warehouse:    job.warehouse,
		stagingFiles: job.stagingFiles,
		dbHandle:     job.dbHandle,
	}
	job.schemaHandle = &schemaHandle
	schemaHandle.localSchema = schemaHandle.getLocalSchema()
	schemaHandle.schemaInWarehouse, err = schemaHandle.fetchSchemaFromWarehouse(job.whManager)
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

func (job *UploadJobT) getTotalRowsInStagingFiles() int64 {
	var total sql.NullInt64
	sqlStatement := fmt.Sprintf(`SELECT sum(total_events)
                                FROM %[1]s
								WHERE %[1]s.id >= %[2]v AND %[1]s.id <= %[3]v AND %[1]s.source_id='%[4]s' AND %[1]s.destination_id='%[5]s'`,
		warehouseutils.WarehouseStagingFilesTable, job.upload.StartStagingFileID, job.upload.EndStagingFileID, job.warehouse.Source.ID, job.warehouse.Destination.ID)
	err := dbHandle.QueryRow(sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`Error in getTotalRowsInStagingFiles: %v`, err)
	}
	return total.Int64
}

func (job *UploadJobT) getTotalRowsInLoadFiles() int64 {
	var total sql.NullInt64

	sqlStatement := fmt.Sprintf(`
		WITH row_numbered_load_files as (
			SELECT
				total_events,
				table_name,
				row_number() OVER (PARTITION BY staging_file_id, table_name ORDER BY id DESC) AS row_number
				FROM %[1]s
				WHERE staging_file_id IN (%[2]v)
		)
		SELECT SUM(total_events)
			FROM row_numbered_load_files
			WHERE
				row_number=1 AND table_name != '%[3]s'`,
		warehouseutils.WarehouseLoadFilesTable, misc.IntArrayToString(job.stagingFileIDs, ","), warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.DiscardsTable))
	err := dbHandle.QueryRow(sqlStatement).Scan(&total)
	if err != nil {
		pkgLogger.Errorf(`Error in getTotalRowsInLoadFiles: %v`, err)
	}
	return total.Int64
}

func (job *UploadJobT) matchRowsInStagingAndLoadFiles() {
	rowsInStagingFiles := job.getTotalRowsInStagingFiles()
	rowsInLoadFiles := job.getTotalRowsInLoadFiles()
	if (rowsInStagingFiles != rowsInLoadFiles) || rowsInStagingFiles == 0 || rowsInLoadFiles == 0 {
		pkgLogger.Errorf(`Error: Rows count mismatch between staging and load files for upload:%d. rowsInStagingFiles: %d, rowsInLoadFiles: %d`, job.upload.ID, rowsInStagingFiles, rowsInLoadFiles)
		job.guageStat("warehouse_staging_load_file_events_count_mismatched").Gauge(rowsInStagingFiles - rowsInLoadFiles)
	}
}

func (job *UploadJobT) run() (err error) {
	timerStat := job.timerStat("upload_time")
	timerStat.Start()
	ch := job.trackLongRunningUpload()
	defer func() {
		job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumnT{{Column: UploadInProgress, Value: false}}})

		timerStat.End()
		ch <- struct{}{}
	}()

	// set last_exec_at to record last upload start time
	// sync scheduling with syncStartAt depends on this determine to start upload or not
	// job.setUploadColumns(
	// 	UploadColumnT{Column: UploadLastExecAtField, Value: timeutil.Now()},
	// )
	job.uploadLock.Lock()
	defer job.uploadLock.Unlock()
	job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumnT{{Column: UploadLastExecAtField, Value: timeutil.Now()}, {Column: UploadInProgress, Value: true}}})

	if len(job.stagingFiles) == 0 {
		err := fmt.Errorf("No staging files found")
		job.setUploadError(err, InternalProcessingFailed)
		return err
	}

	whManager := job.whManager
	err = whManager.Setup(job.warehouse, job)
	if err != nil {
		job.setUploadError(err, InternalProcessingFailed)
		return err
	}
	defer whManager.Cleanup()

	hasSchemaChanged, err := job.syncRemoteSchema()
	if err != nil {
		job.setUploadError(err, FetchingRemoteSchemaFailed)
		return err
	}
	if hasSchemaChanged {
		pkgLogger.Infof("[WH] Remote schema changed for Warehouse: %s", job.warehouse.Identifier)
	}
	schemaHandle := job.schemaHandle
	schemaHandle.uploadSchema = job.upload.UploadSchema

	userTables := []string{job.identifiesTableName(), job.usersTableName()}
	identityTables := []string{job.identityMergeRulesTableName(), job.identityMappingsTableName()}

	var newStatus string
	var nextUploadState *uploadStateT
	// do not set nextUploadState if hasSchemaChanged to make it start from 1st step again
	if !hasSchemaChanged {
		nextUploadState = getNextUploadState(job.upload.Status)
	}
	if nextUploadState == nil {
		nextUploadState = stateTransitions[GeneratedUploadSchema]
	}

	for {
		stateStartTime := time.Now()
		err = nil

		job.setUploadStatus(UploadStatusOpts{Status: nextUploadState.inProgress})
		pkgLogger.Debugf("[WH] Upload: %d, Current state: %s", job.upload.ID, nextUploadState.inProgress)

		targetStatus := nextUploadState.completed

		switch targetStatus {
		case GeneratedUploadSchema:
			newStatus = nextUploadState.failed
			err = job.generateUploadSchema(schemaHandle)
			if err != nil {
				break
			}
			newStatus = nextUploadState.completed

		case CreatedTableUploads:
			newStatus = nextUploadState.failed
			err = job.initTableUploads()
			if err != nil {
				break
			}
			newStatus = nextUploadState.completed

		case GeneratedLoadFiles:
			newStatus = nextUploadState.failed
			// generate load files for all staging files(including succeeded) if hasSchemaChanged or if its snowflake(to have all load files in same folder in bucket) or set via toml/env
			generateAll := hasSchemaChanged || misc.ContainsString(warehousesToAlwaysRegenerateAllLoadFilesOnResume, job.warehouse.Type) || config.GetBool("Warehouse.alwaysRegenerateAllLoadFiles", true)
			var startLoadFileID, endLoadFileID int64
			startLoadFileID, endLoadFileID, err = job.createLoadFiles(generateAll)
			if err != nil {
				job.setStagingFilesStatus(job.stagingFiles, warehouseutils.StagingFileFailedState)
				break
			}

			err = job.setLoadFileIDs(startLoadFileID, endLoadFileID)
			if err != nil {
				break
			}

			job.matchRowsInStagingAndLoadFiles()
			job.recordLoadFileGenerationTimeStat(startLoadFileID, endLoadFileID)

			newStatus = nextUploadState.completed

		case UpdatedTableUploadsCounts:
			newStatus = nextUploadState.failed
			for tableName := range job.upload.UploadSchema {
				tableUpload := NewTableUpload(job.upload.ID, tableName)
				err = tableUpload.updateTableEventsCount(job)
				if err != nil {
					break
				}
			}
			if err != nil {
				break
			}
			newStatus = nextUploadState.completed

		case CreatedRemoteSchema:
			newStatus = nextUploadState.failed
			if len(schemaHandle.schemaInWarehouse) == 0 {
				err = whManager.CreateSchema()
				if err != nil {
					break
				}
			}
			newStatus = nextUploadState.completed

		case ExportedData:
			newStatus = nextUploadState.failed
			_, currentJobSucceededTables := job.getTablesToSkip()

			var loadErrors []error
			var loadErrorLock sync.Mutex
			var loadFilesTableMap map[tableNameT]bool

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
			if len(loadErrors) > 0 {
				err = misc.ConcatErrors(loadErrors)
				break
			}
			job.generateUploadSuccessMetrics()

			newStatus = nextUploadState.completed

		default:
			// If unknown state, start again
			newStatus = Waiting
		}

		if err != nil {
			pkgLogger.Errorf("[WH] Upload: %d, TargetState: %s, NewState: %s, Error: %v", job.upload.ID, targetStatus, newStatus, err.Error())
			state, err := job.setUploadError(err, newStatus)
			if err == nil && state == Aborted {
				job.generateUploadAbortedMetrics()
			}
			break
		}

		pkgLogger.Debugf("[WH] Upload: %d, Next state: %s", job.upload.ID, newStatus)

		uploadStatusOpts := UploadStatusOpts{Status: newStatus}
		if newStatus == ExportedData {
			reportingMetric := types.PUReportedMetric{
				ConnectionDetails: types.ConnectionDetails{
					SourceID:        job.upload.SourceID,
					DestinationID:   job.upload.DestinationID,
					SourceBatchID:   job.upload.SourceBatchID,
					SourceTaskID:    job.upload.SourceTaskID,
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
					Count:       job.getTotalRowsInStagingFiles(),
					SampleEvent: []byte("{}"),
				},
			}
			uploadStatusOpts.ReportingMetric = reportingMetric
		}
		job.setUploadStatus(uploadStatusOpts)

		// record metric for time taken by the current state
		job.timerStat(nextUploadState.inProgress).SendTiming(time.Since(stateStartTime))

		if newStatus == ExportedData {
			break
		}

		nextUploadState = getNextUploadState(newStatus)
	}

	if newStatus != ExportedData {
		return fmt.Errorf("Upload Job failed: %w", err)
	}

	return nil
}

func (job *UploadJobT) exportUserTables(loadFilesTableMap map[tableNameT]bool) (err error) {
	uploadSchema := job.upload.UploadSchema
	if _, ok := uploadSchema[job.identifiesTableName()]; ok {

		loadTimeStat := job.timerStat("user_tables_load_time")
		loadTimeStat.Start()
		defer loadTimeStat.End()
		var loadErrors []error
		loadErrors, err = job.loadUserTables(loadFilesTableMap)
		if err != nil {
			return
		}
		job.hasAllTablesSkipped = areAllTableSkipErrors(loadErrors)

		if len(loadErrors) > 0 {
			err = misc.ConcatErrors(loadErrors)
			return
		}
	}
	return
}

func (job *UploadJobT) exportIdentities() (err error) {
	// Load Identitties if enabled
	uploadSchema := job.upload.UploadSchema
	if warehouseutils.IDResolutionEnabled() && misc.ContainsString(warehouseutils.IdentityEnabledWarehouses, job.warehouse.Type) {
		if _, ok := uploadSchema[job.identityMergeRulesTableName()]; ok {
			loadTimeStat := job.timerStat("identity_tables_load_time")
			loadTimeStat.Start()
			defer loadTimeStat.End()

			var loadErrors []error
			loadErrors, err = job.loadIdentityTables(false)
			if err != nil {
				return
			}
			job.hasAllTablesSkipped = areAllTableSkipErrors(loadErrors)

			if len(loadErrors) > 0 {
				err = misc.ConcatErrors(loadErrors)
				return
			}
		}
	}
	return
}

func (job *UploadJobT) exportRegularTables(specialTables []string, loadFilesTableMap map[tableNameT]bool) (err error) {
	//[]string{job.identifiesTableName(), job.usersTableName(), job.identityMergeRulesTableName(), job.identityMappingsTableName()}
	// Export all other tables
	loadTimeStat := job.timerStat("other_tables_load_time")
	loadTimeStat.Start()
	defer loadTimeStat.End()

	loadErrors := job.loadAllTablesExcept(specialTables, loadFilesTableMap)
	job.hasAllTablesSkipped = areAllTableSkipErrors(loadErrors)

	if len(loadErrors) > 0 {
		err = misc.ConcatErrors(loadErrors)
		return
	}

	return
}

func areAllTableSkipErrors(loadErrors []error) bool {
	res := true
	for _, lErr := range loadErrors {
		if _, ok := lErr.(*TableSkipError); !ok {
			res = false
			break
		}
	}
	return res
}

// TableUploadStatusT captures the status of each table upload along with its parent upload_job's info like destionation_id and namespace
type TableUploadStatusT struct {
	uploadID      int64
	destinationID string
	namespace     string
	tableName     string
	status        string
	error         string
}

// TableUploadStatusInfoT captures the status and error for [uploadID][tableName]
type TableUploadStatusInfoT struct {
	status string
	error  string
}

// TableUploadIDInfoT captures the uploadID and error for [uploadID][tableName]
type TableUploadIDInfoT struct {
	uploadID int64
	error    string
}

func (job *UploadJobT) fetchPendingUploadTableStatus() []*TableUploadStatusT {
	if job.tableUploadStatuses != nil {
		return job.tableUploadStatuses
	}
	sqlStatement := fmt.Sprintf(`
		SELECT
			%[1]s.id,
			%[1]s.destination_id,
			%[1]s.namespace,
			%[2]s.table_name,
			%[2]s.status,
			%[2]s.error
		FROM
			%[1]s INNER JOIN %[2]s
		ON
			%[1]s.id = %[2]s.wh_upload_id
		WHERE
			%[1]s.id <= '%[3]d'
			AND %[1]s.destination_id = '%[4]s'
			AND %[1]s.namespace = '%[5]s'
			AND %[1]s.status != '%[6]s'
			AND %[1]s.status != '%[7]s'
			AND %[2]s.table_name in (SELECT table_name FROM %[2]s WHERE %[2]s.wh_upload_id = '%[3]d')
		ORDER BY
			%[1]s.id ASC`,
		warehouseutils.WarehouseUploadsTable,
		warehouseutils.WarehouseTableUploadsTable,
		job.upload.ID,
		job.upload.DestinationID,
		job.upload.Namespace,
		ExportedData,
		Aborted)
	rows, err := job.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	defer rows.Close()

	tableUploadStatuses := make([]*TableUploadStatusT, 0)

	for rows.Next() {
		var tableUploadStatus TableUploadStatusT
		err := rows.Scan(
			&tableUploadStatus.uploadID,
			&tableUploadStatus.destinationID,
			&tableUploadStatus.namespace,
			&tableUploadStatus.tableName,
			&tableUploadStatus.status,
			&tableUploadStatus.error,
		)
		if err != nil {
			panic(err)
		}
		tableUploadStatuses = append(tableUploadStatuses, &tableUploadStatus)
	}
	job.tableUploadStatuses = tableUploadStatuses
	return tableUploadStatuses
}

func getTableUploadStatusMap(tableUploadStatuses []*TableUploadStatusT) map[int64]map[string]*TableUploadStatusInfoT {
	tableUploadStatus := make(map[int64]map[string]*TableUploadStatusInfoT)
	for _, tUploadStatus := range tableUploadStatuses {
		if _, ok := tableUploadStatus[tUploadStatus.uploadID]; !ok {
			tableUploadStatus[tUploadStatus.uploadID] = make(map[string]*TableUploadStatusInfoT)
		}
		tableUploadStatus[tUploadStatus.uploadID][tUploadStatus.tableName] = &TableUploadStatusInfoT{
			status: tUploadStatus.status,
			error:  tUploadStatus.error,
		}
	}
	return tableUploadStatus
}

func (job *UploadJobT) getTablesToSkip() (map[string]*TableUploadIDInfoT, map[string]bool) {
	tableUploadStatuses := job.fetchPendingUploadTableStatus()
	tableUploadStatus := getTableUploadStatusMap(tableUploadStatuses)
	previouslyFailedTableMap := make(map[string]*TableUploadIDInfoT)
	currentlySucceededTableMap := make(map[string]bool)
	for uploadID, tableStatusMap := range tableUploadStatus {
		for tableName, tableStatus := range tableStatusMap {
			status := tableStatus.status
			if uploadID < job.upload.ID && (status == TableUploadExportingFailed ||
				status == UserTableUploadExportingFailed ||
				status == IdentityTableUploadExportingFailed) { // Previous upload and table upload failed
				previouslyFailedTableMap[tableName] = &TableUploadIDInfoT{
					uploadID: uploadID,
					error:    tableStatus.error,
				}
			}
			if uploadID == job.upload.ID && status == TableUploadExported { // Current upload and table upload succeeded
				currentlySucceededTableMap[tableName] = true
			}
		}
	}
	return previouslyFailedTableMap, currentlySucceededTableMap
}

func (job *UploadJobT) resolveIdentities(populateHistoricIdentities bool) (err error) {
	idr := identity.HandleT{
		Warehouse:        job.warehouse,
		DbHandle:         job.dbHandle,
		UploadID:         job.upload.ID,
		Uploader:         job,
		WarehouseManager: job.whManager,
	}
	if populateHistoricIdentities {
		return idr.ResolveHistoricIdentities()
	}
	return idr.Resolve()
}

func (job *UploadJobT) updateTableSchema(tName string, tableSchemaDiff warehouseutils.TableSchemaDiffT) (err error) {
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

	for columnName, columnType := range tableSchemaDiff.ColumnMap {
		err = job.whManager.AddColumn(tName, columnName, columnType)
		if err != nil {
			pkgLogger.Errorf("Column %s already exists on %s.%s \nResponse: %v", columnName, job.warehouse.Namespace, tName, err)
			break
		}
		job.counterStat("columns_added").Increment()
	}

	if err != nil {
		return err
	}

	for _, columnName := range tableSchemaDiff.StringColumnsToBeAlteredToText {
		err = job.whManager.AlterColumn(tName, columnName, "text")
		if err != nil {
			pkgLogger.Errorf("Altering column %s in table: %s.%s failed. Error: %v", columnName, job.warehouse.Namespace, tName, err)
			break
		}
	}

	return err
}

// TableSkipError is a custom error type to capture if a table load is skipped because of a previously failed table load
type TableSkipError struct {
	tableName        string
	previousJobID    int64
	previousJobError string
}

func (tse *TableSkipError) Error() string {
	return fmt.Sprintf("Skipping %s table because it previously failed to load in an earlier job: %d with error: %s", tse.tableName, tse.previousJobID, tse.previousJobError)
}

func (job *UploadJobT) loadAllTablesExcept(skipLoadForTables []string, loadFilesTableMap map[tableNameT]bool) []error {
	uploadSchema := job.upload.UploadSchema
	var parallelLoads int
	var ok bool
	if parallelLoads, ok = maxParallelLoads[job.warehouse.Type]; !ok {
		parallelLoads = 1
	}
	pkgLogger.Infof(`[WH]: Running %d parallel loads in namespace %s of destination %s:%s`, parallelLoads, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)

	var loadErrors []error
	var loadErrorLock sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(uploadSchema))

	var alteredSchemaInAtleastOneTable bool
	loadChan := make(chan struct{}, parallelLoads)
	previouslyFailedTables, currentJobSucceededTables := job.getTablesToSkip()
	for tableName := range uploadSchema {
		if misc.ContainsString(skipLoadForTables, tableName) {
			wg.Done()
			continue
		}
		if _, ok := currentJobSucceededTables[tableName]; ok {
			wg.Done()
			continue
		}
		if prevJobStatus, ok := previouslyFailedTables[tableName]; ok {
			loadErrors = append(loadErrors, &TableSkipError{tableName: tableName, previousJobID: prevJobStatus.uploadID, previousJobError: prevJobStatus.error})
			wg.Done()
			continue
		}
		hasLoadFiles := loadFilesTableMap[tableNameT(tableName)]
		if !hasLoadFiles {
			wg.Done()
			if misc.ContainsString(alwaysMarkExported, strings.ToLower(tableName)) {
				tableUpload := NewTableUpload(job.upload.ID, tableName)
				tableUpload.setStatus(TableUploadExported)
			}
			continue
		}
		tName := tableName
		loadChan <- struct{}{}
		rruntime.GoForWarehouse(func() {
			alteredSchema, err := job.loadTable(tName)
			if alteredSchema {
				alteredSchemaInAtleastOneTable = true
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

	if alteredSchemaInAtleastOneTable {
		pkgLogger.Infof("loadAllTablesExcept: schema changed - updating local schema for %s", job.warehouse.Identifier)
		job.schemaHandle.updateLocalSchema(job.schemaHandle.schemaInWarehouse)
	}

	return loadErrors
}

func (job *UploadJobT) updateSchema(tName string) (alteredSchema bool, err error) {
	tableSchemaDiff := getTableSchemaDiff(tName, job.schemaHandle.schemaInWarehouse, job.upload.UploadSchema)
	if tableSchemaDiff.Exists {
		err = job.updateTableSchema(tName, tableSchemaDiff)
		if err != nil {
			return
		}

		job.setUpdatedTableSchema(tName, tableSchemaDiff.UpdatedSchema)
		alteredSchema = true
	}
	return
}

func (job *UploadJobT) getTotalCount(tName string) (int64, error) {
	var total int64
	operation := func() error {
		var countErr error
		total, countErr = job.whManager.GetTotalCountInTable(tName)
		return countErr
	}
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 5 * time.Second
	expBackoff.RandomizationFactor = 0
	expBackoff.Reset()
	backoffWithMaxRetry := backoff.WithMaxRetries(expBackoff, 5)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf(`Error getting total count in table:%s error: %v`, tName, err)
	})
	return total, err
}

func (job *UploadJobT) loadTable(tName string) (alteredSchema bool, err error) {
	tableUpload := NewTableUpload(job.upload.ID, tName)
	alteredSchema, err = job.updateSchema(tName)
	if err != nil {
		tableUpload.setError(TableUploadUpdatingSchemaFailed, err)
		return
	}

	pkgLogger.Infof(`[WH]: Starting load for table %s in namespace %s of destination %s:%s`, tName, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)
	tableUpload.setStatus(TableUploadExecuting)

	generateTableLoadCountVerificationsMetrics := config.GetBool("Warehouse.generateTableLoadCountMetrics", true)
	var totalBeforeLoad, totalAfterLoad int64
	if generateTableLoadCountVerificationsMetrics {
		var errTotalCount error
		totalBeforeLoad, errTotalCount = job.getTotalCount(tName)
		if errTotalCount != nil {
			pkgLogger.Errorf(`Error getting total count in table:%s before load: %v`, tName, errTotalCount)
		}
	}

	err = job.whManager.LoadTable(tName)
	if err != nil {
		tableUpload.setError(TableUploadExportingFailed, err)
		return
	}

	generateTableLoadMetrics := func() {
		if !generateTableLoadCountVerificationsMetrics {
			return
		}
		var errTotalCount error
		totalAfterLoad, errTotalCount = job.getTotalCount(tName)
		if errTotalCount != nil {
			pkgLogger.Errorf(`Error getting total count in table:%s after load: %v`, tName, errTotalCount)
			return
		}
		eventsInTableUpload, errEventCount := tableUpload.getTotalEvents()
		if errEventCount != nil {
			return
		}
		job.guageStat(`pre_load_table_rows`, tag{name: "tableName", value: strings.ToLower(tName)}).Gauge(int(totalBeforeLoad))
		job.guageStat(`post_load_table_rows_estimate`, tag{name: "tableName", value: strings.ToLower(tName)}).Gauge(int(totalBeforeLoad + eventsInTableUpload))
		job.guageStat(`post_load_table_rows`, tag{name: "tableName", value: strings.ToLower(tName)}).Gauge(int(totalAfterLoad))
	}

	generateTableLoadMetrics()

	tableUpload.setStatus(TableUploadExported)
	numEvents, queryErr := tableUpload.getNumEvents()
	if queryErr == nil {
		job.recordTableLoad(tName, numEvents)
	}

	if columnThreshold, ok := columnCountThresholds[job.warehouse.Type]; ok {
		columnCount := len(job.schemaHandle.schemaInWarehouse[tName])
		if columnCount > columnThreshold {
			job.counterStat(`warehouse_load_table_column_count`, tag{name: "tableName", value: strings.ToLower(tName)}).Count(columnCount)
		}
	}
	return
}

func (job *UploadJobT) loadUserTables(loadFilesTableMap map[tableNameT]bool) ([]error, error) {
	var hasLoadFiles bool
	userTables := []string{job.identifiesTableName(), job.usersTableName()}

	var err error
	previouslyFailedTables, currentJobSucceededTables := job.getTablesToSkip()
	for _, tName := range userTables {
		if prevJobStatus, ok := previouslyFailedTables[tName]; ok {
			err = &TableSkipError{tableName: tName, previousJobID: prevJobStatus.uploadID, previousJobError: prevJobStatus.error}
			return []error{err}, nil
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
	if err != nil {
		return []error{err}, nil
	}

	if !hasLoadFiles {
		return []error{}, nil
	}

	loadTimeStat := job.timerStat("user_tables_load_time")
	loadTimeStat.Start()

	// Load all user tables
	identityTableUpload := NewTableUpload(job.upload.ID, job.identifiesTableName())
	identityTableUpload.setStatus(TableUploadExecuting)
	alteredIdentitySchema, err := job.updateSchema(job.identifiesTableName())
	if err != nil {
		identityTableUpload.setError(TableUploadUpdatingSchemaFailed, err)
		return job.processLoadTableResponse(map[string]error{job.identifiesTableName(): err})
	}
	var alteredUserSchema bool
	if _, ok := job.upload.UploadSchema[job.usersTableName()]; ok {
		userTableUpload := NewTableUpload(job.upload.ID, job.usersTableName())
		userTableUpload.setStatus(TableUploadExecuting)
		alteredUserSchema, err = job.updateSchema(job.usersTableName())
		if err != nil {
			userTableUpload.setError(TableUploadUpdatingSchemaFailed, err)
			return job.processLoadTableResponse(map[string]error{job.usersTableName(): err})
		}
	}
	errorMap := job.whManager.LoadUserTables()

	if alteredIdentitySchema || alteredUserSchema {
		pkgLogger.Infof("loadUserTables: schema changed - updating local schema for %s", job.warehouse.Identifier)
		job.schemaHandle.updateLocalSchema(job.schemaHandle.schemaInWarehouse)
	}
	return job.processLoadTableResponse(errorMap)
}

func (job *UploadJobT) loadIdentityTables(populateHistoricIdentities bool) (loadErrors []error, tableUploadErr error) {
	pkgLogger.Infof(`[WH]: Starting load for identity tables in namespace %s of destination %s:%s`, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)
	identityTables := []string{job.identityMergeRulesTableName(), job.identityMappingsTableName()}
	previouslyFailedTables, currentJobSucceededTables := job.getTablesToSkip()
	for _, tableName := range identityTables {
		if prevJobStatus, ok := previouslyFailedTables[tableName]; ok {
			return []error{&TableSkipError{tableName: tableName, previousJobID: prevJobStatus.uploadID, previousJobError: prevJobStatus.error}}, nil
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
		tableUpload := NewTableUpload(job.upload.ID, tableName)

		tableSchemaDiff := getTableSchemaDiff(tableName, job.schemaHandle.schemaInWarehouse, job.upload.UploadSchema)
		if tableSchemaDiff.Exists {
			err := job.updateTableSchema(tableName, tableSchemaDiff)
			if err != nil {
				tableUpload.setError(TableUploadUpdatingSchemaFailed, err)
				errorMap := map[string]error{tableName: err}
				return job.processLoadTableResponse(errorMap)
			}
			job.setUpdatedTableSchema(tableName, tableSchemaDiff.UpdatedSchema)
			tableUpload.setStatus(TableUploadUpdatedSchema)
			alteredSchema = true
		}
		err := tableUpload.setStatus(TableUploadExecuting)
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
		job.schemaHandle.updateLocalSchema(job.schemaHandle.schemaInWarehouse)
	}

	return job.processLoadTableResponse(errorMap)
}

func (job *UploadJobT) setUpdatedTableSchema(tableName string, updatedSchema map[string]string) {
	job.schemaLock.Lock()
	job.schemaHandle.schemaInWarehouse[tableName] = updatedSchema
	job.schemaLock.Unlock()
}

func (job *UploadJobT) processLoadTableResponse(errorMap map[string]error) (errors []error, tableUploadErr error) {
	for tName, loadErr := range errorMap {
		// TODO: set last_exec_time
		tableUpload := NewTableUpload(job.upload.ID, tName)
		if loadErr != nil {
			errors = append(errors, loadErr)
			tableUploadErr = tableUpload.setError(TableUploadExportingFailed, loadErr)
		} else {
			tableUploadErr = tableUpload.setStatus(TableUploadExported)
			if tableUploadErr == nil {
				// Since load is successful, we assume all events in load files are uploaded
				numEvents, queryErr := tableUpload.getNumEvents()
				if queryErr == nil {
					job.recordTableLoad(tName, numEvents)
				}
			}
		}

		if tableUploadErr != nil {
			break
		}

	}
	return errors, tableUploadErr
}

// getUploadTimings returns timings json column
// eg. timings: [{exporting_data: 2020-04-21 15:16:19.687716, exported_data: 2020-04-21 15:26:34.344356}]
func (job *UploadJobT) getUploadTimings() (timings []map[string]string) {
	var rawJSON json.RawMessage
	sqlStatement := fmt.Sprintf(`SELECT timings FROM %s WHERE id=%d`, warehouseutils.WarehouseUploadsTable, job.upload.ID)
	err := job.dbHandle.QueryRow(sqlStatement).Scan(&rawJSON)
	if err != nil {
		return
	}
	json.Unmarshal(rawJSON, &timings)
	return
}

// getNewTimings appends current status with current time to timings column
// eg. status: exported_data, timings: [{exporting_data: 2020-04-21 15:16:19.687716] -> [{exporting_data: 2020-04-21 15:16:19.687716, exported_data: 2020-04-21 15:26:34.344356}]
func (job *UploadJobT) getNewTimings(status string) ([]byte, []map[string]string) {
	timings := job.getUploadTimings()
	timing := map[string]string{status: timeutil.Now().Format(misc.RFC3339Milli)}
	timings = append(timings, timing)
	marshalledTimings, err := json.Marshal(timings)
	if err != nil {
		panic(err)
	}
	return marshalledTimings, timings
}

func (job *UploadJobT) getUploadFirstAttemptTime() (timing time.Time) {
	var firstTiming sql.NullString
	sqlStatement := fmt.Sprintf(`SELECT timings->0 as firstTimingObj FROM %s WHERE id=%d`, warehouseutils.WarehouseUploadsTable, job.upload.ID)
	err := job.dbHandle.QueryRow(sqlStatement).Scan(&firstTiming)
	if err != nil {
		return
	}
	_, timing = warehouseutils.TimingFromJSONString(firstTiming)
	return timing
}

type UploadStatusOpts struct {
	Status           string
	AdditionalFields []UploadColumnT
	ReportingMetric  types.PUReportedMetric
}

func (job *UploadJobT) setUploadStatus(statusOpts UploadStatusOpts) (err error) {
	pkgLogger.Debugf("[WH]: Setting status of %s for wh_upload:%v", statusOpts.Status, job.upload.ID)
	marshalledTimings, timings := job.getNewTimings(statusOpts.Status)
	opts := []UploadColumnT{
		{Column: UploadStatusField, Value: statusOpts.Status},
		{Column: UploadTimingsField, Value: marshalledTimings},
		{Column: UploadUpdatedAtField, Value: timeutil.Now()},
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

		if config.GetBool("Reporting.enabled", types.DEFAULT_REPORTING_ENABLED) {
			application.Features().Reporting.GetReportingInstance().Report([]*types.PUReportedMetric{&statusOpts.ReportingMetric}, txn)
		}
		err = txn.Commit()
		return err
	}
	return job.setUploadColumns(uploadColumnOpts)
	// return job.setUploadColumns(
	// 	additionalFields...,
	// )
}

// SetUploadSchema
func (job *UploadJobT) setUploadSchema(consolidatedSchema warehouseutils.SchemaT) error {
	marshalledSchema, err := json.Marshal(consolidatedSchema)
	if err != nil {
		panic(err)
	}
	job.upload.UploadSchema = consolidatedSchema
	// return job.setUploadColumns(
	// 	UploadColumnT{Column: UploadSchemaField, Value: marshalledSchema},
	// )
	return job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumnT{{Column: UploadSchemaField, Value: marshalledSchema}}})
}

func (job *UploadJobT) setMergedSchema(mergedSchema warehouseutils.SchemaT) error {
	marshalledSchema, err := json.Marshal(mergedSchema)
	if err != nil {
		panic(err)
	}
	job.upload.MergedSchema = mergedSchema
	return job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumnT{{Column: MergedSchemaField, Value: marshalledSchema}}})
}

// Set LoadFileIDs
func (job *UploadJobT) setLoadFileIDs(startLoadFileID, endLoadFileID int64) error {
	job.upload.StartLoadFileID = startLoadFileID
	job.upload.EndLoadFileID = endLoadFileID

	// return job.setUploadColumns(
	// 	UploadColumnT{Column: UploadStartLoadFileIDField, Value: startLoadFileID},
	// 	UploadColumnT{Column: UploadEndLoadFileIDField, Value: endLoadFileID},
	// )
	return job.setUploadColumns(UploadColumnsOpts{Fields: []UploadColumnT{{Column: UploadStartLoadFileIDField, Value: startLoadFileID}, {Column: UploadEndLoadFileIDField, Value: endLoadFileID}}})
}

type UploadColumnsOpts struct {
	Fields []UploadColumnT
	Txn    *sql.Tx
}

// SetUploadColumns sets any column values passed as args in UploadColumnT format for WarehouseUploadsTable
func (job *UploadJobT) setUploadColumns(opts UploadColumnsOpts) (err error) {
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
	sqlStatement := fmt.Sprintf(`UPDATE %s SET %s WHERE id=$1`, warehouseutils.WarehouseUploadsTable, columns)
	if opts.Txn != nil {
		_, err = opts.Txn.Exec(sqlStatement, values...)
	} else {
		_, err = dbHandle.Exec(sqlStatement, values...)
	}

	return err
}

func (job *UploadJobT) triggerUploadNow() (err error) {
	job.uploadLock.Lock()
	defer job.uploadLock.Unlock()
	upload := job.upload
	newjobState := Waiting
	var metadata map[string]interface{}
	unmarshallErr := json.Unmarshal(upload.Metadata, &metadata)
	if unmarshallErr != nil {
		metadata = make(map[string]interface{})
	}
	metadata["nextRetryTime"] = time.Now().Add(-time.Hour * 1).Format(time.RFC3339)
	metadata["retried"] = true
	metadata["priority"] = 50
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	uploadColumns := []UploadColumnT{
		{Column: "status", Value: newjobState},
		{Column: "metadata", Value: metadataJSON},
		{Column: "updated_at", Value: timeutil.Now()},
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

	job.upload.Status = newjobState
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

// Aborted makes a check that if the state of the job
// should be aborted
func (job *UploadJobT) Aborted(attempts int, startTime time.Time) bool {
	// Defensive check to prevent garbage startTime
	if startTime.IsZero() {
		return false
	}

	// Do not mark aborted as tables skipped.
	if job.hasAllTablesSkipped {
		return false
	}
	return attempts > minRetryAttempts && timeutil.Now().Sub(startTime) > retryTimeWindow
}

func (job *UploadJobT) setUploadError(statusError error, state string) (string, error) {
	pkgLogger.Errorf("[WH]: Failed during %s stage: %v\n", state, statusError.Error())

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
		state = Aborted
	}

	var metadata map[string]interface{}
	unmarshallErr := json.Unmarshal(upload.Metadata, &metadata)
	if unmarshallErr != nil {
		metadata = make(map[string]interface{})
	}
	metadata["nextRetryTime"] = timeutil.Now().Add(durationBeforeNextAttempt(upload.Attempts + 1)).Format(time.RFC3339)
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		metadataJSON = []byte("{}")
	}

	serializedErr, _ := json.Marshal(&uploadErrors)
	// sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, metadata=$3, updated_at=$4 WHERE id=$5`, warehouseutils.WarehouseUploadsTable)

	uploadColumns := []UploadColumnT{
		{Column: "status", Value: state},
		{Column: "metadata", Value: metadataJSON},
		{Column: "error", Value: serializedErr},
		{Column: "updated_at", Value: timeutil.Now()},
	}

	txn, err := job.dbHandle.Begin()
	if err != nil {
		return "", fmt.Errorf("unable to start transaction: %w", err)
	}

	err = job.setUploadColumns(UploadColumnsOpts{Fields: uploadColumns, Txn: txn})
	if err != nil {
		return "", fmt.Errorf("unable to change upload columns: %w", err)
	}

	inputCount := job.getTotalRowsInStagingFiles()
	outputCount, _ := job.getTotalEventsUploaded(false)
	failCount := inputCount - outputCount
	reportingStatus := jobsdb.Failed.State
	if state == Aborted {
		reportingStatus = jobsdb.Aborted.State
	}

	reportingMetrics := []*types.PUReportedMetric{{
		ConnectionDetails: types.ConnectionDetails{
			SourceID:        job.upload.SourceID,
			DestinationID:   job.upload.DestinationID,
			SourceBatchID:   job.upload.SourceBatchID,
			SourceTaskID:    job.upload.SourceTaskID,
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
				SourceBatchID:   job.upload.SourceBatchID,
				SourceTaskID:    job.upload.SourceTaskID,
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
	if config.GetBool("Reporting.enabled", types.DEFAULT_REPORTING_ENABLED) {
		application.Features().Reporting.GetReportingInstance().Report(reportingMetrics, txn)
	}
	err = txn.Commit()

	// TODO: Add reporting metrics in txn
	// _, err = job.dbHandle.Exec(sqlStatement, state, serializedErr, metadataJSON, timeutil.Now(), upload.ID)

	job.upload.Status = state
	job.upload.Error = serializedErr

	attempts := job.getAttemptNumber()

	if !job.hasAllTablesSkipped {
		job.counterStat("warehouse_failed_uploads", tag{name: "attempt_number", value: strconv.Itoa(attempts)}).Count(1)
	}

	// On aborted state, validate credentials to allow
	// us to differentiate between user caused abort vs platform issue.
	if state == Aborted {
		// base tag to be sent as stat

		tags := []tag{{name: "attempt_number", value: strconv.Itoa(attempts)}}
		valid, err := job.validateDestinationCredentials()
		if err == nil {
			tags = append(tags, tag{name: "destination_creds_valid", value: strconv.FormatBool(valid)})
		}

		job.counterStat("upload_aborted", tags...).Count(1)
	}

	return state, err
}

func (job *UploadJobT) validateDestinationCredentials() (bool, error) {
	validationResult, err := job.destinationValidator.ValidateCredentials(&configuration_testing.DestinationValidationRequest{Destination: job.warehouse.Destination})
	if err != nil {
		pkgLogger.Errorf("Unable to successfully validate destination: %s credentials, err: %v", job.warehouse.Destination.ID, err)
		return false, err
	}

	return validationResult.Success, nil
}

func (job *UploadJobT) getAttemptNumber() int {
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

func (job *UploadJobT) setStagingFilesStatus(stagingFiles []*StagingFileT, status string) (err error) {
	var ids []int64
	for _, stagingFile := range stagingFiles {
		ids = append(ids, stagingFile.ID)
	}
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2 WHERE id=ANY($3)`, warehouseutils.WarehouseStagingFilesTable)
	_, err = dbHandle.Exec(sqlStatement, status, timeutil.Now(), pq.Array(ids))
	if err != nil {
		panic(err)
	}
	return
}

func (job *UploadJobT) getLoadFilesTableMap() (loadFilesMap map[tableNameT]bool, err error) {
	loadFilesMap = make(map[tableNameT]bool)

	sourceID := job.warehouse.Source.ID
	destID := job.warehouse.Destination.ID

	sqlStatement := fmt.Sprintf(`SELECT distinct table_name FROM %s WHERE ( source_id = $1 AND destination_id = $2 AND id >= $3 AND id <= $4 );`,
		warehouseutils.WarehouseLoadFilesTable,
	)
	sqlStatementArgs := []interface{}{
		sourceID,
		destID,
		job.upload.StartLoadFileID,
		job.upload.EndLoadFileID,
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
	defer rows.Close()

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

func (job *UploadJobT) destinationRevisionIDMap() (revisionIDMap map[string]backendconfig.DestinationT, err error) {
	revisionIDMap = make(map[string]backendconfig.DestinationT)
	revisionRequest := struct {
		sourceID           string
		destinationID      string
		startStagingFileID int64
		endStagingFileID   int64
	}{
		sourceID:           job.warehouse.Source.ID,
		destinationID:      job.warehouse.Destination.ID,
		startStagingFileID: job.upload.StartStagingFileID,
		endStagingFileID:   job.upload.EndStagingFileID,
	}
	revisionIDs, err := distinctDestinationRevisionIdsFromStagingFiles(context.TODO(), revisionRequest)
	if err != nil {
		return
	}

	var response []byte
	var responseCode int

	for _, revisionID := range revisionIDs {
		// No need to make config backend api call for the current config
		if revisionID == job.warehouse.Destination.RevisionID {
			revisionIDMap[revisionID] = job.warehouse.Destination
			continue
		}

		urlStr := fmt.Sprintf("%s/workspaces/destinationHistory/%s", configBackendURL, revisionID)
		response, err = warehouseutils.GetRequestWithTimeout(context.TODO(), urlStr, time.Second*60)
		if err == nil {
			var destination backendconfig.DestinationT
			err = json.Unmarshal(response, &destination)
			if err != nil {
				err = fmt.Errorf("error occurred while unmarshalling response for Dest revisionID %s with error: %w", revisionID, err)
				return
			}
			revisionIDMap[revisionID] = destination
		} else {
			err = fmt.Errorf("error occurred while getting destination history for revisionID %s, responseCode: %d, error: %w", revisionID, responseCode, err)
			return
		}
	}
	return
}

func (job *UploadJobT) getLoadFileIDRange() (startLoadFileID, endLoadFileID int64, err error) {
	stmt := fmt.Sprintf(`
		SELECT
			MIN(id), MAX(id)
		FROM (
			SELECT
				ROW_NUMBER() OVER (PARTITION BY staging_file_id, table_name ORDER BY id DESC) AS row_number,
				t.id
			FROM
				%s t
			WHERE
				t.staging_file_id = ANY($1)
		) grouped_load_files
		WHERE
			grouped_load_files.row_number = 1;
	`, warehouseutils.WarehouseLoadFilesTable)

	pkgLogger.Debugf(`Querying for load_file_id range for the uploadJob:%d with stagingFileIDs:%v Query:%v`, job.upload.ID, job.stagingFileIDs, stmt)
	var minID, maxID sql.NullInt64
	err = job.dbHandle.QueryRow(stmt, pq.Array(job.stagingFileIDs)).Scan(&minID, &maxID)
	if err != nil {
		return 0, 0, fmt.Errorf("Error while querying for load_file_id range for uploadJob:%d with stagingFileIDs:%v : %w", job.upload.ID, job.stagingFileIDs, err)
	}
	return minID.Int64, maxID.Int64, nil
}

func (job *UploadJobT) deleteLoadFiles(stagingFiles []*StagingFileT) {
	var stagingFileIDs []int64
	for _, stagingFile := range stagingFiles {
		stagingFileIDs = append(stagingFileIDs, stagingFile.ID)
	}

	sqlStatement := fmt.Sprintf(`DELETE FROM %[1]s WHERE staging_file_id IN (%v)`, warehouseutils.WarehouseLoadFilesTable, misc.IntArrayToString(stagingFileIDs, ","))
	pkgLogger.Debugf(`Deleting any load files present for staging files (upload:%d) before generating them for the staging files again. Query: %s`, job.upload.ID, sqlStatement)

	_, err := job.dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf(`Error deleting any load files present for staging files (upload:%d) before generating them for the staging files again, Query: %s`, job.upload.ID, sqlStatement)
	}
}

func (job *UploadJobT) createLoadFiles(generateAll bool) (startLoadFileID, endLoadFileID int64, err error) {
	destID := job.upload.DestinationID
	destType := job.upload.DestinationType
	stagingFiles := job.stagingFiles

	publishBatchSize := config.GetInt("Warehouse.pgNotifierPublishBatchSize", 100)
	pkgLogger.Infof("[WH]: Starting batch processing %v stage files for %s:%s", publishBatchSize, destType, destID)
	uniqueLoadGenID := uuid.Must(uuid.NewV4()).String()
	job.upload.LoadFileGenStartTime = timeutil.Now()

	// Getting distinct destination revision ID from staging files metadata
	destinationRevisionIDMap, err := job.destinationRevisionIDMap()
	if err != nil {
		err = fmt.Errorf("error occurred while populating destination revision ID Map with error: %w", err)
		return
	}

	var wg sync.WaitGroup

	var toProcessStagingFiles []*StagingFileT
	if generateAll {
		toProcessStagingFiles = stagingFiles
	} else {
		// skip processing staging files marked succeeded
		for _, stagingFile := range stagingFiles {
			if stagingFile.Status != warehouseutils.StagingFileSucceededState {
				toProcessStagingFiles = append(toProcessStagingFiles, stagingFile)
			}
		}
	}
	job.deleteLoadFiles(toProcessStagingFiles)

	job.setStagingFilesStatus(toProcessStagingFiles, warehouseutils.StagingFileExecutingState)

	saveLoadFileErrs := []error{}
	var sampleError error
	for i := 0; i < len(toProcessStagingFiles); i += publishBatchSize {
		j := i + publishBatchSize
		if j > len(toProcessStagingFiles) {
			j = len(toProcessStagingFiles)
		}

		// td : add prefix to payload for s3 dest
		var messages []pgnotifier.JobPayload
		for _, stagingFile := range toProcessStagingFiles[i:j] {
			payload := PayloadT{
				UploadID:                     job.upload.ID,
				StagingFileID:                stagingFile.ID,
				StagingFileLocation:          stagingFile.Location,
				LoadFileType:                 job.upload.LoadFileType,
				SourceID:                     job.warehouse.Source.ID,
				SourceName:                   job.warehouse.Source.Name,
				DestinationID:                destID,
				DestinationName:              job.warehouse.Destination.Name,
				DestinationType:              destType,
				DestinationNamespace:         job.warehouse.Namespace,
				DestinationConfig:            job.warehouse.Destination.Config,
				UniqueLoadGenID:              uniqueLoadGenID,
				RudderStoragePrefix:          misc.GetRudderObjectStoragePrefix(),
				UseRudderStorage:             job.upload.UseRudderStorage,
				StagingUseRudderStorage:      stagingFile.UseRudderStorage,
				DestinationRevisionID:        job.warehouse.Destination.RevisionID,
				StagingDestinationRevisionID: stagingFile.DestinationRevisionID,
			}
			if revisionConfig, ok := destinationRevisionIDMap[stagingFile.DestinationRevisionID]; ok {
				payload.StagingDestinationConfig = revisionConfig.Config
			}
			if misc.ContainsString(warehouseutils.TimeWindowDestinations, job.warehouse.Type) {
				payload.LoadFilePrefix = warehouseutils.GetLoadFilePrefix(stagingFile.TimeWindow, job.warehouse)
			}

			payloadJSON, err := json.Marshal(payload)
			if err != nil {
				panic(err)
			}
			messages = append(messages, payloadJSON)
		}

		schema := &job.upload.UploadSchema
		if job.upload.LoadFileType == warehouseutils.LOAD_FILE_TYPE_PARQUET {
			schema = &job.upload.MergedSchema
		}

		pkgLogger.Infof("[WH]: Publishing %d staging files for %s:%s to PgNotifier", len(messages), destType, destID)
		ch, err := job.pgNotifier.Publish(messages, schema, job.upload.Priority)
		if err != nil {
			panic(err)
		}
		// set messages to nil to release mem allocated
		messages = nil
		wg.Add(1)
		batchStartIdx := i
		batchEndIdx := j
		rruntime.GoForWarehouse(func() {
			responses := <-ch
			pkgLogger.Infof("[WH]: Received responses for staging files %d:%d for %s:%s from PgNotifier", toProcessStagingFiles[batchStartIdx].ID, toProcessStagingFiles[batchEndIdx-1].ID, destType, destID)
			var loadFiles []loadFileUploadOutputT
			var successfulStagingFileIDs []int64
			for _, resp := range responses {
				// Error handling during generating_load_files step:
				// 1. any error returned by pgnotifier is set on corresponding staging_gile
				// 2. any error effecting a batch/all of the staging files like saving load file records to wh db
				//    is returned as error to caller of the func to set error on all staging files and the whole generating_load_files step
				if resp.Status == "aborted" {
					pkgLogger.Errorf("[WH]: Error in genrating load files: %v", resp.Error)
					sampleError = fmt.Errorf(resp.Error)
					job.setStagingFileErr(resp.JobID, sampleError)
					continue
				}
				var output []loadFileUploadOutputT
				err = json.Unmarshal(resp.Output, &output)
				if err != nil {
					panic(err)
				}
				if len(output) == 0 {
					pkgLogger.Errorf("[WH]: No LoadFiles returned by wh worker")
					continue
				}
				loadFiles = append(loadFiles, output...)
				successfulStagingFileIDs = append(successfulStagingFileIDs, resp.JobID)
			}
			err = job.bulkInsertLoadFileRecords(loadFiles)
			if err != nil {
				saveLoadFileErrs = append(saveLoadFileErrs, err)
			}
			job.setStagingFileSuccess(successfulStagingFileIDs)
			wg.Done()
		})
	}

	wg.Wait()

	if len(saveLoadFileErrs) > 0 {
		err = misc.ConcatErrors(saveLoadFileErrs)
		pkgLogger.Errorf(`[WH]: Encountered errors in creating load file records in wh_load_files: %v`, err)
		return startLoadFileID, endLoadFileID, err
	}

	startLoadFileID, endLoadFileID, err = job.getLoadFileIDRange()
	if err != nil {
		panic(err)
	}

	if startLoadFileID == 0 || endLoadFileID == 0 {
		err = fmt.Errorf(`No load files generated. Sample error: %v`, sampleError)
		return startLoadFileID, endLoadFileID, err
	}

	// verify if all load files are in same folder in object storage
	if misc.ContainsString(warehousesToVerifyLoadFilesFolder, job.warehouse.Type) {
		locations := job.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT{
			StartID: startLoadFileID,
			EndID:   endLoadFileID,
		})
		for _, location := range locations {
			if !strings.Contains(location.Location, uniqueLoadGenID) {
				err = fmt.Errorf(`All loadfiles do not contain the same uniqueLoadGenID: %s`, uniqueLoadGenID)
				return
			}
		}
	}

	return startLoadFileID, endLoadFileID, nil
}

func (job *UploadJobT) setStagingFileSuccess(stagingFileIDs []int64) {
	// using ANY instead of IN as WHERE clause filtering on primary key index uses index scan in both cases
	// use IN for cases where filtering on composite indexes
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2 WHERE id=ANY($3)`, warehouseutils.WarehouseStagingFilesTable)
	_, err := dbHandle.Exec(sqlStatement, warehouseutils.StagingFileSucceededState, timeutil.Now(), pq.Array(stagingFileIDs))
	if err != nil {
		panic(err)
	}
}

func (job *UploadJobT) setStagingFileErr(stagingFileID int64, statusErr error) {
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=$4`, warehouseutils.WarehouseStagingFilesTable)
	_, err := dbHandle.Exec(sqlStatement, warehouseutils.StagingFileFailedState, misc.QuoteLiteral(statusErr.Error()), timeutil.Now(), stagingFileID)
	if err != nil {
		panic(err)
	}
}

func (job *UploadJobT) bulkInsertLoadFileRecords(loadFiles []loadFileUploadOutputT) (err error) {
	// Using transactions for bulk copying
	txn, err := dbHandle.Begin()
	if err != nil {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn("wh_load_files", "staging_file_id", "location", "source_id", "destination_id", "destination_type", "table_name", "total_events", "created_at", "metadata"))
	if err != nil {
		pkgLogger.Errorf(`[WH]: Error starting bulk copy using CopyIn: %v`, err)
		return
	}
	defer stmt.Close()

	for _, loadFile := range loadFiles {
		metadata := fmt.Sprintf(`{"content_length": %d, "destination_revision_id": %q, "use_rudder_storage": %t}`, loadFile.ContentLength, loadFile.DestinationRevisionID, loadFile.UseRudderStorage)
		_, err = stmt.Exec(loadFile.StagingFileID, loadFile.Location, job.upload.SourceID, job.upload.DestinationID, job.upload.DestinationType, loadFile.TableName, loadFile.TotalRows, timeutil.Now(), metadata)
		if err != nil {
			pkgLogger.Errorf(`[WH]: Error copying row in pq.CopyIn for loadFules: %v Error: %v`, loadFile, err)
			txn.Rollback()
			return
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		pkgLogger.Errorf("[WH]: Error creating load file records: %v", err)
		txn.Rollback()
		return
	}
	err = txn.Commit()
	if err != nil {
		pkgLogger.Errorf("[WH]: Error committing load file records txn: %v", err)
		return
	}
	return
}

func (job *UploadJobT) areIdentityTablesLoadFilesGenerated() (generated bool, err error) {
	var mergeRulesLocation sql.NullString
	sqlStatement := fmt.Sprintf(`SELECT location FROM %s WHERE wh_upload_id=%d AND table_name='%s'`, warehouseutils.WarehouseTableUploadsTable, job.upload.ID, warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentityMergeRulesTable))
	err = job.dbHandle.QueryRow(sqlStatement).Scan(&mergeRulesLocation)
	if err != nil {
		return
	}
	if !mergeRulesLocation.Valid {
		generated = false
		return
	}

	var mappingsLocation sql.NullString
	sqlStatement = fmt.Sprintf(`SELECT location FROM %s WHERE wh_upload_id=%d AND table_name='%s'`, warehouseutils.WarehouseTableUploadsTable, job.upload.ID, warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentityMappingsTable))
	err = job.dbHandle.QueryRow(sqlStatement).Scan(&mappingsLocation)
	if err != nil {
		return
	}
	if !mappingsLocation.Valid {
		generated = false
		return
	}
	generated = true
	return
}

func (job *UploadJobT) GetLoadFilesMetadata(options warehouseutils.GetLoadFilesOptionsT) (loadFiles []warehouseutils.LoadFileT) {
	var tableFilterSQL string
	if options.Table != "" {
		tableFilterSQL = fmt.Sprintf(` AND table_name='%s'`, options.Table)
	}

	startID := options.StartID
	endID := options.EndID
	if startID == 0 || endID == 0 {
		startID = job.upload.StartLoadFileID
		endID = job.upload.EndLoadFileID
	}

	var limitSQL string
	if options.Limit != 0 {
		limitSQL = fmt.Sprintf(`LIMIT %d`, options.Limit)
	}

	sqlStatement := fmt.Sprintf(`
		WITH row_numbered_load_files as (
			SELECT
				location, metadata,
				row_number() OVER (PARTITION BY staging_file_id, table_name ORDER BY id DESC) AS row_number
				FROM %[1]s
				WHERE staging_file_id IN (%[2]v) %[3]s
		)
		SELECT location, metadata
			FROM row_numbered_load_files
			WHERE
				row_number=1
			%[4]s`,
		warehouseutils.WarehouseLoadFilesTable, misc.IntArrayToString(job.stagingFileIDs, ","), tableFilterSQL, limitSQL)

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
		loadFiles = append(loadFiles, warehouseutils.LoadFileT{
			Location: location,
			Metadata: metadata,
		})
	}
	return
}

func (job *UploadJobT) GetSampleLoadFileLocation(tableName string) (location string, err error) {
	locations := job.GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT{Table: tableName, Limit: 1})
	if len(locations) == 0 {
		return "", fmt.Errorf(`No load file found for table:%s`, tableName)
	}
	return locations[0].Location, nil
}

func (job *UploadJobT) GetSchemaInWarehouse() (schema warehouseutils.SchemaT) {
	if job.schemaHandle == nil {
		return
	}
	return job.schemaHandle.schemaInWarehouse
}

func (job *UploadJobT) GetTableSchemaInWarehouse(tableName string) warehouseutils.TableSchemaT {
	return job.schemaHandle.schemaInWarehouse[tableName]
}

func (job *UploadJobT) GetTableSchemaInUpload(tableName string) warehouseutils.TableSchemaT {
	return job.schemaHandle.uploadSchema[tableName]
}

func (job *UploadJobT) GetSingleLoadFile(tableName string) (warehouseutils.LoadFileT, error) {
	sqlStatement := fmt.Sprintf(`SELECT location FROM %s WHERE wh_upload_id=%d AND table_name='%s'`, warehouseutils.WarehouseTableUploadsTable, job.upload.ID, tableName)
	pkgLogger.Infof("SF: Fetching load file location for %s: %s", tableName, sqlStatement)
	var location string
	err := job.dbHandle.QueryRow(sqlStatement).Scan(&location)
	return warehouseutils.LoadFileT{Location: location}, err
}

func (job *UploadJobT) ShouldOnDedupUseNewRecord() bool {
	if job.upload.SourceCategory == CloudSourceCateogry {
		return true
	}
	return false
}

func (job *UploadJobT) UseRudderStorage() bool {
	return job.upload.UseRudderStorage
}

func (job *UploadJobT) GetLoadFileGenStartTIme() time.Time {
	if !job.upload.LoadFileGenStartTime.IsZero() {
		return job.upload.LoadFileGenStartTime
	}
	return warehouseutils.GetLoadFileGenTime(job.upload.TimingsObj)
}

func (job *UploadJobT) GetLoadFileType() string {
	return job.upload.LoadFileType
}

func (job *UploadJobT) GetFirstLastEvent() (time.Time, time.Time) {
	return job.upload.FirstEventAt, job.upload.LastEventAt
}

/*
 * State Machine for upload job lifecycle
 */

func getNextUploadState(dbStatus string) *uploadStateT {
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
		panic(fmt.Errorf("Invalid Upload state: %s", state))
	}
	return uploadState.inProgress
}

func getFailedState(state string) string {
	uploadState, ok := stateTransitions[state]
	if !ok {
		panic(fmt.Errorf("Invalid Upload state : %s", state))
	}
	return uploadState.failed
}

func initializeStateMachine() {
	stateTransitions = make(map[string]*uploadStateT)

	waitingState := &uploadStateT{
		completed: Waiting,
	}
	stateTransitions[Waiting] = waitingState

	generateUploadSchemaState := &uploadStateT{
		inProgress: "generating_upload_schema",
		failed:     "generating_upload_schema_failed",
		completed:  GeneratedUploadSchema,
	}
	stateTransitions[GeneratedUploadSchema] = generateUploadSchemaState

	createTableUploadsState := &uploadStateT{
		inProgress: "creating_table_uploads",
		failed:     "creating_table_uploads_failed",
		completed:  CreatedTableUploads,
	}
	stateTransitions[CreatedTableUploads] = createTableUploadsState

	generateLoadFilesState := &uploadStateT{
		inProgress: "generating_load_files",
		failed:     "generating_load_files_failed",
		completed:  GeneratedLoadFiles,
	}
	stateTransitions[GeneratedLoadFiles] = generateLoadFilesState

	updateTableUploadCountsState := &uploadStateT{
		inProgress: "updating_table_uploads_counts",
		failed:     "updating_table_uploads_counts_failed",
		completed:  UpdatedTableUploadsCounts,
	}
	stateTransitions[UpdatedTableUploadsCounts] = updateTableUploadCountsState

	createRemoteSchemaState := &uploadStateT{
		inProgress: "creating_remote_schema",
		failed:     "creating_remote_schema_failed",
		completed:  CreatedRemoteSchema,
	}
	stateTransitions[CreatedRemoteSchema] = createRemoteSchemaState

	exportDataState := &uploadStateT{
		inProgress: "exporting_data",
		failed:     "exporting_data_failed",
		completed:  ExportedData,
	}
	stateTransitions[ExportedData] = exportDataState

	abortState := &uploadStateT{
		completed: Aborted,
	}
	stateTransitions[Aborted] = abortState

	waitingState.nextState = generateUploadSchemaState
	generateUploadSchemaState.nextState = createTableUploadsState
	createTableUploadsState.nextState = generateLoadFilesState
	generateLoadFilesState.nextState = updateTableUploadCountsState
	updateTableUploadCountsState.nextState = createRemoteSchemaState
	createRemoteSchemaState.nextState = exportDataState
	exportDataState.nextState = nil
	abortState.nextState = nil
}

func (job *UploadJobT) GetLocalSchema() warehouseutils.SchemaT {
	return job.schemaHandle.getLocalSchema()
}

func (job *UploadJobT) UpdateLocalSchema(schema warehouseutils.SchemaT) error {
	return job.schemaHandle.updateLocalSchema(schema)
}
