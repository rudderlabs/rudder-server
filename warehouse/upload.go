package warehouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
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
	UpdatedRemoteSchema       = "updated_remote_schema"
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
)

// Table Upload status
const (
	TableUploadExecuting       = "executing"
	TableUploadExporting       = "exporting_data"
	TableUploadExportingFailed = "exporting_data"
	TableUploadExported        = "exported_data"
)

var stateTransitions map[string]*uploadStateT

type uploadStateT struct {
	inProgress string
	failed     string
	completed  string
	task       string
	nextState  *uploadStateT
}

type UploadT struct {
	ID                 int64
	Namespace          string
	SourceID           string
	DestinationID      string
	DestinationType    string
	StartStagingFileID int64
	EndStagingFileID   int64
	StartLoadFileID    int64
	EndLoadFileID      int64
	Status             string
	Schema             warehouseutils.SchemaT
	Error              json.RawMessage
	Timings            []map[string]string
	FirstAttemptAt     time.Time
	LastAttemptAt      time.Time
	Attempts           int64
}

type UploadJobT struct {
	upload       *UploadT
	dbHandle     *sql.DB
	warehouse    warehouseutils.WarehouseT
	whManager    manager.ManagerI
	stagingFiles []*StagingFileT
	pgNotifier   *pgnotifier.PgNotifierT
	schemaHandle *SchemaHandleT
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
	UploadLastExecAtField      = "last_exec_at"
)

var maxParallelLoads map[string]int

func init() {
	setMaxParallelLoads()
	initializeStateMachine()
}

func setMaxParallelLoads() {
	maxParallelLoads = map[string]int{
		"BQ":         config.GetInt("Warehouse.bigquery.maxParallelLoads", 20),
		"RS":         config.GetInt("Warehouse.redshift.maxParallelLoads", 3),
		"POSTGRES":   config.GetInt("Warehouse.postgres.maxParallelLoads", 3),
		"SNOWFLAKE":  config.GetInt("Warehouse.snowflake.maxParallelLoads", 3),
		"CLICKHOUSE": config.GetInt("Warehouse.clickhouse.maxParallelLoads", 3),
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
	rruntime.Go(func() {
		select {
		case _ = <-ch:
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
	err := job.setSchema(schemaHandle.uploadSchema)
	return err
}

func (job *UploadJobT) initTableUploads() error {
	schemaForUpload := job.upload.Schema
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

func (job *UploadJobT) shouldTableBeLoaded(tableName string) (bool, error) {
	tableUpload := NewTableUpload(job.upload.ID, tableName)
	loaded, err := tableUpload.hasBeenLoaded()
	if err != nil {
		return false, err
	}

	// TODO: Do we need this?
	hasLoadfiles, err := job.hasLoadFiles(tableName)
	if err != nil {
		return false, err
	}

	return (!loaded && hasLoadfiles), nil
}

func (job *UploadJobT) syncRemoteSchema() (hasSchemaChanged bool, err error) {
	schemaHandle := SchemaHandleT{
		warehouse:    job.warehouse,
		stagingFiles: job.stagingFiles,
		dbHandle:     job.dbHandle,
	}
	job.schemaHandle = &schemaHandle
	schemaHandle.localSchema = schemaHandle.getLocalSchema()
	schemaHandle.schemaInWarehouse, err = schemaHandle.fetchSchemaFromWarehouse()
	if err != nil {
		return false, err
	}

	hasSchemaChanged = !compareSchema(schemaHandle.localSchema, schemaHandle.schemaInWarehouse)
	if hasSchemaChanged {
		err = schemaHandle.updateLocalSchema(schemaHandle.schemaInWarehouse)
		if err != nil {
			return false, err
		}
		schemaHandle.localSchema = schemaHandle.schemaInWarehouse
	}

	return hasSchemaChanged, nil
}

func (job *UploadJobT) run() (err error) {
	ch := job.trackLongRunningUpload()
	defer func() {
		ch <- struct{}{}
	}()

	if len(job.stagingFiles) == 0 {
		err := fmt.Errorf("No staging files found")
		job.setUploadError(err, job.upload.Status)
		return err
	}

	hasSchemaChanged, err := job.syncRemoteSchema()
	if err != nil {
		job.setUploadError(err, job.upload.Status)
		return err
	}

	schemaHandle := job.schemaHandle
	if !hasSchemaChanged {
		schemaHandle.uploadSchema = job.upload.Schema
	} else {
		pkgLogger.Infof("[WH] Remote schema changed for Warehouse: %s", job.warehouse.Identifier)
	}

	whManager := job.whManager
	err = whManager.Setup(job.warehouse, job)
	if err != nil {
		job.setUploadError(err, job.upload.Status)
		return err
	}
	defer whManager.Cleanup()

	var newStatus string

	nextUploadState := getNextUploadState(job.upload.Status)
	if nextUploadState == nil {
		nextUploadState = stateTransitions[GeneratedUploadSchema]
	}

	for {
		err = nil

		job.setUploadStatus(nextUploadState.inProgress)
		pkgLogger.Debugf("[WH] Upload: %d, Current state: %s", job.upload.ID, nextUploadState.inProgress)

		targetStatus := nextUploadState.completed

		switch targetStatus {

		case GeneratedUploadSchema:
			newStatus = nextUploadState.failed
			err := job.generateUploadSchema(schemaHandle)
			if err != nil {
				break
			}
			newStatus = nextUploadState.completed

		case CreatedTableUploads:
			newStatus = nextUploadState.failed
			err := job.initTableUploads()
			if err != nil {
				break
			}
			newStatus = nextUploadState.completed

		case GeneratedLoadFiles:
			newStatus = nextUploadState.failed
			var loadFileIDs []int64
			loadFileIDs, err = job.createLoadFiles()
			if err != nil {
				job.setStagingFilesStatus(warehouseutils.StagingFileFailedState, err)
				break
			}

			err = job.setLoadFileIDs(loadFileIDs[0], loadFileIDs[len(loadFileIDs)-1])
			if err != nil {
				break
			}
			job.setStagingFilesStatus(warehouseutils.StagingFileSucceededState, err)

			newStatus = nextUploadState.completed

		case UpdatedTableUploadsCounts:
			newStatus = nextUploadState.failed
			for tableName := range job.upload.Schema {
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

		case UpdatedRemoteSchema:
			newStatus = nextUploadState.failed
			diff := getSchemaDiff(schemaHandle.schemaInWarehouse, job.upload.Schema)
			if diff.Exists {
				err = whManager.MigrateSchema(diff)
				if err != nil {
					break
				}
				// update all schemas in handle to the updated version from warehouse after successful migration
				err = schemaHandle.updateLocalSchema(diff.UpdatedSchema)
				if err != nil {
					break
				}
				schemaHandle.schemaAfterUpload = diff.UpdatedSchema
				schemaHandle.schemaInWarehouse = schemaHandle.schemaAfterUpload
			} else {
				// no alter done to schema in this upload
				schemaHandle.schemaAfterUpload = schemaHandle.schemaInWarehouse
			}
			newStatus = nextUploadState.completed

		case ExportedUserTables:
			newStatus = nextUploadState.failed
			uploadSchema := job.upload.Schema
			if _, ok := uploadSchema[job.identifiesTableName()]; ok {

				loadTimeStat := job.timerStat("user_tables_load_time")
				loadTimeStat.Start()
				var loadErrors []error
				loadErrors, err = job.loadUserTables()
				if err != nil {
					break
				}

				if len(loadErrors) > 0 {
					err = warehouseutils.ConcatErrors(loadErrors)
					break
				}
				loadTimeStat.End()
			}
			newStatus = nextUploadState.completed

		case ExportedIdentities:
			newStatus = nextUploadState.failed
			// Load Identitties if enabled
			uploadSchema := job.upload.Schema
			if warehouseutils.IDResolutionEnabled() && misc.ContainsString(warehouseutils.IdentityEnabledWarehouses, job.warehouse.Type) {
				if _, ok := uploadSchema[job.identityMergeRulesTableName()]; ok {
					loadTimeStat := job.timerStat("identity_tables_load_time")
					loadTimeStat.Start()

					var loadErrors []error
					loadErrors, err = job.loadIdentityTables(false)
					if err != nil {
						break
					}

					if len(loadErrors) > 0 {
						err = warehouseutils.ConcatErrors(loadErrors)
						break
					}
					loadTimeStat.End()
				}
			}
			newStatus = nextUploadState.completed

		case ExportedData:
			newStatus = nextUploadState.failed
			skipPrevLoadedTableNames := []string{job.identifiesTableName(), job.usersTableName(), job.identityMergeRulesTableName(), job.identityMappingsTableName()}
			// Export all other tables
			loadTimeStat := job.timerStat("other_tables_load_time")
			loadTimeStat.Start()

			loadErrors := job.loadAllTablesExcept(skipPrevLoadedTableNames)

			if len(loadErrors) > 0 {
				err = warehouseutils.ConcatErrors(loadErrors)
				break
			}
			loadTimeStat.End()
			job.generateUploadSuccessMetrics()
			newStatus = nextUploadState.completed

		default:
			// If unknown state, start again
			newStatus = Waiting
		}

		pkgLogger.Debugf("[WH] Upload: %d, Next state: %s", job.upload.ID, newStatus)
		job.setUploadStatus(newStatus)

		if newStatus == ExportedData {
			break
		}

		if err != nil {
			pkgLogger.Errorf("[WH] Upload: %d, TargetState: %s, NewState: %s, Error: %w", job.upload.ID, targetStatus, newStatus, err.Error())
			state, err := job.setUploadError(err, newStatus)
			if err == nil && state == Aborted {
				job.generateUploadAbortedMetrics()
			}
			break
		}

		nextUploadState = getNextUploadState(newStatus)
	}

	if newStatus != ExportedData {
		return fmt.Errorf("Upload Job failed: %w", err)
	}

	return nil
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

func (job *UploadJobT) loadAllTablesExcept(skipPrevLoadedTableNames []string) []error {
	uploadSchema := job.upload.Schema
	var parallelLoads int
	var ok bool
	if parallelLoads, ok = maxParallelLoads[job.warehouse.Type]; !ok {
		parallelLoads = 1
	}

	var loadErrors []error
	var loadErrorLock sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(uploadSchema))

	loadChan := make(chan struct{}, parallelLoads)
	for tableName := range uploadSchema {
		if misc.ContainsString(skipPrevLoadedTableNames, tableName) {
			wg.Done()
			continue
		}
		var loadTable bool
		loadTable, err := job.shouldTableBeLoaded(tableName)
		if err != nil {
			panic(err)
		}
		if !loadTable {
			wg.Done()
			continue
		}

		tName := tableName
		loadChan <- struct{}{}
		rruntime.Go(func() {
			pkgLogger.Infof(`[WH]: Starting load for table %s in namespace %s of destination %s:%s`, tName, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)
			tableUpload := NewTableUpload(job.upload.ID, tName)
			tableUpload.setStatus(TableUploadExecuting)
			err := job.whManager.LoadTable(tName)
			if err != nil {
				loadErrorLock.Lock()
				loadErrors = append(loadErrors, err)
				loadErrorLock.Unlock()
				tableUpload.setError(TableUploadExportingFailed, err)
			} else {
				tableUpload.setStatus(TableUploadExported)
			}
			wg.Done()
			<-loadChan
		})
	}
	wg.Wait()

	return loadErrors
}

func (job *UploadJobT) loadUserTables() (loadErrors []error, tableUploadErr error) {
	var loadTables bool
	userTables := []string{job.identifiesTableName(), job.usersTableName()}

	var err error
	for _, tName := range userTables {
		loadTables, err = job.shouldTableBeLoaded(tName)
		if err != nil {
			break
		}
		if loadTables {
			// There is at least one table to load
			break
		}
	}

	if err != nil {
		return []error{err}, nil
	}

	if !loadTables {
		return []error{}, nil
	}

	loadTimeStat := job.timerStat("user_tables_load_time")
	loadTimeStat.Start()

	// Load all user tables
	for _, tName := range userTables {
		tableUpload := NewTableUpload(job.upload.ID, tName)
		tableUpload.setStatus(TableUploadExecuting)
	}

	errorMap := job.whManager.LoadUserTables()
	return job.processLoadTableResponse(errorMap)
}

func (job *UploadJobT) loadIdentityTables(populateHistoricIdentities bool) (loadErrors []error, tableUploadErr error) {
	pkgLogger.Infof(`[WH]: Starting load for identity tables in namespace %s of destination %s:%s`, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)
	errorMap := make(map[string]error)
	// var generated bool
	if generated, err := job.areIdentityTablesLoadFilesGenerated(); !generated {
		err = job.resolveIdentities(populateHistoricIdentities)
		if err != nil {
			pkgLogger.Errorf(`SF: ID Resolution operation failed: %v`, err)
			errorMap[job.identityMergeRulesTableName()] = err
			return job.processLoadTableResponse(errorMap)
		}
	}

	identityTables := []string{job.identityMergeRulesTableName(), job.identityMappingsTableName()}

	for _, tableName := range identityTables {
		loadTable, err := job.shouldTableBeLoaded(tableName)
		if err != nil {
			errorMap[tableName] = err
			break
		}

		if loadTable {
			errorMap[tableName] = nil
			tableUpload := NewTableUpload(job.upload.ID, tableName)
			err := tableUpload.setStatus(TableUploadExecuting)
			if err != nil {
				errorMap[tableName] = err
				break
			}

			if tableName == job.identityMergeRulesTableName() {
				err = job.whManager.LoadIdentityMergeRulesTable()
			} else if tableName == job.identityMappingsTableName() {
				err = job.whManager.LoadIdentityMappingsTable()
			}
			if err != nil {
				errorMap[tableName] = err
				break
			}
		}
	}

	return job.processLoadTableResponse(errorMap)
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
				numEvents, tableUploadErr := tableUpload.getNumEvents()
				if tableUploadErr == nil {
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
	err = json.Unmarshal(rawJSON, &timings)
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

func (job *UploadJobT) setUploadStatus(status string, additionalFields ...UploadColumnT) (err error) {
	pkgLogger.Debugf("[WH]: Setting status of %s for wh_upload:%v", status, job.upload.ID)
	marshalledTimings, timings := job.getNewTimings(status)
	opts := []UploadColumnT{
		{Column: UploadStatusField, Value: status},
		{Column: UploadTimingsField, Value: marshalledTimings},
		{Column: UploadUpdatedAtField, Value: timeutil.Now()},
	}

	job.upload.Status = status
	job.upload.Timings = timings
	additionalFields = append(additionalFields, opts...)

	return job.setUploadColumns(
		additionalFields...,
	)
}

// SetSchema
func (job *UploadJobT) setSchema(consolidatedSchema warehouseutils.SchemaT) error {
	marshalledSchema, err := json.Marshal(consolidatedSchema)
	if err != nil {
		panic(err)
	}
	job.upload.Schema = consolidatedSchema
	return job.setUploadColumns(
		UploadColumnT{Column: UploadSchemaField, Value: marshalledSchema},
	)
}

// Set LoadFileIDs
func (job *UploadJobT) setLoadFileIDs(startLoadFileID int64, endLoadFileID int64) error {
	job.upload.StartLoadFileID = startLoadFileID
	job.upload.EndLoadFileID = endLoadFileID

	return job.setUploadColumns(
		UploadColumnT{Column: UploadStartLoadFileIDField, Value: startLoadFileID},
		UploadColumnT{Column: UploadEndLoadFileIDField, Value: endLoadFileID},
	)
}

// SetUploadColumns sets any column values passed as args in UploadColumnT format for WarehouseUploadsTable
func (job *UploadJobT) setUploadColumns(fields ...UploadColumnT) (err error) {
	var columns string
	values := []interface{}{job.upload.ID}
	// setting values using syntax $n since Exec can correctly format time.Time strings
	for idx, f := range fields {
		// start with $2 as $1 is upload.ID
		columns += fmt.Sprintf(`%s=$%d`, f.Column, idx+2)
		if idx < len(fields)-1 {
			columns += ","
		}
		values = append(values, f.Value)
	}
	sqlStatement := fmt.Sprintf(`UPDATE %s SET %s WHERE id=$1`, warehouseutils.WarehouseUploadsTable, columns)
	_, err = dbHandle.Exec(sqlStatement, values...)

	return err
}

func (job *UploadJobT) setUploadError(statusError error, state string) (newstate string, err error) {
	pkgLogger.Errorf("[WH]: Failed during %s stage: %v\n", state, statusError.Error())
	job.counterStat("failed_uploads").Count(1)
	job.counterStat(fmt.Sprintf("error_%s", state)).Count(1)

	upload := job.upload

	job.setUploadStatus(state)
	var e map[string]map[string]interface{}
	json.Unmarshal(job.upload.Error, &e)
	if e == nil {
		e = make(map[string]map[string]interface{})
	}
	if _, ok := e[state]; !ok {
		e[state] = make(map[string]interface{})
	}
	errorByState := e[state]
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
	// abort after configured retry attempts
	if errorByState["attempt"].(int) > minRetryAttempts {
		firstTiming := job.getUploadFirstAttemptTime()
		if !firstTiming.IsZero() && (timeutil.Now().Sub(firstTiming) > retryTimeWindow) {
			state = Aborted
		}
	}
	serializedErr, _ := json.Marshal(&e)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=$4`, warehouseutils.WarehouseUploadsTable)
	_, err = job.dbHandle.Exec(sqlStatement, state, serializedErr, timeutil.Now(), upload.ID)

	job.upload.Status = state
	job.upload.Error = serializedErr

	return state, err
}

func (job *UploadJobT) setStagingFilesStatus(status string, statusError error) (err error) {
	var ids []int64
	for _, stagingFile := range job.stagingFiles {
		ids = append(ids, stagingFile.ID)
	}
	// TODO: json.Marshal error instead of quoteliteral
	if statusError == nil {
		statusError = fmt.Errorf("{}")
	}
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=ANY($4)`, warehouseutils.WarehouseStagingFilesTable)
	_, err = dbHandle.Exec(sqlStatement, status, misc.QuoteLiteral(statusError.Error()), timeutil.Now(), pq.Array(ids))
	if err != nil {
		panic(err)
	}
	return
}

func (job *UploadJobT) setStagingFilesError(ids []int64, status string, statusError error) (err error) {
	pkgLogger.Errorf("[WH]: Failed processing staging files: %v", statusError.Error())
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=ANY($4)`, warehouseutils.WarehouseStagingFilesTable)
	_, err = job.dbHandle.Exec(sqlStatement, status, misc.QuoteLiteral(statusError.Error()), timeutil.Now(), pq.Array(ids))
	if err != nil {
		panic(err)
	}
	return
}

func (job *UploadJobT) hasLoadFiles(tableName string) (bool, error) {
	sourceID := job.warehouse.Source.ID
	destID := job.warehouse.Destination.ID

	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM %[1]s
								WHERE ( %[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND %[1]s.table_name='%[4]s' AND %[1]s.id >= %[5]v AND %[1]s.id <= %[6]v)`,
		warehouseutils.WarehouseLoadFilesTable, sourceID, destID, tableName, job.upload.StartLoadFileID, job.upload.EndLoadFileID)
	var count int64
	err := dbHandle.QueryRow(sqlStatement).Scan(&count)
	return count > 0, err
}

func (job *UploadJobT) createLoadFiles() (loadFileIDs []int64, err error) {
	destID := job.upload.DestinationID
	destType := job.upload.DestinationType
	stagingFiles := job.stagingFiles

	// stat for time taken to process staging files in a single job
	timer := job.timerStat("load_files_creation")
	timer.Start()

	job.setStagingFilesStatus(warehouseutils.StagingFileExecutingState, nil)

	publishBatchSize := config.GetInt("Warehouse.pgNotifierPublishBatchSize", 100)
	pkgLogger.Infof("[WH]: Starting batch processing %v stage files with %v workers for %s:%s", publishBatchSize, noOfWorkers, destType, destID)
	for i := 0; i < len(stagingFiles); i += publishBatchSize {
		j := i + publishBatchSize
		if j > len(stagingFiles) {
			j = len(stagingFiles)
		}

		var messages []pgnotifier.MessageT
		for _, stagingFile := range stagingFiles[i:j] {
			payload := PayloadT{
				UploadID:            job.upload.ID,
				StagingFileID:       stagingFile.ID,
				StagingFileLocation: stagingFile.Location,
				Schema:              job.upload.Schema,
				SourceID:            job.warehouse.Source.ID,
				DestinationID:       destID,
				DestinationType:     destType,
				DestinationConfig:   job.warehouse.Destination.Config,
			}

			payloadJSON, err := json.Marshal(payload)
			if err != nil {
				panic(err)
			}
			message := pgnotifier.MessageT{
				Payload: payloadJSON,
			}
			messages = append(messages, message)
		}

		pkgLogger.Infof("[WH]: Publishing %d staging files for %s:%s to PgNotifier", len(messages), destType, destID)
		ch, err := job.pgNotifier.Publish(StagingFilesPGNotifierChannel, messages)
		if err != nil {
			panic(err)
		}

		responses := <-ch
		pkgLogger.Infof("[WH]: Received responses for staging files %d:%d for %s:%s from PgNotifier", stagingFiles[i].ID, stagingFiles[j-1].ID, destType, destID)
		for _, resp := range responses {
			// TODO: make it aborted
			if resp.Status == "aborted" {
				pkgLogger.Errorf("[WH]: Error in genrating load files: %v", resp.Error)
				continue
			}
			var payload map[string]interface{}
			err = json.Unmarshal(resp.Payload, &payload)
			if err != nil {
				panic(err)
			}
			respIDs, ok := payload["LoadFileIDs"].([]interface{})
			if !ok {
				pkgLogger.Errorf("[WH]: No LoadFileIDS returned by wh worker")
				continue
			}
			ids := make([]int64, len(respIDs))
			for i := range respIDs {
				ids[i] = int64(respIDs[i].(float64))
			}
			loadFileIDs = append(loadFileIDs, ids...)
		}
	}

	timer.End()

	if len(loadFileIDs) == 0 {
		err = fmt.Errorf("No load files generated")
		return loadFileIDs, err
	}
	sort.Slice(loadFileIDs, func(i, j int) bool { return loadFileIDs[i] < loadFileIDs[j] })
	return loadFileIDs, nil
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
	sqlStatement = fmt.Sprintf(`SELECT location FROM %s WHERE wh_upload_id=%d AND table_name='%s'`, warehouseutils.WarehouseTableUploadsTable, job.upload.ID, warehouseutils.ToProviderCase(job.warehouse.Type, warehouseutils.IdentityMergeRulesTable))
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

func (job *UploadJobT) GetLoadFileLocations(tableName string) (locations []string, err error) {
	sqlStatement := fmt.Sprintf(`SELECT location from %[1]s right join (
		SELECT  staging_file_id, MAX(id) AS id FROM wh_load_files
		WHERE ( source_id='%[2]s'
			AND destination_id='%[3]s'
			AND table_name='%[4]s'
			AND id >= %[5]v
			AND id <= %[6]v)
		GROUP BY staging_file_id ) uniqueStagingFiles
		ON  wh_load_files.id = uniqueStagingFiles.id `,
		warehouseutils.WarehouseLoadFilesTable,
		job.warehouse.Source.ID,
		job.warehouse.Destination.ID,
		tableName,
		job.upload.StartLoadFileID,
		job.upload.EndLoadFileID,
	)
	rows, err := dbHandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var location string
		err := rows.Scan(&location)
		if err != nil {
			panic(err)
		}
		locations = append(locations, location)
	}
	return
}

func (job *UploadJobT) GetSampleLoadFileLocation(tableName string) (location string, err error) {
	sqlStatement := fmt.Sprintf(`SELECT location FROM %[1]s RIGHT JOIN (
		SELECT  staging_file_id, MAX(id) AS id FROM %[1]s
		WHERE ( source_id='%[2]s'
			AND destination_id='%[3]s'
			AND table_name='%[4]s'
			AND id >= %[5]v
			AND id <= %[6]v)
		GROUP BY staging_file_id ) uniqueStagingFiles
		ON  wh_load_files.id = uniqueStagingFiles.id `,
		warehouseutils.WarehouseLoadFilesTable,
		job.warehouse.Source.ID,
		job.warehouse.Destination.ID,
		tableName,
		job.upload.StartLoadFileID,
		job.upload.EndLoadFileID,
	)
	err = dbHandle.QueryRow(sqlStatement).Scan(&location)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	return
}

func (job *UploadJobT) GetSchemaInWarehouse() (schema warehouseutils.SchemaT) {
	if job.schemaHandle == nil {
		return
	}
	return job.schemaHandle.schemaInWarehouse
}

func (job *UploadJobT) GetTableSchemaAfterUpload(tableName string) warehouseutils.TableSchemaT {
	return job.schemaHandle.schemaAfterUpload[tableName]
}

func (job *UploadJobT) GetTableSchemaInUpload(tableName string) warehouseutils.TableSchemaT {
	return job.schemaHandle.uploadSchema[tableName]
}

func (job *UploadJobT) GetSingleLoadFileLocation(tableName string) (string, error) {
	sqlStatement := fmt.Sprintf(`SELECT location FROM %s WHERE wh_upload_id=%d AND table_name='%s'`, warehouseutils.WarehouseTableUploadsTable, job.upload.ID, tableName)
	pkgLogger.Infof("SF: Fetching load file location for %s: %s", tableName, sqlStatement)
	var location string
	err := job.dbHandle.QueryRow(sqlStatement).Scan(&location)
	return location, err
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
		panic("Invalid Upload state")
	}
	return uploadState.inProgress
}

func getFailedState(state string) string {
	uploadState, ok := stateTransitions[state]
	if !ok {
		panic("Invalid Upload state")
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

	updateRemoteSchemaState := &uploadStateT{
		inProgress: "updating_remote_schema",
		failed:     "updating_remote_schema_failed",
		completed:  UpdatedRemoteSchema,
	}
	stateTransitions[UpdatedRemoteSchema] = updateRemoteSchemaState

	exportUserTablesState := &uploadStateT{
		inProgress: "exporting_user_tables",
		failed:     "exporting_user_tables_failed",
		completed:  ExportedUserTables,
	}
	stateTransitions[ExportedUserTables] = exportUserTablesState

	loadIdentitiesState := &uploadStateT{
		inProgress: "exporting_identities",
		failed:     "exporting_identities_failed",
		completed:  ExportedIdentities,
	}
	stateTransitions[ExportedIdentities] = loadIdentitiesState

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
	updateTableUploadCountsState.nextState = updateRemoteSchemaState
	updateRemoteSchemaState.nextState = exportUserTablesState
	exportUserTablesState.nextState = loadIdentitiesState
	loadIdentitiesState.nextState = exportDataState
	exportDataState.nextState = nil
	abortState.nextState = nil
}
