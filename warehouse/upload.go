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
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	WaitingState                     = "waiting"
	ExecutingState                   = "executing"
	GeneratingLoadFileState          = "generating_load_file"
	GeneratingLoadFileFailedState    = "generating_load_file_failed"
	GeneratedLoadFileState           = "generated_load_file"
	UpdatingSchemaState              = "updating_schema"
	UpdatingSchemaFailedState        = "updating_schema_failed"
	UpdatedSchemaState               = "updated_schema"
	ExportingDataState               = "exporting_data"
	ExportingDataFailedState         = "exporting_data_failed"
	ExportedDataState                = "exported_data"
	AbortedState                     = "aborted"
	GeneratingStagingFileFailedState = "generating_staging_file_failed"
	GeneratedStagingFileState        = "generated_staging_file"
	FetchingSchemaState              = "fetching_schema"
	FetchingSchemaFailedState        = "fetching_schema_failed"
	PreLoadingIdentities             = "pre_loading_identities"
	PreLoadIdentitiesFailed          = "pre_load_identities_failed"
	ConnectFailedState               = "connect_failed"
)

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

func (job *UploadJobT) run() (err error) {
	// START: processing of upload job
	if len(job.stagingFiles) == 0 {
		err = fmt.Errorf("No staging files found")
		job.setUploadError(err, GeneratingLoadFileFailedState)
		return
	}

	job.setUploadStatus(FetchingSchemaState)
	schemaHandle := SchemaHandleT{
		warehouse:    job.warehouse,
		stagingFiles: job.stagingFiles,
		dbHandle:     job.dbHandle,
	}
	job.schemaHandle = &schemaHandle
	schemaHandle.localSchema = schemaHandle.getLocalSchema()
	schemaHandle.schemaInWarehouse, err = schemaHandle.fetchSchemaFromWarehouse()
	if err != nil {
		job.setUploadError(err, FetchingSchemaFailedState)
		return
	}

	hasSchemaChanged := !compareSchema(schemaHandle.localSchema, schemaHandle.schemaInWarehouse)
	if hasSchemaChanged {
		schemaHandle.updateLocalSchema(schemaHandle.schemaInWarehouse)
		schemaHandle.localSchema = schemaHandle.schemaInWarehouse
	}

	// TODO:Why would this be 0?
	if len(job.upload.Schema) == 0 || hasSchemaChanged {
		// merge schemas over all staging files in this batch
		logger.Infof("[WH]: Consolidating all staging files schema's with schema in wh_schemas...")
		schemaHandle.uploadSchema = schemaHandle.consolidateStagingFilesSchemaUsingWarehouseSchema()

		// TODO: remove err?
		err = job.setSchema(schemaHandle.uploadSchema)
		if err != nil {
			job.setUploadError(err, UpdatingSchemaFailedState)
			return
		}
	}

	if !job.areTableUploadsCreated() {
		err := job.initTableUploads()
		if err != nil {
			// TODO: Handle error / Retry
			logger.Error("[WH]: Error creating records in wh_table_uploads", err)
		}
	}

	// TODO: rename metric
	createPlusUploadTimer := warehouseutils.DestStat(stats.TimerType, "stagingfileset_total_handling_time", job.upload.DestinationID)
	createPlusUploadTimer.Start()

	// generate load files only if not done before
	// upload records have start_load_file_id and end_load_file_id set to 0 on creation
	// and are updated on creation of load files
	logger.Infof("[WH]: Processing %d staging files in upload job:%v with staging files from %v to %v for %s:%s", len(job.stagingFiles), job.upload.ID, job.stagingFiles[0].ID, job.stagingFiles[len(job.stagingFiles)-1].ID, job.warehouse.Type, job.upload.DestinationID)
	job.setUploadColumns(
		UploadColumnT{Column: UploadLastExecAtField, Value: timeutil.Now()},
	)

	// generate load files
	if job.upload.StartLoadFileID == 0 || hasSchemaChanged {
		job.setUploadStatus(GeneratingLoadFileState)
		var loadFileIDs []int64
		loadFileIDs, err = job.createLoadFiles()
		if err != nil {
			//Unreachable code. So not modifying the stat 'failed_uploads', which is reused later for copying.
			job.setUploadError(err, GeneratingLoadFileFailedState)
			job.setStagingFilesStatus(warehouseutils.StagingFileFailedState, err)
			warehouseutils.DestStat(stats.CountType, "process_staging_files_failures", job.warehouse.Destination.ID).Count(len(job.stagingFiles))
			return
		}
		job.setStagingFilesStatus(warehouseutils.StagingFileSucceededState, nil)
		warehouseutils.DestStat(stats.CountType, "process_staging_files_success", job.warehouse.Destination.ID).Count(len(job.stagingFiles))
		warehouseutils.DestStat(stats.CountType, "generate_load_files", job.warehouse.Destination.ID).Count(len(loadFileIDs))

		startLoadFileID := loadFileIDs[0]
		endLoadFileID := loadFileIDs[len(loadFileIDs)-1]

		err = job.setUploadStatus(
			GeneratedLoadFileState,
			UploadColumnT{Column: UploadStartLoadFileIDField, Value: startLoadFileID},
			UploadColumnT{Column: UploadEndLoadFileIDField, Value: endLoadFileID},
		)
		if err != nil {
			panic(err)
		}

		job.upload.StartLoadFileID = startLoadFileID
		job.upload.EndLoadFileID = endLoadFileID
		job.upload.Status = GeneratedLoadFileState

		for tableName := range job.upload.Schema {
			err := job.updateTableEventsCount(tableName)
			if err != nil {
				panic(err)
			}
		}

	}

	whManager := job.whManager

	err = whManager.Setup(job.warehouse, job)
	if err != nil {
		job.setUploadError(err, ConnectFailedState)
		return
	}
	defer whManager.Cleanup()

	// TODO: move this func to schema_handle
	job.setUploadStatus(UpdatingSchemaState)
	diff := getSchemaDiff(schemaHandle.schemaInWarehouse, job.upload.Schema)
	err = whManager.MigrateSchema(diff, schemaHandle.schemaInWarehouse)
	if err != nil {
		job.setUploadError(err, UpdatingSchemaFailedState)
		return
	}
	schemaHandle.updateLocalSchema(diff.UpdatedSchema)
	schemaHandle.localSchema = schemaHandle.schemaAfterUpload
	schemaHandle.schemaAfterUpload = diff.UpdatedSchema
	schemaHandle.schemaInWarehouse = schemaHandle.schemaAfterUpload
	job.setUploadStatus(UpdatedSchemaState)

	// TODO: set updated schema on whManager

	job.setUploadStatus(ExportingDataState)
	var loadErrors []error
	// TODO: use provider case specific
	uploadSchema := job.upload.Schema
	if _, ok := uploadSchema["identifies"]; ok {
		// TODO: check only on users table sufficient?
		if !job.shouldBeLoaded("identifies") && !job.shouldBeLoaded("users") {
			// do nothing
		} else {
			err := whManager.LoadUserTables()
			// TODO: set status and error for individual table after hasRecords check
			if err != nil {
				loadErrors = append(loadErrors, err)
			}
			job.setTableUploadStatus("identifies", ExportedDataState)
			job.setTableUploadStatus("users", ExportedDataState)
		}

	}

	// TODO: move this to warehousemanager
	var parallelLoads int
	var ok bool
	if parallelLoads, ok = maxParallelLoads[job.warehouse.Type]; !ok {
		parallelLoads = 1
	}

	var wg sync.WaitGroup
	wg.Add(len(uploadSchema))
	loadChan := make(chan struct{}, parallelLoads)
	for tableName := range uploadSchema {
		if tableName == "users" || tableName == "identifies" {
			wg.Done()
			continue
		}
		if !job.shouldBeLoaded(tableName) {
			wg.Done()
			continue
		}

		tName := tableName
		loadChan <- struct{}{}
		rruntime.Go(func() {
			job.setTableUploadStatus(tName, ExecutingState)
			err := whManager.LoadTable(tName)
			// TODO: set wh_table_uploads error
			if err != nil {
				loadErrors = append(loadErrors, err)
				job.setTableUploadError(tName, ExportingDataFailedState, err)
			}
			job.setTableUploadStatus(tName, ExportedDataState)
			wg.Done()
			<-loadChan
		})
	}
	wg.Wait()

	if len(loadErrors) > 0 {
		// rs.dropDanglingStagingTables()
		errStr := ""
		for idx, err := range loadErrors {
			errStr += err.Error()
			if idx < len(loadErrors)-1 {
				errStr += ", "
			}
		}
		err = fmt.Errorf(errStr)
		job.setUploadError(err, ExportingDataFailedState)
		return
	}
	job.setUploadStatus(ExportedDataState)

	return nil
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
func (job *UploadJobT) getNewTimings(status string) []byte {
	timings := job.getUploadTimings()
	timing := map[string]string{status: timeutil.Now().Format(misc.RFC3339Milli)}
	timings = append(timings, timing)
	marshalledTimings, err := json.Marshal(timings)
	if err != nil {
		panic(err)
	}
	return marshalledTimings
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
	logger.Infof("WH: Setting status of %s for wh_upload:%v", status, job.upload.ID)
	marshalledTimings := job.getNewTimings(status)
	opts := []UploadColumnT{
		{Column: UploadStatusField, Value: status},
		{Column: UploadTimingsField, Value: marshalledTimings},
		{Column: UploadUpdatedAtField, Value: timeutil.Now()},
	}

	additionalFields = append(additionalFields, opts...)

	job.setUploadColumns(
		additionalFields...,
	)
	return
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
	if err != nil {
		panic(err)
	}
	return
}

func (job *UploadJobT) setUploadError(statusError error, state string) (err error) {
	logger.Errorf("WH: Failed during %s stage: %v\n", state, statusError.Error())
	upload := job.upload
	dbHandle := job.dbHandle

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
			state = AbortedState
		}
	}
	serializedErr, _ := json.Marshal(&e)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=$4`, warehouseutils.WarehouseUploadsTable)
	_, err = dbHandle.Exec(sqlStatement, state, serializedErr, timeutil.Now(), upload.ID)
	if err != nil {
		panic(err)
	}
	return
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
	logger.Errorf("WH: Failed processing staging files: %v", statusError.Error())
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=ANY($4)`, warehouseutils.WarehouseStagingFilesTable)
	_, err = job.dbHandle.Exec(sqlStatement, status, misc.QuoteLiteral(statusError.Error()), timeutil.Now(), pq.Array(ids))
	if err != nil {
		panic(err)
	}
	return
}

func (job *UploadJobT) areTableUploadsCreated() bool {
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE wh_upload_id=%d`, warehouseutils.WarehouseTableUploadsTable, job.upload.ID)
	var count int
	err := job.dbHandle.QueryRow(sqlStatement).Scan(&count)
	if err != nil {
		panic(err)
	}
	return count > 0
}

func (job *UploadJobT) initTableUploads() (err error) {

	//Using transactions for bulk copying
	txn, err := job.dbHandle.Begin()
	if err != nil {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(warehouseutils.WarehouseTableUploadsTable, "wh_upload_id", "table_name", "status", "error", "created_at", "updated_at"))
	if err != nil {
		return
	}

	destType := job.warehouse.Type
	schemaForUpload := job.upload.Schema
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

	now := timeutil.Now()
	for _, table := range tables {
		_, err = stmt.Exec(job.upload.ID, table, "waiting", "{}", now, now)
		if err != nil {
			return
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return
	}

	err = stmt.Close()
	if err != nil {
		return
	}

	err = txn.Commit()
	if err != nil {
		return
	}
	return
}

func (job *UploadJobT) getTableUploadStatus(tableName string) (status string, err error) {
	sqlStatement := fmt.Sprintf(`SELECT status from %s WHERE wh_upload_id=%d AND table_name='%s' ORDER BY id DESC`, warehouseutils.WarehouseTableUploadsTable, job.upload.ID, tableName)
	err = dbHandle.QueryRow(sqlStatement).Scan(&status)
	return
}

func (job *UploadJobT) hasLoadFiles(tableName string) bool {
	sourceID := job.warehouse.Source.ID
	destID := job.warehouse.Destination.ID

	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM %[1]s
								WHERE ( %[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND %[1]s.table_name='%[4]s' AND %[1]s.id >= %[5]v AND %[1]s.id <= %[6]v)`,
		warehouseutils.WarehouseLoadFilesTable, sourceID, destID, tableName, job.upload.StartLoadFileID, job.upload.EndLoadFileID)
	var count int64
	err := dbHandle.QueryRow(sqlStatement).Scan(&count)
	if err != nil {
		panic(err)
	}
	return count > 0
}

func (job *UploadJobT) shouldBeLoaded(tableName string) bool {
	status, _ := job.getTableUploadStatus(tableName)
	if status == ExportedDataState {
		return false
	}
	if !job.hasLoadFiles(tableName) {
		return false
	}
	return true
}

func (job *UploadJobT) setTableUploadStatus(tableName string, status string) (err error) {
	// set last_exec_time only if status is executing
	execValues := []interface{}{status, timeutil.Now(), job.upload.ID, tableName}
	var lastExec string
	if status == ExecutingState {
		// setting values using syntax $n since Exec can correctlt format time.Time strings
		lastExec = fmt.Sprintf(`, last_exec_time=$%d`, len(execValues)+1)
		execValues = append(execValues, timeutil.Now())
	}
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2 %s WHERE wh_upload_id=$3 AND table_name=$4`, warehouseutils.WarehouseTableUploadsTable, lastExec)
	logger.Debugf("WH: Setting table upload status: %v", sqlStatement)
	_, err = dbHandle.Exec(sqlStatement, execValues...)
	if err != nil {
		panic(err)
	}
	return
}

func (job *UploadJobT) setTableUploadError(tableName string, status string, statusError error) (err error) {
	logger.Errorf("WH: Failed uploading table-%s for upload-%v: %v", tableName, job.upload.ID, statusError.Error())
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, updated_at=$2, error=$3 WHERE wh_upload_id=$4 AND table_name=$5`, warehouseutils.WarehouseTableUploadsTable)
	logger.Debugf("WH: Setting table upload error: %v", sqlStatement)
	_, err = dbHandle.Exec(sqlStatement, status, timeutil.Now(), misc.QuoteLiteral(statusError.Error()), job.upload.ID, tableName)
	if err != nil {
		panic(err)
	}
	return
}

func (job *UploadJobT) createLoadFiles() (loadFileIDs []int64, err error) {
	destID := job.upload.DestinationID
	destType := job.upload.DestinationType
	stagingFiles := job.stagingFiles

	// stat for time taken to process staging files in a single job
	timer := warehouseutils.DestStat(stats.TimerType, "process_staging_files_batch_time", destID)
	timer.Start()

	job.setStagingFilesStatus(warehouseutils.StagingFileExecutingState, nil)

	logger.Debugf("[WH]: Starting batch processing %v stage files with %v workers for %s:%s", len(stagingFiles), noOfWorkers, destType, destID)
	var messages []pgnotifier.MessageT
	for _, stagingFile := range stagingFiles {
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

	logger.Infof("[WH]: Publishing %d staging files to PgNotifier", len(messages))
	// var loadFileIDs []int64
	ch, err := job.pgNotifier.Publish(StagingFileProcessPGChannel, messages)
	if err != nil {
		panic(err)
	}

	responses := <-ch
	logger.Infof("[WH]: Received responses from PgNotifier")
	for _, resp := range responses {
		// TODO: make it aborted
		if resp.Status == "aborted" {
			logger.Errorf("Error in genrating load files: %v", resp.Error)
			continue
		}
		var payload map[string]interface{}
		err = json.Unmarshal(resp.Payload, &payload)
		if err != nil {
			panic(err)
		}
		respIDs, ok := payload["LoadFileIDs"].([]interface{})
		if !ok {
			logger.Errorf("No LoadFIleIDS returned by wh worker")
			continue
		}
		ids := make([]int64, len(respIDs))
		for i := range respIDs {
			ids[i] = int64(respIDs[i].(float64))
		}
		loadFileIDs = append(loadFileIDs, ids...)
	}

	timer.End()
	if len(loadFileIDs) == 0 {
		err = fmt.Errorf(responses[0].Error)
		return loadFileIDs, err
	}
	sort.Slice(loadFileIDs, func(i, j int) bool { return loadFileIDs[i] < loadFileIDs[j] })
	return loadFileIDs, nil
}

func (job *UploadJobT) updateTableEventsCount(tableName string) (err error) {
	subQuery := fmt.Sprintf(`SELECT sum(total_events) as total from %[1]s right join (
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
		warehouseutils.WarehouseTableUploadsTable)

	sqlStatement := fmt.Sprintf(`update %[1]s set total_events = subquery.total FROM (%[2]s) AS subquery WHERE table_name = '%[3]s' AND wh_upload_id = %[4]d`,
		warehouseutils.WarehouseTableUploadsTable,
		subQuery,
		tableName,
		job.upload.ID)
	_, err = job.dbHandle.Exec(sqlStatement)
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

func (job *UploadJobT) GetSchemaInWarehouse() warehouseutils.SchemaT {
	return job.schemaHandle.schemaInWarehouse
}

func (job *UploadJobT) GetTableSchemaAfterUpload(tableName string) warehouseutils.TableSchemaT {
	return job.schemaHandle.schemaAfterUpload[tableName]
}

func (job *UploadJobT) GetTableSchemaInUpload(tableName string) warehouseutils.TableSchemaT {
	return job.schemaHandle.uploadSchema[tableName]
}
