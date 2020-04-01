package warehouse

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bugsnag/bugsnag-go"
	"github.com/iancoleman/strcase"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
)

var (
	webPort                    int
	dbHandle                   *sql.DB
	notifier                   pgnotifier.PgNotifierT
	warehouseDestinations      []string
	jobQueryBatchSize          int
	noOfWorkers                int
	noOfSlaveWorkerRoutines    int
	slaveWorkerRoutineBusy     []bool //Busy-true
	uploadFreqInS              int64
	mainLoopSleep              time.Duration
	stagingFilesBatchSize      int
	configSubscriberLock       sync.RWMutex
	availableWarehouses        []string
	crashRecoverWarehouses     []string
	inProgressMap              map[string]bool
	inRecoveryMap              map[string]bool
	inProgressMapLock          sync.RWMutex
	lastExecMap                map[string]int64
	lastExecMapLock            sync.RWMutex
	warehouseLoadFilesTable    string
	warehouseStagingFilesTable string
	warehouseUploadsTable      string
	warehouseTableUploadsTable string
	warehouseSchemasTable      string
	warehouseMode              string
)
var (
	host, user, password, dbname string
	port                         int
)

// warehouses worker modes
const (
	MasterMode      = "master"
	SlaveMode       = "slave"
	MasterSlaveMode = "master_and_slave"
	EmbeddedMode    = "embedded"
)

type HandleT struct {
	destType           string
	warehouses         []warehouseutils.WarehouseT
	dbHandle           *sql.DB
	notifier           pgnotifier.PgNotifierT
	uploadToWarehouseQ chan []ProcessStagingFilesJobT
	createLoadFilesQ   chan LoadFileJobT
	isEnabled          bool
}

type ProcessStagingFilesJobT struct {
	Upload    warehouseutils.UploadT
	List      []*StagingFileT
	Warehouse warehouseutils.WarehouseT
}

type LoadFileJobT struct {
	Upload                     warehouseutils.UploadT
	StagingFile                *StagingFileT
	Schema                     map[string]map[string]string
	Warehouse                  warehouseutils.WarehouseT
	Wg                         *misc.WaitGroup
	LoadFileIDsChan            chan []int64
	TableToBucketFolderMap     map[string]string
	TableToBucketFolderMapLock *sync.RWMutex
}

type StagingFileT struct {
	ID        int64
	Location  string
	SourceID  string
	Schema    json.RawMessage
	Status    string // enum
	CreatedAt time.Time
}

func init() {
	config.Initialize()
	loadConfig()
}

func loadConfig() {
	//Port where WH is running
	webPort = config.GetInt("Warehouse.webPort", 8082)
	warehouseDestinations = []string{"RS", "BQ", "SNOWFLAKE"}
	jobQueryBatchSize = config.GetInt("Router.jobQueryBatchSize", 10000)
	noOfWorkers = config.GetInt("Warehouse.noOfWorkers", 8)
	noOfSlaveWorkerRoutines = config.GetInt("Warehouse.noOfSlaveWorkerRoutines", 4)
	stagingFilesBatchSize = config.GetInt("Warehouse.stagingFilesBatchSize", 240)
	uploadFreqInS = config.GetInt64("Warehouse.uploadFreqInS", 1800)
	warehouseStagingFilesTable = config.GetString("Warehouse.stagingFilesTable", "wh_staging_files")
	warehouseLoadFilesTable = config.GetString("Warehouse.loadFilesTable", "wh_load_files")
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
	warehouseTableUploadsTable = config.GetString("Warehouse.tableUploadsTable", "wh_table_uploads")
	warehouseSchemasTable = config.GetString("Warehouse.schemasTable", "wh_schemas")
	mainLoopSleep = config.GetDuration("Warehouse.mainLoopSleepInS", 60) * time.Second
	availableWarehouses = []string{"RS", "BQ", "SNOWFLAKE"}
	crashRecoverWarehouses = []string{"RS"}
	inProgressMap = map[string]bool{}
	inRecoveryMap = map[string]bool{}
	lastExecMap = map[string]int64{}
	warehouseMode = config.GetString("Warehouse.mode", "embedded")
	host = config.GetEnv("WAREHOUSE_JOBS_DB_HOST", "localhost")
	user = config.GetEnv("WAREHOUSE_JOBS_DB_USER", "ubuntu")
	dbname = config.GetEnv("WAREHOUSE_JOBS_DB_DB_NAME", "ubuntu")
	port, _ = strconv.Atoi(config.GetEnv("WAREHOUSE_JOBS_DB_PORT", "5432"))
	password = config.GetEnv("WAREHOUSE_JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from
}

func (wh *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, "backendConfig")
	for {
		config := <-ch
		configSubscriberLock.Lock()
		wh.warehouses = []warehouseutils.WarehouseT{}
		allSources := config.Data.(backendconfig.SourcesT)
		for _, source := range allSources.Sources {
			if len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if destination.DestinationDefinition.Name == wh.destType {
						wh.warehouses = append(wh.warehouses, warehouseutils.WarehouseT{Source: source, Destination: destination})
					}
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

func (wh *HandleT) getStagingFiles(warehouse warehouseutils.WarehouseT, startID int64, endID int64) ([]*StagingFileT, error) {
	sqlStatement := fmt.Sprintf(`SELECT id, location, source_id, schema, status, created_at
                                FROM %[1]s
								WHERE %[1]s.id >= %[2]v AND %[1]s.id <= %[3]v AND %[1]s.source_id='%[4]s' AND %[1]s.destination_id='%[5]s'
								ORDER BY id ASC`,
		warehouseStagingFilesTable, startID, endID, warehouse.Source.ID, warehouse.Destination.ID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	defer rows.Close()

	var stagingFilesList []*StagingFileT
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &jsonUpload.SourceID, &jsonUpload.Schema,
			&jsonUpload.Status, &jsonUpload.CreatedAt)
		if err != nil {
			panic(err)
		}
		stagingFilesList = append(stagingFilesList, &jsonUpload)
	}

	return stagingFilesList, nil
}

func (wh *HandleT) getPendingStagingFiles(warehouse warehouseutils.WarehouseT) ([]*StagingFileT, error) {
	var lastStagingFileID int64
	sqlStatement := fmt.Sprintf(`SELECT end_staging_file_id FROM %[1]s WHERE %[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND (%[1]s.status= '%[4]s' OR %[1]s.status = '%[5]s') ORDER BY %[1]s.id DESC`, warehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID, warehouseutils.ExportedDataState, warehouseutils.AbortedState)
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&lastStagingFileID)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`SELECT id, location, source_id, schema, status, created_at
                                FROM %[1]s
								WHERE %[1]s.id > %[2]v AND %[1]s.source_id='%[3]s' AND %[1]s.destination_id='%[4]s'
								ORDER BY id ASC`,
		warehouseStagingFilesTable, lastStagingFileID, warehouse.Source.ID, warehouse.Destination.ID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	defer rows.Close()

	var stagingFilesList []*StagingFileT
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &jsonUpload.SourceID, &jsonUpload.Schema,
			&jsonUpload.Status, &jsonUpload.CreatedAt)
		if err != nil {
			panic(err)
		}
		stagingFilesList = append(stagingFilesList, &jsonUpload)
	}

	return stagingFilesList, nil
}

func (wh *HandleT) consolidateSchema(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT) map[string]map[string]string {
	schemaInDB, err := warehouseutils.GetCurrentSchema(wh.dbHandle, warehouse)
	if err != nil {
		panic(err)
	}
	schemaMap := make(map[string]map[string]string)
	for _, upload := range jsonUploadsList {
		var schema map[string]map[string]string
		err := json.Unmarshal(upload.Schema, &schema)
		if err != nil {
			panic(err)
		}
		for tableName, columnMap := range schema {
			if schemaMap[tableName] == nil {
				schemaMap[tableName] = make(map[string]string)
			}
			for columnName, columnType := range columnMap {
				// if column already has a type in db, use that
				if len(schemaInDB.Schema) > 0 {
					if _, ok := schemaInDB.Schema[tableName]; ok {
						if columnTypeInDB, ok := schemaInDB.Schema[tableName][columnName]; ok {
							schemaMap[tableName][columnName] = columnTypeInDB
							continue
						}
					}
				}
				// check if we already set the columnType in schemaMap
				if _, ok := schemaMap[tableName][columnName]; !ok {
					schemaMap[tableName][columnName] = columnType
				}
			}
		}
	}
	return schemaMap
}

func (wh *HandleT) initTableUploads(upload warehouseutils.UploadT, schema map[string]map[string]string) (err error) {
	//Using transactions for bulk copying
	txn, err := wh.dbHandle.Begin()
	if err != nil {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(warehouseTableUploadsTable, "wh_upload_id", "table_name", "status", "error", "created_at", "updated_at"))
	if err != nil {
		return
	}

	tables := make([]string, 0, len(schema))
	for t := range schema {
		tables = append(tables, t)
	}

	for _, table := range tables {
		_, err = stmt.Exec(upload.ID, table, "waiting", "{}", time.Now(), time.Now())
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

func (wh *HandleT) initUpload(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT, schema map[string]map[string]string) warehouseutils.UploadT {
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (source_id, namespace, destination_id, destination_type, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, status, schema, error, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10, $11, $12, $13) RETURNING id`, warehouseUploadsTable)
	logger.Infof("WH: %s: Creating record in wh_load_files id: %v", wh.destType, sqlStatement)
	stmt, err := wh.dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	startJSONID := jsonUploadsList[0].ID
	endJSONID := jsonUploadsList[len(jsonUploadsList)-1].ID
	currentSchema, err := json.Marshal(schema)
	namespace := misc.TruncateStr(strings.ToLower(strcase.ToSnake(warehouseutils.ToSafeDBString(wh.destType, warehouse.Source.Name))), 127)
	row := stmt.QueryRow(warehouse.Source.ID, namespace, warehouse.Destination.ID, wh.destType, startJSONID, endJSONID, 0, 0, warehouseutils.WaitingState, currentSchema, "{}", time.Now(), time.Now())

	var uploadID int64
	err = row.Scan(&uploadID)
	if err != nil {
		panic(err)
	}

	upload := warehouseutils.UploadT{
		ID:                 uploadID,
		Namespace:          namespace,
		SourceID:           warehouse.Source.ID,
		DestinationID:      warehouse.Destination.ID,
		DestinationType:    wh.destType,
		StartStagingFileID: startJSONID,
		EndStagingFileID:   endJSONID,
		Status:             warehouseutils.WaitingState,
		Schema:             schema,
	}

	err = wh.initTableUploads(upload, schema)
	if err != nil {
		// TODO: Handle error / Retry
		logger.Error("WH: Error creating records in wh_table_uploads", err)
	}

	return upload
}

func (wh *HandleT) getPendingUploads(warehouse warehouseutils.WarehouseT) ([]warehouseutils.UploadT, bool) {

	sqlStatement := fmt.Sprintf(`SELECT id, status, schema, namespace, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, error FROM %[1]s WHERE (%[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND %[1]s.status!='%[4]s' AND %[1]s.status!='%[5]s') ORDER BY id asc`, warehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID, warehouseutils.ExportedDataState, warehouseutils.AbortedState)

	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	if err == sql.ErrNoRows {
		return []warehouseutils.UploadT{}, false
	}
	defer rows.Close()

	var uploads []warehouseutils.UploadT
	for rows.Next() {
		var upload warehouseutils.UploadT
		var schema json.RawMessage
		err := rows.Scan(&upload.ID, &upload.Status, &schema, &upload.Namespace, &upload.StartStagingFileID, &upload.EndStagingFileID, &upload.StartLoadFileID, &upload.EndLoadFileID, &upload.Error)
		upload.Schema = warehouseutils.JSONSchemaToMap(schema)
		if err != nil {
			panic(err)
		}
		uploads = append(uploads, upload)
	}

	var anyPending bool
	if len(uploads) > 0 {
		anyPending = true
	}
	return uploads, anyPending
}

func connectionString(warehouse warehouseutils.WarehouseT) string {
	return fmt.Sprintf(`source:%s:destination:%s`, warehouse.Source.ID, warehouse.Destination.ID)
}

func setDestInProgress(warehouse warehouseutils.WarehouseT, starting bool) {
	inProgressMapLock.Lock()
	if starting {
		inProgressMap[connectionString(warehouse)] = true
	} else {
		delete(inProgressMap, connectionString(warehouse))
	}
	inProgressMapLock.Unlock()
}

func isDestInProgress(warehouse warehouseutils.WarehouseT) bool {
	inProgressMapLock.RLock()
	if inProgressMap[connectionString(warehouse)] {
		inProgressMapLock.RUnlock()
		return true
	}
	inProgressMapLock.RUnlock()
	return false
}

func uploadFrequencyExceeded(warehouse warehouseutils.WarehouseT) bool {
	lastExecMapLock.Lock()
	defer lastExecMapLock.Unlock()
	if lastExecTime, ok := lastExecMap[connectionString(warehouse)]; ok && time.Now().Unix()-lastExecTime < uploadFreqInS {
		return true
	}
	lastExecMap[connectionString(warehouse)] = time.Now().Unix()
	return false
}

type WarehouseManager interface {
	Process(config warehouseutils.ConfigT) error
	CrashRecover(config warehouseutils.ConfigT) (err error)
}

func NewWhManager(destType string) (WarehouseManager, error) {
	switch destType {
	case "RS":
		var rs redshift.HandleT
		return &rs, nil
	case "BQ":
		var bq bigquery.HandleT
		return &bq, nil
	case "SNOWFLAKE":
		var sf snowflake.HandleT
		return &sf, nil
	}
	return nil, errors.New("No provider configured for WarehouseManager")
}

func (wh *HandleT) mainLoop() {
	for {
		if !wh.isEnabled {
			time.Sleep(mainLoopSleep)
			continue
		}
		for _, warehouse := range wh.warehouses {
			if isDestInProgress(warehouse) {
				logger.Debugf("WH: Skipping upload loop since %s:%s upload in progess", wh.destType, warehouse.Destination.ID)
				continue
			}
			setDestInProgress(warehouse, true)

			_, ok := inRecoveryMap[warehouse.Destination.ID]
			if ok {
				whManager, err := NewWhManager(wh.destType)
				if err != nil {
					panic(err)
				}
				logger.Infof("WH: Crash recovering for %s:%s", wh.destType, warehouse.Destination.ID)
				err = whManager.CrashRecover(warehouseutils.ConfigT{
					DbHandle:  wh.dbHandle,
					Warehouse: warehouse,
				})
				if err != nil {
					setDestInProgress(warehouse, false)
					continue
				}
				delete(inRecoveryMap, warehouse.Destination.ID)
			}

			// fetch any pending wh_uploads records (query for not successful/aborted uploads)
			pendingUploads, ok := wh.getPendingUploads(warehouse)
			if ok {
				logger.Infof("WH: Found pending uploads: %v for %s:%s", len(pendingUploads), wh.destType, warehouse.Destination.ID)
				jobs := []ProcessStagingFilesJobT{}
				for _, pendingUpload := range pendingUploads {
					stagingFilesList, err := wh.getStagingFiles(warehouse, pendingUpload.StartStagingFileID, pendingUpload.EndStagingFileID)
					if err != nil {
						panic(err)
					}
					job := ProcessStagingFilesJobT{
						List:      stagingFilesList,
						Warehouse: warehouse,
						Upload:    pendingUpload,
					}
					logger.Debugf("WH: Adding job %+v", job)
					jobs = append(jobs, job)
				}
				wh.uploadToWarehouseQ <- jobs
			} else {
				if uploadFrequencyExceeded(warehouse) {
					logger.Debugf("WH: Skipping upload loop since %s:%s upload freq not exceeded", wh.destType, warehouse.Destination.ID)
					setDestInProgress(warehouse, false)
					continue
				}
				// fetch staging files that are not processed yet
				stagingFilesList, err := wh.getPendingStagingFiles(warehouse)
				if err != nil {
					panic(err)
				}
				if len(stagingFilesList) == 0 {
					logger.Debugf("WH: Found no pending staging files for %s:%s", wh.destType, warehouse.Destination.ID)
					setDestInProgress(warehouse, false)
					continue
				}
				logger.Infof("WH: Found %v pending staging files for %s:%s", len(stagingFilesList), wh.destType, warehouse.Destination.ID)

				count := 0
				jobs := []ProcessStagingFilesJobT{}
				// process staging files in batches of stagingFilesBatchSize
				for {
					lastIndex := count + stagingFilesBatchSize
					if lastIndex >= len(stagingFilesList) {
						lastIndex = len(stagingFilesList)
					}
					// merge schemas over all staging files in this batch
					consolidatedSchema := wh.consolidateSchema(warehouse, stagingFilesList[count:lastIndex])
					// create record in wh_uploads to mark start of upload to warehouse flow
					upload := wh.initUpload(warehouse, stagingFilesList[count:lastIndex], consolidatedSchema)
					job := ProcessStagingFilesJobT{
						List:      stagingFilesList[count:lastIndex],
						Warehouse: warehouse,
						Upload:    upload,
					}
					logger.Debugf("WH: Adding job %+v", job)
					jobs = append(jobs, job)
					count += stagingFilesBatchSize
					if count >= len(stagingFilesList) {
						break
					}
				}
				wh.uploadToWarehouseQ <- jobs
			}
		}
		time.Sleep(mainLoopSleep)
	}
}

type PayloadT struct {
	BatchID             string
	UploadID            int64
	StagingFileID       int64
	StagingFileLocation string
	Schema              map[string]map[string]string
	SourceID            string
	DestinationID       string
	DestinationType     string
	DestinationConfig   interface{}
	LoadFileIDs         []int64
}

func (wh *HandleT) createLoadFiles(job *ProcessStagingFilesJobT) (err error) {
	// stat for time taken to process staging files in a single job
	timer := warehouseutils.DestStat(stats.TimerType, "process_staging_files_batch_time", job.Warehouse.Destination.ID)
	timer.Start()

	var jsonIDs []int64
	for _, job := range job.List {
		jsonIDs = append(jsonIDs, job.ID)
	}
	warehouseutils.SetStagingFilesStatus(jsonIDs, warehouseutils.StagingFileExecutingState, wh.dbHandle)

	logger.Debugf("WH: Starting batch processing %v stage files with %v workers for %s:%s", len(job.List), noOfWorkers, wh.destType, job.Warehouse.Destination.ID)
	var messages []pgnotifier.MessageT
	for _, stagingFile := range job.List {
		payload := PayloadT{
			UploadID:            job.Upload.ID,
			StagingFileID:       stagingFile.ID,
			StagingFileLocation: stagingFile.Location,
			Schema:              job.Upload.Schema,
			SourceID:            job.Warehouse.Source.ID,
			DestinationID:       job.Warehouse.Destination.ID,
			DestinationType:     wh.destType,
			DestinationConfig:   job.Warehouse.Destination.Config,
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

	logger.Infof("WH: Publishing %d staging files to PgNotifier", len(messages))
	var loadFileIDs []int64
	ch, err := wh.notifier.Publish("process_staging_file", messages)
	if err != nil {
		panic(err)
	}

	responses := <-ch
	logger.Infof("WH: Received responses from PgNotifier")
	for _, resp := range responses {
		// TODO: make it aborted
		if resp.Status == "aborted" {
			continue
		}
		var payload map[string]interface{}
		err = json.Unmarshal(resp.Payload, &payload)
		if err != nil {
			panic(err)
		}
		respIDs, ok := payload["LoadFileIDs"].([]interface{})
		if !ok {
			panic(err)
		}
		ids := make([]int64, len(respIDs))
		for i := range respIDs {
			ids[i] = int64(respIDs[i].(float64))
		}
		loadFileIDs = append(loadFileIDs, ids...)
	}

	timer.End()
	if err != nil {
		warehouseutils.SetStagingFilesError(jsonIDs, warehouseutils.StagingFileFailedState, wh.dbHandle, err)
		warehouseutils.DestStat(stats.CountType, "process_staging_files_failures", job.Warehouse.Destination.ID).Count(len(job.List))
		return err
	}
	warehouseutils.SetStagingFilesStatus(jsonIDs, warehouseutils.StagingFileSucceededState, wh.dbHandle)
	warehouseutils.DestStat(stats.CountType, "process_staging_files_success", job.Warehouse.Destination.ID).Count(len(job.List))
	warehouseutils.DestStat(stats.CountType, "generate_load_files", job.Warehouse.Destination.ID).Count(len(loadFileIDs))

	sort.Slice(loadFileIDs, func(i, j int) bool { return loadFileIDs[i] < loadFileIDs[j] })
	startLoadFileID := loadFileIDs[0]
	endLoadFileID := loadFileIDs[len(loadFileIDs)-1]

	// update wh_uploads records with end_load_file_id
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, start_load_file_id=$2, end_load_file_id=$3, updated_at=$4 WHERE id=$5`, warehouseUploadsTable)
	_, err = wh.dbHandle.Exec(sqlStatement, warehouseutils.GeneratedLoadFileState, startLoadFileID, endLoadFileID, time.Now(), job.Upload.ID)
	if err != nil {
		panic(err)
	}

	job.Upload.StartLoadFileID = startLoadFileID
	job.Upload.EndLoadFileID = endLoadFileID
	job.Upload.Status = warehouseutils.GeneratedLoadFileState
	return
}

func (wh *HandleT) SyncLoadFilesToWarehouse(job *ProcessStagingFilesJobT) (err error) {
	logger.Infof("WH: Starting load flow for %s:%s", wh.destType, job.Warehouse.Destination.ID)
	whManager, err := NewWhManager(wh.destType)
	if err != nil {
		panic(err)
	}
	err = whManager.Process(warehouseutils.ConfigT{
		DbHandle:  wh.dbHandle,
		Upload:    job.Upload,
		Warehouse: job.Warehouse,
	})
	return
}

func (wh *HandleT) initWorkers() {
	for i := 0; i < noOfWorkers; i++ {
		rruntime.Go(func() {
			func() {
				for {
					// handle job to process staging files and convert them into load files
					processStagingFilesJobList := <-wh.uploadToWarehouseQ
					whOneFullPassTimer := warehouseutils.DestStat(stats.TimerType, "total_end_to_end_step_time", processStagingFilesJobList[0].Warehouse.Destination.ID)
					whOneFullPassTimer.Start()
					for _, job := range processStagingFilesJobList {
						createPlusUploadTimer := warehouseutils.DestStat(stats.TimerType, "stagingfileset_total_handling_time", job.Warehouse.Destination.ID)
						createPlusUploadTimer.Start()

						// generate load files only if not done before
						// upload records have start_load_file_id and end_load_file_id set to 0 on creation
						// and are updated on creation of load files
						logger.Infof("WH: Processing %d staging files in upload job:%v with staging files from %v to %v for %s:%s", len(job.List), job.Upload.ID, job.List[0].ID, job.List[len(job.List)-1].ID, wh.destType, job.Warehouse.Destination.ID)
						if job.Upload.StartLoadFileID == 0 {
							warehouseutils.SetUploadStatus(job.Upload, warehouseutils.GeneratingLoadFileState, wh.dbHandle)
							err := wh.createLoadFiles(&job)
							if err != nil {
								//Unreachable code. So not modifying the stat 'failed_uploads', which is reused later for copying.
								warehouseutils.DestStat(stats.CountType, "failed_uploads", job.Warehouse.Destination.ID).Count(1)
								warehouseutils.SetUploadError(job.Upload, err, warehouseutils.GeneratingLoadFileFailedState, wh.dbHandle)
								break
							}
						}
						err := wh.SyncLoadFilesToWarehouse(&job)

						createPlusUploadTimer.End()

						if err != nil {
							warehouseutils.DestStat(stats.CountType, "failed_uploads", job.Warehouse.Destination.ID).Count(1)
							break
						}
						warehouseutils.DestStat(stats.CountType, "load_staging_files_into_warehouse", job.Warehouse.Destination.ID).Count(len(job.List))
					}
					if whOneFullPassTimer != nil {
						whOneFullPassTimer.End()
					}
					setDestInProgress(processStagingFilesJobList[0].Warehouse, false)
				}
			}()
		})
	}
}

func getBucketFolder(batchID string, tableName string) string {
	return fmt.Sprintf(`%v-%v`, batchID, tableName)
}

func (wh *HandleT) setupTables() {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
									  id BIGSERIAL PRIMARY KEY,
									  staging_file_id BIGINT,
									  location TEXT NOT NULL,
									  source_id VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  destination_type VARCHAR(64) NOT NULL,
									  table_name VARCHAR(64) NOT NULL,
									  created_at TIMESTAMP NOT NULL);`, warehouseLoadFilesTable)

	_, err := wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// change table_name type to text to support table_names upto length 127
	sqlStatement = fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s TYPE TEXT`, warehouseLoadFilesTable, "table_name")
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// index on source_id, destination_id combination
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_source_destination_id_index ON %[1]s (source_id, destination_id);`, warehouseLoadFilesTable)
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = `DO $$ BEGIN
                                CREATE TYPE wh_upload_state_type
                                     AS ENUM(
										 	  'waiting',
											  'generating_load_file',
											  'generating_load_file_failed',
											  'generated_load_file',
											  'updating_schema',
											  'updating_schema_failed',
											  'updated_schema',
											  'exporting_data',
											  'exporting_data_failed',
											  'exported_data',
											  'aborted');
                                     EXCEPTION
                                        WHEN duplicate_object THEN null;
                            END $$;`

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = `ALTER TYPE wh_upload_state_type ADD VALUE IF NOT EXISTS 'waiting';`
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
	sqlStatement = `ALTER TYPE wh_upload_state_type ADD VALUE IF NOT EXISTS 'aborted';`
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
                                      id BIGSERIAL PRIMARY KEY,
									  source_id VARCHAR(64) NOT NULL,
									  namespace VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  destination_type VARCHAR(64) NOT NULL,
									  start_staging_file_id BIGINT,
									  end_staging_file_id BIGINT,
									  start_load_file_id BIGINT,
									  end_load_file_id BIGINT,
									  status wh_upload_state_type NOT NULL,
									  schema JSONB NOT NULL,
									  error JSONB,
									  created_at TIMESTAMP NOT NULL,
									  updated_at TIMESTAMP NOT NULL);`, warehouseUploadsTable)

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// index on status
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_status_index ON %[1]s (status);`, warehouseUploadsTable)
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// index on source_id, destination_id combination
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_source_destination_id_index ON %[1]s (source_id, destination_id);`, warehouseUploadsTable)
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
									  id BIGSERIAL PRIMARY KEY,
									  wh_upload_id BIGSERIAL,
									  source_id VARCHAR(64) NOT NULL,
									  namespace VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  destination_type VARCHAR(64) NOT NULL,
									  schema JSONB NOT NULL,
									  error TEXT,
									  created_at TIMESTAMP NOT NULL);`, warehouseSchemasTable)

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// index on source_id, destination_id combination
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_source_destination_id_index ON %[1]s (source_id, destination_id);`, warehouseSchemasTable)
	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
}

//Enable enables a router :)
func (wh *HandleT) Enable() {
	wh.isEnabled = true
}

//Disable disables a router:)
func (wh *HandleT) Disable() {
	wh.isEnabled = false
}

func (wh *HandleT) setInterruptedDestinations() (err error) {
	if !misc.Contains(crashRecoverWarehouses, wh.destType) {
		return
	}
	sqlStatement := fmt.Sprintf(`SELECT destination_id FROM %s WHERE destination_type='%s' AND (status='%s' OR status='%s')`, warehouseUploadsTable, wh.destType, warehouseutils.ExportingDataState, warehouseutils.ExportingDataFailedState)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var destID string
		err := rows.Scan(&destID)
		if err != nil {
			panic(err)
		}
		inRecoveryMap[destID] = true
	}
	return err
}

func (wh *HandleT) Setup(whType string) {
	logger.Infof("WH: Warehouse Router started: %s", whType)
	wh.dbHandle = dbHandle
	wh.notifier = notifier
	wh.destType = whType
	wh.setInterruptedDestinations()
	wh.Enable()
	wh.uploadToWarehouseQ = make(chan []ProcessStagingFilesJobT)
	wh.createLoadFilesQ = make(chan LoadFileJobT)
	rruntime.Go(func() {
		wh.backendConfigSubscriber()
	})
	rruntime.Go(func() {
		wh.initWorkers()
	})
	rruntime.Go(func() {
		wh.mainLoop()
	})
}

var loadFileFormatMap = map[string]string{
	"BQ":        "json",
	"RS":        "csv",
	"SNOWFLAKE": "csv",
}

func processStagingFile(job PayloadT) (loadFileIDs []int64, err error) {
	logger.Debugf("WH: Starting processing staging file: %v at %v for %s:%s", job.StagingFileID, job.StagingFileLocation, job.DestinationType, job.DestinationID)
	// download staging file into a temp dir
	dirName := "/rudder-warehouse-json-uploads-tmp/"
	tmpDirPath, err := misc.CreateTMPDIR()
	if err != nil {
		panic(err)
	}
	jsonPath := tmpDirPath + dirName + fmt.Sprintf(`%s_%s/`, job.DestinationType, job.DestinationID) + job.StagingFileLocation
	err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
	jsonFile, err := os.Create(jsonPath)
	if err != nil {
		panic(err)
	}

	downloader, err := filemanager.New(&filemanager.SettingsT{
		Provider: warehouseutils.ObjectStorageMap[job.DestinationType],
		Config:   job.DestinationConfig.(map[string]interface{}),
	})
	if err != nil {
		return loadFileIDs, err
	}

	timer := warehouseutils.DestStat(stats.TimerType, "download_staging_file_time", job.DestinationID)
	timer.Start()

	err = downloader.Download(jsonFile, job.StagingFileLocation)
	if err != nil {
		return loadFileIDs, err
	}
	jsonFile.Close()
	defer os.Remove(jsonPath)
	timer.End()

	fi, err := os.Stat(jsonPath)
	if err != nil {
		logger.Errorf("WH: Error getting file size of downloaded staging file: ", err)
	}
	fileSize := fi.Size()
	logger.Debugf("WH: Downloaded staging file %s size:%v", job.StagingFileLocation, fileSize)
	warehouseutils.DestStat(stats.CountType, "downloaded_staging_file_size", job.DestinationID).Count(int(fileSize))

	sortedTableColumnMap := make(map[string][]string)
	// sort columns per table so as to maintaing same order in load file (needed in case of csv load file)
	for tableName, columnMap := range job.Schema {
		sortedTableColumnMap[tableName] = []string{}
		for k := range columnMap {
			sortedTableColumnMap[tableName] = append(sortedTableColumnMap[tableName], k)
		}
		sort.Strings(sortedTableColumnMap[tableName])
	}

	logger.Debugf("Starting read from downloaded staging file: %s", job.StagingFileLocation)
	rawf, err := os.Open(jsonPath)
	if err != nil {
		logger.Errorf("WH: Error opening file using os.Open at path:%s downloaded from %s", jsonPath, job.StagingFileLocation)
		panic(err)
	}
	reader, err := gzip.NewReader(rawf)
	if err != nil {
		if err.Error() == "EOF" {
			return loadFileIDs, nil
		}
		logger.Errorf("WH: Error reading file using gzip.NewReader at path:%s downloaded from %s", jsonPath, job.StagingFileLocation)
		panic(err)
	}

	// read from staging file and write a separate load file for each table in warehouse
	// tableContentMap := make(map[string]string)
	outputFileMap := make(map[string]misc.GZipWriter)
	uuidTS := time.Now()
	sc := bufio.NewScanner(reader)
	misc.PrintMemUsage()

	timer = warehouseutils.DestStat(stats.TimerType, "process_staging_file_to_csv_time", job.DestinationID)
	timer.Start()

	lineBytesCounter := 0
	for sc.Scan() {
		lineBytes := sc.Bytes()
		lineBytesCounter += len(lineBytes)
		var jsonLine map[string]interface{}
		json.Unmarshal(lineBytes, &jsonLine)
		metadata, ok := jsonLine["metadata"]
		if !ok {
			continue
		}
		columnData, ok := jsonLine["data"].(map[string]interface{})
		if !ok {
			continue
		}
		tableName, ok := metadata.(map[string]interface{})["table"].(string)
		if !ok {
			continue
		}
		columns, ok := metadata.(map[string]interface{})["columns"].(map[string]interface{})
		if !ok {
			continue
		}
		if _, ok := outputFileMap[tableName]; !ok {
			outputFilePath := strings.TrimSuffix(jsonPath, "json.gz") + tableName + fmt.Sprintf(`.%s`, loadFileFormatMap[job.DestinationType]) + ".gz"
			gzWriter, err := misc.CreateGZ(outputFilePath)
			defer gzWriter.CloseGZ()
			if err != nil {
				return nil, err
			}
			outputFileMap[tableName] = gzWriter
		}
		if job.DestinationType == "BQ" {
			for _, columnName := range sortedTableColumnMap[tableName] {
				if columnName == "uuid_ts" {
					// add uuid_ts to track when event was processed into load_file
					columnData["uuid_ts"] = uuidTS.Format("2006-01-02 15:04:05 Z")
					continue
				}
				columnVal, ok := columnData[columnName]
				if !ok {
					continue
				}
				columnType, ok := columns[columnName].(string)
				// json.Unmarshal returns int as float
				// convert int's back to int to avoid writing integers like 123456789 as 1.23456789e+08
				// most warehouses only support scientific notation only for floats and not integers
				if columnType == "int" || columnType == "bigint" {
					columnData[columnName] = int(columnVal.(float64))
				}
				// if the current data type doesnt match the one in warehouse, set value as NULL
				dataTypeInSchema := job.Schema[tableName][columnName]
				if ok && columnType != dataTypeInSchema {
					if (columnType == "int" || columnType == "bigint") && dataTypeInSchema == "float" {
						// pass it along
					} else if columnType == "float" && (dataTypeInSchema == "int" || dataTypeInSchema == "bigint") {
						columnData[columnName] = int(columnVal.(float64))
					} else {
						columnData[columnName] = nil
						continue
					}
				}

			}
			line, err := json.Marshal(columnData)
			if err != nil {
				return loadFileIDs, err
			}
			outputFileMap[tableName].WriteGZ(string(line) + "\n")
		} else {
			csvRow := []string{}
			var buff bytes.Buffer
			csvWriter := csv.NewWriter(&buff)
			for _, columnName := range sortedTableColumnMap[tableName] {
				if columnName == "uuid_ts" {
					// add uuid_ts to track when event was processed into load_file
					csvRow = append(csvRow, fmt.Sprintf("%v", uuidTS.Format(misc.RFC3339Milli)))
					continue
				}
				columnVal, ok := columnData[columnName]
				if !ok {
					csvRow = append(csvRow, "")
					continue
				}

				columnType, ok := columns[columnName].(string)
				// json.Unmarshal returns int as float
				// convert int's back to int to avoid writing integers like 123456789 as 1.23456789e+08
				// most warehouses only support scientific notation only for floats and not integers
				if columnType == "int" || columnType == "bigint" {
					columnVal = int(columnVal.(float64))
				}
				// if the current data type doesnt match the one in warehouse, set value as NULL
				dataTypeInSchema := job.Schema[tableName][columnName]
				if ok && columnType != dataTypeInSchema {
					if (columnType == "int" || columnType == "bigint") && dataTypeInSchema == "float" {
						// pass it along
					} else if columnType == "float" && (dataTypeInSchema == "int" || dataTypeInSchema == "bigint") {
						columnVal = int(columnVal.(float64))
					} else {
						csvRow = append(csvRow, "")
						continue
					}
				}
				csvRow = append(csvRow, fmt.Sprintf("%v", columnVal))
			}
			csvWriter.Write(csvRow)
			csvWriter.Flush()
			outputFileMap[tableName].WriteGZ(buff.String())
		}
	}
	reader.Close()
	timer.End()
	misc.PrintMemUsage()

	logger.Debugf("Process %v bytes from downloaded staging file: %s", lineBytesCounter, job.StagingFileLocation)
	warehouseutils.DestStat(stats.CountType, "bytes_processed_in_staging_file", job.DestinationID).Count(lineBytesCounter)

	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: warehouseutils.ObjectStorageMap[job.DestinationType],
		Config:   job.DestinationConfig.(map[string]interface{}),
	})
	if err != nil {
		panic(err)
	}

	timer = warehouseutils.DestStat(stats.TimerType, "upload_load_files_per_staging_file_time", job.DestinationID)
	timer.Start()
	for tableName, outputFile := range outputFileMap {
		outputFile.CloseGZ()
		file, err := os.Open(outputFile.File.Name())
		defer os.Remove(outputFile.File.Name())
		logger.Debugf("WH: %s: Uploading load_file to %s for table: %s in staging_file id: %v", job.DestinationType, warehouseutils.ObjectStorageMap[job.DestinationType], tableName, job.StagingFileID)
		uploadLocation, err := uploader.Upload(file, config.GetEnv("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", "rudder-warehouse-load-objects"), tableName, job.SourceID, getBucketFolder(job.BatchID, tableName))
		// tableName, job.Warehouse.Source.ID, fmt.Sprintf(`%v-%v`, strconv.FormatInt(job.Upload.ID, 10), uuid.NewV4().String()))
		if err != nil {
			return loadFileIDs, err
		}
		sqlStatement := fmt.Sprintf(`INSERT INTO %s (staging_file_id, location, source_id, destination_id, destination_type, table_name, created_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`, warehouseLoadFilesTable)
		stmt, err := dbHandle.Prepare(sqlStatement)
		if err != nil {
			panic(err)
		}
		defer stmt.Close()

		var fileID int64
		err = stmt.QueryRow(job.StagingFileID, uploadLocation.Location, job.SourceID, job.DestinationID, job.DestinationType, tableName, time.Now()).Scan(&fileID)
		if err != nil {
			panic(err)
		}
		loadFileIDs = append(loadFileIDs, fileID)
	}
	timer.End()
	return
}

// Gets the config from config backend and extracts enabled writekeys
func monitorDestRouters() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, "backendConfig")
	dstToWhRouter := make(map[string]*HandleT)

	for {
		config := <-ch
		logger.Debug("Got config from config-backend", config)
		sources := config.Data.(backendconfig.SourcesT)
		enabledDestinations := make(map[string]bool)
		for _, source := range sources.Sources {
			for _, destination := range source.Destinations {
				enabledDestinations[destination.DestinationDefinition.Name] = true
				if misc.Contains(warehouseDestinations, destination.DestinationDefinition.Name) {
					wh, ok := dstToWhRouter[destination.DestinationDefinition.Name]
					if !ok {
						logger.Info("Starting a new Warehouse Destination Router: ", destination.DestinationDefinition.Name)
						var wh HandleT
						wh.Setup(destination.DestinationDefinition.Name)
						dstToWhRouter[destination.DestinationDefinition.Name] = &wh
					} else {
						logger.Debug("Enabling existing Destination: ", destination.DestinationDefinition.Name)
						wh.Enable()
					}
				}
			}
		}

		keys := misc.StringKeys(dstToWhRouter)
		for _, key := range keys {
			if _, ok := enabledDestinations[key]; !ok {
				if whHandle, ok := dstToWhRouter[key]; ok {
					logger.Info("Disabling a existing warehouse destination: ", key)
					whHandle.Disable()
				}
			}
		}
	}
}

func setupTables(dbHandle *sql.DB) {
	sqlStatement := `DO $$ BEGIN
                                CREATE TYPE wh_staging_state_type
                                     AS ENUM(
                                              'waiting',
                                              'executing',
											  'failed',
											  'succeeded');
                                     EXCEPTION
                                        WHEN duplicate_object THEN null;
                            END $$;`

	_, err := dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
                                      id BIGSERIAL PRIMARY KEY,
									  location TEXT NOT NULL,
									  source_id VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  schema JSONB NOT NULL,
									  error TEXT,
									  status wh_staging_state_type,
									  created_at TIMESTAMP NOT NULL,
									  updated_at TIMESTAMP NOT NULL);`, warehouseStagingFilesTable)

	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// index on source_id, destination_id combination
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_id_index ON %[1]s (source_id, destination_id);`, warehouseStagingFilesTable)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
									  id BIGSERIAL PRIMARY KEY,
									  staging_file_id BIGINT,
									  location TEXT NOT NULL,
									  source_id VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  destination_type VARCHAR(64) NOT NULL,
									  table_name VARCHAR(64) NOT NULL,
									  created_at TIMESTAMP NOT NULL);`, warehouseLoadFilesTable)

	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// change table_name type to text to support table_names upto length 127
	sqlStatement = fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s TYPE TEXT`, warehouseLoadFilesTable, "table_name")
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// index on source_id, destination_id, table_name combination
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_source_destination_id_table_name_index ON %[1]s (source_id, destination_id, table_name);`, warehouseLoadFilesTable)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = `DO $$ BEGIN
                                CREATE TYPE wh_upload_state_type
                                     AS ENUM(
										 	  'waiting',
											  'generating_load_file',
											  'generating_load_file_failed',
											  'generated_load_file',
											  'updating_schema',
											  'updating_schema_failed',
											  'updated_schema',
											  'exporting_data',
											  'exporting_data_failed',
											  'exported_data',
											  'aborted');
                                     EXCEPTION
                                        WHEN duplicate_object THEN null;
                            END $$;`

	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = `ALTER TYPE wh_upload_state_type ADD VALUE IF NOT EXISTS 'waiting';`
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
	sqlStatement = `ALTER TYPE wh_upload_state_type ADD VALUE IF NOT EXISTS 'aborted';`
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
                                      id BIGSERIAL PRIMARY KEY,
									  source_id VARCHAR(64) NOT NULL,
									  namespace VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  destination_type VARCHAR(64) NOT NULL,
									  start_staging_file_id BIGINT,
									  end_staging_file_id BIGINT,
									  start_load_file_id BIGINT,
									  end_load_file_id BIGINT,
									  status wh_upload_state_type NOT NULL,
									  schema JSONB NOT NULL,
									  error JSONB,
									  created_at TIMESTAMP NOT NULL,
									  updated_at TIMESTAMP NOT NULL);`, warehouseUploadsTable)

	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// index on status
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_status_index ON %[1]s (status);`, warehouseUploadsTable)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// index on source_id, destination_id combination
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_source_destination_id_index ON %[1]s (source_id, destination_id);`, warehouseUploadsTable)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = `DO $$ BEGIN
                                CREATE TYPE wh_table_upload_state_type
                                     AS ENUM(
											  'waiting',
											  'executing',
											  'exporting_data',
											  'exporting_data_failed',
											  'exported_data',
											  'aborted');
                                     EXCEPTION
                                        WHEN duplicate_object THEN null;
                            END $$;`

	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
                                      id BIGSERIAL PRIMARY KEY,
									  wh_upload_id BIGSERIAL NOT NULL,
									  table_name VARCHAR(64),
									  status wh_table_upload_state_type NOT NULL,
									  error TEXT,
									  last_exec_time TIMESTAMP,
									  created_at TIMESTAMP NOT NULL,
									  updated_at TIMESTAMP NOT NULL);`, warehouseTableUploadsTable)

	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// change error type to text
	sqlStatement = fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %s TYPE TEXT`, warehouseTableUploadsTable, "error")
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
									  id BIGSERIAL PRIMARY KEY,
									  wh_upload_id BIGSERIAL,
									  source_id VARCHAR(64) NOT NULL,
									  namespace VARCHAR(64) NOT NULL,
									  destination_id VARCHAR(64) NOT NULL,
									  destination_type VARCHAR(64) NOT NULL,
									  schema JSONB NOT NULL,
									  error TEXT,
									  created_at TIMESTAMP NOT NULL);`, warehouseSchemasTable)

	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	// index on source_id, destination_id combination
	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %[1]s_source_destination_id_index ON %[1]s (source_id, destination_id);`, warehouseSchemasTable)
	_, err = dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
}

func CheckPGHealth() bool {
	rows, err := dbHandle.Query(fmt.Sprintf(`SELECT 'Rudder Warehouse DB Health Check'::text as message`))
	if err != nil {
		logger.Error(err)
		return false
	}
	defer rows.Close()
	return true
}

func processHandler(w http.ResponseWriter, r *http.Request) {
	logger.LogRequest(r)

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var stagingFile warehouseutils.StagingFileT
	json.Unmarshal(body, &stagingFile)

	logger.Debugf("BRT: Creating record for uploaded json in %s table with schema: %+v", warehouseStagingFilesTable, stagingFile.Schema)
	schemaPayload, err := json.Marshal(stagingFile.Schema)
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (location, schema, source_id, destination_id, status, created_at, updated_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $6)`, warehouseStagingFilesTable)
	stmt, err := dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(stagingFile.Location, schemaPayload, stagingFile.BatchDestination.Source.ID, stagingFile.BatchDestination.Destination.ID, warehouseutils.StagingFileWaitingState, time.Now())
	if err != nil {
		panic(err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	var dbService string = "UP"
	if !CheckPGHealth() {
		dbService = "DOWN"
	}
	healthVal := fmt.Sprintf(`{"server":"UP", "db":"%s","acceptingEvents":"TRUE","warehouseMode":"%s","goroutines":"%d"}`, dbService, strings.ToUpper(warehouseMode), runtime.NumGoroutine())
	w.Write([]byte(healthVal))
}

func getConnectionString() string {
	if warehouseMode == config.EmbeddedMode {
		return jobsdb.GetConnectionString()
	}
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
}

func startWebHandler() {
	// do not register same endpoint when running embedded in rudder backend
	if isStandAlone() {
		http.HandleFunc("/health", healthHandler)
	}
	if isMaster() {
		backendconfig.WaitForConfig()
		http.HandleFunc("/v1/process", processHandler)
		logger.Infof("WH: Starting warehouse master service in %d", webPort)
	} else {
		webPort = 8083
		logger.Infof("WH: Starting warehouse slave service in %d", webPort)
	}
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(webPort), bugsnag.Handler(nil)))
}

func claimAndProcess(workerIdx int, slaveID string) {
	logger.Infof("WH: Attempting to claim job by slave worker-%v-%v", workerIdx, slaveID)
	workerID := warehouseutils.GetSlaveWorkerId(workerIdx, slaveID)
	claim, claimed := notifier.Claim(workerID)
	if claimed {
		logger.Infof("WH: Successfully claimed job:%v by slave worker-%v-%v", claim.ID, workerIdx, slaveID)
		var payload PayloadT
		json.Unmarshal(claim.Payload, &payload)
		payload.BatchID = claim.BatchID
		ids, err := processStagingFile(payload)
		if err != nil {
			response := pgnotifier.ClaimResponseT{
				Err: err,
			}
			claim.ClaimResponseChan <- response
		}
		payload.LoadFileIDs = ids
		output, err := json.Marshal(payload)
		response := pgnotifier.ClaimResponseT{
			Err:     err,
			Payload: output,
		}
		claim.ClaimResponseChan <- response
	}
	slaveWorkerRoutineBusy[workerIdx-1] = false
	logger.Infof("WH: Setting free slave worker %d: %v", workerIdx, slaveWorkerRoutineBusy)
}

func setupSlave() {
	slaveWorkerRoutineBusy = make([]bool, noOfSlaveWorkerRoutines)
	slaveID := uuid.NewV4().String()
	rruntime.Go(func() {
		jobNotificationChannel, err := notifier.Subscribe("process_staging_file")
		if err != nil {
			panic(err)
		}
		for {
			ev := <-jobNotificationChannel
			logger.Infof("WH: Notification recieved, event: %v, workers: %v", ev, slaveWorkerRoutineBusy)
			for workerIdx := 1; workerIdx <= noOfSlaveWorkerRoutines; workerIdx++ {
				if !slaveWorkerRoutineBusy[workerIdx-1] {
					slaveWorkerRoutineBusy[workerIdx-1] = true
					idx := workerIdx
					rruntime.Go(func() {
						claimAndProcess(idx, slaveID)
					})
					break
				}
			}
		}
	})
}

func isStandAlone() bool {
	return warehouseMode != EmbeddedMode
}

func isMaster() bool {
	return warehouseMode == config.MasterMode || warehouseMode == config.MasterSlaveMode || warehouseMode == config.EmbeddedMode
}

func isSlave() bool {
	return warehouseMode == config.SlaveMode || warehouseMode == config.MasterSlaveMode || warehouseMode == config.EmbeddedMode
}

func Start() {
	time.Sleep(1 * time.Second)
	// do not start warehouse service if rudder core is not in normal mode and warehouse is running in same process as rudder core
	if !isStandAlone() && !db.IsNormalMode() {
		logger.Infof("Skipping start of warehouse service...")
		return
	}

	logger.Infof("WH: Starting Warehouse service...")
	var err error
	psqlInfo := getConnectionString()

	if !misc.IsPostgresCompatible(psqlInfo) {
		err := errors.New("Rudder Warehouse Service needs postgres version >= 10. Exiting")
		logger.Error(err)
		panic(err)
	}

	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	setupTables(dbHandle)
	notifier, err = pgnotifier.New(psqlInfo)
	if err != nil {
		panic(err)
	}

	if isSlave() {
		logger.Infof("WH: Starting warehouse slave...")
		setupSlave()
	}

	if isMaster() {
		backendconfig.Setup()
		logger.Infof("WH: Starting warehouse master...")
		err = notifier.AddTopic("process_staging_file")
		if err != nil {
			panic(err)
		}
		rruntime.Go(func() {
			monitorDestRouters()
		})
	}

	startWebHandler()
}
