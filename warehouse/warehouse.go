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
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/destination-debugger"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	uuid "github.com/satori/go.uuid"
)

var (
	webPort                          int
	dbHandle                         *sql.DB
	notifier                         pgnotifier.PgNotifierT
	warehouseDestinations            []string
	jobQueryBatchSize                int
	noOfWorkers                      int
	noOfSlaveWorkerRoutines          int
	slaveWorkerRoutineBusy           []bool //Busy-true
	uploadFreqInS                    int64
	stagingFilesSchemaPaginationSize int
	mainLoopSleep                    time.Duration
	stagingFilesBatchSize            int
	configSubscriberLock             sync.RWMutex
	crashRecoverWarehouses           []string
	inProgressMap                    map[string]bool
	inRecoveryMap                    map[string]bool
	inProgressMapLock                sync.RWMutex
	lastExecMap                      map[string]int64
	lastExecMapLock                  sync.RWMutex
	warehouseMode                    string
	warehouseSyncPreFetchCount       int
	warehouseSyncFreqIgnore          bool
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
	ID           int64
	Location     string
	SourceID     string
	Schema       json.RawMessage
	Status       string // enum
	CreatedAt    time.Time
	FirstEventAt time.Time
	LastEventAt  time.Time
}

type ErrorResponseT struct {
	Error string
}

func init() {
	loadConfig()
}

func loadConfig() {
	//Port where WH is running
	webPort = config.GetInt("Warehouse.webPort", 8082)
	warehouseDestinations = []string{"RS", "BQ", "SNOWFLAKE", "POSTGRES"}
	jobQueryBatchSize = config.GetInt("Router.jobQueryBatchSize", 10000)
	noOfWorkers = config.GetInt("Warehouse.noOfWorkers", 8)
	noOfSlaveWorkerRoutines = config.GetInt("Warehouse.noOfSlaveWorkerRoutines", 4)
	stagingFilesBatchSize = config.GetInt("Warehouse.stagingFilesBatchSize", 240)
	uploadFreqInS = config.GetInt64("Warehouse.uploadFreqInS", 1800)
	mainLoopSleep = config.GetDuration("Warehouse.mainLoopSleepInS", 60) * time.Second
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
	warehouseSyncPreFetchCount = config.GetInt("Warehouse.warehouseSyncPreFetchCount", 10)
	stagingFilesSchemaPaginationSize = config.GetInt("Warehouse.stagingFilesSchemaPaginationSize", 100)
	warehouseSyncFreqIgnore = config.GetBool("Warehouse.warehouseSyncFreqIgnore", false)
}

func (wh *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	for {
		config := <-ch
		configSubscriberLock.Lock()
		wh.warehouses = []warehouseutils.WarehouseT{}
		allSources := config.Data.(backendconfig.SourcesT)
		for _, source := range allSources.Sources {
			if len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if destination.DestinationDefinition.Name == wh.destType {
						namespace := wh.getNamespace(destination.Config, source, destination, wh.destType)
						wh.warehouses = append(wh.warehouses, warehouseutils.WarehouseT{Source: source, Destination: destination, Namespace: namespace})
						if destination.Config != nil && destination.Enabled && destination.Config["eventDelivery"] == true {
							sourceID := source.ID
							destinationID := destination.ID
							rruntime.Go(func() {
								wh.syncLiveWarehouseStatus(sourceID, destinationID)
							})
						}
					}
				}
			}
		}
		configSubscriberLock.Unlock()
	}
}

func (wh *HandleT) syncLiveWarehouseStatus(sourceID string, destinationID string) {
	rows, err := wh.dbHandle.Query(fmt.Sprintf(`select id from %s where source_id='%s' and destination_id='%s' order by updated_at asc limit %d`, warehouseutils.WarehouseUploadsTable, sourceID, destinationID, warehouseSyncPreFetchCount))
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	defer rows.Close()
	var uploadIDs []int64
	for rows.Next() {
		var uploadID int64
		rows.Scan(&uploadID)
		uploadIDs = append(uploadIDs, uploadID)
	}
	for _, uploadID := range uploadIDs {
		wh.recordDeliveryStatus(uploadID)
	}
}

// getNamespace sets namespace name in the following order
// 	1. user set name from destinationConfig
// 	2. from existing record in wh_schemas with same source + dest combo
// 	3. convert source name
func (wh *HandleT) getNamespace(config interface{}, source backendconfig.SourceT, destination backendconfig.DestinationT, destType string) string {
	configMap := config.(map[string]interface{})
	var namespace string
	if configMap["namespace"] != nil {
		namespace = configMap["namespace"].(string)
		if len(strings.TrimSpace(namespace)) > 0 {
			return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, namespace))
		}
	}
	var exists bool
	if namespace, exists = warehouseutils.GetNamespace(source, destination, wh.dbHandle); !exists {
		namespace = warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, source.Name))
	}
	return namespace
}

func (wh *HandleT) getStagingFiles(warehouse warehouseutils.WarehouseT, startID int64, endID int64) ([]*StagingFileT, error) {
	sqlStatement := fmt.Sprintf(`SELECT id, location
                                FROM %[1]s
								WHERE %[1]s.id >= %[2]v AND %[1]s.id <= %[3]v AND %[1]s.source_id='%[4]s' AND %[1]s.destination_id='%[5]s'
								ORDER BY id ASC`,
		warehouseutils.WarehouseStagingFilesTable, startID, endID, warehouse.Source.ID, warehouse.Destination.ID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	defer rows.Close()

	var stagingFilesList []*StagingFileT
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location)
		if err != nil {
			panic(err)
		}
		stagingFilesList = append(stagingFilesList, &jsonUpload)
	}

	return stagingFilesList, nil
}

func (wh *HandleT) getPendingStagingFiles(warehouse warehouseutils.WarehouseT) ([]*StagingFileT, error) {
	var lastStagingFileID int64
	sqlStatement := fmt.Sprintf(`SELECT end_staging_file_id FROM %[1]s WHERE %[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND (%[1]s.status= '%[4]s' OR %[1]s.status = '%[5]s') ORDER BY %[1]s.id DESC`, warehouseutils.WarehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID, warehouseutils.ExportedDataState, warehouseutils.AbortedState)
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&lastStagingFileID)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`SELECT id, location, first_event_at, last_event_at
                                FROM %[1]s
								WHERE %[1]s.id > %[2]v AND %[1]s.source_id='%[3]s' AND %[1]s.destination_id='%[4]s'
								ORDER BY id ASC`,
		warehouseutils.WarehouseStagingFilesTable, lastStagingFileID, warehouse.Source.ID, warehouse.Destination.ID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	defer rows.Close()

	var stagingFilesList []*StagingFileT
	var firstEventAt, lastEventAt sql.NullTime
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &firstEventAt, &lastEventAt)
		if err != nil {
			panic(err)
		}
		jsonUpload.FirstEventAt = firstEventAt.Time
		jsonUpload.LastEventAt = lastEventAt.Time
		stagingFilesList = append(stagingFilesList, &jsonUpload)
	}

	return stagingFilesList, nil
}

func (wh *HandleT) mergeSchema(currentSchema map[string]map[string]string, schemaList []map[string]map[string]string) map[string]map[string]string {
	schemaMap := make(map[string]map[string]string)
	for _, schema := range schemaList {
		for tableName, columnMap := range schema {
			if schemaMap[tableName] == nil {
				schemaMap[tableName] = make(map[string]string)
			}
			for columnName, columnType := range columnMap {
				// if column already has a type in db, use that
				if len(currentSchema) > 0 {

					if _, ok := currentSchema[tableName]; ok {
						if columnTypeInDB, ok := currentSchema[tableName][columnName]; ok {
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

func (wh *HandleT) consolidateSchema(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT) map[string]map[string]string {
	schemaInDB, err := warehouseutils.GetCurrentSchema(wh.dbHandle, warehouse)
	if err != nil {
		panic(err)
	}
	currSchema := schemaInDB

	count := 0
	for {
		lastIndex := count + stagingFilesSchemaPaginationSize
		if lastIndex >= len(jsonUploadsList) {
			lastIndex = len(jsonUploadsList)
		}

		var ids []int64
		for _, upload := range jsonUploadsList[count:lastIndex] {
			ids = append(ids, upload.ID)
		}

		sqlStatement := fmt.Sprintf(`SELECT schema FROM %s WHERE id IN (%s)`, warehouseutils.WarehouseStagingFilesTable, misc.IntArrayToString(ids, ","))
		rows, err := wh.dbHandle.Query(sqlStatement)
		if err != nil && err != sql.ErrNoRows {
			panic(err)
		}

		var schemas []map[string]map[string]string
		for rows.Next() {
			var s json.RawMessage
			err := rows.Scan(&s)
			if err != nil {
				panic(err)
			}
			var schema map[string]map[string]string
			err = json.Unmarshal(s, &schema)
			if err != nil {
				panic(err)
			}

			schemas = append(schemas, schema)
		}
		rows.Close()

		currSchema = wh.mergeSchema(currSchema, schemas)

		count += stagingFilesSchemaPaginationSize
		if count >= len(jsonUploadsList) {
			break
		}
	}

	// add rudder_discards table
	destType := warehouse.Destination.DestinationDefinition.Name
	discards := map[string]string{
		warehouseutils.ToProviderCase(destType, "table_name"):   "string",
		warehouseutils.ToProviderCase(destType, "row_id"):       "string",
		warehouseutils.ToProviderCase(destType, "column_name"):  "string",
		warehouseutils.ToProviderCase(destType, "column_value"): "string",
		warehouseutils.ToProviderCase(destType, "received_at"):  "datetime",
		warehouseutils.ToProviderCase(destType, "uuid_ts"):      "datetime",
	}
	currSchema[warehouseutils.ToProviderCase(destType, warehouseutils.DiscardsTable)] = discards
	return currSchema
}

func (wh *HandleT) areTableUploadsCreated(upload warehouseutils.UploadT) bool {
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE wh_upload_id=%d`, warehouseutils.WarehouseTableUploadsTable, upload.ID)
	var count int
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&count)
	if err != nil {
		panic(err)
	}
	return count > 0
}

func (wh *HandleT) initTableUploads(upload warehouseutils.UploadT, schema map[string]map[string]string) (err error) {
	//Using transactions for bulk copying
	txn, err := wh.dbHandle.Begin()
	if err != nil {
		return
	}

	stmt, err := txn.Prepare(pq.CopyIn(warehouseutils.WarehouseTableUploadsTable, "wh_upload_id", "table_name", "status", "error", "created_at", "updated_at"))
	if err != nil {
		return
	}

	tables := make([]string, 0, len(schema))
	for t := range schema {
		tables = append(tables, t)
	}

	now := timeutil.Now()
	for _, table := range tables {
		_, err = stmt.Exec(upload.ID, table, "waiting", "{}", now, now)
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

func (wh *HandleT) initUpload(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT) warehouseutils.UploadT {
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (source_id, namespace, destination_id, destination_type, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, status, schema, error, first_event_at, last_event_at, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10, $11, $12, $13, $14, $15) RETURNING id`, warehouseutils.WarehouseUploadsTable)
	logger.Infof("WH: %s: Creating record in %s table: %v", wh.destType, warehouseutils.WarehouseUploadsTable, sqlStatement)
	stmt, err := wh.dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	startJSONID := jsonUploadsList[0].ID
	endJSONID := jsonUploadsList[len(jsonUploadsList)-1].ID
	namespace := warehouse.Namespace

	var firstEventAt, lastEventAt time.Time
	if ok := jsonUploadsList[0].FirstEventAt.IsZero(); !ok {
		firstEventAt = jsonUploadsList[0].FirstEventAt
	}
	if ok := jsonUploadsList[0].LastEventAt.IsZero(); !ok {
		lastEventAt = jsonUploadsList[0].LastEventAt
	}

	now := timeutil.Now()
	row := stmt.QueryRow(warehouse.Source.ID, namespace, warehouse.Destination.ID, wh.destType, startJSONID, endJSONID, 0, 0, warehouseutils.WaitingState, "{}", "{}", firstEventAt, lastEventAt, now, now)

	var uploadID int64
	err = row.Scan(&uploadID)
	if err != nil {
		panic(err)
	}

	upload := warehouseutils.UploadT{
		ID:                 uploadID,
		Namespace:          warehouse.Namespace,
		SourceID:           warehouse.Source.ID,
		DestinationID:      warehouse.Destination.ID,
		DestinationType:    wh.destType,
		StartStagingFileID: startJSONID,
		EndStagingFileID:   endJSONID,
		Status:             warehouseutils.WaitingState,
	}

	return upload
}

func (wh *HandleT) getPendingUploads(warehouse warehouseutils.WarehouseT) ([]warehouseutils.UploadT, bool) {

	sqlStatement := fmt.Sprintf(`SELECT id, status, schema, namespace, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, error FROM %[1]s WHERE (%[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND %[1]s.status!='%[4]s' AND %[1]s.status!='%[5]s') ORDER BY id asc`, warehouseutils.WarehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID, warehouseutils.ExportedDataState, warehouseutils.AbortedState)

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
		if err != nil {
			panic(err)
		}
		upload.Schema = warehouseutils.JSONSchemaToMap(schema)
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

func uploadFrequencyExceeded(warehouse warehouseutils.WarehouseT, syncFrequency string) bool {
	freqInS := uploadFreqInS
	if syncFrequency != "" {
		freqInMin, _ := strconv.ParseInt(syncFrequency, 10, 64)
		freqInS = freqInMin * 60
	}
	lastExecMapLock.Lock()
	defer lastExecMapLock.Unlock()
	if lastExecTime, ok := lastExecMap[connectionString(warehouse)]; ok && timeutil.Now().Unix()-lastExecTime < freqInS {
		return true
	}
	return false
}

func setLastExec(warehouse warehouseutils.WarehouseT) {
	lastExecMapLock.Lock()
	defer lastExecMapLock.Unlock()
	lastExecMap[connectionString(warehouse)] = timeutil.Now().Unix()
}

type WarehouseManager interface {
	Process(config warehouseutils.ConfigT) error
	CrashRecover(config warehouseutils.ConfigT) (err error)
	FetchSchema(warehouse warehouseutils.WarehouseT, namespace string) (map[string]map[string]string, error)
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
	case "POSTGRES":
		var pg postgres.HandleT
		return &pg, nil
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
				if !wh.canStartUpload(warehouse) {
					logger.Debugf("WH: Skipping upload loop since %s:%s upload freq not exceeded", wh.destType, warehouse.Destination.ID)
					setDestInProgress(warehouse, false)
					continue
				}
				setLastExec(warehouse)
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
				var jobs []ProcessStagingFilesJobT
				// process staging files in batches of stagingFilesBatchSize
				for {
					lastIndex := count + stagingFilesBatchSize
					if lastIndex >= len(stagingFilesList) {
						lastIndex = len(stagingFilesList)
					}
					upload := wh.initUpload(warehouse, stagingFilesList[count:lastIndex])
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
			panic(err)
		}
		ids := make([]int64, len(respIDs))
		for i := range respIDs {
			ids[i] = int64(respIDs[i].(float64))
		}
		loadFileIDs = append(loadFileIDs, ids...)
	}

	timer.End()
	if len(loadFileIDs) == 0 {
		err = errors.New(responses[0].Error)
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

	err = warehouseutils.SetUploadStatus(
		job.Upload,
		warehouseutils.GeneratedLoadFileState,
		wh.dbHandle,
		warehouseutils.UploadColumnT{Column: warehouseutils.UploadStartLoadFileIDField, Value: startLoadFileID},
		warehouseutils.UploadColumnT{Column: warehouseutils.UploadEndLoadFileIDField, Value: endLoadFileID},
	)
	if err != nil {
		panic(err)
	}

	job.Upload.StartLoadFileID = startLoadFileID
	job.Upload.EndLoadFileID = endLoadFileID
	job.Upload.Status = warehouseutils.GeneratedLoadFileState
	return
}

func (wh *HandleT) updateTableEventsCount(tableName string, upload warehouseutils.UploadT, warehouse warehouseutils.WarehouseT) (err error) {
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
		warehouse.Source.ID,
		warehouse.Destination.ID,
		tableName,
		upload.StartLoadFileID,
		upload.EndLoadFileID,
		warehouseutils.WarehouseTableUploadsTable)

	sqlStatement := fmt.Sprintf(`update %[1]s set total_events = subquery.total FROM (%[2]s) AS subquery WHERE table_name = '%[3]s' AND wh_upload_id = %[4]d`,
		warehouseutils.WarehouseTableUploadsTable,
		subQuery,
		tableName,
		upload.ID)
	_, err = wh.dbHandle.Exec(sqlStatement)
	return
}

func (wh *HandleT) SyncLoadFilesToWarehouse(job *ProcessStagingFilesJobT) (err error) {
	for tableName := range job.Upload.Schema {
		err := wh.updateTableEventsCount(tableName, job.Upload, job.Warehouse)
		if err != nil {
			panic(err)
		}
	}
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

func (wh *HandleT) recordDeliveryStatus(uploadID int64) {
	var (
		sourceID      string
		destinationID string
		status        string
		errorCode     string
		errorResp     string
		updatedAt     time.Time
		tableName     string
		tableStatus   string
		attemptNum    int
	)
	successfulTableUploads := make([]string, 0)
	failedTableUploads := make([]string, 0)

	row := wh.dbHandle.QueryRow(fmt.Sprintf(`select source_id, destination_id, status, error, updated_at from %s where id=%d`, warehouseutils.WarehouseUploadsTable, uploadID))
	err := row.Scan(&sourceID, &destinationID, &status, &errorResp, &updatedAt)
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	rows, err := wh.dbHandle.Query(fmt.Sprintf(`select table_name, status from %s where wh_upload_id=%d`, warehouseutils.WarehouseTableUploadsTable, uploadID))
	if err != nil && err != sql.ErrNoRows {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		rows.Scan(&tableName, &tableStatus)
		if tableStatus == warehouseutils.ExportedDataState {
			successfulTableUploads = append(successfulTableUploads, tableName)
		} else {
			failedTableUploads = append(failedTableUploads, tableName)
		}
	}

	var errJson map[string]map[string]interface{}
	err = json.Unmarshal([]byte(errorResp), &errJson)
	if err != nil {
		panic(err)
	}
	if stateErr, ok := errJson[status]; ok {
		if attempt, ok := stateErr["attempt"]; ok {
			if floatAttempt, ok := attempt.(float64); ok {
				attemptNum = attemptNum + int(floatAttempt)
			}
		}
	}
	if attemptNum == 0 {
		attemptNum = 1
	}
	var errorRespB []byte
	if errorResp == "{}" {
		errorCode = "200"
	} else {
		errorCode = "400"

	}
	errorRespB, _ = json.Marshal(ErrorResponseT{Error: errorResp})

	payloadMap := map[string]interface{}{
		"lastSyncedAt":             updatedAt,
		"successful_table_uploads": successfulTableUploads,
		"failed_table_uploads":     failedTableUploads,
		"uploadID":                 uploadID,
		"error":                    errorResp,
	}
	payload, _ := json.Marshal(payloadMap)
	deliveryStatus := destinationdebugger.DeliveryStatusT{
		DestinationID: destinationID,
		SourceID:      sourceID,
		Payload:       payload,
		AttemptNum:    attemptNum,
		JobState:      status,
		ErrorCode:     errorCode,
		ErrorResponse: errorRespB,
	}
	destinationdebugger.RecordEventDeliveryStatus(destinationID, &deliveryStatus)
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
						if len(job.List) == 0 {
							warehouseutils.DestStat(stats.CountType, "failed_uploads", job.Warehouse.Destination.ID).Count(1)
							warehouseutils.SetUploadError(job.Upload, errors.New("no staging files found"), warehouseutils.GeneratingLoadFileFailedState, wh.dbHandle)
							wh.recordDeliveryStatus(job.Upload.ID)
							break
						}
						// consolidate schema if not already done
						schemaInDB, err := warehouseutils.GetCurrentSchema(wh.dbHandle, job.Warehouse)
						whManager, err := NewWhManager(wh.destType)
						if err != nil {
							panic(err)
						}

						syncedSchema, err := whManager.FetchSchema(job.Warehouse, job.Warehouse.Namespace)
						if err != nil {
							logger.Errorf(`WH: Failed fetching schema from warehouse: %v`, err)
							warehouseutils.DestStat(stats.CountType, "failed_uploads", job.Warehouse.Destination.ID).Count(1)
							warehouseutils.SetUploadError(job.Upload, err, warehouseutils.GeneratingLoadFileFailedState, wh.dbHandle)
							wh.recordDeliveryStatus(job.Upload.ID)
							break
						}

						hasSchemaChanged := !warehouseutils.CompareSchema(schemaInDB, syncedSchema)
						if hasSchemaChanged {
							err = warehouseutils.UpdateCurrentSchema(job.Warehouse.Namespace, job.Warehouse, job.Upload.ID, syncedSchema, wh.dbHandle)
							if err != nil {
								panic(err)
							}
						}

						if len(job.Upload.Schema) == 0 || hasSchemaChanged {
							// merge schemas over all staging files in this batch
							consolidatedSchema := wh.consolidateSchema(job.Warehouse, job.List)
							marshalledSchema, err := json.Marshal(consolidatedSchema)
							if err != nil {
								panic(err)
							}
							warehouseutils.SetUploadColumns(
								job.Upload,
								wh.dbHandle,
								warehouseutils.UploadColumnT{Column: warehouseutils.UploadSchemaField, Value: marshalledSchema},
							)
							job.Upload.Schema = consolidatedSchema
						}
						if !wh.areTableUploadsCreated(job.Upload) {
							err := wh.initTableUploads(job.Upload, job.Upload.Schema)
							if err != nil {
								// TODO: Handle error / Retry
								logger.Error("WH: Error creating records in wh_table_uploads", err)
							}
						}

						createPlusUploadTimer := warehouseutils.DestStat(stats.TimerType, "stagingfileset_total_handling_time", job.Warehouse.Destination.ID)
						createPlusUploadTimer.Start()

						// generate load files only if not done before
						// upload records have start_load_file_id and end_load_file_id set to 0 on creation
						// and are updated on creation of load files
						logger.Infof("WH: Processing %d staging files in upload job:%v with staging files from %v to %v for %s:%s", len(job.List), job.Upload.ID, job.List[0].ID, job.List[len(job.List)-1].ID, wh.destType, job.Warehouse.Destination.ID)
						warehouseutils.SetUploadColumns(
							job.Upload,
							wh.dbHandle,
							warehouseutils.UploadColumnT{Column: warehouseutils.UploadLastExecAtField, Value: timeutil.Now()},
						)
						if job.Upload.StartLoadFileID == 0 || hasSchemaChanged {
							warehouseutils.SetUploadStatus(job.Upload, warehouseutils.GeneratingLoadFileState, wh.dbHandle)
							err := wh.createLoadFiles(&job)
							if err != nil {
								//Unreachable code. So not modifying the stat 'failed_uploads', which is reused later for copying.
								warehouseutils.DestStat(stats.CountType, "failed_uploads", job.Warehouse.Destination.ID).Count(1)
								warehouseutils.SetUploadError(job.Upload, err, warehouseutils.GeneratingLoadFileFailedState, wh.dbHandle)
								wh.recordDeliveryStatus(job.Upload.ID)
								break
							}
						}
						err = wh.SyncLoadFilesToWarehouse(&job)
						wh.recordDeliveryStatus(job.Upload.ID)

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
	sqlStatement := fmt.Sprintf(`SELECT destination_id FROM %s WHERE destination_type='%s' AND (status='%s' OR status='%s')`, warehouseutils.WarehouseUploadsTable, wh.destType, warehouseutils.ExportingDataState, warehouseutils.ExportingDataFailedState)
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
	"POSTGRES":  "csv",
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
		Provider: warehouseutils.ObjectStorageType(job.DestinationType, job.DestinationConfig),
		Config:   misc.GetObjectStorageConfig(warehouseutils.ObjectStorageType(job.DestinationType, job.DestinationConfig), job.DestinationConfig),
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
	// sort columns per table so as to maintaining same order in load file (needed in case of csv load file)
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
	eventsCountMap := make(map[string]int)
	uuidTS := timeutil.Now()
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
			eventsCountMap[tableName] = 0
		}
		if job.DestinationType == "BQ" {
			for _, columnName := range sortedTableColumnMap[tableName] {
				if columnName == warehouseutils.ToProviderCase(job.DestinationType, "uuid_ts") {
					// add uuid_ts to track when event was processed into load_file
					columnData[warehouseutils.ToProviderCase(job.DestinationType, "uuid_ts")] = uuidTS.Format("2006-01-02 15:04:05 Z")
					continue
				}
				columnVal, ok := columnData[columnName]
				if !ok {
					continue
				}
				columnType, ok := columns[columnName].(string)
				if !ok {
					continue
				}
				// json.Unmarshal returns int as float
				// convert int's back to int to avoid writing integers like 123456789 as 1.23456789e+08
				// most warehouses only support scientific notation only for floats and not integers
				if columnType == "int" || columnType == "bigint" {
					floatVal, ok := columnVal.(float64)
					if !ok {
						continue
					}
					columnData[columnName] = int(floatVal)
				}
				// if the current data type doesnt match the one in warehouse, set value as NULL
				dataTypeInSchema := job.Schema[tableName][columnName]
				if ok && columnType != dataTypeInSchema {
					if dataTypeInSchema == "string" {
						// cast to string since string type column can accomodate any kind of value
						columnData[columnName] = fmt.Sprintf(`%v`, columnVal)
					} else if (columnType == "int" || columnType == "bigint") && dataTypeInSchema == "float" {
						// pass it along
					} else if columnType == "float" && (dataTypeInSchema == "int" || dataTypeInSchema == "bigint") {
						floatVal, ok := columnVal.(float64)
						if !ok {
							continue
						}
						columnData[columnName] = int(floatVal)
					} else {
						columnData[columnName] = nil
						rowID, hasID := columnData[warehouseutils.ToProviderCase(job.DestinationType, "id")]
						receivedAt, hasReceivedAt := columnData[warehouseutils.ToProviderCase(job.DestinationType, "received_at")]
						if hasID && hasReceivedAt {
							discardsTable := warehouseutils.ToProviderCase(job.DestinationType, warehouseutils.DiscardsTable)
							if _, ok := outputFileMap[discardsTable]; !ok {
								discardsOutputFilePath := strings.TrimSuffix(jsonPath, "json.gz") + discardsTable + fmt.Sprintf(`.%s`, loadFileFormatMap[job.DestinationType]) + ".gz"
								gzWriter, err := misc.CreateGZ(discardsOutputFilePath)
								defer gzWriter.CloseGZ()
								if err != nil {
									return nil, err
								}
								outputFileMap[discardsTable] = gzWriter
							}

							discardsData := map[string]interface{}{
								"column_name":  columnName,
								"column_value": fmt.Sprintf(`%v`, columnVal),
								"received_at":  receivedAt,
								"row_id":       rowID,
								"table_name":   tableName,
								"uuid_ts":      uuidTS.Format("2006-01-02 15:04:05 Z"),
							}
							dLine, err := json.Marshal(discardsData)
							if err != nil {
								return loadFileIDs, err
							}
							outputFileMap[discardsTable].WriteGZ(string(dLine) + "\n")
						}
						continue
					}
				}

			}
			line, err := json.Marshal(columnData)
			if err != nil {
				return loadFileIDs, err
			}
			outputFileMap[tableName].WriteGZ(string(line) + "\n")
			eventsCountMap[tableName]++
		} else {
			csvRow := []string{}
			var buff bytes.Buffer
			csvWriter := csv.NewWriter(&buff)
			for _, columnName := range sortedTableColumnMap[tableName] {
				if columnName == warehouseutils.ToProviderCase(job.DestinationType, "uuid_ts") {
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
				if !ok {
					csvRow = append(csvRow, "")
					continue
				}
				// json.Unmarshal returns int as float
				// convert int's back to int to avoid writing integers like 123456789 as 1.23456789e+08
				// most warehouses only support scientific notation only for floats and not integers
				if columnType == "int" || columnType == "bigint" {
					floatVal, ok := columnVal.(float64)
					if !ok {
						csvRow = append(csvRow, "")
						continue
					}
					columnVal = int(floatVal)
				}
				// if the current data type doesnt match the one in warehouse, set value as NULL
				dataTypeInSchema := job.Schema[tableName][columnName]
				if ok && columnType != dataTypeInSchema {
					if dataTypeInSchema == "string" {
						// pass it along since string type column can accomodate any kind of value
					} else if (columnType == "int" || columnType == "bigint") && dataTypeInSchema == "float" {
						// pass it along
					} else if columnType == "float" && (dataTypeInSchema == "int" || dataTypeInSchema == "bigint") {
						floatVal, ok := columnVal.(float64)
						if !ok {
							csvRow = append(csvRow, "")
							continue
						}
						columnVal = int(floatVal)
					} else {
						csvRow = append(csvRow, "")
						rowID, hasID := columnData[warehouseutils.ToProviderCase(job.DestinationType, "id")]
						receivedAt, hasReceivedAt := columnData[warehouseutils.ToProviderCase(job.DestinationType, "received_at")]
						if hasID && hasReceivedAt {
							discardsTable := warehouseutils.ToProviderCase(job.DestinationType, warehouseutils.DiscardsTable)
							if _, ok := outputFileMap[discardsTable]; !ok {
								discardsOutputFilePath := strings.TrimSuffix(jsonPath, "json.gz") + discardsTable + fmt.Sprintf(`.%s`, loadFileFormatMap[job.DestinationType]) + ".gz"
								gzWriter, err := misc.CreateGZ(discardsOutputFilePath)
								defer gzWriter.CloseGZ()
								if err != nil {
									return nil, err
								}
								outputFileMap[discardsTable] = gzWriter
							}

							discardRow := []string{}
							var dBuff bytes.Buffer
							dCsvWriter := csv.NewWriter(&dBuff)
							// sorted discard columns: column_name, column_value, received_at, row_id, table_name, uuid_ts
							discardRow = append(discardRow, columnName, fmt.Sprintf("%v", columnVal), fmt.Sprintf("%v", receivedAt), fmt.Sprintf("%v", rowID), tableName, uuidTS.Format(misc.RFC3339Milli))
							dCsvWriter.Write(discardRow)
							dCsvWriter.Flush()
							outputFileMap[discardsTable].WriteGZ(dBuff.String())
						}
						continue
					}
				}
				csvRow = append(csvRow, fmt.Sprintf("%v", columnVal))
			}
			csvWriter.Write(csvRow)
			csvWriter.Flush()
			outputFileMap[tableName].WriteGZ(buff.String())
			eventsCountMap[tableName]++
		}
	}
	reader.Close()
	timer.End()
	misc.PrintMemUsage()

	logger.Debugf("Process %v bytes from downloaded staging file: %s", lineBytesCounter, job.StagingFileLocation)
	warehouseutils.DestStat(stats.CountType, "bytes_processed_in_staging_file", job.DestinationID).Count(lineBytesCounter)

	uploader, err := filemanager.New(&filemanager.SettingsT{
		Provider: warehouseutils.ObjectStorageType(job.DestinationType, job.DestinationConfig),
		Config:   misc.GetObjectStorageConfig(warehouseutils.ObjectStorageType(job.DestinationType, job.DestinationConfig), job.DestinationConfig),
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
		logger.Debugf("WH: %s: Uploading load_file to %s for table: %s in staging_file id: %v", job.DestinationType, warehouseutils.ObjectStorageType(job.DestinationType, job.DestinationConfig), tableName, job.StagingFileID)
		uploadLocation, err := uploader.Upload(file, config.GetEnv("WAREHOUSE_BUCKET_LOAD_OBJECTS_FOLDER_NAME", "rudder-warehouse-load-objects"), tableName, job.SourceID, getBucketFolder(job.BatchID, tableName))
		if err != nil {
			return loadFileIDs, err
		}
		sqlStatement := fmt.Sprintf(`INSERT INTO %s (staging_file_id, location, source_id, destination_id, destination_type, table_name, total_events, created_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`, warehouseutils.WarehouseLoadFilesTable)
		stmt, err := dbHandle.Prepare(sqlStatement)
		if err != nil {
			panic(err)
		}
		defer stmt.Close()

		var fileID int64
		err = stmt.QueryRow(job.StagingFileID, uploadLocation.Location, job.SourceID, job.DestinationID, job.DestinationType, tableName, eventsCountMap[tableName], timeutil.Now()).Scan(&fileID)
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
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
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
	m := &migrator.Migrator{
		Handle:          dbHandle,
		MigrationsTable: "wh_schema_migrations",
	}

	err := m.Migrate("warehouse")
	if err != nil {
		panic(fmt.Errorf("Could not run warehouse database migrations: %w", err))
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

	var firstEventAt, lastEventAt interface{}
	firstEventAt = stagingFile.FirstEventAt
	lastEventAt = stagingFile.LastEventAt
	if stagingFile.FirstEventAt == "" || stagingFile.LastEventAt == "" {
		firstEventAt = nil
		lastEventAt = nil
	}

	logger.Debugf("BRT: Creating record for uploaded json in %s table with schema: %+v", warehouseutils.WarehouseStagingFilesTable, stagingFile.Schema)
	schemaPayload, err := json.Marshal(stagingFile.Schema)
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (location, schema, source_id, destination_id, status, total_events, first_event_at, last_event_at, created_at, updated_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`, warehouseutils.WarehouseStagingFilesTable)
	stmt, err := dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(stagingFile.Location, schemaPayload, stagingFile.BatchDestination.Source.ID, stagingFile.BatchDestination.Destination.ID, warehouseutils.StagingFileWaitingState, stagingFile.TotalEvents, firstEventAt, lastEventAt, timeutil.Now())
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
			logger.Debugf("WH: Notification recieved, event: %v, workers: %v", ev, slaveWorkerRoutineBusy)
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

	dbHandle, err = sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	if !validators.IsPostgresCompatible(dbHandle) {
		err := errors.New("Rudder Warehouse Service needs postgres version >= 10. Exiting")
		logger.Error(err)
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
		if warehouseMode != config.EmbeddedMode {
			backendconfig.Setup()
		}
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
