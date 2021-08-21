package warehouse

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bugsnag/bugsnag-go"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	destinationConnectionTester "github.com/rudderlabs/rudder-server/services/destination-connection-tester"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/thoas/go-funk"
	"github.com/tidwall/gjson"
)

var (
	application                         app.Interface
	webPort                             int
	dbHandle                            *sql.DB
	notifier                            pgnotifier.PgNotifierT
	WarehouseDestinations               []string
	noOfWorkers                         int
	noOfSlaveWorkerRoutines             int
	slaveWorkerRoutineBusy              []bool //Busy-true
	uploadFreqInS                       int64
	stagingFilesSchemaPaginationSize    int
	mainLoopSleep                       time.Duration
	stagingFilesBatchSize               int
	crashRecoverWarehouses              []string
	inRecoveryMap                       map[string]bool
	lastProcessedMarkerMap              map[string]int64
	lastProcessedMarkerMapLock          sync.RWMutex
	warehouseMode                       string
	warehouseSyncPreFetchCount          int
	warehouseSyncFreqIgnore             bool
	activeWorkerCount                   int
	activeWorkerCountLock               sync.RWMutex
	minRetryAttempts                    int
	retryTimeWindow                     time.Duration
	maxStagingFileReadBufferCapacityInK int
	connectionsMap                      map[string]map[string]warehouseutils.WarehouseT // destID -> sourceID -> warehouse map
	connectionsMapLock                  sync.RWMutex
	triggerUploadsMap                   map[string]bool // `whType:sourceID:destinationID` -> boolean value representing if an upload was triggered or not
	triggerUploadsMapLock               sync.RWMutex
	sourceIDsByWorkspace                map[string][]string // workspaceID -> []sourceIDs
	sourceIDsByWorkspaceLock            sync.RWMutex
	longRunningUploadStatThresholdInMin time.Duration
	pkgLogger                           logger.LoggerI
	numLoadFileUploadWorkers            int
	slaveUploadTimeout                  time.Duration
	runningMode                         string
	uploadStatusTrackFrequency          time.Duration
	uploadAllocatorSleep                time.Duration
	waitForConfig                       time.Duration
	waitForWorkerSleep                  time.Duration
	uploadBufferTimeInMin               int
	ShouldForceSetLowerVersion          bool
)

var (
	host, user, password, dbname, sslmode string
	port                                  int
)

// warehouses worker modes
const (
	MasterMode        = "master"
	SlaveMode         = "slave"
	MasterSlaveMode   = "master_and_slave"
	EmbeddedMode      = "embedded"
	PooledWHSlaveMode = "embedded_master"
)

const (
	DegradedMode                  = "degraded"
	StagingFilesPGNotifierChannel = "process_staging_file"
	triggerUploadQPName           = "triggerUpload"
)

type HandleT struct {
	destType             string
	warehouses           []warehouseutils.WarehouseT
	dbHandle             *sql.DB
	notifier             pgnotifier.PgNotifierT
	uploadToWarehouseQ   chan []ProcessStagingFilesJobT
	createLoadFilesQ     chan LoadFileJobT
	isEnabled            bool
	configSubscriberLock sync.RWMutex
	workerChannelMap     map[string]chan *UploadJobT
	workerChannelMapLock sync.RWMutex
	initialConfigFetched bool
	inProgressMap        map[string]int64
	inProgressMapLock    sync.RWMutex
	areBeingEnqueuedLock sync.RWMutex
}

type ErrorResponseT struct {
	Error string
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse")
}

func loadConfig() {
	//Port where WH is running
	config.RegisterIntConfigVariable(8082, &webPort, false, 1, "Warehouse.webPort")
	WarehouseDestinations = []string{"RS", "BQ", "SNOWFLAKE", "POSTGRES", "CLICKHOUSE", "MSSQL", "AZURE_SYNAPSE"}
	config.RegisterIntConfigVariable(8, &noOfWorkers, true, 1, "Warehouse.noOfWorkers")
	config.RegisterIntConfigVariable(4, &noOfSlaveWorkerRoutines, true, 1, "Warehouse.noOfSlaveWorkerRoutines")
	config.RegisterIntConfigVariable(960, &stagingFilesBatchSize, true, 1, "Warehouse.stagingFilesBatchSize")
	config.RegisterInt64ConfigVariable(1800, &uploadFreqInS, true, 1, "Warehouse.uploadFreqInS")
	config.RegisterDurationConfigVariable(time.Duration(5), &mainLoopSleep, true, time.Second, []string{"Warehouse.mainLoopSleep", "Warehouse.mainLoopSleepInS"}...)
	crashRecoverWarehouses = []string{"RS", "POSTGRES", "MSSQL", "AZURE_SYNAPSE"}
	inRecoveryMap = map[string]bool{}
	lastProcessedMarkerMap = map[string]int64{}
	config.RegisterStringConfigVariable("embedded", &warehouseMode, false, "Warehouse.mode")
	host = config.GetEnv("WAREHOUSE_JOBS_DB_HOST", "localhost")
	user = config.GetEnv("WAREHOUSE_JOBS_DB_USER", "ubuntu")
	dbname = config.GetEnv("WAREHOUSE_JOBS_DB_DB_NAME", "ubuntu")
	port, _ = strconv.Atoi(config.GetEnv("WAREHOUSE_JOBS_DB_PORT", "5432"))
	password = config.GetEnv("WAREHOUSE_JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from
	sslmode = config.GetEnv("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	config.RegisterIntConfigVariable(10, &warehouseSyncPreFetchCount, true, 1, "Warehouse.warehouseSyncPreFetchCount")
	config.RegisterIntConfigVariable(100, &stagingFilesSchemaPaginationSize, true, 1, "Warehouse.stagingFilesSchemaPaginationSize")
	config.RegisterBoolConfigVariable(false, &warehouseSyncFreqIgnore, true, "Warehouse.warehouseSyncFreqIgnore")
	config.RegisterIntConfigVariable(3, &minRetryAttempts, true, 1, "Warehouse.minRetryAttempts")
	config.RegisterDurationConfigVariable(time.Duration(180), &retryTimeWindow, true, time.Minute, []string{"Warehouse.retryTimeWindow", "Warehouse.retryTimeWindowInMins"}...)
	connectionsMap = map[string]map[string]warehouseutils.WarehouseT{}
	triggerUploadsMap = map[string]bool{}
	sourceIDsByWorkspace = map[string][]string{}
	config.RegisterIntConfigVariable(10240, &maxStagingFileReadBufferCapacityInK, true, 1, "Warehouse.maxStagingFileReadBufferCapacityInK")
	config.RegisterDurationConfigVariable(time.Duration(120), &longRunningUploadStatThresholdInMin, true, time.Minute, []string{"Warehouse.longRunningUploadStatThreshold", "Warehouse.longRunningUploadStatThresholdInMin"}...)
	config.RegisterDurationConfigVariable(time.Duration(10), &slaveUploadTimeout, true, time.Minute, []string{"Warehouse.slaveUploadTimeout", "Warehouse.slaveUploadTimeoutInMin"}...)
	config.RegisterIntConfigVariable(8, &numLoadFileUploadWorkers, true, 1, "Warehouse.numLoadFileUploadWorkers")
	runningMode = config.GetEnv("RSERVER_WAREHOUSE_RUNNING_MODE", "")
	config.RegisterDurationConfigVariable(time.Duration(30), &uploadStatusTrackFrequency, false, time.Minute, []string{"Warehouse.uploadStatusTrackFrequency", "Warehouse.uploadStatusTrackFrequencyInMin"}...)
	config.RegisterIntConfigVariable(180, &uploadBufferTimeInMin, false, 1, "Warehouse.uploadBufferTimeInMin")
	config.RegisterDurationConfigVariable(time.Duration(5), &uploadAllocatorSleep, false, time.Second, []string{"Warehouse.uploadAllocatorSleep", "Warehouse.uploadAllocatorSleepInS"}...)
	config.RegisterDurationConfigVariable(time.Duration(5), &waitForConfig, false, time.Second, []string{"Warehouse.waitForConfig", "Warehouse.waitForConfigInS"}...)
	config.RegisterDurationConfigVariable(time.Duration(5), &waitForWorkerSleep, false, time.Second, []string{"Warehouse.waitForWorkerSleep", "Warehouse.waitForWorkerSleepInS"}...)
	config.RegisterBoolConfigVariable(false, &ShouldForceSetLowerVersion, false, "SQLMigrator.forceSetLowerVersion")
}

// get name of the worker (`destID_namespace`) to be stored in map wh.workerChannelMap
func workerIdentifier(warehouse warehouseutils.WarehouseT) string {
	return fmt.Sprintf(`%s_%s`, warehouse.Destination.ID, warehouse.Namespace)
}

func getActiveWorkerCount() int {
	activeWorkerCountLock.Lock()
	defer activeWorkerCountLock.Unlock()
	return activeWorkerCount
}

func (wh *HandleT) decrementActiveWorkers() {
	// decrement number of workers actively engaged
	activeWorkerCountLock.Lock()
	activeWorkerCount--
	activeWorkerCountLock.Unlock()
}

func (wh *HandleT) incrementActiveWorkers() {
	// increment number of workers actively engaged
	activeWorkerCountLock.Lock()
	activeWorkerCount++
	activeWorkerCountLock.Unlock()
}

func (wh *HandleT) initWorker() chan *UploadJobT {
	workerChan := make(chan *UploadJobT, 1000)
	rruntime.Go(func() {
		for {
			uploadJob := <-workerChan
			wh.incrementActiveWorkers()
			err := wh.handleUploadJob(uploadJob)
			if err != nil {
				pkgLogger.Errorf("[WH] Failed in handle Upload jobs for worker: %+w", err)
			}
			wh.removeDestInProgress(uploadJob.warehouse)
			wh.decrementActiveWorkers()
		}
	})
	return workerChan
}

func (wh *HandleT) handleUploadJob(uploadJob *UploadJobT) (err error) {
	// Process the upload job
	err = uploadJob.run()
	wh.recordDeliveryStatus(uploadJob.warehouse.Destination.ID, uploadJob.upload.ID)
	return
}

func (wh *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	for {
		config := <-ch
		wh.configSubscriberLock.Lock()
		wh.warehouses = []warehouseutils.WarehouseT{}
		allSources := config.Data.(backendconfig.ConfigT)
		sourceIDsByWorkspaceLock.Lock()
		sourceIDsByWorkspace = map[string][]string{}
		pkgLogger.Infof(`Received updated workspace config`)
		for _, source := range allSources.Sources {
			if _, ok := sourceIDsByWorkspace[source.WorkspaceID]; !ok {
				sourceIDsByWorkspace[source.WorkspaceID] = []string{}
			}
			sourceIDsByWorkspace[source.WorkspaceID] = append(sourceIDsByWorkspace[source.WorkspaceID], source.ID)

			if len(source.Destinations) == 0 {
				continue
			}
			for _, destination := range source.Destinations {
				if destination.DestinationDefinition.Name != wh.destType {
					continue
				}
				namespace := wh.getNamespace(destination.Config, source, destination, wh.destType)
				warehouse := warehouseutils.WarehouseT{
					Source:      source,
					Destination: destination,
					Namespace:   namespace,
					Type:        wh.destType,
					Identifier:  warehouseutils.GetWarehouseIdentifier(wh.destType, source.ID, destination.ID),
				}
				wh.warehouses = append(wh.warehouses, warehouse)

				workerName := workerIdentifier(warehouse)
				wh.workerChannelMapLock.Lock()
				// spawn one worker for each unique destID_namespace
				// check this commit to https://github.com/rudderlabs/rudder-server/pull/476/commits/fbfddf167aa9fc63485fe006d34e6881f5019667
				// to avoid creating goroutine for disabled sources/destiantions
				if _, ok := wh.workerChannelMap[workerName]; !ok {
					workerChan := wh.initWorker()
					wh.workerChannelMap[workerName] = workerChan
				}
				wh.workerChannelMapLock.Unlock()

				connectionsMapLock.Lock()
				if connectionsMap[destination.ID] == nil {
					connectionsMap[destination.ID] = map[string]warehouseutils.WarehouseT{}
				}
				connectionsMap[destination.ID][source.ID] = warehouse
				connectionsMapLock.Unlock()

				// send last 10 warehouse upload's status to control plane
				if destination.Config != nil && destination.Enabled && destination.Config["eventDelivery"] == true {
					sourceID := source.ID
					destinationID := destination.ID
					rruntime.Go(func() {
						wh.syncLiveWarehouseStatus(sourceID, destinationID)
					})
				}
				// test and send connection status to control plane
				if val, ok := destination.Config["testConnection"].(bool); ok && val {
					destination := destination
					rruntime.Go(func() {
						testResponse := destinationConnectionTester.TestWarehouseDestinationConnection(destination)
						destinationConnectionTester.UploadDestinationConnectionTesterResponse(testResponse, destination.ID)
					})
				}

				if warehouseutils.IDResolutionEnabled() && misc.ContainsString(warehouseutils.IdentityEnabledWarehouses, warehouse.Type) {
					wh.setupIdentityTables(warehouse)
					if shouldPopulateHistoricIdentities && warehouse.Destination.Enabled {
						// non blocking populate historic identities
						wh.populateHistoricIdentities(warehouse)
					}
				}
			}
		}
		if val, ok := allSources.ConnectionFlags.Services["warehouse"]; ok {
			if UploadAPI.connectionManager != nil {
				UploadAPI.connectionManager.Apply(allSources.ConnectionFlags.URL, val)
			}
		}
		pkgLogger.Infof("Releasing config subscriber lock: %s", wh.destType)
		sourceIDsByWorkspaceLock.Unlock()
		wh.configSubscriberLock.Unlock()
		wh.initialConfigFetched = true
	}
}

// getNamespace sets namespace name in the following order
// 	1. user set name from destinationConfig
// 	2. from existing record in wh_schemas with same source + dest combo
// 	3. convert source name
func (wh *HandleT) getNamespace(config interface{}, source backendconfig.SourceT, destination backendconfig.DestinationT, destType string) string {
	configMap := config.(map[string]interface{})
	var namespace string
	if destType == "CLICKHOUSE" {
		//TODO: Handle if configMap["database"] is nil
		return configMap["database"].(string)
	}
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
	sqlStatement := fmt.Sprintf(`SELECT id, location, status
                                FROM %[1]s
								WHERE %[1]s.id >= %[2]v AND %[1]s.id <= %[3]v AND %[1]s.source_id='%[4]s' AND %[1]s.destination_id='%[5]s'
								ORDER BY id ASC`,
		warehouseutils.WarehouseStagingFilesTable, startID, endID, warehouse.Source.ID, warehouse.Destination.ID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("Query: %s failed with Error : %w", sqlStatement, err))
	}
	defer rows.Close()

	var stagingFilesList []*StagingFileT
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &jsonUpload.Status)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		stagingFilesList = append(stagingFilesList, &jsonUpload)
	}

	return stagingFilesList, nil
}

func (wh *HandleT) getPendingStagingFiles(warehouse warehouseutils.WarehouseT) ([]*StagingFileT, error) {
	var lastStagingFileID int64
	sqlStatement := fmt.Sprintf(`SELECT end_staging_file_id FROM %[1]s WHERE %[1]s.destination_type='%[2]s' AND %[1]s.source_id='%[3]s' AND %[1]s.destination_id='%[4]s' ORDER BY %[1]s.id DESC`, warehouseutils.WarehouseUploadsTable, warehouse.Type, warehouse.Source.ID, warehouse.Destination.ID)

	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&lastStagingFileID)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("Query: %s failed with Error : %w", sqlStatement, err))
	}

	sqlStatement = fmt.Sprintf(`SELECT id, location, status, first_event_at, last_event_at, metadata->>'source_batch_id', metadata->>'source_task_id', metadata->>'source_task_run_id', metadata->>'source_job_id', metadata->>'source_job_run_id', metadata->>'use_rudder_storage'
                                FROM %[1]s
								WHERE %[1]s.id > %[2]v AND %[1]s.source_id='%[3]s' AND %[1]s.destination_id='%[4]s'
								ORDER BY id ASC`,
		warehouseutils.WarehouseStagingFilesTable, lastStagingFileID, warehouse.Source.ID, warehouse.Destination.ID)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("Query: %s failed with Error : %w", sqlStatement, err))
	}
	defer rows.Close()

	var stagingFilesList []*StagingFileT
	var firstEventAt, lastEventAt sql.NullTime
	var sourceBatchID, sourceTaskID, sourceTaskRunID, sourceJobID, sourceJobRunID sql.NullString
	var UseRudderStorage sql.NullBool
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &jsonUpload.Status, &firstEventAt, &lastEventAt, &sourceBatchID, &sourceTaskID, &sourceTaskRunID, &sourceJobID, &sourceJobRunID, &UseRudderStorage)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		jsonUpload.FirstEventAt = firstEventAt.Time
		jsonUpload.LastEventAt = lastEventAt.Time
		jsonUpload.UseRudderStorage = UseRudderStorage.Bool
		// add cloud sources metadata
		jsonUpload.SourceBatchID = sourceBatchID.String
		jsonUpload.SourceTaskID = sourceTaskID.String
		jsonUpload.SourceTaskRunID = sourceTaskRunID.String
		jsonUpload.SourceJobID = sourceJobID.String
		jsonUpload.SourceJobRunID = sourceJobRunID.String
		stagingFilesList = append(stagingFilesList, &jsonUpload)
	}

	return stagingFilesList, nil
}

func (wh *HandleT) initUpload(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT, isUploadTriggered bool) {
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (source_id, namespace, destination_id, destination_type, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, status, schema, error, metadata, first_event_at, last_event_at, created_at, updated_at)
	VALUES ($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10, $11, $12, $13, $14, $15, $16) RETURNING id`, warehouseutils.WarehouseUploadsTable)
	pkgLogger.Infof("WH: %s: Creating record in %s table: %v", wh.destType, warehouseutils.WarehouseUploadsTable, sqlStatement)
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
	if ok := jsonUploadsList[len(jsonUploadsList)-1].LastEventAt.IsZero(); !ok {
		lastEventAt = jsonUploadsList[len(jsonUploadsList)-1].LastEventAt
	}

	now := timeutil.Now()
	metadataMap := map[string]interface{}{
		"use_rudder_storage": jsonUploadsList[0].UseRudderStorage,
		"source_batch_id":    jsonUploadsList[0].SourceBatchID,
		"source_task_id":     jsonUploadsList[0].SourceTaskID,
		"source_task_run_id": jsonUploadsList[0].SourceTaskRunID,
		"source_job_id":      jsonUploadsList[0].SourceJobID,
		"source_job_run_id":  jsonUploadsList[0].SourceJobRunID,
	}
	if isUploadTriggered {
		// set priority to 50 if the upload was manually triggered
		metadataMap["priority"] = 50
	}
	metadata, err := json.Marshal(metadataMap)
	if err != nil {
		panic(err)
	}
	row := stmt.QueryRow(warehouse.Source.ID, namespace, warehouse.Destination.ID, wh.destType, startJSONID, endJSONID, 0, 0, Waiting, "{}", "{}", metadata, firstEventAt, lastEventAt, now, now)

	var uploadID int64
	err = row.Scan(&uploadID)
	if err != nil {
		panic(err)
	}
}

func (wh *HandleT) setDestInProgress(warehouse warehouseutils.WarehouseT, jobID int64) {
	identifier := workerIdentifier(warehouse)
	wh.inProgressMapLock.Lock()
	defer wh.inProgressMapLock.Unlock()
	wh.inProgressMap[identifier] = jobID
}

func (wh *HandleT) removeDestInProgress(warehouse warehouseutils.WarehouseT) {
	identifier := workerIdentifier(warehouse)
	wh.inProgressMapLock.Lock()
	defer wh.inProgressMapLock.Unlock()
	delete(wh.inProgressMap, identifier)
}

func getUploadFreqInS(syncFrequency string) int64 {
	freqInS := uploadFreqInS
	if syncFrequency != "" {
		freqInMin, _ := strconv.ParseInt(syncFrequency, 10, 64)
		freqInS = freqInMin * 60
	}
	return freqInS
}

func uploadFrequencyExceeded(warehouse warehouseutils.WarehouseT, syncFrequency string) bool {
	freqInS := getUploadFreqInS(syncFrequency)
	lastProcessedMarkerMapLock.Lock()
	defer lastProcessedMarkerMapLock.Unlock()
	if lastExecTime, ok := lastProcessedMarkerMap[warehouse.Identifier]; ok && timeutil.Now().Unix()-lastExecTime < freqInS {
		return true
	}
	return false
}

func setLastProcessedMarker(warehouse warehouseutils.WarehouseT) {
	lastProcessedMarkerMapLock.Lock()
	defer lastProcessedMarkerMapLock.Unlock()
	lastProcessedMarkerMap[warehouse.Identifier] = time.Now().Unix()
}

func (wh *HandleT) createUploadJobsFromStagingFiles(warehouse warehouseutils.WarehouseT, whManager manager.ManagerI, stagingFilesList []*StagingFileT) {
	// count := 0
	// Process staging files in batches of stagingFilesBatchSize
	// Eg. If there are 1000 pending staging files and stagingFilesBatchSize is 100,
	// Then we create 10 new entries in wh_uploads table each with 100 staging files
	var stagingFilesInUpload []*StagingFileT
	var counter int
	uploadTriggered := isUploadTriggered(warehouse)
	initUpload := func() {
		wh.initUpload(warehouse, stagingFilesInUpload, uploadTriggered)
		stagingFilesInUpload = []*StagingFileT{}
		counter = 0
	}
	for idx, sFile := range stagingFilesList {
		if idx > 0 && counter > 0 && sFile.UseRudderStorage != stagingFilesList[idx-1].UseRudderStorage {
			initUpload()
		}

		stagingFilesInUpload = append(stagingFilesInUpload, sFile)
		counter++
		if counter == stagingFilesBatchSize || idx == len(stagingFilesList)-1 {
			initUpload()
		}
	}

	// reset upload trigger if the upload was triggered
	if uploadTriggered {
		clearTriggeredUpload(warehouse)
	}
}

func (wh *HandleT) getLatestUploadStatus(warehouse warehouseutils.WarehouseT) (uploadID int64, status string) {
	sqlStatement := fmt.Sprintf(`SELECT id, status FROM %[1]s WHERE %[1]s.destination_type='%[2]s' AND %[1]s.source_id='%[3]s' AND %[1]s.destination_id='%[4]s' ORDER BY id DESC LIMIT 1`, warehouseutils.WarehouseUploadsTable, wh.destType, warehouse.Source.ID, warehouse.Destination.ID)
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&uploadID, &status)
	if err != nil && err != sql.ErrNoRows {
		pkgLogger.Errorf(`Error getting latest upload status for warehouse: %v`, err)
	}
	return
}

func (wh *HandleT) deleteWaitingUploadJob(jobID int64) {
	sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE id = %d AND status = '%s'`, warehouseutils.WarehouseUploadsTable, jobID, Waiting)
	_, err := wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf(`Error deleting upload job: %d in waiting state: %v`, jobID, err)
	}
}

func (wh *HandleT) createJobs(warehouse warehouseutils.WarehouseT) (err error) {
	whManager, err := manager.New(wh.destType)
	if err != nil {
		return err
	}

	// Step 1: Crash recovery after restart
	// Remove pending temp tables in Redshift etc.
	_, ok := inRecoveryMap[warehouse.Destination.ID]
	if ok {
		pkgLogger.Infof("[WH]: Crash recovering for %s:%s", wh.destType, warehouse.Destination.ID)
		err = whManager.CrashRecover(warehouse)
		if err != nil {
			return err
		}
		delete(inRecoveryMap, warehouse.Destination.ID)
	}

	if !wh.canCreateUpload(warehouse) {
		pkgLogger.Debugf("[WH]: Skipping upload loop since %s upload freq not exceeded", warehouse.Identifier)
		return nil
	}

	wh.areBeingEnqueuedLock.Lock()
	uploadID, uploadStatus := wh.getLatestUploadStatus(warehouse)
	if uploadStatus == Waiting {
		identifier := workerIdentifier(warehouse)
		if uID, ok := wh.inProgressMap[identifier]; ok && (uID == uploadID) {
			// do nothing
		} else {
			// delete it
			wh.deleteWaitingUploadJob(uploadID)
		}
	}
	wh.areBeingEnqueuedLock.Unlock()

	stagingFilesList, err := wh.getPendingStagingFiles(warehouse)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to get pending staging files: %s with error %v", warehouse.Identifier, err)
		return err
	}
	if len(stagingFilesList) == 0 {
		pkgLogger.Debugf("[WH]: Found no pending staging files for %s", warehouse.Identifier)
		return nil
	}

	wh.createUploadJobsFromStagingFiles(warehouse, whManager, stagingFilesList)
	setLastProcessedMarker(warehouse)
	return nil
}

func (wh *HandleT) sortWarehousesByOldestUnSyncedEventAt() (err error) {
	sqlStatement := fmt.Sprintf(`
		SELECT
			concat('%s', ':', source_id, ':', destination_id) as wh_identifier,
			CASE
				WHEN (status='exported_data' or status='aborted') THEN last_event_at
				ELSE first_event_at
				END AS oldest_unsynced_event
		FROM (
			SELECT
				ROW_NUMBER() OVER (PARTITION BY source_id, destination_id ORDER BY id desc) AS row_number,
				t.source_id, t.destination_id, t.last_event_at, t.first_event_at, t.status
			FROM
				wh_uploads t) grouped_uploads
		WHERE
			grouped_uploads.row_number = 1;`,
		wh.destType)

	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	defer rows.Close()

	oldestEventAtMap := map[string]time.Time{}

	for rows.Next() {
		var whIdentifier string
		var oldestUnSyncedEventAtNullTime sql.NullTime
		err := rows.Scan(&whIdentifier, &oldestUnSyncedEventAtNullTime)
		if err != nil {
			return err
		}
		oldestUnSyncedEventAt := oldestUnSyncedEventAtNullTime.Time
		if !oldestUnSyncedEventAtNullTime.Valid {
			oldestUnSyncedEventAt = timeutil.Now()
		}
		oldestEventAtMap[whIdentifier] = oldestUnSyncedEventAt
	}

	sort.Slice(wh.warehouses, func(i, j int) bool {
		var firstTime, secondTime time.Time
		var ok bool
		if firstTime, ok = oldestEventAtMap[warehouseutils.GetWarehouseIdentifier(wh.destType, wh.warehouses[i].Source.ID, wh.warehouses[i].Destination.ID)]; !ok {
			firstTime = timeutil.Now()
		}
		if secondTime, ok = oldestEventAtMap[warehouseutils.GetWarehouseIdentifier(wh.destType, wh.warehouses[j].Source.ID, wh.warehouses[j].Destination.ID)]; !ok {
			secondTime = timeutil.Now()
		}
		return firstTime.Before(secondTime)
	})
	return
}

func (wh *HandleT) mainLoop() {
	for {
		wh.configSubscriberLock.RLock()
		if !wh.isEnabled {
			time.Sleep(mainLoopSleep)
			wh.configSubscriberLock.RUnlock()
			continue
		}

		err := wh.sortWarehousesByOldestUnSyncedEventAt()
		wh.configSubscriberLock.RUnlock()
		if err != nil {
			pkgLogger.Errorf(`[WH] Error sorting warehouses by last event time: %v`, err)
		}

		for _, warehouse := range wh.warehouses {
			pkgLogger.Debugf("[WH] Processing Jobs for warehouse: %s", warehouse.Identifier)
			err := wh.createJobs(warehouse)
			if err != nil {
				pkgLogger.Errorf("[WH] Failed to process warehouse Jobs: %v", err)
			}
		}
		time.Sleep(mainLoopSleep)
	}
}

func (wh *HandleT) getUploadsToProcess(availableWorkers int, skipIdentifiers []string) ([]*UploadJobT, error) {

	var skipIdentifiersSQL string
	if len(skipIdentifiers) > 0 {
		skipIdentifiersSQL = `and ((destination_id || '_' || namespace)) != ALL($1)`
	}

	sqlStatement := fmt.Sprintf(`
			SELECT
					id, status, schema, namespace, source_id, destination_id, destination_type, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, error, metadata, timings->0 as firstTiming, timings->-1 as lastTiming, metadata->>'use_rudder_storage', timings
				FROM (
					SELECT
						ROW_NUMBER() OVER (PARTITION BY destination_id, namespace ORDER BY COALESCE(metadata->>'priority', '100')::int ASC, id ASC) AS row_number,
						t.*
					FROM
						%s t
					WHERE
						t.destination_type = '%s' and t.status != '%s' and t.status != '%s' %s and COALESCE(metadata->>'nextRetryTime', now()::text)::timestamptz <= now()
				) grouped_uplaods
				WHERE
					grouped_uplaods.row_number = 1
				ORDER BY
					COALESCE(metadata->>'priority', '100')::int ASC, id ASC
				LIMIT %d;

		`, warehouseutils.WarehouseUploadsTable, wh.destType, ExportedData, Aborted, skipIdentifiersSQL, availableWorkers)

	var rows *sql.Rows
	var err error
	if len(skipIdentifiers) > 0 {
		rows, err = wh.dbHandle.Query(sqlStatement, pq.Array(skipIdentifiers))
	} else {
		rows, err = wh.dbHandle.Query(sqlStatement)
	}

	if err != nil && err != sql.ErrNoRows {
		return []*UploadJobT{}, err
	}

	if err == sql.ErrNoRows {
		return []*UploadJobT{}, nil
	}
	defer rows.Close()

	var uploadJobs []*UploadJobT
	for rows.Next() {
		var upload UploadT
		var schema json.RawMessage
		var firstTiming sql.NullString
		var lastTiming sql.NullString
		var useRudderStorage sql.NullBool
		err := rows.Scan(&upload.ID, &upload.Status, &schema, &upload.Namespace, &upload.SourceID, &upload.DestinationID, &upload.DestinationType, &upload.StartStagingFileID, &upload.EndStagingFileID, &upload.StartLoadFileID, &upload.EndLoadFileID, &upload.Error, &upload.Metadata, &firstTiming, &lastTiming, &useRudderStorage, &upload.TimingsObj)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		upload.Schema = warehouseutils.JSONSchemaToMap(schema)
		upload.UseRudderStorage = useRudderStorage.Bool
		// cloud sources info
		upload.SourceBatchID = gjson.GetBytes(upload.Metadata, "source_batch_id").String()
		upload.SourceTaskID = gjson.GetBytes(upload.Metadata, "source_task_id").String()
		upload.SourceTaskRunID = gjson.GetBytes(upload.Metadata, "source_task_run_id").String()
		upload.SourceJobID = gjson.GetBytes(upload.Metadata, "source_job_id").String()
		upload.SourceJobRunID = gjson.GetBytes(upload.Metadata, "source_job_run_id").String()

		_, upload.FirstAttemptAt = warehouseutils.TimingFromJSONString(firstTiming)
		var lastStatus string
		lastStatus, upload.LastAttemptAt = warehouseutils.TimingFromJSONString(lastTiming)
		upload.Attempts = gjson.Get(string(upload.Error), fmt.Sprintf(`%s.attempt`, lastStatus)).Int()

		wh.configSubscriberLock.RLock()
		warehouse, ok := funk.Find(wh.warehouses, func(w warehouseutils.WarehouseT) bool {
			return w.Source.ID == upload.SourceID && w.Destination.ID == upload.DestinationID
		}).(warehouseutils.WarehouseT)
		wh.configSubscriberLock.RUnlock()

		if !ok {
			uploadJob := UploadJobT{
				upload:   &upload,
				dbHandle: wh.dbHandle,
			}
			err := fmt.Errorf("Unable to find source : %s or destination : %s, both or the connection between them", upload.SourceID, upload.DestinationID)
			uploadJob.setUploadError(err, Aborted)
			pkgLogger.Errorf("%v", err)
			continue
		}

		upload.SourceType = warehouse.Source.SourceDefinition.Name
		upload.SourceCategory = warehouse.Source.SourceDefinition.Category

		stagingFilesList, err := wh.getStagingFiles(warehouse, upload.StartStagingFileID, upload.EndStagingFileID)
		if err != nil {
			return nil, err
		}
		var stagingFileIDs []int64
		for _, stagingFile := range stagingFilesList {
			stagingFileIDs = append(stagingFileIDs, stagingFile.ID)
		}

		whManager, err := manager.New(wh.destType)
		if err != nil {
			return nil, err
		}

		uploadJob := UploadJobT{
			upload:         &upload,
			stagingFiles:   stagingFilesList,
			stagingFileIDs: stagingFileIDs,
			warehouse:      warehouse,
			whManager:      whManager,
			dbHandle:       wh.dbHandle,
			pgNotifier:     &wh.notifier,
		}

		uploadJobs = append(uploadJobs, &uploadJob)
	}

	return uploadJobs, nil
}

func (wh *HandleT) getInProgressNamespaces() (identifiers []string) {
	wh.inProgressMapLock.Lock()
	defer wh.inProgressMapLock.Unlock()
	return misc.StringKeys(wh.inProgressMap)
}

func (wh *HandleT) runUploadJobAllocator() {
	for {
		if !wh.initialConfigFetched {
			time.Sleep(waitForConfig)
			continue
		}

		availableWorkers := noOfWorkers - getActiveWorkerCount()
		if availableWorkers < 1 {
			time.Sleep(waitForWorkerSleep)
			continue
		}

		wh.areBeingEnqueuedLock.Lock()

		inProgressNamespaces := wh.getInProgressNamespaces()
		pkgLogger.Debugf(`Current inProgress namespace identifiers for %s: %v`, wh.destType, inProgressNamespaces)

		uploadJobsToProcess, err := wh.getUploadsToProcess(availableWorkers, inProgressNamespaces)
		if err != nil {
			pkgLogger.Errorf(`Error executing getUploadsToProcess: %v`, err)
			panic(err)
		}

		for _, uploadJob := range uploadJobsToProcess {
			wh.setDestInProgress(uploadJob.warehouse, uploadJob.upload.ID)
		}
		wh.areBeingEnqueuedLock.Unlock()

		for _, uploadJob := range uploadJobsToProcess {
			workerName := workerIdentifier(uploadJob.warehouse)
			wh.workerChannelMapLock.Lock()
			wh.workerChannelMap[workerName] <- uploadJob
			wh.workerChannelMapLock.Unlock()
		}

		time.Sleep(uploadAllocatorSleep)
	}
}

func (wh *HandleT) uploadStatusTrack() {
	for {
		for _, warehouse := range wh.warehouses {
			source := warehouse.Source
			destination := warehouse.Destination

			if !source.Enabled || !destination.Enabled {
				continue
			}

			config := destination.Config
			// Default frequency
			syncFrequency := "1440"
			if config[warehouseutils.SyncFrequency] != nil {
				syncFrequency, _ = config[warehouseutils.SyncFrequency].(string)
			}

			timeWindow := uploadBufferTimeInMin
			if value, err := strconv.Atoi(syncFrequency); err == nil {
				timeWindow += value
			}

			sqlStatement := fmt.Sprintf(`
				select created_at from %[1]s where source_id='%[2]s' and destination_id='%[3]s' and created_at > now() - interval '%[4]d MIN' and created_at < now() - interval '%[5]d MIN' order by created_at desc limit 1`,
				warehouseutils.WarehouseStagingFilesTable, source.ID, destination.ID, 2*timeWindow, timeWindow)

			var createdAt sql.NullTime
			err := wh.dbHandle.QueryRow(sqlStatement).Scan(&createdAt)
			if err != nil && err != sql.ErrNoRows {
				panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
			}

			if !createdAt.Valid {
				continue
			}

			sqlStatement = fmt.Sprintf(`
				select cast( case when count(*) > 0 then 1 else 0 end as bit )
				from %[1]s where source_id='%[2]s' and destination_id='%[3]s' and (status='%[4]s' or status='%[5]s' or status like '%[6]s') and updated_at > '%[7]s'`,
				warehouseutils.WarehouseUploadsTable, source.ID, destination.ID, ExportedData, Aborted, "%_failed", createdAt.Time.Format(misc.RFC3339Milli))

			var uploaded int
			err = wh.dbHandle.QueryRow(sqlStatement).Scan(&uploaded)
			if err != nil && err != sql.ErrNoRows {
				panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
			}

			getUploadStatusStat("warehouse_successful_upload_exists", warehouse.Type, warehouse.Destination.ID, warehouse.Source.Name, warehouse.Destination.Name).Count(uploaded)
		}
		time.Sleep(uploadStatusTrackFrequency)
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

func (wh *HandleT) setInterruptedDestinations() {
	if !misc.Contains(crashRecoverWarehouses, wh.destType) {
		return
	}
	sqlStatement := fmt.Sprintf(`SELECT destination_id FROM %s WHERE destination_type='%s' AND (status='%s' OR status='%s')`, warehouseutils.WarehouseUploadsTable, wh.destType, getInProgressState(ExportedData), getFailedState(ExportedData))
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil {
		panic(fmt.Errorf("Query: %s failed with Error : %w", sqlStatement, err))
	}
	defer rows.Close()

	for rows.Next() {
		var destID string
		err := rows.Scan(&destID)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		inRecoveryMap[destID] = true
	}
}

func (wh *HandleT) Setup(whType string) {
	pkgLogger.Infof("WH: Warehouse Router started: %s", whType)
	wh.dbHandle = dbHandle
	wh.notifier = notifier
	wh.destType = whType
	wh.setInterruptedDestinations()
	wh.Enable()
	wh.uploadToWarehouseQ = make(chan []ProcessStagingFilesJobT)
	wh.createLoadFilesQ = make(chan LoadFileJobT)
	wh.workerChannelMap = make(map[string]chan *UploadJobT)
	wh.inProgressMap = make(map[string]int64)
	rruntime.Go(func() {
		wh.backendConfigSubscriber()
	})
	rruntime.Go(func() {
		wh.runUploadJobAllocator()
	})
	rruntime.Go(func() {
		wh.mainLoop()
	})
}

func (wh *HandleT) monitorUploadStatus() {
	pkgLogger.Infof("WH: Warehouse Idle upload tracker started")
	rruntime.Go(func() {
		wh.uploadStatusTrack()
	})
}

var loadFileFormatMap = map[string]string{
	"BQ":         "json",
	"RS":         "csv",
	"SNOWFLAKE":  "csv",
	"POSTGRES":   "csv",
	"CLICKHOUSE": "csv",
	"MSSQL":      "csv",
}

func minimalConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	for {
		config := <-ch
		pkgLogger.Debug("Got config from config-backend", config)
		sources := config.Data.(backendconfig.ConfigT)
		sourceIDsByWorkspaceLock.Lock()
		sourceIDsByWorkspace = map[string][]string{}
		for _, source := range sources.Sources {
			if _, ok := sourceIDsByWorkspace[source.WorkspaceID]; !ok {
				sourceIDsByWorkspace[source.WorkspaceID] = []string{}
			}
			sourceIDsByWorkspace[source.WorkspaceID] = append(sourceIDsByWorkspace[source.WorkspaceID], source.ID)
			for _, destination := range source.Destinations {
				if misc.Contains(WarehouseDestinations, destination.DestinationDefinition.Name) {
					wh := &HandleT{
						dbHandle: dbHandle,
						destType: destination.DestinationDefinition.Name,
					}
					namespace := wh.getNamespace(destination.Config, source, destination, wh.destType)
					connectionsMapLock.Lock()
					if connectionsMap[destination.ID] == nil {
						connectionsMap[destination.ID] = map[string]warehouseutils.WarehouseT{}
					}
					connectionsMap[destination.ID][source.ID] = warehouseutils.WarehouseT{
						Destination: destination,
						Namespace:   namespace,
						Type:        wh.destType,
						Source:      source,
						Identifier:  warehouseutils.GetWarehouseIdentifier(wh.destType, source.ID, destination.ID),
					}
					connectionsMapLock.Unlock()
				}
			}
		}
		sourceIDsByWorkspaceLock.Unlock()
		if val, ok := sources.ConnectionFlags.Services["warehouse"]; ok {
			if UploadAPI.connectionManager != nil {
				UploadAPI.connectionManager.Apply(sources.ConnectionFlags.URL, val)
			}
		}
	}
}

// Gets the config from config backend and extracts enabled writekeys
func monitorDestRouters() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	dstToWhRouter := make(map[string]*HandleT)

	for {
		config := <-ch
		pkgLogger.Debug("Got config from config-backend", config)
		sources := config.Data.(backendconfig.ConfigT)
		enabledDestinations := make(map[string]bool)
		for _, source := range sources.Sources {
			for _, destination := range source.Destinations {
				enabledDestinations[destination.DestinationDefinition.Name] = true
				if misc.Contains(WarehouseDestinations, destination.DestinationDefinition.Name) {
					wh, ok := dstToWhRouter[destination.DestinationDefinition.Name]
					if !ok {
						pkgLogger.Info("Starting a new Warehouse Destination Router: ", destination.DestinationDefinition.Name)
						wh = &HandleT{}
						wh.configSubscriberLock.Lock()
						wh.Setup(destination.DestinationDefinition.Name)
						wh.configSubscriberLock.Unlock()
						dstToWhRouter[destination.DestinationDefinition.Name] = wh
						wh.monitorUploadStatus()
					} else {
						pkgLogger.Debug("Enabling existing Destination: ", destination.DestinationDefinition.Name)
						wh.configSubscriberLock.Lock()
						wh.Enable()
						wh.configSubscriberLock.Unlock()
					}
				}
			}
		}

		keys := misc.StringKeys(dstToWhRouter)
		for _, key := range keys {
			if _, ok := enabledDestinations[key]; !ok {
				if wh, ok := dstToWhRouter[key]; ok {
					pkgLogger.Info("Disabling a existing warehouse destination: ", key)
					wh.configSubscriberLock.Lock()
					wh.Disable()
					wh.configSubscriberLock.Unlock()
				}
			}
		}
	}
}

func setupTables(dbHandle *sql.DB) {
	m := &migrator.Migrator{
		Handle:                     dbHandle,
		MigrationsTable:            "wh_schema_migrations",
		ShouldForceSetLowerVersion: ShouldForceSetLowerVersion,
	}

	err := m.Migrate("warehouse")
	if err != nil {
		panic(fmt.Errorf("Could not run warehouse database migrations: %w", err))
	}
}

func CheckPGHealth(dbHandle *sql.DB) bool {
	if dbHandle == nil {
		return false
	}
	rows, err := dbHandle.Query(`SELECT 'Rudder Warehouse DB Health Check'::text as message`)
	if err != nil {
		pkgLogger.Error(err)
		return false
	}
	defer rows.Close()
	return true
}

func processHandler(w http.ResponseWriter, r *http.Request) {
	pkgLogger.LogRequest(r)

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error reading body: %v", err)
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
	metadataMap := map[string]interface{}{
		"use_rudder_storage": stagingFile.UseRudderStorage,
		"source_batch_id":    stagingFile.SourceBatchID,
		"source_task_id":     stagingFile.SourceTaskID,
		"source_task_run_id": stagingFile.SourceTaskRunID,
		"source_job_id":      stagingFile.SourceJobID,
		"source_job_run_id":  stagingFile.SourceJobRunID,
	}
	metadata, err := json.Marshal(metadataMap)
	if err != nil {
		panic(err)
	}

	pkgLogger.Debugf("BRT: Creating record for uploaded json in %s table with schema: %+v", warehouseutils.WarehouseStagingFilesTable, stagingFile.Schema)
	schemaPayload, _ := json.Marshal(stagingFile.Schema)
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (location, schema, source_id, destination_id, status, total_events, first_event_at, last_event_at, created_at, updated_at, metadata)
									   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9, $10)`, warehouseutils.WarehouseStagingFilesTable)
	stmt, err := dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(stagingFile.Location, schemaPayload, stagingFile.BatchDestination.Source.ID, stagingFile.BatchDestination.Destination.ID, warehouseutils.StagingFileWaitingState, stagingFile.TotalEvents, firstEventAt, lastEventAt, timeutil.Now(), metadata)
	if err != nil {
		panic(err)
	}
	recordStagedRowsStat(stagingFile.TotalEvents, stagingFile.BatchDestination.Destination.DestinationDefinition.Name, stagingFile.BatchDestination.Destination.ID, stagingFile.BatchDestination.Source.Name, stagingFile.BatchDestination.Destination.Name)
}

func pendingEventsHandler(w http.ResponseWriter, r *http.Request) {
	// TODO : respond with errors in a common way
	pkgLogger.LogRequest(r)

	// read body
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// unmarshall body
	var pendingEventsReq warehouseutils.PendingEventsRequestT
	err = json.Unmarshal(body, &pendingEventsReq)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error unmarshalling body: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}

	sourceID := pendingEventsReq.SourceID

	// return error if source id is empty
	if sourceID == "" {
		pkgLogger.Errorf("[WH]: pending-events:  Empty source id")
		http.Error(w, "empty source id", http.StatusBadRequest)
		return
	}

	pendingEvents := false
	var pendingStagingFileCount int64
	var pendingUploadCount int64

	// check whether there are any pending staging files or uploads for the given source id
	// get pending staging files
	pendingStagingFileCount, err = getPendingStagingFileCount(sourceID, true)
	if err != nil {
		err := fmt.Errorf("Error getting pending staging file count : %v", err)
		pkgLogger.Errorf("[WH]: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// get pending uploads only if there are no pending staging files
	if pendingStagingFileCount == 0 {
		pendingUploadCount, err = getPendingUploadCount(sourceID, true)
		if err != nil {
			err := fmt.Errorf("Error getting pending uploads : %v", err)
			pkgLogger.Errorf("[WH]: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// if there are any pending staging files or uploads, set pending events as true
	if (pendingStagingFileCount + pendingUploadCount) > int64(0) {
		pendingEvents = true
	}

	// read `triggerUpload` queryParam
	var triggerPendingUpload bool
	triggerUploadQP := r.URL.Query().Get(triggerUploadQPName)
	if triggerUploadQP != "" {
		triggerPendingUpload, _ = strconv.ParseBool(triggerUploadQP)
	}

	// trigger upload if there are pending events and triggerPendingUpload is true
	if pendingEvents && triggerPendingUpload {
		pkgLogger.Infof("[WH]: Triggering upload for all wh destinations connected to source '%s'", sourceID)
		wh := make([]warehouseutils.WarehouseT, 0)

		// get all wh destinations for given source id
		connectionsMapLock.Lock()
		for _, srcMap := range connectionsMap {
			for srcID, w := range srcMap {
				if srcID == sourceID {
					wh = append(wh, w)
				}
			}
		}
		connectionsMapLock.Unlock()

		// return error if no such destinations found
		if len(wh) == 0 {
			err := fmt.Errorf("No warehouse destinations found for source id '%s'", sourceID)
			pkgLogger.Errorf("[WH]: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		for _, warehouse := range wh {
			triggerUpload(warehouse)
		}
	}

	// create and write response
	res := warehouseutils.PendingEventsResponseT{
		PendingEvents:            pendingEvents,
		PendingStagingFilesCount: pendingStagingFileCount,
		PendingUploadCount:       pendingUploadCount,
	}

	resBody, err := json.Marshal(res)
	if err != nil {
		err := fmt.Errorf("Failed to marshall pending events response : %v", err)
		pkgLogger.Errorf("[WH]: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Write(resBody)
}

func getPendingStagingFileCount(sourceOrDestId string, isSourceId bool) (fileCount int64, err error) {
	sourceOrDestColumn := ""
	if isSourceId {
		sourceOrDestColumn = "source_id"
	} else {
		sourceOrDestColumn = "destination_id"
	}
	var lastStagingFileID int64
	sqlStatement := fmt.Sprintf(`SELECT end_staging_file_id FROM %[1]s WHERE %[1]s.%[3]s='%[2]s' ORDER BY %[1]s.id DESC`, warehouseutils.WarehouseUploadsTable, sourceOrDestId, sourceOrDestColumn)

	err = dbHandle.QueryRow(sqlStatement).Scan(&lastStagingFileID)
	if err != nil && err != sql.ErrNoRows {
		err = fmt.Errorf("Query: %s failed with Error : %w", sqlStatement, err)
		return
	}

	sqlStatement = fmt.Sprintf(`SELECT COUNT(*)
                                FROM %[1]s
								WHERE %[1]s.id > %[2]v AND %[1]s.%[4]s='%[3]s'`,
		warehouseutils.WarehouseStagingFilesTable, lastStagingFileID, sourceOrDestId, sourceOrDestColumn)

	err = dbHandle.QueryRow(sqlStatement).Scan(&fileCount)
	if err != nil && err != sql.ErrNoRows {
		err = fmt.Errorf("Query: %s failed with Error : %w", sqlStatement, err)
		return
	}

	return fileCount, nil
}

func getPendingUploadCount(sourceOrDestId string, isSourceId bool) (uploadCount int64, err error) {
	sourceOrDestColumn := ""
	if isSourceId {
		sourceOrDestColumn = "source_id"
	} else {
		sourceOrDestColumn = "destination_id"
	}
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*)
								FROM %[1]s
								WHERE %[1]s.status NOT IN ('%[2]s', '%[3]s') AND %[1]s.%[5]s='%[4]s'
	`, warehouseutils.WarehouseUploadsTable, ExportedData, Aborted, sourceOrDestId, sourceOrDestColumn)

	err = dbHandle.QueryRow(sqlStatement).Scan(&uploadCount)
	if err != nil && err != sql.ErrNoRows {
		err = fmt.Errorf("Query: %s failed with Error : %w", sqlStatement, err)
		return
	}

	return uploadCount, nil
}

func triggerUploadHandler(w http.ResponseWriter, r *http.Request) {
	// TODO : respond with errors in a common way
	pkgLogger.LogRequest(r)

	// read body
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// unmarshall body
	var triggerUploadReq warehouseutils.TriggerUploadRequestT
	err = json.Unmarshal(body, &triggerUploadReq)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error unmarshalling body: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}

	err = TriggerUploadHandler(triggerUploadReq.SourceID, triggerUploadReq.DestinationID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func TriggerUploadHandler(sourceID string, destID string) (error){

	// return error if source id and dest id is empty
	if sourceID == "" && destID == "" {
		err := fmt.Errorf("Empty source and destination id")
		pkgLogger.Errorf("[WH]: trigger upload : %v", err)
		return err
	}

	wh := make([]warehouseutils.WarehouseT, 0)

	if sourceID != "" && destID == "" {
		// get all wh destinations for given source id
		connectionsMapLock.Lock()
		for _, srcMap := range connectionsMap {
			for srcID, w := range srcMap {
				if srcID == sourceID {
					wh = append(wh, w)
				}
			}
		}
		connectionsMapLock.Unlock()
	}
	if destID != "" {
		connectionsMapLock.Lock()
		for destinationId, srcMap := range connectionsMap {
			if destinationId == destID {
				for _, w := range srcMap {
					wh = append(wh, w)
				}
			}
		}
		connectionsMapLock.Unlock()
	}

	// return error if no such destinations found
	if len(wh) == 0 {
		err := fmt.Errorf("No warehouse destinations found for source id '%s'", sourceID)
		pkgLogger.Errorf("[WH]: %v", err)
		return err
	}

	// iterate over each wh destination and trigger upload
	for _, warehouse := range wh {
		triggerUpload(warehouse)
	}
	return nil
}

func isUploadTriggered(wh warehouseutils.WarehouseT) bool {
	triggerUploadsMapLock.Lock()
	isTriggered := triggerUploadsMap[wh.Identifier]
	triggerUploadsMapLock.Unlock()
	return isTriggered
}

func triggerUpload(wh warehouseutils.WarehouseT) {
	triggerUploadsMapLock.Lock()
	triggerUploadsMap[wh.Identifier] = true
	triggerUploadsMapLock.Unlock()
	pkgLogger.Infof("[WH]: Upload triggered for warehouse '%s'", wh.Identifier)
}

func clearTriggeredUpload(wh warehouseutils.WarehouseT) {
	triggerUploadsMapLock.Lock()
	delete(triggerUploadsMap, wh.Identifier)
	triggerUploadsMapLock.Unlock()
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	dbService := ""
	pgNotifierService := ""
	if runningMode != DegradedMode {
		if !CheckPGHealth(notifier.GetDBHandle()) {
			http.Error(w, "Cannot connect to pgNotifierService", http.StatusInternalServerError)
			return
		}
		pgNotifierService = "UP"
	}

	if isMaster() {
		if !CheckPGHealth(dbHandle) {
			http.Error(w, "Cannot connect to dbService", http.StatusInternalServerError)
			return
		}
		dbService = "UP"
	}

	healthVal := fmt.Sprintf(`{"server":"UP", "db":"%s","pgNotifier":"%s","acceptingEvents":"TRUE","warehouseMode":"%s","goroutines":"%d"}`, dbService, pgNotifierService, strings.ToUpper(warehouseMode), runtime.NumGoroutine())
	w.Write([]byte(healthVal))
}

func getConnectionString() string {
	if !CheckForWarehouseEnvVars() {
		return jobsdb.GetConnectionString()
	}
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)
}

func startWebHandler() {
	// do not register same endpoint when running embedded in rudder backend
	if isStandAlone() {
		http.HandleFunc("/health", healthHandler)
	}
	if isMaster() {
		backendconfig.WaitForConfig()
		http.HandleFunc("/v1/process", processHandler)
		// triggers uploads only when there are pending events and triggerUpload is sent for a sourceId
		http.HandleFunc("/v1/warehouse/pending-events", pendingEventsHandler)
		// triggers uploads for a source
		http.HandleFunc("/v1/warehouse/trigger-upload", triggerUploadHandler)
		pkgLogger.Infof("WH: Starting warehouse master service in %d", webPort)
	} else {
		pkgLogger.Infof("WH: Starting warehouse slave service in %d", webPort)
	}
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(webPort), bugsnag.Handler(nil)))
}

// CheckForWarehouseEnvVars Checks if all the required Env Variables for Warehouse are present
func CheckForWarehouseEnvVars() bool {
	return config.IsEnvSet("WAREHOUSE_JOBS_DB_HOST") &&
		config.IsEnvSet("WAREHOUSE_JOBS_DB_USER") &&
		config.IsEnvSet("WAREHOUSE_JOBS_DB_DB_NAME") &&
		config.IsEnvSet("WAREHOUSE_JOBS_DB_PASSWORD")
}

// This checks if gateway is running or not
func isStandAlone() bool {
	return warehouseMode != EmbeddedMode && warehouseMode != PooledWHSlaveMode
}

func isMaster() bool {
	return warehouseMode == config.MasterMode ||
		warehouseMode == config.MasterSlaveMode ||
		warehouseMode == config.EmbeddedMode ||
		warehouseMode == config.PooledWHSlaveMode
}

func isSlave() bool {
	return warehouseMode == config.SlaveMode || warehouseMode == config.MasterSlaveMode || warehouseMode == config.EmbeddedMode
}

func isStandAloneSlave() bool {
	return warehouseMode == config.SlaveMode
}

func setupDB(connInfo string) {
	if isStandAloneSlave() {
		return
	}

	var err error
	dbHandle, err = sql.Open("postgres", connInfo)
	if err != nil {
		panic(err)
	}

	isDBCompatible, err := validators.IsPostgresCompatible(dbHandle)
	if err != nil {
		panic(err)
	}

	if !isDBCompatible {
		err := errors.New("Rudder Warehouse Service needs postgres version >= 10. Exiting")
		pkgLogger.Error(err)
		panic(err)
	}
	setupTables(dbHandle)
}

func Start(app app.Interface) {
	application = app
	time.Sleep(1 * time.Second)
	// do not start warehouse service if rudder core is not in normal mode and warehouse is running in same process as rudder core
	if !isStandAlone() && !db.IsNormalMode() {
		pkgLogger.Infof("Skipping start of warehouse service...")
		return
	}

	pkgLogger.Infof("WH: Starting Warehouse service...")
	psqlInfo := getConnectionString()

	setupDB(psqlInfo)
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Fatal(r)
			panic(r)
		}
		startWebHandler()
	}()

	runningMode := config.GetEnv("RSERVER_WAREHOUSE_RUNNING_MODE", "")
	if runningMode == DegradedMode {
		pkgLogger.Infof("WH: Running warehouse service in degared mode...")
		if isMaster() {
			rruntime.Go(func() {
				minimalConfigSubscriber()
			})
			InitWarehouseAPI(dbHandle, pkgLogger.Child("upload_api"))
		}
		return
	}
	var err error
	workspaceIdentifier := fmt.Sprintf(`%s::%s`, config.GetKubeNamespace(), misc.GetMD5Hash(config.GetWorkspaceToken()))
	notifier, err = pgnotifier.New(workspaceIdentifier, psqlInfo)
	if err != nil {
		panic(err)
	}

	//Setting up reporting client
	// only if standalone or embeded connecting to diff DB for warehouse
	if (isStandAlone() && isMaster()) || (jobsdb.GetConnectionString() != psqlInfo) {
		if application.Features().Reporting != nil {
			reporting := application.Features().Reporting.Setup(backendconfig.DefaultBackendConfig)
			reporting.AddClient(types.Config{ConnInfo: psqlInfo, ClientName: types.WAREHOUSE_REPORTING_CLIENT})
		}
	}

	if isStandAlone() && isMaster() {
		destinationdebugger.Setup()
	}

	if isSlave() {
		pkgLogger.Infof("WH: Starting warehouse slave...")
		setupSlave()
	}

	if isMaster() {
		pkgLogger.Infof("[WH]: Starting warehouse master...")
		err = notifier.AddTopic(StagingFilesPGNotifierChannel)
		if err != nil {
			panic(err)
		}
		rruntime.Go(func() {
			monitorDestRouters()
		})
		rruntime.Go(func() {
			runArchiver(dbHandle)
		})
		InitWarehouseAPI(dbHandle, pkgLogger.Child("upload_api"))
	}
}
