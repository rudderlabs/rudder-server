package warehouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bugsnag/bugsnag-go/v2"
	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/pubsub"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/thoas/go-funk"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"
)

var (
	application                         app.Interface
	webPort                             int
	dbHandle                            *sql.DB
	notifier                            pgnotifier.PgNotifierT
	noOfSlaveWorkerRoutines             int
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
	useParquetLoadFilesRS               bool
	skipDeepEqualSchemas                bool
	maxParallelJobCreation              int
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
	DegradedMode        = "degraded"
	triggerUploadQPName = "triggerUpload"
)

type WorkerIdentifierT string
type JobIDT int64

type HandleT struct {
	destType                          string
	warehouses                        []warehouseutils.WarehouseT
	dbHandle                          *sql.DB
	notifier                          pgnotifier.PgNotifierT
	isEnabled                         bool
	configSubscriberLock              sync.RWMutex
	workerChannelMap                  map[string]chan *UploadJobT
	workerChannelMapLock              sync.RWMutex
	initialConfigFetched              bool
	inProgressMap                     map[WorkerIdentifierT][]JobIDT
	inProgressMapLock                 sync.RWMutex
	areBeingEnqueuedLock              sync.RWMutex
	noOfWorkers                       int
	activeWorkerCount                 int
	activeWorkerCountLock             sync.RWMutex
	maxConcurrentUploadJobs           int
	allowMultipleSourcesForJobsPickup bool

	backgroundCancel context.CancelFunc
	backgroundGroup  errgroup.Group
	backgroundWait   func() error
}

type ErrorResponseT struct {
	Error string
}

func Init4() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse")
}

func loadConfig() {
	//Port where WH is running
	config.RegisterIntConfigVariable(8082, &webPort, false, 1, "Warehouse.webPort")
	config.RegisterIntConfigVariable(4, &noOfSlaveWorkerRoutines, true, 1, "Warehouse.noOfSlaveWorkerRoutines")
	config.RegisterIntConfigVariable(960, &stagingFilesBatchSize, true, 1, "Warehouse.stagingFilesBatchSize")
	config.RegisterInt64ConfigVariable(1800, &uploadFreqInS, true, 1, "Warehouse.uploadFreqInS")
	config.RegisterDurationConfigVariable(time.Duration(5), &mainLoopSleep, true, time.Second, []string{"Warehouse.mainLoopSleep", "Warehouse.mainLoopSleepInS"}...)
	crashRecoverWarehouses = []string{warehouseutils.RS, warehouseutils.POSTGRES, warehouseutils.MSSQL, warehouseutils.AZURE_SYNAPSE, warehouseutils.DELTALAKE}
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
	config.RegisterBoolConfigVariable(true, &ShouldForceSetLowerVersion, false, "SQLMigrator.forceSetLowerVersion")
	config.RegisterBoolConfigVariable(false, &skipDeepEqualSchemas, true, "Warehouse.skipDeepEqualSchemas")
	config.RegisterIntConfigVariable(8, &maxParallelJobCreation, true, 1, "Warehouse.maxParallelJobCreation")
}

// get name of the worker (`destID_namespace`) to be stored in map wh.workerChannelMap
func (wh *HandleT) workerIdentifier(warehouse warehouseutils.WarehouseT) (identifier string) {
	identifier = fmt.Sprintf(`%s_%s`, warehouse.Destination.ID, warehouse.Namespace)

	if wh.allowMultipleSourcesForJobsPickup {
		identifier = fmt.Sprintf(`%s_%s_%s`, warehouse.Source.ID, warehouse.Destination.ID, warehouse.Namespace)
	}
	return
}

func (wh *HandleT) getActiveWorkerCount() int {
	wh.activeWorkerCountLock.Lock()
	defer wh.activeWorkerCountLock.Unlock()
	return wh.activeWorkerCount
}

func (wh *HandleT) decrementActiveWorkers() {
	// decrement number of workers actively engaged
	wh.activeWorkerCountLock.Lock()
	wh.activeWorkerCount--
	wh.activeWorkerCountLock.Unlock()
}

func (wh *HandleT) incrementActiveWorkers() {
	// increment number of workers actively engaged
	wh.activeWorkerCountLock.Lock()
	wh.activeWorkerCount++
	wh.activeWorkerCountLock.Unlock()
}

func (wh *HandleT) initWorker() chan *UploadJobT {
	workerChan := make(chan *UploadJobT, 1000)
	for i := 0; i < wh.maxConcurrentUploadJobs; i++ {
		wh.backgroundGroup.Go(func() error {
			for uploadJob := range workerChan {
				wh.incrementActiveWorkers()
				err := wh.handleUploadJob(uploadJob)
				if err != nil {
					pkgLogger.Errorf("[WH] Failed in handle Upload jobs for worker: %+w", err)
				}
				wh.removeDestInProgress(uploadJob.warehouse, uploadJob.upload.ID)
				wh.decrementActiveWorkers()
			}
			return nil
		})
	}
	return workerChan
}

func (wh *HandleT) handleUploadJob(uploadJob *UploadJobT) (err error) {
	// Process the upload job
	err = uploadJob.run()
	return
}

func (wh *HandleT) backendConfigSubscriber() {
	ch := make(chan pubsub.DataEvent)
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

				workerName := wh.workerIdentifier(warehouse)
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
				if warehouse.Destination.Config["sslMode"] == "verify-ca" {
					if err := warehouseutils.WriteSSLKeys(warehouse.Destination); err.IsError() {
						pkgLogger.Error(err.Error())
						persisteSSLFileErrorStat(wh.destType, destination.Name, destination.ID, source.Name, source.ID, err.GetErrTag())
					}
				}
				connectionsMap[destination.ID][source.ID] = warehouse
				connectionsMapLock.Unlock()

				if warehouseutils.IDResolutionEnabled() && misc.ContainsString(warehouseutils.IdentityEnabledWarehouses, warehouse.Type) {
					wh.setupIdentityTables(warehouse)
					if shouldPopulateHistoricIdentities && warehouse.Destination.Enabled {
						// non blocking populate historic identities
						wh.populateHistoricIdentities(warehouse)
					}
				}
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
func (wh *HandleT) getNamespace(configI interface{}, source backendconfig.SourceT, destination backendconfig.DestinationT, destType string) string {
	configMap := configI.(map[string]interface{})
	var namespace string
	if destType == warehouseutils.CLICKHOUSE {
		//TODO: Handle if configMap["database"] is nil
		return configMap["database"].(string)
	}
	if configMap["namespace"] != nil {
		namespace = configMap["namespace"].(string)
		if len(strings.TrimSpace(namespace)) > 0 {
			return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, namespace))
		}
	}
	// TODO: Move config to global level based on use case
	namespacePrefix := config.GetString(fmt.Sprintf("Warehouse.%s.customDatasetPrefix", warehouseutils.WHDestNameMap[destType]), "")
	if namespacePrefix != "" {
		return warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, fmt.Sprintf(`%s_%s`, namespacePrefix, source.Name)))
	}
	var exists bool
	if namespace, exists = warehouseutils.GetNamespace(source, destination, wh.dbHandle); !exists {
		namespace = warehouseutils.ToProviderCase(destType, warehouseutils.ToSafeNamespace(destType, source.Name))
	}
	return namespace
}

func (wh *HandleT) getStagingFiles(warehouse warehouseutils.WarehouseT, startID int64, endID int64) ([]*StagingFileT, error) {
	sqlStatement := fmt.Sprintf(`SELECT id, location, status, metadata->>'time_window_year', metadata->>'time_window_month', metadata->>'time_window_day', metadata->>'time_window_hour'
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
		var timeWindowYear, timeWindowMonth, timeWindowDay, timeWindowHour sql.NullInt64
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &jsonUpload.Status, &timeWindowYear, &timeWindowMonth, &timeWindowDay, &timeWindowHour)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		jsonUpload.TimeWindow = time.Date(int(timeWindowYear.Int64), time.Month(timeWindowMonth.Int64), int(timeWindowDay.Int64), int(timeWindowHour.Int64), 0, 0, 0, time.UTC)
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

	sqlStatement = fmt.Sprintf(`SELECT id, location, status, first_event_at, last_event_at, metadata->>'source_batch_id', metadata->>'source_task_id', metadata->>'source_task_run_id', metadata->>'source_job_id', metadata->>'source_job_run_id', metadata->>'use_rudder_storage', metadata->>'time_window_year', metadata->>'time_window_month', metadata->>'time_window_day', metadata->>'time_window_hour'
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
	var timeWindowYear, timeWindowMonth, timeWindowDay, timeWindowHour sql.NullInt64
	var UseRudderStorage sql.NullBool
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &jsonUpload.Status, &firstEventAt, &lastEventAt, &sourceBatchID, &sourceTaskID, &sourceTaskRunID, &sourceJobID, &sourceJobRunID, &UseRudderStorage, &timeWindowYear, &timeWindowMonth, &timeWindowDay, &timeWindowHour)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		jsonUpload.FirstEventAt = firstEventAt.Time
		jsonUpload.LastEventAt = lastEventAt.Time
		jsonUpload.TimeWindow = time.Date(int(timeWindowYear.Int64), time.Month(timeWindowMonth.Int64), int(timeWindowDay.Int64), int(timeWindowHour.Int64), 0, 0, 0, time.UTC)
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

func (wh *HandleT) initUpload(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT, isUploadTriggered bool, priority int) {
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
		"load_file_type":     warehouseutils.GetLoadFileType(wh.destType),
	}
	if isUploadTriggered {
		// set priority to 50 if the upload was manually triggered
		metadataMap["priority"] = 50
	}
	if priority != 0 {
		metadataMap["priority"] = priority
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
	identifier := wh.workerIdentifier(warehouse)
	wh.inProgressMapLock.Lock()
	defer wh.inProgressMapLock.Unlock()
	wh.inProgressMap[WorkerIdentifierT(identifier)] = append(wh.inProgressMap[WorkerIdentifierT(identifier)], JobIDT(jobID))
}

func (wh *HandleT) removeDestInProgress(warehouse warehouseutils.WarehouseT, jobID int64) {
	wh.inProgressMapLock.Lock()
	defer wh.inProgressMapLock.Unlock()
	if idx, inProgess := wh.isUploadJobInProgress(warehouse, jobID); inProgess {
		identifier := wh.workerIdentifier(warehouse)
		wh.inProgressMap[WorkerIdentifierT(identifier)] = removeFromJobsIDT(wh.inProgressMap[WorkerIdentifierT(identifier)], idx)
	}
}

func (wh *HandleT) isUploadJobInProgress(warehouse warehouseutils.WarehouseT, jobID int64) (inProgressIdx int, inProgress bool) {
	identifier := wh.workerIdentifier(warehouse)
	for idx, id := range wh.inProgressMap[WorkerIdentifierT(identifier)] {
		if jobID == int64(id) {
			inProgress = true
			inProgressIdx = idx
			return
		}
	}
	return
}

func removeFromJobsIDT(slice []JobIDT, idx int) []JobIDT {
	return append(slice[:idx], slice[idx+1:]...)
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

func (wh *HandleT) createUploadJobsFromStagingFiles(warehouse warehouseutils.WarehouseT, whManager manager.ManagerI, stagingFilesList []*StagingFileT, priority int) {
	// count := 0
	// Process staging files in batches of stagingFilesBatchSize
	// Eg. If there are 1000 pending staging files and stagingFilesBatchSize is 100,
	// Then we create 10 new entries in wh_uploads table each with 100 staging files
	var stagingFilesInUpload []*StagingFileT
	var counter int
	uploadTriggered := isUploadTriggered(warehouse)
	initUpload := func() {
		wh.initUpload(warehouse, stagingFilesInUpload, uploadTriggered, priority)
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

func (wh *HandleT) getLatestUploadStatus(warehouse warehouseutils.WarehouseT) (uploadID int64, status string, priority int) {
	sqlStatement := fmt.Sprintf(`SELECT id, status, COALESCE(metadata->>'priority', '100')::int FROM %[1]s WHERE %[1]s.destination_type='%[2]s' AND %[1]s.source_id='%[3]s' AND %[1]s.destination_id='%[4]s' ORDER BY id DESC LIMIT 1`, warehouseutils.WarehouseUploadsTable, wh.destType, warehouse.Source.ID, warehouse.Destination.ID)
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&uploadID, &status, &priority)
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
	uploadID, uploadStatus, priority := wh.getLatestUploadStatus(warehouse)
	if uploadStatus == Waiting {
		// If it is present do nothing else delete it
		if _, inProgess := wh.isUploadJobInProgress(warehouse, uploadID); !inProgess {
			wh.deleteWaitingUploadJob(uploadID)
		}
	}
	wh.areBeingEnqueuedLock.Unlock()

	stagingFilesFetchStat := stats.DefaultStats.NewTaggedStat("wh_scheduler.pending_staging_files", stats.TimerType, stats.Tags{
		"destinationID": warehouse.Destination.ID,
		"destType":      warehouse.Destination.DestinationDefinition.Name,
	})
	stagingFilesFetchStat.Start()
	stagingFilesList, err := wh.getPendingStagingFiles(warehouse)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to get pending staging files: %s with error %v", warehouse.Identifier, err)
		return err
	}
	stagingFilesFetchStat.End()

	if len(stagingFilesList) == 0 {
		pkgLogger.Debugf("[WH]: Found no pending staging files for %s", warehouse.Identifier)
		return nil
	}

	uploadJobCreationStat := stats.DefaultStats.NewTaggedStat("wh_scheduler.create_upload_jobs", stats.TimerType, stats.Tags{
		"destinationID": warehouse.Destination.ID,
		"destType":      warehouse.Destination.DestinationDefinition.Name,
	})
	uploadJobCreationStat.Start()
	wh.createUploadJobsFromStagingFiles(warehouse, whManager, stagingFilesList, priority)
	setLastProcessedMarker(warehouse)
	uploadJobCreationStat.End()

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

func (wh *HandleT) mainLoop(ctx context.Context) {
	for {
		if !wh.isEnabled {
			select {
			case <-ctx.Done():
				return
			case <-time.After(mainLoopSleep):
			}
			continue
		}

		jobCreationChan := make(chan struct{}, maxParallelJobCreation)
		wh.configSubscriberLock.RLock()
		wg := sync.WaitGroup{}
		wg.Add(len(wh.warehouses))

		// We will be measuring the overall lag
		// as part of the process.
		whSchedulerLag := stats.DefaultStats.NewStat("wh_scheduler.total_scheduling_time", stats.TimerType)
		whSchedulerLag.Start()

		for _, warehouse := range wh.warehouses {
			w := warehouse
			rruntime.GoForWarehouse(func() {
				jobCreationChan <- struct{}{}
				defer func() {
					wg.Done()
					<-jobCreationChan
				}()

				pkgLogger.Debugf("[WH] Processing Jobs for warehouse: %s", w.Identifier)
				err := wh.createJobs(w)
				if err != nil {
					pkgLogger.Errorf("[WH] Failed to process warehouse Jobs: %v", err)
				}
			})
		}
		wh.configSubscriberLock.RUnlock()
		wg.Wait()

		whSchedulerLag.End()
		select {
		case <-ctx.Done():
			return
		case <-time.After(mainLoopSleep):
		}

	}
}

func (wh *HandleT) getUploadsToProcess(availableWorkers int, skipIdentifiers []string) ([]*UploadJobT, error) {

	var skipIdentifiersSQL string
	var partitionIdentifierSQL = `destination_id, namespace`

	if len(skipIdentifiers) > 0 {
		skipIdentifiersSQL = `and ((destination_id || '_' || namespace)) != ALL($1)`
	}

	if wh.allowMultipleSourcesForJobsPickup {
		if len(skipIdentifiers) > 0 {
			skipIdentifiersSQL = `and ((source_id || '_' || destination_id || '_' || namespace)) != ALL($1)`
		}
		partitionIdentifierSQL = fmt.Sprintf(`%s, %s`, "source_id", partitionIdentifierSQL)
	}

	sqlStatement := fmt.Sprintf(`
			SELECT
					id, status, schema, mergedSchema, namespace, source_id, destination_id, destination_type, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, error, metadata, timings->0 as firstTiming, timings->-1 as lastTiming, metadata->>'use_rudder_storage', timings, COALESCE(metadata->>'priority', '100')::int
				FROM (
					SELECT
						ROW_NUMBER() OVER (PARTITION BY %s ORDER BY COALESCE(metadata->>'priority', '100')::int ASC, id ASC) AS row_number,
						t.*
					FROM
						%s t
					WHERE
						t.destination_type = '%s' and t.in_progress=%t and t.status != '%s' and t.status != '%s' %s and COALESCE(metadata->>'nextRetryTime', now()::text)::timestamptz <= now()
				) grouped_uplaods
				WHERE
					grouped_uplaods.row_number = 1
				ORDER BY
					COALESCE(metadata->>'priority', '100')::int ASC, id ASC
				LIMIT %d;

		`, partitionIdentifierSQL, warehouseutils.WarehouseUploadsTable, wh.destType, false, ExportedData, Aborted, skipIdentifiersSQL, availableWorkers)

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
		var mergedSchema json.RawMessage
		var firstTiming sql.NullString
		var lastTiming sql.NullString
		var useRudderStorage sql.NullBool
		err := rows.Scan(&upload.ID, &upload.Status, &schema, &mergedSchema, &upload.Namespace, &upload.SourceID, &upload.DestinationID, &upload.DestinationType, &upload.StartStagingFileID, &upload.EndStagingFileID, &upload.StartLoadFileID, &upload.EndLoadFileID, &upload.Error, &upload.Metadata, &firstTiming, &lastTiming, &useRudderStorage, &upload.TimingsObj, &upload.Priority)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		upload.UploadSchema = warehouseutils.JSONSchemaToMap(schema)
		upload.MergedSchema = warehouseutils.JSONSchemaToMap(mergedSchema)
		upload.UseRudderStorage = useRudderStorage.Bool
		// cloud sources info
		upload.SourceBatchID = gjson.GetBytes(upload.Metadata, "source_batch_id").String()
		upload.SourceTaskID = gjson.GetBytes(upload.Metadata, "source_task_id").String()
		upload.SourceTaskRunID = gjson.GetBytes(upload.Metadata, "source_task_run_id").String()
		upload.SourceJobID = gjson.GetBytes(upload.Metadata, "source_job_id").String()
		upload.SourceJobRunID = gjson.GetBytes(upload.Metadata, "source_job_run_id").String()
		// load file type
		upload.LoadFileType = gjson.GetBytes(upload.Metadata, "load_file_type").String()

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
	for k, v := range wh.inProgressMap {
		if len(v) >= wh.maxConcurrentUploadJobs {
			identifiers = append(identifiers, string(k))
		}
	}
	return
}

func (wh *HandleT) runUploadJobAllocator(ctx context.Context) {
loop:
	for {
		if !wh.initialConfigFetched {
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(waitForConfig):
			}
			continue
		}

		availableWorkers := wh.noOfWorkers - wh.getActiveWorkerCount()
		if availableWorkers < 1 {
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(waitForWorkerSleep):
			}
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
			workerName := wh.workerIdentifier(uploadJob.warehouse)
			wh.workerChannelMapLock.Lock()
			wh.workerChannelMap[workerName] <- uploadJob
			wh.workerChannelMapLock.Unlock()
		}

		select {
		case <-ctx.Done():
			break loop
		case <-time.After(uploadAllocatorSleep):
		}
	}

	wh.workerChannelMapLock.Lock()
	for _, workerChannel := range wh.workerChannelMap {
		close(workerChannel)
	}
	wh.workerChannelMapLock.Unlock()
}

func (wh *HandleT) uploadStatusTrack(ctx context.Context) {
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

			getUploadStatusStat("warehouse_successful_upload_exists", warehouse.Type, warehouse.Destination.ID, warehouse.Source.Name, warehouse.Destination.Name, warehouse.Source.ID).Count(uploaded)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(uploadStatusTrackFrequency):
		}
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
	if !misc.ContainsString(crashRecoverWarehouses, wh.destType) {
		return
	}
	sqlStatement := fmt.Sprintf(`SELECT destination_id FROM %s WHERE destination_type='%s' AND (status='%s' OR status='%s') and in_progress=%t`, warehouseutils.WarehouseUploadsTable, wh.destType, getInProgressState(ExportedData), getFailedState(ExportedData), true)
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

func (wh *HandleT) Setup(whType string, whName string) {
	pkgLogger.Infof("WH: Warehouse Router started: %s", whType)
	wh.dbHandle = dbHandle
	wh.notifier = notifier
	wh.destType = whType
	wh.setInterruptedDestinations()
	wh.resetInProgressJobs()
	wh.Enable()
	wh.workerChannelMap = make(map[string]chan *UploadJobT)
	wh.inProgressMap = make(map[WorkerIdentifierT][]JobIDT)
	config.RegisterIntConfigVariable(8, &wh.noOfWorkers, true, 1, fmt.Sprintf(`Warehouse.%v.noOfWorkers`, whName), "Warehouse.noOfWorkers")
	config.RegisterIntConfigVariable(1, &wh.maxConcurrentUploadJobs, false, 1, fmt.Sprintf(`Warehouse.%v.maxConcurrentUploadJobs`, whName))
	config.RegisterBoolConfigVariable(false, &wh.allowMultipleSourcesForJobsPickup, false, fmt.Sprintf(`Warehouse.%v.allowMultipleSourcesForJobsPickup`, whName))

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	wh.backgroundCancel = cancel
	wh.backgroundWait = g.Wait

	rruntime.GoForWarehouse(func() {
		wh.backendConfigSubscriber()
	})

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		wh.runUploadJobAllocator(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnagForWarehouse(func() error {
		wh.mainLoop(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		pkgLogger.Infof("WH: Warehouse Idle upload tracker started")
		wh.uploadStatusTrack(ctx)
		return nil
	}))
}

func (wh *HandleT) Shutdown() {
	wh.backgroundCancel()
	wh.backgroundWait()
}

func (wh *HandleT) resetInProgressJobs() {
	sqlStatement := fmt.Sprintf(`UPDATE %s SET in_progress=%t WHERE destination_type='%s' AND in_progress=%t`, warehouseutils.WarehouseUploadsTable, false, wh.destType, true)
	_, err := wh.dbHandle.Query(sqlStatement)
	if err != nil {
		panic(fmt.Errorf("Query: %s failed with Error : %w", sqlStatement, err))
	}
}

func minimalConfigSubscriber() {
	ch := make(chan pubsub.DataEvent)
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
				if misc.ContainsString(warehouseutils.WarehouseDestinations, destination.DestinationDefinition.Name) {
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
func monitorDestRouters(ctx context.Context) {
	ch := make(chan pubsub.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	dstToWhRouter := make(map[string]*HandleT)

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case config := <-ch:
			onConfigDataEvent(config, dstToWhRouter)
		}
	}

	g, _ := errgroup.WithContext(context.Background())
	for _, wh := range dstToWhRouter {
		wh := wh
		g.Go(func() error {
			wh.Shutdown()
			return nil
		})
	}
	g.Wait()

}

func onConfigDataEvent(config pubsub.DataEvent, dstToWhRouter map[string]*HandleT) {
	pkgLogger.Debug("Got config from config-backend", config)
	sources := config.Data.(backendconfig.ConfigT)
	enabledDestinations := make(map[string]bool)
	for _, source := range sources.Sources {
		for _, destination := range source.Destinations {
			enabledDestinations[destination.DestinationDefinition.Name] = true
			if misc.ContainsString(warehouseutils.WarehouseDestinations, destination.DestinationDefinition.Name) {
				wh, ok := dstToWhRouter[destination.DestinationDefinition.Name]
				if !ok {
					pkgLogger.Info("Starting a new Warehouse Destination Router: ", destination.DestinationDefinition.Name)
					wh = &HandleT{}
					wh.configSubscriberLock.Lock()
					wh.Setup(destination.DestinationDefinition.Name, destination.DestinationDefinition.DisplayName)
					wh.configSubscriberLock.Unlock()
					dstToWhRouter[destination.DestinationDefinition.Name] = wh
				} else {
					pkgLogger.Debug("Enabling existing Destination: ", destination.DestinationDefinition.Name)
					wh.configSubscriberLock.Lock()
					wh.Enable()
					wh.configSubscriberLock.Unlock()
				}
			}
		}
	}
	if val, ok := sources.ConnectionFlags.Services["warehouse"]; ok {
		if UploadAPI.connectionManager != nil {
			UploadAPI.connectionManager.Apply(sources.ConnectionFlags.URL, val)
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

	body, err := io.ReadAll(r.Body)
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
		"time_window_year":   stagingFile.TimeWindow.Year(),
		"time_window_month":  stagingFile.TimeWindow.Month(),
		"time_window_day":    stagingFile.TimeWindow.Day(),
		"time_window_hour":   stagingFile.TimeWindow.Hour(),
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
	recordStagedRowsStat(stagingFile.TotalEvents, stagingFile.BatchDestination.Destination.DestinationDefinition.Name, stagingFile.BatchDestination.Destination.ID, stagingFile.BatchDestination.Source.Name, stagingFile.BatchDestination.Destination.Name, stagingFile.BatchDestination.Source.ID)
}

func pendingEventsHandler(w http.ResponseWriter, r *http.Request) {
	// TODO : respond with errors in a common way
	pkgLogger.LogRequest(r)

	// read body
	body, err := io.ReadAll(r.Body)
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
	var lastStagingFileIDRes sql.NullInt64
	sqlStatement := fmt.Sprintf(`SELECT MAX(end_staging_file_id) FROM %[1]s WHERE %[1]s.%[3]s='%[2]s'`, warehouseutils.WarehouseUploadsTable, sourceOrDestId, sourceOrDestColumn)

	err = dbHandle.QueryRow(sqlStatement).Scan(&lastStagingFileIDRes)
	if err != nil && err != sql.ErrNoRows {
		err = fmt.Errorf("Query: %s failed with Error : %w", sqlStatement, err)
		return
	}
	lastStagingFileID := int64(0)
	if lastStagingFileIDRes.Valid {
		lastStagingFileID = lastStagingFileIDRes.Int64
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
	body, err := io.ReadAll(r.Body)
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

func TriggerUploadHandler(sourceID string, destID string) error {

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

func databricksVersionHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(deltalake.GetDatabricksVersion()))
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

func startWebHandler(ctx context.Context) error {
	mux := http.NewServeMux()

	// do not register same endpoint when running embedded in rudder backend
	if isStandAlone() {
		mux.HandleFunc("/health", healthHandler)
	}
	if runningMode != DegradedMode {
		if isMaster() {
			if err := backendconfig.WaitForConfig(ctx); err != nil {
				return err
			}
			mux.HandleFunc("/v1/process", processHandler)
			// triggers uploads only when there are pending events and triggerUpload is sent for a sourceId
			mux.HandleFunc("/v1/warehouse/pending-events", pendingEventsHandler)
			// triggers uploads for a source
			mux.HandleFunc("/v1/warehouse/trigger-upload", triggerUploadHandler)
			mux.HandleFunc("/databricksVersion", databricksVersionHandler)
			pkgLogger.Infof("WH: Starting warehouse master service in %d", webPort)
		} else {
			pkgLogger.Infof("WH: Starting warehouse slave service in %d", webPort)
		}
	}

	srv := http.Server{
		Addr:    fmt.Sprintf(":%d", webPort),
		Handler: bugsnag.Handler(mux),
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return srv.ListenAndServe()
	})
	g.Go(func() error {
		<-ctx.Done()
		return srv.Shutdown(context.Background())
	})

	return g.Wait()
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

func Start(ctx context.Context, app app.Interface) error {
	application = app
	time.Sleep(1 * time.Second)
	// do not start warehouse service if rudder core is not in normal mode and warehouse is running in same process as rudder core
	if !isStandAlone() && !db.IsNormalMode() {
		pkgLogger.Infof("Skipping start of warehouse service...")
		return nil
	}

	pkgLogger.Infof("WH: Starting Warehouse service...")
	psqlInfo := getConnectionString()

	setupDB(psqlInfo)
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Fatal(r)
			panic(r)
		}
	}()

	runningMode := config.GetEnv("RSERVER_WAREHOUSE_RUNNING_MODE", "")
	if runningMode == DegradedMode {
		pkgLogger.Infof("WH: Running warehouse service in degraded mode...")
		if isMaster() {
			rruntime.GoForWarehouse(func() {
				minimalConfigSubscriber()
			})
			InitWarehouseAPI(dbHandle, pkgLogger.Child("upload_api"))
		}
		return startWebHandler(ctx)
	}
	var err error
	workspaceIdentifier := fmt.Sprintf(`%s::%s`, config.GetKubeNamespace(), misc.GetMD5Hash(config.GetWorkspaceToken()))
	notifier, err = pgnotifier.New(workspaceIdentifier, psqlInfo)
	if err != nil {
		panic(err)
	}

	g, ctx := errgroup.WithContext(ctx)

	//Setting up reporting client
	// only if standalone or embeded connecting to diff DB for warehouse
	if (isStandAlone() && isMaster()) || (jobsdb.GetConnectionString() != psqlInfo) {
		if application.Features().Reporting != nil {
			reporting := application.Features().Reporting.Setup(backendconfig.DefaultBackendConfig)

			g.Go(misc.WithBugsnagForWarehouse(func() error {
				reporting.AddClient(ctx, types.Config{ConnInfo: psqlInfo, ClientName: types.WAREHOUSE_REPORTING_CLIENT})
				return nil
			}))
		}
	}

	if isStandAlone() && isMaster() {
		destinationdebugger.Setup(backendconfig.DefaultBackendConfig)
	}

	if isSlave() {
		pkgLogger.Infof("WH: Starting warehouse slave...")
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return setupSlave(ctx)
		}))
	}

	if isMaster() {
		pkgLogger.Infof("[WH]: Starting warehouse master...")

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return notifier.ClearJobs(ctx)
		}))
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			monitorDestRouters(ctx)
			return nil
		}))
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			runArchiver(ctx, dbHandle)
			return nil
		}))
		InitWarehouseAPI(dbHandle, pkgLogger.Child("upload_api"))
	}

	g.Go(func() error {
		return startWebHandler(ctx)
	})

	return g.Wait()
}
