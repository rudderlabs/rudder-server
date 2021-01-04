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
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
	destinationConnectionTester "github.com/rudderlabs/rudder-server/services/destination-connection-tester"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/tidwall/gjson"
)

var (
	webPort                             int
	dbHandle                            *sql.DB
	notifier                            pgnotifier.PgNotifierT
	WarehouseDestinations               []string
	jobQueryBatchSize                   int
	noOfWorkers                         int
	noOfSlaveWorkerRoutines             int
	slaveWorkerRoutineBusy              []bool //Busy-true
	uploadFreqInS                       int64
	stagingFilesSchemaPaginationSize    int
	mainLoopSleep                       time.Duration
	workerRetrySleep                    time.Duration
	stagingFilesBatchSize               int
	crashRecoverWarehouses              []string
	inProgressMap                       map[string]bool
	inRecoveryMap                       map[string]bool
	inProgressMapLock                   sync.RWMutex
	lastEventTimeMap                    map[string]int64
	lastEventTimeMapLock                sync.RWMutex
	warehouseMode                       string
	warehouseSyncPreFetchCount          int
	warehouseSyncFreqIgnore             bool
	activeWorkerCount                   int
	activeWorkerCountLock               sync.RWMutex
	minRetryAttempts                    int
	retryTimeWindow                     time.Duration
	maxStagingFileReadBufferCapacityInK int
	destinationsMap                     map[string]warehouseutils.WarehouseT // destID -> warehouse map
	destinationsMapLock                 sync.RWMutex
	longRunningUploadStatThresholdInMin time.Duration
	pkgLogger                           logger.LoggerI
	numLoadFileUploadWorkers            int
	slaveUploadTimeout                  time.Duration
)

var (
	host, user, password, dbname, sslmode string
	port                                  int
)

// warehouses worker modes
const (
	MasterMode      = "master"
	SlaveMode       = "slave"
	MasterSlaveMode = "master_and_slave"
	EmbeddedMode    = "embedded"
)

const (
	DegradedMode                  = "degraded"
	StagingFilesPGNotifierChannel = "process_staging_file"
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
	uploadJobsQ          chan *UploadJobT
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
	webPort = config.GetInt("Warehouse.webPort", 8082)
	WarehouseDestinations = []string{"RS", "BQ", "SNOWFLAKE", "POSTGRES", "CLICKHOUSE"}
	jobQueryBatchSize = config.GetInt("Router.jobQueryBatchSize", 10000)
	noOfWorkers = config.GetInt("Warehouse.noOfWorkers", 8)
	noOfSlaveWorkerRoutines = config.GetInt("Warehouse.noOfSlaveWorkerRoutines", 4)
	stagingFilesBatchSize = config.GetInt("Warehouse.stagingFilesBatchSize", 240)
	uploadFreqInS = config.GetInt64("Warehouse.uploadFreqInS", 1800)
	mainLoopSleep = config.GetDuration("Warehouse.mainLoopSleepInS", 1) * time.Second
	workerRetrySleep = config.GetDuration("Warehouse.workerRetrySleepInS", 5) * time.Second
	crashRecoverWarehouses = []string{"RS"}
	inProgressMap = map[string]bool{}
	inRecoveryMap = map[string]bool{}
	lastEventTimeMap = map[string]int64{}
	warehouseMode = config.GetString("Warehouse.mode", "embedded")
	host = config.GetEnv("WAREHOUSE_JOBS_DB_HOST", "localhost")
	user = config.GetEnv("WAREHOUSE_JOBS_DB_USER", "ubuntu")
	dbname = config.GetEnv("WAREHOUSE_JOBS_DB_DB_NAME", "ubuntu")
	port, _ = strconv.Atoi(config.GetEnv("WAREHOUSE_JOBS_DB_PORT", "5432"))
	password = config.GetEnv("WAREHOUSE_JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from
	sslmode = config.GetEnv("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	warehouseSyncPreFetchCount = config.GetInt("Warehouse.warehouseSyncPreFetchCount", 10)
	stagingFilesSchemaPaginationSize = config.GetInt("Warehouse.stagingFilesSchemaPaginationSize", 100)
	warehouseSyncFreqIgnore = config.GetBool("Warehouse.warehouseSyncFreqIgnore", false)
	minRetryAttempts = config.GetInt("Warehouse.minRetryAttempts", 3)
	retryTimeWindow = config.GetDuration("Warehouse.retryTimeWindowInMins", time.Duration(180)) * time.Minute
	destinationsMap = map[string]warehouseutils.WarehouseT{}
	maxStagingFileReadBufferCapacityInK = config.GetInt("Warehouse.maxStagingFileReadBufferCapacityInK", 10240)
	longRunningUploadStatThresholdInMin = config.GetDuration("Warehouse.longRunningUploadStatThresholdInMin", time.Duration(120)) * time.Minute
	slaveUploadTimeout = config.GetDuration("Warehouse.slaveUploadTimeoutInMin", time.Duration(10)) * time.Minute
	numLoadFileUploadWorkers = config.GetInt("Warehouse.numLoadFileUploadWorkers", 8)
}

// get name of the worker (`destID_namespace`) to be stored in map wh.workerChannelMap
func workerIdentifier(warehouse warehouseutils.WarehouseT) string {
	return fmt.Sprintf(`%s_%s`, warehouse.Destination.ID, warehouse.Namespace)
}

func (wh *HandleT) waitAndLockAvailableWorker() {
	// infinite loop to check for active workers count and retry if not
	// break after handling
	for {
		// check number of workers actively enagaged
		// if limit hit, sleep and check again
		// activeWorkerCount is across all wh.destType's
		activeWorkerCountLock.Lock()
		activeWorkers := activeWorkerCount
		if activeWorkers >= noOfWorkers {
			activeWorkerCountLock.Unlock()
			pkgLogger.Debugf("WH: Setting to sleep and waiting till activeWorkers are less than %d", noOfWorkers)
			// TODO: add randomness to this ?
			time.Sleep(workerRetrySleep)
			continue
		}
		activeWorkerCount++
		activeWorkerCountLock.Unlock()
		break
	}
}

func (wh *HandleT) releaseWorker() {
	// decrement number of workers actively engaged
	activeWorkerCountLock.Lock()
	activeWorkerCount--
	activeWorkerCountLock.Unlock()
}

func (wh *HandleT) initWorker() chan *UploadJobT {
	workerChan := make(chan *UploadJobT, 1000)
	rruntime.Go(func() {
		for {
			uploadJob := <-workerChan
			err := wh.handleUploadJob(uploadJob)
			if err != nil {
				pkgLogger.Errorf("[WH] Failed in handle Upload jobs for worker: %+w", err)
			}
			setDestInProgress(uploadJob.warehouse, false)
		}
	})
	return workerChan
}

func (wh *HandleT) handleUploadJob(uploadJob *UploadJobT) (err error) {
	// Process the upload job
	err = uploadJob.run()
	wh.recordDeliveryStatus(uploadJob.warehouse.Destination.ID, uploadJob.upload.ID)
	wh.releaseWorker()

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

		for _, source := range allSources.Sources {
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
					Identifier:  fmt.Sprintf("%s:%s:%s", wh.destType, source.ID, destination.ID),
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

				destinationsMapLock.Lock()
				destinationsMap[destination.ID] = warehouseutils.WarehouseT{Destination: destination, Namespace: namespace, Type: wh.destType}
				destinationsMapLock.Unlock()

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
		pkgLogger.Debug("[WH] Unlocking config sub lock: %s", wh.destType)
		wh.configSubscriberLock.Unlock()
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
	sqlStatement := fmt.Sprintf(`SELECT id, location
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
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location)
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

	sqlStatement = fmt.Sprintf(`SELECT id, location, first_event_at, last_event_at
                                FROM %[1]s
								WHERE %[1]s.id > %[2]v AND %[1]s.source_id='%[3]s' AND %[1]s.destination_id='%[4]s'
								ORDER BY id ASC
								LIMIT %[5]d`,
		warehouseutils.WarehouseStagingFilesTable, lastStagingFileID, warehouse.Source.ID, warehouse.Destination.ID, stagingFilesBatchSize)
	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		panic(fmt.Errorf("Query: %s failed with Error : %w", sqlStatement, err))
	}
	defer rows.Close()

	var stagingFilesList []*StagingFileT
	var firstEventAt, lastEventAt sql.NullTime
	for rows.Next() {
		var jsonUpload StagingFileT
		err := rows.Scan(&jsonUpload.ID, &jsonUpload.Location, &firstEventAt, &lastEventAt)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		jsonUpload.FirstEventAt = firstEventAt.Time
		jsonUpload.LastEventAt = lastEventAt.Time
		stagingFilesList = append(stagingFilesList, &jsonUpload)
	}

	return stagingFilesList, nil
}

func (wh *HandleT) initUpload(warehouse warehouseutils.WarehouseT, jsonUploadsList []*StagingFileT) UploadT {
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
	row := stmt.QueryRow(warehouse.Source.ID, namespace, warehouse.Destination.ID, wh.destType, startJSONID, endJSONID, 0, 0, Waiting, "{}", "{}", "{}", firstEventAt, lastEventAt, now, now)

	var uploadID int64
	err = row.Scan(&uploadID)
	if err != nil {
		panic(err)
	}

	upload := UploadT{
		ID:                 uploadID,
		Namespace:          warehouse.Namespace,
		SourceID:           warehouse.Source.ID,
		DestinationID:      warehouse.Destination.ID,
		DestinationType:    wh.destType,
		StartStagingFileID: startJSONID,
		EndStagingFileID:   endJSONID,
		Status:             Waiting,
		FirstEventAt:       firstEventAt,
		LastEventAt:        lastEventAt,
	}

	return upload
}

func (wh *HandleT) getPendingUploads(warehouse warehouseutils.WarehouseT) ([]UploadT, error) {

	sqlStatement := fmt.Sprintf(`SELECT id, status, schema, namespace, source_id, destination_id, destination_type, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, error, metadata, timings->0 as firstTiming, timings->-1 as lastTiming 
								FROM %[1]s 
								WHERE (%[1]s.destination_type='%[2]s' 
									AND %[1]s.source_id='%[3]s' 
									AND %[1]s.destination_id = '%[4]s' 
									AND %[1]s.status != '%[5]s' 
									AND %[1]s.status != '%[6]s') 
								ORDER BY id asc`, warehouseutils.WarehouseUploadsTable, wh.destType, warehouse.Source.ID, warehouse.Destination.ID, ExportedData, Aborted)

	rows, err := wh.dbHandle.Query(sqlStatement)
	if err != nil && err != sql.ErrNoRows {
		return []UploadT{}, err
	}

	if err == sql.ErrNoRows {
		return []UploadT{}, nil
	}
	defer rows.Close()

	var uploads []UploadT
	for rows.Next() {
		var upload UploadT
		var schema json.RawMessage
		var firstTiming sql.NullString
		var lastTiming sql.NullString
		err := rows.Scan(&upload.ID, &upload.Status, &schema, &upload.Namespace, &upload.SourceID, &upload.DestinationID, &upload.DestinationType, &upload.StartStagingFileID, &upload.EndStagingFileID, &upload.StartLoadFileID, &upload.EndLoadFileID, &upload.Error, &upload.Metadata, &firstTiming, &lastTiming)
		if err != nil {
			panic(fmt.Errorf("Failed to scan result from query: %s\nwith Error : %w", sqlStatement, err))
		}
		upload.Schema = warehouseutils.JSONSchemaToMap(schema)

		_, upload.FirstAttemptAt = warehouseutils.TimingFromJSONString(firstTiming)
		var lastStatus string
		lastStatus, upload.LastAttemptAt = warehouseutils.TimingFromJSONString(lastTiming)
		upload.Attempts = gjson.Get(string(upload.Error), fmt.Sprintf(`%s.attempt`, lastStatus)).Int()

		uploads = append(uploads, upload)
	}

	return uploads, nil
}

func setDestInProgress(warehouse warehouseutils.WarehouseT, starting bool) {
	inProgressMapLock.Lock()
	defer inProgressMapLock.Unlock()
	if starting {
		inProgressMap[warehouse.Identifier] = true
	} else {
		delete(inProgressMap, warehouse.Identifier)
	}
}

func isDestInProgress(warehouse warehouseutils.WarehouseT) bool {
	inProgressMapLock.RLock()
	defer inProgressMapLock.RUnlock()
	if inProgressMap[warehouse.Identifier] {
		return true
	}
	return false
}

func uploadFrequencyExceeded(warehouse warehouseutils.WarehouseT, syncFrequency string) bool {
	freqInS := uploadFreqInS
	if syncFrequency != "" {
		freqInMin, _ := strconv.ParseInt(syncFrequency, 10, 64)
		freqInS = freqInMin * 60
	}
	lastEventTimeMapLock.Lock()
	defer lastEventTimeMapLock.Unlock()
	if lastExecTime, ok := lastEventTimeMap[warehouse.Identifier]; ok && timeutil.Now().Unix()-lastExecTime < freqInS {
		return true
	}
	return false
}

func setLastEventTime(warehouse warehouseutils.WarehouseT, lastEventAt time.Time) {
	lastEventTimeMapLock.Lock()
	defer lastEventTimeMapLock.Unlock()
	lastEventTimeMap[warehouse.Identifier] = lastEventAt.Unix()
}

//TODO: Clean this up
func (wh *HandleT) getUploadJobsForPendingUploads(warehouse warehouseutils.WarehouseT, whManager manager.ManagerI, pendingUploads []UploadT) (*UploadJobT, error) {
	for _, pendingUpload := range pendingUploads {
		copiedUpload := pendingUpload

		if !wh.canStartPendingUpload(pendingUpload, warehouse) {
			pkgLogger.Debugf("[WH]: Skipping pending upload for %s since current time less than next retry time", warehouse.Identifier)
			if pendingUpload.Status != TableUploadExportingFailed {
				//If we don't process the first pending upload, it doesn't make sense to attempt the following jobs. Hence we return here.
				return nil, fmt.Errorf("[WH]: Not a retriable job. Moving on to next unprocessed jobs")
			}
			continue
		}

		stagingFilesList, err := wh.getStagingFiles(warehouse, pendingUpload.StartStagingFileID, pendingUpload.EndStagingFileID)
		if err != nil {
			return nil, err
		}

		uploadJob := UploadJobT{
			upload:       &copiedUpload,
			stagingFiles: stagingFilesList,
			warehouse:    warehouse,
			whManager:    whManager,
			dbHandle:     wh.dbHandle,
			pgNotifier:   &wh.notifier,
		}

		pkgLogger.Debugf("[WH]: Adding job %+v", uploadJob)
		return &uploadJob, nil

	}

	return nil, fmt.Errorf("No upload job eligible")
}

func (wh *HandleT) getUploadJobForNewStagingFiles(warehouse warehouseutils.WarehouseT, whManager manager.ManagerI, stagingFilesList []*StagingFileT) (*UploadJobT, error) {
	// Expects one batch of staging files
	if len(stagingFilesList) > stagingFilesBatchSize {
		panic(fmt.Errorf("Expecting %d staging files. Received %d staging files instead", stagingFilesBatchSize, len(stagingFilesList)))
	}
	upload := wh.initUpload(warehouse, stagingFilesList)

	job := UploadJobT{
		upload:       &upload,
		stagingFiles: stagingFilesList,
		warehouse:    warehouse,
		whManager:    whManager,
		dbHandle:     wh.dbHandle,
		pgNotifier:   &wh.notifier,
	}

	return &job, nil
}

func (wh *HandleT) processJobs(warehouse warehouseutils.WarehouseT) (err error) {
	if isDestInProgress(warehouse) {
		pkgLogger.Debugf("[WH]: Skipping upload loop since %s upload in progress", warehouse.Identifier)
		return nil
	}

	enqueuedJobs := false
	setDestInProgress(warehouse, true)
	defer func() {
		if !enqueuedJobs {
			setDestInProgress(warehouse, false)
		}
	}()

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

	// Step 2: Handle any Pending uploads
	// An upload job is pending if it is neither exported nor aborted

	pendingUploads, err := wh.getPendingUploads(warehouse)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to get pending uploads: %s with error %w", warehouse.Identifier, err)
		return err
	}

	uploadJob, err := wh.getUploadJobsForPendingUploads(warehouse, whManager, pendingUploads)
	if err == nil {
		wh.uploadJobsQ <- uploadJob
		enqueuedJobs = true
		return nil
	}

	// Step 3: Handle pending staging files. Create new uploads for them
	// We will perform only one of Step 2 or Step 3, in every execution

	if !wh.canStartUpload(warehouse) {
		pkgLogger.Debugf("[WH]: Skipping upload loop since %s upload freq not exceeded", warehouse.Identifier)
		return nil
	}

	stagingFilesList, err := wh.getPendingStagingFiles(warehouse)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to get pending staging files: %s with error %w", warehouse.Identifier, err)
		return err
	}
	if len(stagingFilesList) == 0 {
		pkgLogger.Debugf("[WH]: Found no pending staging files for %s", warehouse.Identifier)
		return nil
	}

	uploadJob, err = wh.getUploadJobForNewStagingFiles(warehouse, whManager, stagingFilesList)
	if err != nil {
		pkgLogger.Errorf("[WH]: Failed to create upload jobs for %s for new staging files with error: %w", warehouse.Identifier, err)
		return err
	}

	wh.uploadJobsQ <- uploadJob
	enqueuedJobs = true
	setLastEventTime(warehouse, uploadJob.upload.LastEventAt)
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
			err := wh.processJobs(warehouse)
			if err != nil {
				pkgLogger.Errorf("[WH] Failed to process warehouse Jobs: %w", err)
			}
		}
		time.Sleep(mainLoopSleep)
	}
}

func (wh *HandleT) runUploadJobAllocator() {
	for {
		uploadJob := <-wh.uploadJobsQ
		workerName := workerIdentifier(uploadJob.warehouse)
		// Waits till a worker is available to process
		wh.waitAndLockAvailableWorker()
		wh.workerChannelMapLock.Lock()
		wh.workerChannelMap[workerName] <- uploadJob
		wh.workerChannelMapLock.Unlock()
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
	return
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
	wh.uploadJobsQ = make(chan *UploadJobT, 10000)
	wh.workerChannelMap = make(map[string]chan *UploadJobT)
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

var loadFileFormatMap = map[string]string{
	"BQ":         "json",
	"RS":         "csv",
	"SNOWFLAKE":  "csv",
	"POSTGRES":   "csv",
	"CLICKHOUSE": "csv",
}

func minimalConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	for {
		config := <-ch
		pkgLogger.Debug("Got config from config-backend", config)
		sources := config.Data.(backendconfig.ConfigT)
		for _, source := range sources.Sources {
			for _, destination := range source.Destinations {
				if misc.Contains(WarehouseDestinations, destination.DestinationDefinition.Name) {
					wh := &HandleT{
						dbHandle: dbHandle,
						destType: destination.DestinationDefinition.Name,
					}
					namespace := wh.getNamespace(destination.Config, source, destination, wh.destType)
					destinationsMap[destination.ID] = warehouseutils.WarehouseT{Destination: destination, Namespace: namespace, Type: wh.destType}
				}
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
		ShouldForceSetLowerVersion: config.GetBool("SQLMigrator.forceSetLowerVersion", false),
	}

	err := m.Migrate("warehouse")
	if err != nil {
		panic(fmt.Errorf("Could not run warehouse database migrations: %w", err))
	}
}

func CheckPGHealth() bool {
	rows, err := dbHandle.Query(fmt.Sprintf(`SELECT 'Rudder Warehouse DB Health Check'::text as message`))
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

	pkgLogger.Debugf("BRT: Creating record for uploaded json in %s table with schema: %+v", warehouseutils.WarehouseStagingFilesTable, stagingFile.Schema)
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
	recordStagedRowsStat(stagingFile.TotalEvents, stagingFile.BatchDestination.Destination.DestinationDefinition.Name, stagingFile.BatchDestination.Destination.ID, stagingFile.BatchDestination.Source.Name, stagingFile.BatchDestination.Destination.Name)
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
		pkgLogger.Infof("WH: Starting warehouse master service in %d", webPort)
	} else {
		pkgLogger.Infof("WH: Starting warehouse slave service in %d", webPort)
	}
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(webPort), bugsnag.Handler(nil)))
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
		pkgLogger.Infof("Skipping start of warehouse service...")
		return
	}

	pkgLogger.Infof("WH: Starting Warehouse service...")
	var err error
	psqlInfo := getConnectionString()

	dbHandle, err = sql.Open("postgres", psqlInfo)
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

	defer startWebHandler()

	runningMode := config.GetEnv("RSERVER_WAREHOUSE_RUNNING_MODE", "")
	if runningMode == DegradedMode {
		pkgLogger.Infof("WH: Running warehouse service in degared mode...")
		rruntime.Go(func() {
			minimalConfigSubscriber()
		})
		return
	}

	notifier, err = pgnotifier.New(psqlInfo)
	if err != nil {
		panic(err)
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
	}
}
