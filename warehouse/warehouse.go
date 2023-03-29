package warehouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/logfield"

	"golang.org/x/sync/errgroup"

	"github.com/bugsnag/bugsnag-go/v2"
	"github.com/cenkalti/backoff/v4"
	"github.com/thoas/go-funk"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/info"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/services/validators"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/archive"
	cpclient "github.com/rudderlabs/rudder-server/warehouse/client/controlplane"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/api"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service"
	"github.com/rudderlabs/rudder-server/warehouse/jobs"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

var (
	application                         app.App
	webPort                             int
	dbHandle                            *sql.DB
	notifier                            pgnotifier.PGNotifier
	tenantManager                       *multitenant.Manager
	controlPlaneClient                  *controlplane.Client
	noOfSlaveWorkerRoutines             int
	uploadFreqInS                       int64
	stagingFilesSchemaPaginationSize    int
	mainLoopSleep                       time.Duration
	stagingFilesBatchSize               int
	lastProcessedMarkerMap              map[string]int64
	lastProcessedMarkerExp              = expvar.NewMap("lastProcessedMarkerMap")
	lastProcessedMarkerMapLock          sync.RWMutex
	warehouseMode                       string
	warehouseSyncPreFetchCount          int
	warehouseSyncFreqIgnore             bool
	minRetryAttempts                    int
	retryTimeWindow                     time.Duration
	maxStagingFileReadBufferCapacityInK int
	connectionsMap                      map[string]map[string]model.Warehouse // destID -> sourceID -> warehouse map
	connectionsMapLock                  sync.RWMutex
	triggerUploadsMap                   map[string]bool // `whType:sourceID:destinationID` -> boolean value representing if an upload was triggered or not
	triggerUploadsMapLock               sync.RWMutex
	sourceIDsByWorkspace                map[string][]string // workspaceID -> []sourceIDs
	sourceIDsByWorkspaceLock            sync.RWMutex
	longRunningUploadStatThresholdInMin time.Duration
	pkgLogger                           logger.Logger
	numLoadFileUploadWorkers            int
	slaveUploadTimeout                  time.Duration
	tableCountQueryTimeout              time.Duration
	runningMode                         string
	uploadStatusTrackFrequency          time.Duration
	uploadAllocatorSleep                time.Duration
	waitForConfig                       time.Duration
	waitForWorkerSleep                  time.Duration
	ShouldForceSetLowerVersion          bool
	skipDeepEqualSchemas                bool
	maxParallelJobCreation              int
	enableJitterForSyncs                bool
	asyncWh                             *jobs.AsyncJobWh
	configBackendURL                    string
	enableTunnelling                    bool
)

var (
	host, user, password, dbname, sslMode, appName string
	port                                           int
)

var defaultUploadPriority = 100

// warehouses worker modes
const (
	EmbeddedMode       = "embedded"
	EmbeddedMasterMode = "embedded_master"
)

const (
	DegradedMode        = "degraded"
	triggerUploadQPName = "triggerUpload"
)

type (
	WorkerIdentifierT string
	JobID             int64
)

type HandleT struct {
	destType                          string
	warehouses                        []model.Warehouse
	dbHandle                          *sql.DB
	warehouseDBHandle                 *DB
	stagingRepo                       *repo.StagingFiles
	uploadRepo                        *repo.Uploads
	whSchemaRepo                      *repo.WHSchema
	notifier                          pgnotifier.PGNotifier
	isEnabled                         bool
	configSubscriberLock              sync.RWMutex
	workerChannelMap                  map[string]chan *UploadJob
	workerChannelMapLock              sync.RWMutex
	initialConfigFetched              bool
	inProgressMap                     map[WorkerIdentifierT][]JobID
	inProgressMapLock                 sync.RWMutex
	noOfWorkers                       int
	activeWorkerCount                 int32
	maxConcurrentUploadJobs           int
	allowMultipleSourcesForJobsPickup bool
	workspaceBySourceIDs              map[string]string
	tenantManager                     multitenant.Manager
	stats                             stats.Stats
	uploadJobFactory                  UploadJobFactory
	Now                               func() time.Time
	NowSQL                            string
	Logger                            logger.Logger
	cpInternalClient                  cpclient.InternalControlPlane

	backgroundCancel context.CancelFunc
	backgroundGroup  errgroup.Group
	backgroundWait   func() error
}

func Init4() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("warehouse")
}

func loadConfig() {
	// Port where WH is running
	config.RegisterIntConfigVariable(8082, &webPort, false, 1, "Warehouse.webPort")
	config.RegisterIntConfigVariable(4, &noOfSlaveWorkerRoutines, true, 1, "Warehouse.noOfSlaveWorkerRoutines")
	config.RegisterIntConfigVariable(960, &stagingFilesBatchSize, true, 1, "Warehouse.stagingFilesBatchSize")
	config.RegisterInt64ConfigVariable(1800, &uploadFreqInS, true, 1, "Warehouse.uploadFreqInS")
	config.RegisterDurationConfigVariable(5, &mainLoopSleep, true, time.Second, []string{"Warehouse.mainLoopSleep", "Warehouse.mainLoopSleepInS"}...)
	lastProcessedMarkerMap = map[string]int64{}
	config.RegisterStringConfigVariable("embedded", &warehouseMode, false, "Warehouse.mode")
	host = config.GetString("WAREHOUSE_JOBS_DB_HOST", "localhost")
	user = config.GetString("WAREHOUSE_JOBS_DB_USER", "ubuntu")
	dbname = config.GetString("WAREHOUSE_JOBS_DB_DB_NAME", "ubuntu")
	port = config.GetInt("WAREHOUSE_JOBS_DB_PORT", 5432)
	password = config.GetString("WAREHOUSE_JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from
	sslMode = config.GetString("WAREHOUSE_JOBS_DB_SSL_MODE", "disable")
	configBackendURL = config.GetString("CONFIG_BACKEND_URL", "api.rudderlabs.com")
	enableTunnelling = config.GetBool("ENABLE_TUNNELLING", true)
	config.RegisterIntConfigVariable(10, &warehouseSyncPreFetchCount, true, 1, "Warehouse.warehouseSyncPreFetchCount")
	config.RegisterIntConfigVariable(100, &stagingFilesSchemaPaginationSize, true, 1, "Warehouse.stagingFilesSchemaPaginationSize")
	config.RegisterBoolConfigVariable(false, &warehouseSyncFreqIgnore, true, "Warehouse.warehouseSyncFreqIgnore")
	config.RegisterIntConfigVariable(3, &minRetryAttempts, true, 1, "Warehouse.minRetryAttempts")
	config.RegisterDurationConfigVariable(180, &retryTimeWindow, true, time.Minute, []string{"Warehouse.retryTimeWindow", "Warehouse.retryTimeWindowInMins"}...)
	connectionsMap = map[string]map[string]model.Warehouse{}
	triggerUploadsMap = map[string]bool{}
	sourceIDsByWorkspace = map[string][]string{}
	config.RegisterIntConfigVariable(10240, &maxStagingFileReadBufferCapacityInK, true, 1, "Warehouse.maxStagingFileReadBufferCapacityInK")
	config.RegisterDurationConfigVariable(120, &longRunningUploadStatThresholdInMin, true, time.Minute, []string{"Warehouse.longRunningUploadStatThreshold", "Warehouse.longRunningUploadStatThresholdInMin"}...)
	config.RegisterDurationConfigVariable(10, &slaveUploadTimeout, true, time.Minute, []string{"Warehouse.slaveUploadTimeout", "Warehouse.slaveUploadTimeoutInMin"}...)
	config.RegisterIntConfigVariable(8, &numLoadFileUploadWorkers, true, 1, "Warehouse.numLoadFileUploadWorkers")
	runningMode = config.GetString("Warehouse.runningMode", "")
	config.RegisterDurationConfigVariable(30, &uploadStatusTrackFrequency, false, time.Minute, []string{"Warehouse.uploadStatusTrackFrequency", "Warehouse.uploadStatusTrackFrequencyInMin"}...)
	config.RegisterDurationConfigVariable(5, &uploadAllocatorSleep, false, time.Second, []string{"Warehouse.uploadAllocatorSleep", "Warehouse.uploadAllocatorSleepInS"}...)
	config.RegisterDurationConfigVariable(5, &waitForConfig, false, time.Second, []string{"Warehouse.waitForConfig", "Warehouse.waitForConfigInS"}...)
	config.RegisterDurationConfigVariable(5, &waitForWorkerSleep, false, time.Second, []string{"Warehouse.waitForWorkerSleep", "Warehouse.waitForWorkerSleepInS"}...)
	config.RegisterBoolConfigVariable(true, &ShouldForceSetLowerVersion, false, "SQLMigrator.forceSetLowerVersion")
	config.RegisterBoolConfigVariable(false, &skipDeepEqualSchemas, true, "Warehouse.skipDeepEqualSchemas")
	config.RegisterIntConfigVariable(8, &maxParallelJobCreation, true, 1, "Warehouse.maxParallelJobCreation")
	config.RegisterBoolConfigVariable(false, &enableJitterForSyncs, true, "Warehouse.enableJitterForSyncs")
	config.RegisterDurationConfigVariable(30, &tableCountQueryTimeout, true, time.Second, []string{"Warehouse.tableCountQueryTimeout", "Warehouse.tableCountQueryTimeoutInS"}...)

	appName = misc.DefaultString("rudder-server").OnError(os.Hostname())
}

// get name of the worker (`destID_namespace`) to be stored in map wh.workerChannelMap
func (wh *HandleT) workerIdentifier(warehouse model.Warehouse) (identifier string) {
	identifier = fmt.Sprintf(`%s_%s`, warehouse.Destination.ID, warehouse.Namespace)

	if wh.allowMultipleSourcesForJobsPickup {
		identifier = fmt.Sprintf(`%s_%s_%s`, warehouse.Source.ID, warehouse.Destination.ID, warehouse.Namespace)
	}
	return
}

func getDestinationFromConnectionMap(DestinationId, SourceId string) (model.Warehouse, error) {
	if DestinationId == "" || SourceId == "" {
		return model.Warehouse{}, errors.New("invalid Parameters")
	}
	sourceMap, ok := connectionsMap[DestinationId]
	if !ok {
		return model.Warehouse{}, errors.New("invalid Destination Id")
	}

	conn, ok := sourceMap[SourceId]
	if !ok {
		return model.Warehouse{}, errors.New("invalid Source Id")
	}

	return conn, nil
}

func (wh *HandleT) getActiveWorkerCount() int {
	return int(atomic.LoadInt32(&wh.activeWorkerCount))
}

func (wh *HandleT) decrementActiveWorkers() {
	// decrement number of workers actively engaged
	atomic.AddInt32(&wh.activeWorkerCount, -1)
}

func (wh *HandleT) incrementActiveWorkers() {
	// increment number of workers actively engaged
	atomic.AddInt32(&wh.activeWorkerCount, 1)
}

func (wh *HandleT) initWorker() chan *UploadJob {
	workerChan := make(chan *UploadJob, 1000)
	for i := 0; i < wh.maxConcurrentUploadJobs; i++ {
		wh.backgroundGroup.Go(func() error {
			for uploadJob := range workerChan {
				wh.incrementActiveWorkers()
				err := wh.handleUploadJob(uploadJob)
				if err != nil {
					wh.Logger.Errorf("[WH] Failed in handle Upload jobs for worker: %+w", err)
				}
				wh.removeDestInProgress(uploadJob.warehouse, uploadJob.upload.ID)
				wh.decrementActiveWorkers()
			}
			return nil
		})
	}
	return workerChan
}

func (*HandleT) handleUploadJob(uploadJob *UploadJob) error {
	// Process the upload job
	return uploadJob.run()
}

// Backend Config subscriber subscribes to backend-config and gets all the configurations that includes all sources, destinations and their latest values.
func (wh *HandleT) backendConfigSubscriber(ctx context.Context) {
	for configData := range wh.tenantManager.WatchConfig(ctx) {
		var warehouses []model.Warehouse
		sourceIDsByWorkspaceTemp := map[string][]string{}

		workspaceBySourceIDs := map[string]string{}

		wh.Logger.Info(`Received updated workspace config`)
		for workspaceID, wConfig := range configData {
			for _, source := range wConfig.Sources {
				if _, ok := sourceIDsByWorkspaceTemp[workspaceID]; !ok {
					sourceIDsByWorkspaceTemp[workspaceID] = []string{}
				}

				sourceIDsByWorkspaceTemp[workspaceID] = append(sourceIDsByWorkspaceTemp[workspaceID], source.ID)
				workspaceBySourceIDs[source.ID] = workspaceID

				if len(source.Destinations) == 0 {
					continue
				}

				for _, destination := range source.Destinations {

					if destination.DestinationDefinition.Name != wh.destType {
						continue
					}

					if enableTunnelling {
						destination = wh.attachSSHTunnellingInfo(ctx, destination)
					}

					namespace := wh.getNamespace(source, destination)
					warehouse := model.Warehouse{
						WorkspaceID: workspaceID,
						Source:      source,
						Destination: destination,
						Namespace:   namespace,
						Type:        wh.destType,
						Identifier:  warehouseutils.GetWarehouseIdentifier(wh.destType, source.ID, destination.ID),
					}
					warehouses = append(warehouses, warehouse)

					workerName := wh.workerIdentifier(warehouse)
					wh.workerChannelMapLock.Lock()
					// spawn one worker for each unique destID_namespace
					// check this commit to https://github.com/rudderlabs/rudder-server/pull/476/commits/fbfddf167aa9fc63485fe006d34e6881f5019667
					// to avoid creating goroutine for disabled sources/destinations
					if _, ok := wh.workerChannelMap[workerName]; !ok {
						workerChan := wh.initWorker()
						wh.workerChannelMap[workerName] = workerChan
					}
					wh.workerChannelMapLock.Unlock()

					connectionsMapLock.Lock()
					if connectionsMap[destination.ID] == nil {
						connectionsMap[destination.ID] = map[string]model.Warehouse{}
					}
					if warehouse.Destination.Config["sslMode"] == "verify-ca" {
						if err := warehouseutils.WriteSSLKeys(warehouse.Destination); err.IsError() {
							wh.Logger.Error(err.Error())
							persistSSLFileErrorStat(workspaceID, wh.destType, destination.Name, destination.ID, source.Name, source.ID, err.GetErrTag())
						}
					}
					connectionsMap[destination.ID][source.ID] = warehouse
					connectionsMapLock.Unlock()

					if warehouseutils.IDResolutionEnabled() && misc.Contains(warehouseutils.IdentityEnabledWarehouses, warehouse.Type) {
						wh.setupIdentityTables(warehouse)
						if shouldPopulateHistoricIdentities && warehouse.Destination.Enabled {
							// non-blocking populate historic identities
							wh.populateHistoricIdentities(warehouse)
						}
					}
				}
			}
		}
		wh.configSubscriberLock.Lock()
		wh.warehouses = warehouses
		wh.workspaceBySourceIDs = workspaceBySourceIDs
		wh.Logger.Infof("Releasing config subscriber lock: %s", wh.destType)
		wh.configSubscriberLock.Unlock()

		sourceIDsByWorkspaceLock.Lock()
		sourceIDsByWorkspace = sourceIDsByWorkspaceTemp
		sourceIDsByWorkspaceLock.Unlock()
		wh.initialConfigFetched = true
	}
}

func (wh *HandleT) attachSSHTunnellingInfo(
	ctx context.Context,
	upstream backendconfig.DestinationT,
) backendconfig.DestinationT {
	// at destination level, do we have tunnelling enabled.
	if tunnelEnabled := warehouseutils.ReadAsBool("useSSH", upstream.Config); !tunnelEnabled {
		return upstream
	}

	pkgLogger.Debugf("Fetching ssh keys for destination: %s", upstream.ID)
	keys, err := wh.cpInternalClient.GetDestinationSSHKeys(ctx, upstream.ID)
	if err != nil {
		pkgLogger.Errorf("fetching ssh keys for destination: %s", err.Error())
		return upstream
	}

	replica := backendconfig.DestinationT{}
	if err := deepCopy(upstream, &replica); err != nil {
		pkgLogger.Errorf("deep copying the destination: %s failed: %s", upstream.ID, err)
		return upstream
	}

	replica.Config["sshPrivateKey"] = keys.PrivateKey
	return replica
}

func deepCopy(src, dest interface{}) error {
	byt, err := json.Marshal(src)
	if err != nil {
		return err
	}

	return json.Unmarshal(byt, dest)
}

// getNamespace sets namespace name in the following order
//  1. user set name from destinationConfig
//  2. from existing record in wh_schemas with same source + dest combo
//  3. convert source name
func (wh *HandleT) getNamespace(source backendconfig.SourceT, destination backendconfig.DestinationT) string {
	configMap := destination.Config
	if wh.destType == warehouseutils.CLICKHOUSE {
		if _, ok := configMap["database"].(string); ok {
			return configMap["database"].(string)
		}
		return "rudder"
	}
	if configMap["namespace"] != nil {
		namespace, _ := configMap["namespace"].(string)
		if len(strings.TrimSpace(namespace)) > 0 {
			return warehouseutils.ToProviderCase(wh.destType, warehouseutils.ToSafeNamespace(wh.destType, namespace))
		}
	}
	// TODO: Move config to global level based on use case
	namespacePrefix := config.GetString(fmt.Sprintf("Warehouse.%s.customDatasetPrefix", warehouseutils.WHDestNameMap[wh.destType]), "")
	if namespacePrefix != "" {
		return warehouseutils.ToProviderCase(wh.destType, warehouseutils.ToSafeNamespace(wh.destType, fmt.Sprintf(`%s_%s`, namespacePrefix, source.Name)))
	}

	namespace, err := wh.whSchemaRepo.GetNamespace(context.TODO(), source.ID, destination.ID)
	if err != nil {
		pkgLogger.Errorw("getting namespace",
			logfield.SourceID, source.ID,
			logfield.DestinationID, destination.ID,
			logfield.DestinationType, destination.DestinationDefinition.Name,
			logfield.WorkspaceID, destination.WorkspaceID,
		)
		return ""
	}
	if namespace == "" {
		return warehouseutils.ToProviderCase(wh.destType, warehouseutils.ToSafeNamespace(wh.destType, source.Name))
	}
	return namespace
}

func (wh *HandleT) setDestInProgress(warehouse model.Warehouse, jobID int64) {
	identifier := wh.workerIdentifier(warehouse)
	wh.inProgressMapLock.Lock()
	defer wh.inProgressMapLock.Unlock()
	wh.inProgressMap[WorkerIdentifierT(identifier)] = append(wh.inProgressMap[WorkerIdentifierT(identifier)], JobID(jobID))
}

func (wh *HandleT) removeDestInProgress(warehouse model.Warehouse, jobID int64) {
	wh.inProgressMapLock.Lock()
	defer wh.inProgressMapLock.Unlock()
	identifier := wh.workerIdentifier(warehouse)
	if idx, inProgress := wh.checkInProgressMap(jobID, identifier); inProgress {
		wh.inProgressMap[WorkerIdentifierT(identifier)] = removeFromJobsIDT(wh.inProgressMap[WorkerIdentifierT(identifier)], idx)
	}
}

func (wh *HandleT) isUploadJobInProgress(warehouse model.Warehouse, jobID int64) (int, bool) {
	identifier := wh.workerIdentifier(warehouse)
	wh.inProgressMapLock.RLock()
	defer wh.inProgressMapLock.RUnlock()
	return wh.checkInProgressMap(jobID, identifier)
}

func (wh *HandleT) getInProgressNamespaces() []string {
	wh.inProgressMapLock.RLock()
	defer wh.inProgressMapLock.RUnlock()
	var identifiers []string
	for k, v := range wh.inProgressMap {
		if len(v) >= wh.maxConcurrentUploadJobs {
			identifiers = append(identifiers, string(k))
		}
	}
	return identifiers
}

func (wh *HandleT) checkInProgressMap(jobID int64, identifier string) (int, bool) {
	for idx, id := range wh.inProgressMap[WorkerIdentifierT(identifier)] {
		if jobID == int64(id) {
			return idx, true
		}
	}
	return 0, false
}

func removeFromJobsIDT(slice []JobID, idx int) []JobID {
	return append(slice[:idx], slice[idx+1:]...)
}

func getUploadFreqInS(syncFrequency string) int64 {
	freqInMin, err := strconv.ParseInt(syncFrequency, 10, 64)
	if err != nil {
		return uploadFreqInS
	}
	return freqInMin * 60
}

func uploadFrequencyExceeded(warehouse model.Warehouse, syncFrequency string) bool {
	freqInS := getUploadFreqInS(syncFrequency)
	lastProcessedMarkerMapLock.RLock()
	defer lastProcessedMarkerMapLock.RUnlock()
	if lastExecTime, ok := lastProcessedMarkerMap[warehouse.Identifier]; ok && timeutil.Now().Unix()-lastExecTime < freqInS {
		return true
	}
	return false
}

func setLastProcessedMarker(warehouse model.Warehouse, lastProcessedTime time.Time) {
	lastProcessedMarkerMapLock.Lock()
	defer lastProcessedMarkerMapLock.Unlock()
	lastProcessedMarkerMap[warehouse.Identifier] = lastProcessedTime.Unix()
	lastProcessedMarkerExp.Set(warehouse.Identifier, lastProcessedTime)
}

func (wh *HandleT) createUploadJobsFromStagingFiles(ctx context.Context, warehouse model.Warehouse, stagingFiles []*model.StagingFile, priority int, uploadStartAfter time.Time) error {
	// count := 0
	// Process staging files in batches of stagingFilesBatchSize
	// E.g. If there are 1000 pending staging files and stagingFilesBatchSize is 100,
	// Then we create 10 new entries in wh_uploads table each with 100 staging files
	uploadTriggered := isUploadTriggered(warehouse)
	if uploadTriggered {
		priority = 50
	}

	batches := service.StageFileBatching(stagingFiles, stagingFilesBatchSize)
	for _, batch := range batches {
		upload := model.Upload{
			SourceID:        warehouse.Source.ID,
			Namespace:       warehouse.Namespace,
			WorkspaceID:     warehouse.WorkspaceID,
			DestinationID:   warehouse.Destination.ID,
			DestinationType: wh.destType,
			Status:          model.Waiting,

			LoadFileType:  warehouseutils.GetLoadFileType(wh.destType),
			NextRetryTime: uploadStartAfter,
			Priority:      priority,

			// The following will be populated by staging files:
			// FirstEventAt:     0,
			// LastEventAt:      0,
			// UseRudderStorage: false,
			// SourceTaskRunID:  "",
			// SourceJobID:      "",
			// SourceJobRunID:   "",
		}

		_, err := wh.uploadRepo.CreateWithStagingFiles(ctx, upload, batch)
		if err != nil {
			return fmt.Errorf("creating upload: %w", err)
		}
	}

	// reset upload trigger if the upload was triggered
	if uploadTriggered {
		clearTriggeredUpload(warehouse)
	}

	return nil
}

func getUploadStartAfterTime() time.Time {
	if enableJitterForSyncs {
		return timeutil.Now().Add(time.Duration(rand.Intn(15)) * time.Second)
	}
	return time.Now()
}

func (wh *HandleT) getLatestUploadStatus(warehouse *model.Warehouse) (int64, string, int) {
	uploadID, status, priority, err := wh.warehouseDBHandle.GetLatestUploadStatus(
		context.TODO(),
		warehouse.Type,
		warehouse.Source.ID,
		warehouse.Destination.ID)
	if err != nil {
		wh.Logger.Errorf(`Error getting latest upload status for warehouse: %v`, err)
	}

	return uploadID, status, priority
}

func (wh *HandleT) createJobs(ctx context.Context, warehouse model.Warehouse) (err error) {
	if ok, err := wh.canCreateUpload(warehouse); !ok {
		wh.stats.NewTaggedStat("wh_scheduler.upload_sync_skipped", stats.CountType, stats.Tags{
			"workspaceId":   warehouse.WorkspaceID,
			"destinationID": warehouse.Destination.ID,
			"destType":      warehouse.Destination.DestinationDefinition.Name,
			"reason":        err.Error(),
		}).Count(1)
		wh.Logger.Debugf("[WH]: Skipping upload loop since %s upload freq not exceeded: %v", warehouse.Identifier, err)
		return nil
	}

	priority := defaultUploadPriority
	uploadID, uploadStatus, uploadPriority := wh.getLatestUploadStatus(&warehouse)
	if uploadStatus == model.Waiting {
		// If it is present do nothing else delete it
		if _, inProgress := wh.isUploadJobInProgress(warehouse, uploadID); !inProgress {
			err := wh.uploadRepo.DeleteWaiting(ctx, uploadID)
			if err != nil {
				wh.Logger.Error(err, "uploadID", uploadID, "warehouse", warehouse.Identifier)
			}
			priority = uploadPriority // copy the priority from the latest upload job.
		}
	}

	stagingFilesFetchStat := wh.stats.NewTaggedStat("wh_scheduler.pending_staging_files", stats.TimerType, stats.Tags{
		"workspaceId":   warehouse.WorkspaceID,
		"destinationID": warehouse.Destination.ID,
		"destType":      warehouse.Destination.DestinationDefinition.Name,
	})
	stagingFilesFetchStart := time.Now()
	stagingFilesList, err := wh.stagingRepo.Pending(ctx, warehouse.Source.ID, warehouse.Destination.ID)
	if err != nil {
		return fmt.Errorf("pending staging files for %q: %w", warehouse.Identifier, err)
	}
	stagingFilesFetchStat.Since(stagingFilesFetchStart)

	if len(stagingFilesList) == 0 {
		wh.Logger.Debugf("[WH]: Found no pending staging files for %s", warehouse.Identifier)
		return nil
	}

	uploadJobCreationStat := wh.stats.NewTaggedStat("wh_scheduler.create_upload_jobs", stats.TimerType, stats.Tags{
		"workspaceId":   warehouse.WorkspaceID,
		"destinationID": warehouse.Destination.ID,
		"destType":      warehouse.Destination.DestinationDefinition.Name,
	})
	defer uploadJobCreationStat.RecordDuration()()

	uploadStartAfter := getUploadStartAfterTime()
	err = wh.createUploadJobsFromStagingFiles(ctx, warehouse, stagingFilesList, priority, uploadStartAfter)
	if err != nil {
		return err
	}
	setLastProcessedMarker(warehouse, uploadStartAfter)

	return nil
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

		wh.stats.NewTaggedStat("wh_scheduler.warehouse_length", stats.GaugeType, stats.Tags{
			warehouseutils.DestinationType: wh.destType,
		}).Gauge(len(wh.warehouses)) // Correlation between number of warehouses and scheduling time.
		whTotalSchedulingStats := wh.stats.NewTaggedStat("wh_scheduler.total_scheduling_time", stats.TimerType, stats.Tags{
			warehouseutils.DestinationType: wh.destType,
		})
		whTotalSchedulingStart := time.Now()

		for _, warehouse := range wh.warehouses {
			w := warehouse
			rruntime.GoForWarehouse(func() {
				jobCreationChan <- struct{}{}
				defer func() {
					wg.Done()
					<-jobCreationChan
				}()

				wh.Logger.Debugf("[WH] Processing Jobs for warehouse: %s", w.Identifier)
				err := wh.createJobs(ctx, w)
				if err != nil {
					wh.Logger.Errorf("[WH] Failed to process warehouse Jobs: %v", err)
				}
			})
		}
		wh.configSubscriberLock.RUnlock()
		wg.Wait()

		whTotalSchedulingStats.Since(whTotalSchedulingStart)
		select {
		case <-ctx.Done():
			return
		case <-time.After(mainLoopSleep):
		}
	}
}

func (wh *HandleT) processingStats(availableWorkers int, jobStats model.UploadJobsStats) {
	// Get pending jobs
	pendingJobsStat := wh.stats.NewTaggedStat("wh_processing_pending_jobs", stats.GaugeType, stats.Tags{
		"destType": wh.destType,
	})
	pendingJobsStat.Gauge(int(jobStats.PendingJobs))

	availableWorkersStat := wh.stats.NewTaggedStat("wh_processing_available_workers", stats.GaugeType, stats.Tags{
		"destType": wh.destType,
	})
	availableWorkersStat.Gauge(availableWorkers)

	pickupLagStat := wh.stats.NewTaggedStat("wh_processing_pickup_lag", stats.TimerType, stats.Tags{
		"destType": wh.destType,
	})
	pickupLagStat.SendTiming(jobStats.PickupLag)

	pickupWaitTimeStat := wh.stats.NewTaggedStat("wh_processing_pickup_wait_time", stats.TimerType, stats.Tags{
		"destType": wh.destType,
	})
	pickupWaitTimeStat.SendTiming(jobStats.PickupWaitTime)
}

func (wh *HandleT) getUploadsToProcess(ctx context.Context, availableWorkers int, skipIdentifiers []string) ([]*UploadJob, error) {
	uploads, err := wh.uploadRepo.GetToProcess(ctx, wh.destType, availableWorkers, repo.ProcessOptions{
		SkipIdentifiers:                   skipIdentifiers,
		SkipWorkspaces:                    tenantManager.DegradedWorkspaces(),
		AllowMultipleSourcesForJobsPickup: wh.allowMultipleSourcesForJobsPickup,
	})
	if err != nil {
		return nil, err
	}

	var uploadJobs []*UploadJob
	for _, upload := range uploads {
		wh.configSubscriberLock.RLock()
		if upload.WorkspaceID == "" {
			var ok bool
			upload.WorkspaceID, ok = wh.workspaceBySourceIDs[upload.SourceID]
			if !ok {
				wh.Logger.Warnf("could not find workspace id for source id: %s", upload.SourceID)
			}
		}

		warehouse, ok := funk.Find(wh.warehouses, func(w model.Warehouse) bool {
			return w.Source.ID == upload.SourceID && w.Destination.ID == upload.DestinationID
		}).(model.Warehouse)
		wh.configSubscriberLock.RUnlock()

		upload.UseRudderStorage = warehouse.GetBoolDestinationConfig("useRudderStorage")

		if !ok {
			uploadJob := wh.uploadJobFactory.NewUploadJob(&model.UploadJob{
				Upload: upload,
			}, nil)
			err := fmt.Errorf("unable to find source : %s or destination : %s, both or the connection between them", upload.SourceID, upload.DestinationID)
			_, _ = uploadJob.setUploadError(err, model.Aborted)
			wh.Logger.Errorf("%v", err)
			continue
		}

		stagingFilesList, err := wh.stagingRepo.GetForUpload(ctx, upload)
		if err != nil {
			return nil, err
		}

		whManager, err := manager.New(wh.destType)
		if err != nil {
			return nil, err
		}
		uploadJob := wh.uploadJobFactory.NewUploadJob(&model.UploadJob{
			Warehouse:    warehouse,
			Upload:       upload,
			StagingFiles: stagingFilesList,
		}, whManager)

		uploadJobs = append(uploadJobs, uploadJob)
	}

	jobsStats, err := wh.uploadRepo.UploadJobsStats(ctx, wh.destType, repo.ProcessOptions{
		SkipIdentifiers: skipIdentifiers,
		SkipWorkspaces:  tenantManager.DegradedWorkspaces(),
	})
	if err != nil {
		return nil, fmt.Errorf("processing stats: %w", err)
	}

	wh.processingStats(availableWorkers, jobsStats)

	return uploadJobs, nil
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

		inProgressNamespaces := wh.getInProgressNamespaces()
		wh.Logger.Debugf(`Current inProgress namespace identifiers for %s: %v`, wh.destType, inProgressNamespaces)

		uploadJobsToProcess, err := wh.getUploadsToProcess(ctx, availableWorkers, inProgressNamespaces)

		switch err {
		case nil:
		case context.Canceled, context.DeadlineExceeded, ErrCancellingStatement:
			break loop
		default:
			wh.Logger.Errorf(`Error executing getUploadsToProcess: %v`, err)
			panic(err)
		}

		for _, uploadJob := range uploadJobsToProcess {
			wh.setDestInProgress(uploadJob.warehouse, uploadJob.upload.ID)
		}

		for _, uploadJob := range uploadJobsToProcess {
			workerName := wh.workerIdentifier(uploadJob.warehouse)
			wh.workerChannelMapLock.RLock()
			wh.workerChannelMap[workerName] <- uploadJob
			wh.workerChannelMapLock.RUnlock()
		}

		select {
		case <-ctx.Done():
			break loop
		case <-time.After(uploadAllocatorSleep):
		}
	}

	wh.workerChannelMapLock.RLock()
	for _, workerChannel := range wh.workerChannelMap {
		close(workerChannel)
	}
	wh.workerChannelMapLock.RUnlock()
}

func getBucketFolder(batchID, tableName string) string {
	return fmt.Sprintf(`%v-%v`, batchID, tableName)
}

// Enable enables a router :)
func (wh *HandleT) Enable() {
	wh.isEnabled = true
}

// Disable disables a router:)
func (wh *HandleT) Disable() {
	wh.isEnabled = false
}

func (wh *HandleT) Setup(whType string) error {
	pkgLogger.Infof("WH: Warehouse Router started: %s", whType)
	wh.Logger = pkgLogger
	wh.dbHandle = dbHandle
	// We now have access to the warehouseDBHandle through
	// which we will be running the db calls.
	wh.warehouseDBHandle = NewWarehouseDB(dbHandle)
	wh.stagingRepo = repo.NewStagingFiles(dbHandle)
	wh.uploadRepo = repo.NewUploads(dbHandle)
	wh.whSchemaRepo = repo.NewWHSchemas(dbHandle)

	wh.notifier = notifier
	wh.destType = whType
	wh.resetInProgressJobs()
	wh.Enable()
	wh.workerChannelMap = make(map[string]chan *UploadJob)
	wh.inProgressMap = make(map[WorkerIdentifierT][]JobID)
	wh.tenantManager = multitenant.Manager{
		BackendConfig: backendconfig.DefaultBackendConfig,
	}
	wh.stats = stats.Default

	wh.uploadJobFactory = UploadJobFactory{
		stats:                stats.Default,
		dbHandle:             wh.dbHandle,
		pgNotifier:           &wh.notifier,
		destinationValidator: validations.NewDestinationValidator(),
		loadFile: &loadfiles.LoadFileGenerator{
			Logger:             pkgLogger.Child("loadfile"),
			Notifier:           &notifier,
			StageRepo:          repo.NewStagingFiles(dbHandle),
			LoadRepo:           repo.NewLoadFiles(dbHandle),
			ControlPlaneClient: controlPlaneClient,
		},
		recovery: service.NewRecovery(whType, repo.NewUploads(dbHandle)),
	}
	loadfiles.WithConfig(wh.uploadJobFactory.loadFile, config.Default)

	whName := warehouseutils.WHDestNameMap[whType]
	config.RegisterIntConfigVariable(8, &wh.noOfWorkers, true, 1, fmt.Sprintf(`Warehouse.%v.noOfWorkers`, whName), "Warehouse.noOfWorkers")
	config.RegisterIntConfigVariable(1, &wh.maxConcurrentUploadJobs, false, 1, fmt.Sprintf(`Warehouse.%v.maxConcurrentUploadJobs`, whName))
	config.RegisterBoolConfigVariable(false, &wh.allowMultipleSourcesForJobsPickup, false, fmt.Sprintf(`Warehouse.%v.allowMultipleSourcesForJobsPickup`, whName))

	wh.cpInternalClient = cpclient.NewInternalClientWithCache(
		configBackendURL,
		cpclient.BasicAuth{
			Username: config.GetString("CP_INTERNAL_API_USERNAME", ""),
			Password: config.GetString("CP_INTERNAL_API_PASSWORD", ""),
		},
	)

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	wh.backgroundCancel = cancel
	wh.backgroundWait = g.Wait

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		wh.tenantManager.Run(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		wh.backendConfigSubscriber(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		wh.runUploadJobAllocator(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnagForWarehouse(func() error {
		wh.mainLoop(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		return wh.CronTracker(ctx)
	}))

	return nil
}

func (wh *HandleT) Shutdown() error {
	wh.backgroundCancel()
	return wh.backgroundWait()
}

func (wh *HandleT) resetInProgressJobs() {
	sqlStatement := fmt.Sprintf(`
		UPDATE
		  %s
		SET
		  in_progress = %t
		WHERE
		  destination_type = '%s'
		  AND in_progress = %t;
`,
		warehouseutils.WarehouseUploadsTable,
		false,
		wh.destType,
		true,
	)
	_, err := wh.dbHandle.Query(sqlStatement)
	if err != nil {
		panic(fmt.Errorf("query: %s failed with Error : %w", sqlStatement, err))
	}
}

func minimalConfigSubscriber() {
	ch := backendconfig.DefaultBackendConfig.Subscribe(context.TODO(), backendconfig.TopicBackendConfig)
	for data := range ch {
		pkgLogger.Debug("Got config from config-backend", data)
		configData := data.Data.(map[string]backendconfig.ConfigT)

		sourceIDsByWorkspaceTemp := map[string][]string{}

		var connectionFlags backendconfig.ConnectionFlags
		for workspaceID, wConfig := range configData {
			connectionFlags = wConfig.ConnectionFlags // the last connection flags should be enough, since they are all the same in multi-workspace environments
			for _, source := range wConfig.Sources {
				if _, ok := sourceIDsByWorkspaceTemp[workspaceID]; !ok {
					sourceIDsByWorkspaceTemp[workspaceID] = []string{}
				}
				sourceIDsByWorkspaceTemp[workspaceID] = append(sourceIDsByWorkspaceTemp[workspaceID], source.ID)
				for _, destination := range source.Destinations {
					if misc.Contains(warehouseutils.WarehouseDestinations, destination.DestinationDefinition.Name) {
						wh := &HandleT{
							dbHandle: dbHandle,
							destType: destination.DestinationDefinition.Name,
						}
						namespace := wh.getNamespace(source, destination)
						connectionsMapLock.Lock()
						if connectionsMap[destination.ID] == nil {
							connectionsMap[destination.ID] = map[string]model.Warehouse{}
						}
						connectionsMap[destination.ID][source.ID] = model.Warehouse{
							WorkspaceID: workspaceID,
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
		}
		sourceIDsByWorkspaceLock.Lock()
		sourceIDsByWorkspace = sourceIDsByWorkspaceTemp
		sourceIDsByWorkspaceLock.Unlock()

		if val, ok := connectionFlags.Services["warehouse"]; ok {
			if UploadAPI.connectionManager != nil {
				UploadAPI.connectionManager.Apply(connectionFlags.URL, val)
			}
		}
	}
}

// Gets the config from config backend and extracts enabled write keys
func monitorDestRouters(ctx context.Context) error {
	dstToWhRouter := make(map[string]*HandleT)

	ch := tenantManager.WatchConfig(ctx)
	for configData := range ch {
		err := onConfigDataEvent(configData, dstToWhRouter)
		if err != nil {
			return err
		}
	}

	g, _ := errgroup.WithContext(context.Background())
	for _, wh := range dstToWhRouter {
		wh := wh
		g.Go(wh.Shutdown)
	}
	return g.Wait()
}

func onConfigDataEvent(config map[string]backendconfig.ConfigT, dstToWhRouter map[string]*HandleT) error {
	pkgLogger.Debug("Got config from config-backend", config)

	enabledDestinations := make(map[string]bool)
	var connectionFlags backendconfig.ConnectionFlags
	for _, wConfig := range config {
		connectionFlags = wConfig.ConnectionFlags // the last connection flags should be enough, since they are all the same in multi-workspace environments
		for _, source := range wConfig.Sources {
			for _, destination := range source.Destinations {
				enabledDestinations[destination.DestinationDefinition.Name] = true
				if misc.Contains(warehouseutils.WarehouseDestinations, destination.DestinationDefinition.Name) {
					wh, ok := dstToWhRouter[destination.DestinationDefinition.Name]
					if !ok {
						pkgLogger.Info("Starting a new Warehouse Destination Router: ", destination.DestinationDefinition.Name)
						wh = &HandleT{}
						wh.configSubscriberLock.Lock()
						if err := wh.Setup(destination.DestinationDefinition.Name); err != nil {
							return fmt.Errorf("setup warehouse %q: %w", destination.DestinationDefinition.Name, err)
						}
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
	}
	if val, ok := connectionFlags.Services["warehouse"]; ok {
		if UploadAPI.connectionManager != nil {
			UploadAPI.connectionManager.Apply(connectionFlags.URL, val)
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

	return nil
}

func setupTables(dbHandle *sql.DB) error {
	m := &migrator.Migrator{
		Handle:                     dbHandle,
		MigrationsTable:            "wh_schema_migrations",
		ShouldForceSetLowerVersion: ShouldForceSetLowerVersion,
	}

	operation := func() error {
		return m.Migrate("warehouse")
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err := backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Warnf("Failed to setup WH db tables: %v, retrying after %v", err, t)
	})
	if err != nil {
		return fmt.Errorf("could not run warehouse database migrations: %w", err)
	}
	return nil
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
	defer func() { _ = rows.Close() }()
	return true
}

func setConfigHandler(w http.ResponseWriter, r *http.Request) {
	pkgLogger.LogRequest(r)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer func() { _ = r.Body.Close() }()

	var kvs []warehouseutils.KeyValue
	err = json.Unmarshal(body, &kvs)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error unmarshalling body: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}

	for _, kv := range kvs {
		config.Set(kv.Key, kv.Value)
	}
	w.WriteHeader(http.StatusOK)
}

func pendingEventsHandler(w http.ResponseWriter, r *http.Request) {
	// TODO : respond with errors in a common way
	pkgLogger.LogRequest(r)

	ctx := r.Context()

	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// read body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer func() { _ = r.Body.Close() }()

	// unmarshall body
	var pendingEventsReq warehouseutils.PendingEventsRequest
	err = json.Unmarshal(body, &pendingEventsReq)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error unmarshalling body: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}

	sourceID, taskRunID := pendingEventsReq.SourceID, pendingEventsReq.TaskRunID
	// return error if source id is empty
	if sourceID == "" || taskRunID == "" {
		pkgLogger.Errorf("empty source_id or task_run_id in the pending events request")
		http.Error(w, "empty source_id or task_run_id", http.StatusBadRequest)
		return
	}

	workspaceID, err := tenantManager.SourceToWorkspace(ctx, sourceID)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error checking if source is degraded: %v", err)
		http.Error(w, "workspaceID from sourceID not found", http.StatusBadRequest)
		return
	}

	if tenantManager.DegradedWorkspace(workspaceID) {
		pkgLogger.Infof("[WH]: Workspace (id: %q) is degraded: %v", workspaceID, err)
		http.Error(w, "workspace is in degraded mode", http.StatusServiceUnavailable)
		return
	}

	pendingEvents := false
	var (
		pendingStagingFileCount int64
		pendingUploadCount      int64
	)

	// check whether there are any pending staging files or uploads for the given source id
	// get pending staging files
	pendingStagingFileCount, err = repo.NewStagingFiles(dbHandle).CountPendingForSource(ctx, sourceID)
	if err != nil {
		err := fmt.Errorf("error getting pending staging file count : %v", err)
		pkgLogger.Errorf("[WH]: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	filters := []repo.FilterBy{
		{Key: "source_id", Value: sourceID},
		{Key: "metadata->>'source_task_run_id'", Value: taskRunID},
		{Key: "status", NotEquals: true, Value: model.ExportedData},
		{Key: "status", NotEquals: true, Value: model.Aborted},
	}

	pendingUploadCount, err = getFilteredCount(ctx, filters...)

	if err != nil {
		pkgLogger.Errorf("getting pending uploads count", "error", err)
		http.Error(w, fmt.Sprintf(
			"getting pending uploads count: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	filters = []repo.FilterBy{
		{Key: "source_id", Value: sourceID},
		{Key: "metadata->>'source_task_run_id'", Value: pendingEventsReq.TaskRunID},
		{Key: "status", Value: "aborted"},
	}

	abortedUploadCount, err := getFilteredCount(ctx, filters...)
	if err != nil {
		pkgLogger.Errorf("getting aborted uploads count", "error", err.Error())
		http.Error(w, fmt.Sprintf("getting aborted uploads count: %s", err), http.StatusInternalServerError)
		return
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
		wh := make([]model.Warehouse, 0)

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
			err := fmt.Errorf("no warehouse destinations found for source id '%s'", sourceID)
			pkgLogger.Errorf("[WH]: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		for _, warehouse := range wh {
			triggerUpload(warehouse)
		}
	}

	// create and write response
	res := warehouseutils.PendingEventsResponse{
		PendingEvents:            pendingEvents,
		PendingStagingFilesCount: pendingStagingFileCount,
		PendingUploadCount:       pendingUploadCount,
		AbortedEvents:            abortedUploadCount > 0,
	}

	resBody, err := json.Marshal(res)
	if err != nil {
		err := fmt.Errorf("failed to marshall pending events response : %v", err)
		pkgLogger.Errorf("[WH]: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(resBody)
}

func getFilteredCount(ctx context.Context, filters ...repo.FilterBy) (int64, error) {
	pkgLogger.Debugf("fetching filtered count")
	return repo.NewUploads(dbHandle).Count(ctx, filters...)
}

func getPendingUploadCount(filters ...warehouseutils.FilterBy) (uploadCount int64, err error) {
	pkgLogger.Debugf("Fetching pending upload count with filters: %v", filters)

	query := fmt.Sprintf(`
		SELECT
		  COUNT(*)
		FROM
		  %[1]s
		WHERE
		  %[1]s.status NOT IN ('%[2]s', '%[3]s')
	`,
		warehouseutils.WarehouseUploadsTable,
		model.ExportedData,
		model.Aborted,
	)

	args := make([]interface{}, 0)
	for i, filter := range filters {
		query += fmt.Sprintf(" AND %s=$%d", filter.Key, i+1)
		args = append(args, filter.Value)
	}

	err = dbHandle.QueryRow(query, args...).Scan(&uploadCount)
	if err != nil && err != sql.ErrNoRows {
		err = fmt.Errorf("query: %s failed with Error : %w", query, err)
		return
	}

	return uploadCount, nil
}

func triggerUploadHandler(w http.ResponseWriter, r *http.Request) {
	// TODO : respond with errors in a common way
	pkgLogger.LogRequest(r)

	ctx := r.Context()

	// read body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error reading body: %v", err)
		http.Error(w, "can't read body", http.StatusBadRequest)
		return
	}
	defer func() { _ = r.Body.Close() }()

	// unmarshall body
	var triggerUploadReq warehouseutils.TriggerUploadRequest
	err = json.Unmarshal(body, &triggerUploadReq)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error unmarshalling body: %v", err)
		http.Error(w, "can't unmarshall body", http.StatusBadRequest)
		return
	}

	workspaceID, err := tenantManager.SourceToWorkspace(ctx, triggerUploadReq.SourceID)
	if err != nil {
		pkgLogger.Errorf("[WH]: Error checking if source is degraded: %v", err)
		http.Error(w, "workspaceID from sourceID not found", http.StatusBadRequest)
		return
	}

	if tenantManager.DegradedWorkspace(workspaceID) {
		pkgLogger.Infof("[WH]: Workspace (id: %q) is degraded: %v", workspaceID, err)
		http.Error(w, "workspace is in degraded mode", http.StatusServiceUnavailable)
		return
	}

	err = TriggerUploadHandler(triggerUploadReq.SourceID, triggerUploadReq.DestinationID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func TriggerUploadHandler(sourceID, destID string) error {
	// return error if source id and dest id is empty
	if sourceID == "" && destID == "" {
		err := fmt.Errorf("empty source and destination id")
		pkgLogger.Errorf("[WH]: trigger upload : %v", err)
		return err
	}

	wh := make([]model.Warehouse, 0)

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
		err := fmt.Errorf("no warehouse destinations found for source id '%s'", sourceID)
		pkgLogger.Errorf("[WH]: %v", err)
		return err
	}

	// iterate over each wh destination and trigger upload
	for _, warehouse := range wh {
		triggerUpload(warehouse)
	}
	return nil
}

func databricksVersionHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(deltalake.GetDatabricksVersion()))
}

func isUploadTriggered(wh model.Warehouse) bool {
	triggerUploadsMapLock.RLock()
	defer triggerUploadsMapLock.RUnlock()
	return triggerUploadsMap[wh.Identifier]
}

func triggerUpload(wh model.Warehouse) {
	triggerUploadsMapLock.Lock()
	defer triggerUploadsMapLock.Unlock()
	pkgLogger.Infof("[WH]: Upload triggered for warehouse '%s'", wh.Identifier)
	triggerUploadsMap[wh.Identifier] = true
}

func clearTriggeredUpload(wh model.Warehouse) {
	triggerUploadsMapLock.Lock()
	defer triggerUploadsMapLock.Unlock()
	delete(triggerUploadsMap, wh.Identifier)
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
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

	healthVal := fmt.Sprintf(
		`{"server":"UP","db":%q,"pgNotifier":%q,"acceptingEvents":"TRUE","warehouseMode":%q,"goroutines":"%d"}`,
		dbService, pgNotifierService, strings.ToUpper(warehouseMode), runtime.NumGoroutine(),
	)
	_, _ = w.Write([]byte(healthVal))
}

func getConnectionString() string {
	if !CheckForWarehouseEnvVars() {
		return misc.GetConnectionString()
	}
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s application_name=%s",
		host, port, user, password, dbname, sslMode, appName)
}

func startWebHandler(ctx context.Context) error {
	mux := http.NewServeMux()

	// do not register same endpoint when running embedded in rudder backend
	if isStandAlone() {
		mux.HandleFunc("/health", healthHandler)
	}
	if runningMode != DegradedMode {
		if isMaster() {
			pkgLogger.Infof("WH: Warehouse master service waiting for BackendConfig before starting on %d", webPort)
			backendconfig.DefaultBackendConfig.WaitForConfig(ctx)

			mux.Handle("/v1/process", (&api.WarehouseAPI{
				Logger:      pkgLogger,
				Stats:       stats.Default,
				Repo:        repo.NewStagingFiles(dbHandle),
				Multitenant: tenantManager,
			}).Handler())

			// triggers upload only when there are pending events and triggerUpload is sent for a sourceId
			mux.HandleFunc("/v1/warehouse/pending-events", pendingEventsHandler)
			// triggers uploads for a source
			mux.HandleFunc("/v1/warehouse/trigger-upload", triggerUploadHandler)
			mux.HandleFunc("/databricksVersion", databricksVersionHandler)
			mux.HandleFunc("/v1/setConfig", setConfigHandler)

			// Warehouse Async Job end-points
			mux.HandleFunc("/v1/warehouse/jobs", asyncWh.AddWarehouseJobHandler)           // FIXME: add degraded mode
			mux.HandleFunc("/v1/warehouse/jobs/status", asyncWh.StatusWarehouseJobHandler) // FIXME: add degraded mode

			pkgLogger.Infof("WH: Starting warehouse master service in %d", webPort)
		} else {
			pkgLogger.Infof("WH: Starting warehouse slave service in %d", webPort)
		}
	}

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", webPort),
		Handler:           bugsnag.Handler(mux),
		ReadHeaderTimeout: 3 * time.Second,
	}

	return httputil.ListenAndServe(ctx, srv)
}

// CheckForWarehouseEnvVars Checks if all the required Env Variables for Warehouse are present
func CheckForWarehouseEnvVars() bool {
	return config.IsSet("WAREHOUSE_JOBS_DB_HOST") &&
		config.IsSet("WAREHOUSE_JOBS_DB_USER") &&
		config.IsSet("WAREHOUSE_JOBS_DB_DB_NAME") &&
		config.IsSet("WAREHOUSE_JOBS_DB_PASSWORD")
}

// This checks if gateway is running or not
func isStandAlone() bool {
	return warehouseMode != EmbeddedMode && warehouseMode != EmbeddedMasterMode
}

func isMaster() bool {
	return warehouseMode == config.MasterMode ||
		warehouseMode == config.MasterSlaveMode ||
		warehouseMode == config.EmbeddedMode ||
		warehouseMode == config.EmbeddedMasterMode
}

func isSlave() bool {
	return warehouseMode == config.SlaveMode || warehouseMode == config.MasterSlaveMode || warehouseMode == config.EmbeddedMode
}

func isStandAloneSlave() bool {
	return warehouseMode == config.SlaveMode
}

func setupDB(ctx context.Context, connInfo string) error {
	if isStandAloneSlave() {
		return nil
	}

	var err error
	dbHandle, err = sql.Open("postgres", connInfo)
	if err != nil {
		return err
	}

	isDBCompatible, err := validators.IsPostgresCompatible(ctx, dbHandle)
	if err != nil {
		return err
	}

	if !isDBCompatible {
		err := errors.New("rudder Warehouse Service needs postgres version >= 10. Exiting")
		pkgLogger.Error(err)
		return err
	}

	if err = dbHandle.PingContext(ctx); err != nil {
		return fmt.Errorf("could not ping WH db: %w", err)
	}

	return setupTables(dbHandle)
}

// Setup prepares the database connection for warehouse service, verifies database compatibility and creates the required tables
func Setup(ctx context.Context) error {
	if !isStandAlone() && !db.IsNormalMode() {
		return nil
	}
	psqlInfo := getConnectionString()
	if err := setupDB(ctx, psqlInfo); err != nil {
		return fmt.Errorf("cannot setup warehouse db: %w", err)
	}
	return nil
}

// Start starts the warehouse service
func Start(ctx context.Context, app app.App) error {
	application = app

	if dbHandle == nil && !isStandAloneSlave() {
		return errors.New("warehouse service cannot start, database connection is not setup")
	}
	// do not start warehouse service if rudder core is not in normal mode and warehouse is running in same process as rudder core
	if !isStandAlone() && !db.IsNormalMode() {
		pkgLogger.Infof("Skipping start of warehouse service...")
		return nil
	}

	pkgLogger.Infof("WH: Starting Warehouse service...")
	psqlInfo := getConnectionString()

	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Fatal(r)
			panic(r)
		}
	}()

	runningMode := config.GetString("Warehouse.runningMode", "")
	if runningMode == DegradedMode {
		pkgLogger.Infof("WH: Running warehouse service in degraded mode...")
		if isMaster() {
			rruntime.GoForWarehouse(func() {
				minimalConfigSubscriber()
			})
			err := InitWarehouseAPI(dbHandle, pkgLogger.Child("upload_api"))
			if err != nil {
				pkgLogger.Errorf("WH: Failed to start warehouse api: %v", err)
				return err
			}
		}
		return startWebHandler(ctx)
	}
	var err error
	workspaceIdentifier := fmt.Sprintf(`%s::%s`, config.GetKubeNamespace(), misc.GetMD5Hash(config.GetWorkspaceToken()))
	notifier, err = pgnotifier.New(workspaceIdentifier, psqlInfo)
	if err != nil {
		return fmt.Errorf("cannot setup pgnotifier: %w", err)
	}

	g, ctx := errgroup.WithContext(ctx)

	// Setting up reporting client
	// only if standalone or embedded connecting to diff DB for warehouse
	if (isStandAlone() && isMaster()) || (misc.GetConnectionString() != psqlInfo) {
		reporting := application.Features().Reporting.Setup(backendconfig.DefaultBackendConfig)

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			reporting.AddClient(ctx, types.Config{ConnInfo: psqlInfo, ClientName: types.WarehouseReportingClient})
			return nil
		}))
	}

	if isStandAlone() && isMaster() {
		// Report warehouse features
		g.Go(func() error {
			backendconfig.DefaultBackendConfig.WaitForConfig(ctx)

			c := controlplane.NewClient(
				backendconfig.GetConfigBackendURL(),
				backendconfig.DefaultBackendConfig.Identity(),
			)

			err := c.SendFeatures(ctx, info.WarehouseComponent.Name, info.WarehouseComponent.Features)
			if err != nil {
				pkgLogger.Errorf("error sending warehouse features: %v", err)
			}

			// We don't want to exit if we fail to send features
			return nil
		})
	}

	if isSlave() {
		pkgLogger.Infof("WH: Starting warehouse slave...")
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return setupSlave(ctx)
		}))
	}

	if isMaster() {
		pkgLogger.Infof("[WH]: Starting warehouse master...")

		backendconfig.DefaultBackendConfig.WaitForConfig(ctx)

		region := config.GetString("region", "")

		controlPlaneClient = controlplane.NewClient(
			backendconfig.GetConfigBackendURL(),
			backendconfig.DefaultBackendConfig.Identity(),
			controlplane.WithRegion(region),
		)

		tenantManager = &multitenant.Manager{
			BackendConfig: backendconfig.DefaultBackendConfig,
		}
		g.Go(func() error {
			tenantManager.Run(ctx)
			return nil
		})

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return notifier.ClearJobs(ctx)
		}))

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			err := monitorDestRouters(ctx)
			return err
		}))

		archiver := &archive.Archiver{
			DB:          dbHandle,
			Stats:       stats.Default,
			Logger:      pkgLogger.Child("archiver"),
			FileManager: filemanager.DefaultFileManagerFactory,
			Multitenant: tenantManager,
		}
		g.Go(misc.WithBugsnagForWarehouse(func() error {
			archive.CronArchiver(ctx, archiver)
			return nil
		}))

		err := InitWarehouseAPI(dbHandle, pkgLogger.Child("upload_api"))
		if err != nil {
			pkgLogger.Errorf("WH: Failed to start warehouse api: %v", err)
			return err
		}
		asyncWh = jobs.InitWarehouseJobsAPI(ctx, dbHandle, &notifier)
		jobs.WithConfig(asyncWh, config.Default)

		g.Go(misc.WithBugsnagForWarehouse(func() error {
			return asyncWh.InitAsyncJobRunner()
		}))
	}

	g.Go(func() error {
		return startWebHandler(ctx)
	})

	return g.Wait()
}
