package warehouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samber/lo"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	cpclient "github.com/rudderlabs/rudder-server/warehouse/client/controlplane"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/loadfiles"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type Router struct {
	destType             string
	isEnabled            bool
	conf                 *config.Config
	logger               logger.Logger
	stats                stats.Stats
	warehouses           []model.Warehouse
	dbHandle             *sqlquerywrapper.DB
	warehouseDBHandle    *DB
	stagingRepo          *repo.StagingFiles
	uploadRepo           *repo.Uploads
	whSchemaRepo         *repo.WHSchema
	notifier             pgnotifier.PGNotifier
	configSubscriberLock sync.RWMutex
	workerChannelMap     map[string]chan *UploadJob
	workerChannelMapLock sync.RWMutex
	initialConfigFetched bool
	inProgressMap        map[WorkerIdentifierT][]JobID
	inProgressMapLock    sync.RWMutex
	activeWorkerCount    int32
	workspaceBySourceIDs map[string]string
	tenantManager        multitenant.Manager
	uploadJobFactory     UploadJobFactory
	now                  func() time.Time
	nowSQL               string
	cpInternalClient     cpclient.InternalControlPlane
	backgroundCancel     context.CancelFunc
	backgroundGroup      errgroup.Group
	backgroundWait       func() error
	config               struct {
		noOfWorkers                       int
		maxConcurrentUploadJobs           int
		allowMultipleSourcesForJobsPickup bool
		enableTunnelling                  bool
		enableJitterForSyncs              bool
		maxParallelJobCreation            int
		waitForConfig                     time.Duration
		waitForWorkerSleep                time.Duration
		uploadAllocatorSleep              time.Duration
		mainLoopSleep                     time.Duration
		stagingFilesBatchSize             int
		uploadStatusTrackFrequency        time.Duration
	}
}

func (r *Router) Setup(ctx context.Context, whType string, conf *config.Config, logger logger.Logger, stats stats.Stats) error {
	logger.Infof("WH: Warehouse Router started: %s", whType)

	r.conf = conf
	r.logger = logger
	r.stats = stats

	r.dbHandle = wrappedDBHandle
	// We now have access to the warehouseDBHandle through
	// which we will be running the db calls.
	r.warehouseDBHandle = NewWarehouseDB(wrappedDBHandle)
	r.stagingRepo = repo.NewStagingFiles(wrappedDBHandle)
	r.uploadRepo = repo.NewUploads(wrappedDBHandle)
	r.whSchemaRepo = repo.NewWHSchemas(wrappedDBHandle)

	r.notifier = notifier
	r.destType = whType
	err := r.resetInProgressJobs(ctx)
	if err != nil {
		return err
	}
	r.Enable()
	r.workerChannelMap = make(map[string]chan *UploadJob)
	r.inProgressMap = make(map[WorkerIdentifierT][]JobID)
	r.tenantManager = multitenant.Manager{
		BackendConfig: backendconfig.DefaultBackendConfig,
	}

	r.uploadJobFactory = UploadJobFactory{
		stats:                stats,
		dbHandle:             r.dbHandle,
		pgNotifier:           &r.notifier,
		destinationValidator: validations.NewDestinationValidator(),
		loadFile: &loadfiles.LoadFileGenerator{
			Logger:             r.logger.Child("loadfile"),
			Notifier:           &notifier,
			StageRepo:          repo.NewStagingFiles(wrappedDBHandle),
			LoadRepo:           repo.NewLoadFiles(wrappedDBHandle),
			ControlPlaneClient: controlPlaneClient,
		},
		recovery: service.NewRecovery(whType, repo.NewUploads(wrappedDBHandle)),
	}
	loadfiles.WithConfig(r.uploadJobFactory.loadFile, config.Default)

	whName := warehouseutils.WHDestNameMap[whType]

	config.RegisterIntConfigVariable(8, &r.config.noOfWorkers, true, 1, fmt.Sprintf(`Warehouse.%v.noOfWorkers`, whName), "Warehouse.noOfWorkers")
	config.RegisterIntConfigVariable(1, &r.config.maxConcurrentUploadJobs, false, 1, fmt.Sprintf(`Warehouse.%v.maxConcurrentUploadJobs`, whName))
	config.RegisterBoolConfigVariable(false, &r.config.allowMultipleSourcesForJobsPickup, false, fmt.Sprintf(`Warehouse.%v.allowMultipleSourcesForJobsPickup`, whName))
	config.RegisterBoolConfigVariable(false, &r.config.enableJitterForSyncs, true, "Warehouse.enableJitterForSyncs")
	config.RegisterIntConfigVariable(8, &r.config.maxParallelJobCreation, true, 1, "Warehouse.maxParallelJobCreation")
	config.RegisterDurationConfigVariable(5, &r.config.waitForWorkerSleep, false, time.Second, []string{"Warehouse.waitForWorkerSleep", "Warehouse.waitForWorkerSleepInS"}...)
	config.RegisterDurationConfigVariable(5, &r.config.waitForConfig, false, time.Second, []string{"Warehouse.waitForConfig", "Warehouse.waitForConfigInS"}...)
	config.RegisterDurationConfigVariable(5, &r.config.uploadAllocatorSleep, false, time.Second, []string{"Warehouse.uploadAllocatorSleep", "Warehouse.uploadAllocatorSleepInS"}...)
	config.RegisterDurationConfigVariable(5, &r.config.mainLoopSleep, true, time.Second, []string{"Warehouse.mainLoopSleep", "Warehouse.mainLoopSleepInS"}...)
	config.RegisterIntConfigVariable(960, &r.config.stagingFilesBatchSize, true, 1, "Warehouse.stagingFilesBatchSize")
	config.RegisterDurationConfigVariable(30, &r.config.uploadStatusTrackFrequency, false, time.Minute, []string{"Warehouse.uploadStatusTrackFrequency", "Warehouse.uploadStatusTrackFrequencyInMin"}...)

	r.config.enableTunnelling = config.GetBool("ENABLE_TUNNELLING", true)

	r.cpInternalClient = cpclient.NewInternalClientWithCache(
		r.conf.GetString("CONFIG_BACKEND_URL", "api.rudderlabs.com"),
		cpclient.BasicAuth{
			Username: r.conf.GetString("CP_INTERNAL_API_USERNAME", ""),
			Password: r.conf.GetString("CP_INTERNAL_API_PASSWORD", ""),
		},
	)

	ctx, cancel := context.WithCancel(ctx)
	g, ctx := errgroup.WithContext(ctx)

	r.backgroundCancel = cancel
	r.backgroundWait = g.Wait

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		r.tenantManager.Run(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		r.backendConfigSubscriber(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		r.runUploadJobAllocator(ctx)
		return nil
	}))
	g.Go(misc.WithBugsnagForWarehouse(func() error {
		r.mainLoop(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnagForWarehouse(func() error {
		return r.CronTracker(ctx)
	}))

	return nil
}

// workerIdentifier get name of the worker (`destID_namespace`) to be stored in map wh.workerChannelMap
func (r *Router) workerIdentifier(warehouse model.Warehouse) (identifier string) {
	identifier = fmt.Sprintf(`%s_%s`, warehouse.Destination.ID, warehouse.Namespace)

	if r.config.allowMultipleSourcesForJobsPickup {
		identifier = fmt.Sprintf(`%s_%s_%s`, warehouse.Source.ID, warehouse.Destination.ID, warehouse.Namespace)
	}
	return
}

func (r *Router) getActiveWorkerCount() int {
	return int(atomic.LoadInt32(&r.activeWorkerCount))
}

func (r *Router) decrementActiveWorkers() {
	// decrement number of workers actively engaged
	atomic.AddInt32(&r.activeWorkerCount, -1)
}

func (r *Router) incrementActiveWorkers() {
	// increment number of workers actively engaged
	atomic.AddInt32(&r.activeWorkerCount, 1)
}

func (r *Router) initWorker() chan *UploadJob {
	workerChan := make(chan *UploadJob, 1000)
	for i := 0; i < r.config.maxConcurrentUploadJobs; i++ {
		r.backgroundGroup.Go(func() error {
			for uploadJob := range workerChan {
				r.incrementActiveWorkers()
				err := r.handleUploadJob(uploadJob)
				if err != nil {
					r.logger.Errorf("[WH] Failed in handle Upload jobs for worker: %+w", err)
				}
				r.removeDestInProgress(uploadJob.warehouse, uploadJob.upload.ID)
				r.decrementActiveWorkers()
			}
			return nil
		})
	}
	return workerChan
}

func (*Router) handleUploadJob(uploadJob *UploadJob) error {
	// Process the upload job
	return uploadJob.run()
}

// backendConfigSubscriber subscribes to backend-config and gets all the configurations that includes all sources, destinations and their latest values.
func (r *Router) backendConfigSubscriber(ctx context.Context) {
	for configData := range r.tenantManager.WatchConfig(ctx) {
		var warehouses []model.Warehouse
		sourceIDsByWorkspaceTemp := map[string][]string{}

		workspaceBySourceIDs := map[string]string{}

		r.logger.Info(`Received updated workspace config`)
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

					if destination.DestinationDefinition.Name != r.destType {
						continue
					}

					if r.config.enableTunnelling {
						destination = r.attachSSHTunnellingInfo(ctx, destination)
					}

					namespace := r.getNamespace(ctx, source, destination)
					warehouse := model.Warehouse{
						WorkspaceID: workspaceID,
						Source:      source,
						Destination: destination,
						Namespace:   namespace,
						Type:        r.destType,
						Identifier:  warehouseutils.GetWarehouseIdentifier(r.destType, source.ID, destination.ID),
					}
					warehouses = append(warehouses, warehouse)

					workerName := r.workerIdentifier(warehouse)
					r.workerChannelMapLock.Lock()
					// spawn one worker for each unique destID_namespace
					// check this commit to https://github.com/rudderlabs/rudder-server/pull/476/commits/fbfddf167aa9fc63485fe006d34e6881f5019667
					// to avoid creating goroutine for disabled sources/destinations
					if _, ok := r.workerChannelMap[workerName]; !ok {
						workerChan := r.initWorker()
						r.workerChannelMap[workerName] = workerChan
					}
					r.workerChannelMapLock.Unlock()

					connectionsMapLock.Lock()
					if _, ok := connectionsMap[destination.ID]; !ok {
						connectionsMap[destination.ID] = map[string]model.Warehouse{}
					}
					if _, ok := slaveConnectionsMap[destination.ID]; !ok {
						slaveConnectionsMap[destination.ID] = map[string]model.Warehouse{}
					}
					if warehouse.Destination.Config["sslMode"] == "verify-ca" {
						if err := warehouseutils.WriteSSLKeys(warehouse.Destination); err.IsError() {
							r.logger.Error(err.Error())
							persistSSLFileErrorStat(workspaceID, r.destType, destination.Name, destination.ID, source.Name, source.ID, err.GetErrTag())
						}
					}
					connectionsMap[destination.ID][source.ID] = warehouse
					slaveConnectionsMap[destination.ID][source.ID] = warehouse
					connectionsMapLock.Unlock()

					if warehouseutils.IDResolutionEnabled() && slices.Contains(warehouseutils.IdentityEnabledWarehouses, warehouse.Type) {
						r.setupIdentityTables(ctx, warehouse)
						if shouldPopulateHistoricIdentities && warehouse.Destination.Enabled {
							// non-blocking populate historic identities
							r.populateHistoricIdentities(ctx, warehouse)
						}
					}
				}
			}
		}
		r.configSubscriberLock.Lock()
		r.warehouses = warehouses
		r.workspaceBySourceIDs = workspaceBySourceIDs
		r.logger.Infof("Releasing config subscriber lock: %s", r.destType)
		r.configSubscriberLock.Unlock()

		sourceIDsByWorkspaceLock.Lock()
		sourceIDsByWorkspace = sourceIDsByWorkspaceTemp
		sourceIDsByWorkspaceLock.Unlock()
		r.initialConfigFetched = true
	}
}

func (r *Router) attachSSHTunnellingInfo(ctx context.Context, upstream backendconfig.DestinationT) backendconfig.DestinationT {
	// at destination level, do we have tunnelling enabled.
	if tunnelEnabled := warehouseutils.ReadAsBool("useSSH", upstream.Config); !tunnelEnabled {
		return upstream
	}

	r.logger.Debugf("Fetching ssh keys for destination: %s", upstream.ID)
	keys, err := r.cpInternalClient.GetDestinationSSHKeys(ctx, upstream.ID)
	if err != nil {
		r.logger.Errorf("fetching ssh keys for destination: %s", err.Error())
		return upstream
	}

	replica := backendconfig.DestinationT{}
	if err := deepCopy(upstream, &replica); err != nil {
		r.logger.Errorf("deep copying the destination: %s failed: %s", upstream.ID, err)
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
func (r *Router) getNamespace(ctx context.Context, source backendconfig.SourceT, destination backendconfig.DestinationT) string {
	configMap := destination.Config
	if r.destType == warehouseutils.CLICKHOUSE {
		if _, ok := configMap["database"].(string); ok {
			return configMap["database"].(string)
		}
		return "rudder"
	}
	if configMap["namespace"] != nil {
		namespace, _ := configMap["namespace"].(string)
		if len(strings.TrimSpace(namespace)) > 0 {
			return warehouseutils.ToProviderCase(r.destType, warehouseutils.ToSafeNamespace(r.destType, namespace))
		}
	}
	// TODO: Move config to global level based on use case
	namespacePrefix := r.conf.GetString(fmt.Sprintf("Warehouse.%s.customDatasetPrefix", warehouseutils.WHDestNameMap[r.destType]), "")
	if namespacePrefix != "" {
		return warehouseutils.ToProviderCase(r.destType, warehouseutils.ToSafeNamespace(r.destType, fmt.Sprintf(`%s_%s`, namespacePrefix, source.Name)))
	}

	namespace, err := r.whSchemaRepo.GetNamespace(ctx, source.ID, destination.ID)
	if err != nil {
		r.logger.Errorw("getting namespace",
			logfield.SourceID, source.ID,
			logfield.DestinationID, destination.ID,
			logfield.DestinationType, destination.DestinationDefinition.Name,
			logfield.WorkspaceID, destination.WorkspaceID,
		)
		return ""
	}
	if namespace == "" {
		return warehouseutils.ToProviderCase(r.destType, warehouseutils.ToSafeNamespace(r.destType, source.Name))
	}
	return namespace
}

func (r *Router) setDestInProgress(warehouse model.Warehouse, jobID int64) {
	identifier := r.workerIdentifier(warehouse)
	r.inProgressMapLock.Lock()
	defer r.inProgressMapLock.Unlock()
	r.inProgressMap[WorkerIdentifierT(identifier)] = append(r.inProgressMap[WorkerIdentifierT(identifier)], JobID(jobID))
}

func (r *Router) removeDestInProgress(warehouse model.Warehouse, jobID int64) {
	r.inProgressMapLock.Lock()
	defer r.inProgressMapLock.Unlock()
	identifier := r.workerIdentifier(warehouse)
	if idx, inProgress := r.checkInProgressMap(jobID, identifier); inProgress {
		r.inProgressMap[WorkerIdentifierT(identifier)] = removeFromJobsIDT(r.inProgressMap[WorkerIdentifierT(identifier)], idx)
	}
}

func removeFromJobsIDT(slice []JobID, idx int) []JobID {
	return append(slice[:idx], slice[idx+1:]...)
}

func (r *Router) isUploadJobInProgress(warehouse model.Warehouse, jobID int64) (int, bool) {
	identifier := r.workerIdentifier(warehouse)
	r.inProgressMapLock.RLock()
	defer r.inProgressMapLock.RUnlock()
	return r.checkInProgressMap(jobID, identifier)
}

func (r *Router) getInProgressNamespaces() []string {
	r.inProgressMapLock.RLock()
	defer r.inProgressMapLock.RUnlock()
	var identifiers []string
	for k, v := range r.inProgressMap {
		if len(v) >= r.config.maxConcurrentUploadJobs {
			identifiers = append(identifiers, string(k))
		}
	}
	return identifiers
}

func (r *Router) checkInProgressMap(jobID int64, identifier string) (int, bool) {
	for idx, id := range r.inProgressMap[WorkerIdentifierT(identifier)] {
		if jobID == int64(id) {
			return idx, true
		}
	}
	return 0, false
}

func (r *Router) createUploadJobsFromStagingFiles(ctx context.Context, warehouse model.Warehouse, stagingFiles []*model.StagingFile, priority int, uploadStartAfter time.Time) error {
	// count := 0
	// Process staging files in batches of stagingFilesBatchSize
	// E.g. If there are 1000 pending staging files and stagingFilesBatchSize is 100,
	// Then we create 10 new entries in wh_uploads table each with 100 staging files
	uploadTriggered := isUploadTriggered(warehouse)
	if uploadTriggered {
		priority = 50
	}

	batches := service.StageFileBatching(stagingFiles, r.config.stagingFilesBatchSize)
	for _, batch := range batches {
		upload := model.Upload{
			SourceID:        warehouse.Source.ID,
			Namespace:       warehouse.Namespace,
			WorkspaceID:     warehouse.WorkspaceID,
			DestinationID:   warehouse.Destination.ID,
			DestinationType: r.destType,
			Status:          model.Waiting,

			LoadFileType:  warehouseutils.GetLoadFileType(r.destType),
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

		_, err := r.uploadRepo.CreateWithStagingFiles(ctx, upload, batch)
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

func (r *Router) getLatestUploadStatus(ctx context.Context, warehouse *model.Warehouse) (int64, string, int) {
	uploadID, status, priority, err := r.warehouseDBHandle.GetLatestUploadStatus(
		ctx,
		warehouse.Source.ID,
		warehouse.Destination.ID)
	if err != nil {
		r.logger.Errorf(`Error getting latest upload status for warehouse: %v`, err)
	}

	return uploadID, status, priority
}

func (r *Router) createJobs(ctx context.Context, warehouse model.Warehouse) (err error) {
	if ok, err := r.canCreateUpload(warehouse); !ok {
		r.stats.NewTaggedStat("wh_scheduler.upload_sync_skipped", stats.CountType, stats.Tags{
			"workspaceId":   warehouse.WorkspaceID,
			"destinationID": warehouse.Destination.ID,
			"destType":      warehouse.Destination.DestinationDefinition.Name,
			"reason":        err.Error(),
		}).Count(1)
		r.logger.Debugf("[WH]: Skipping upload loop since %s upload freq not exceeded: %v", warehouse.Identifier, err)
		return nil
	}

	priority := defaultUploadPriority
	uploadID, uploadStatus, uploadPriority := r.getLatestUploadStatus(ctx, &warehouse)
	if uploadStatus == model.Waiting {
		// If it is present do nothing else delete it
		if _, inProgress := r.isUploadJobInProgress(warehouse, uploadID); !inProgress {
			err := r.uploadRepo.DeleteWaiting(ctx, uploadID)
			if err != nil {
				r.logger.Error(err, "uploadID", uploadID, "warehouse", warehouse.Identifier)
			}
			priority = uploadPriority // copy the priority from the latest upload job.
		}
	}

	stagingFilesFetchStat := r.stats.NewTaggedStat("wh_scheduler.pending_staging_files", stats.TimerType, stats.Tags{
		"workspaceId":   warehouse.WorkspaceID,
		"destinationID": warehouse.Destination.ID,
		"destType":      warehouse.Destination.DestinationDefinition.Name,
	})
	stagingFilesFetchStart := time.Now()
	stagingFilesList, err := r.stagingRepo.Pending(ctx, warehouse.Source.ID, warehouse.Destination.ID)
	if err != nil {
		return fmt.Errorf("pending staging files for %q: %w", warehouse.Identifier, err)
	}
	stagingFilesFetchStat.Since(stagingFilesFetchStart)

	if len(stagingFilesList) == 0 {
		r.logger.Debugf("[WH]: Found no pending staging files for %s", warehouse.Identifier)
		return nil
	}

	uploadJobCreationStat := r.stats.NewTaggedStat("wh_scheduler.create_upload_jobs", stats.TimerType, stats.Tags{
		"workspaceId":   warehouse.WorkspaceID,
		"destinationID": warehouse.Destination.ID,
		"destType":      warehouse.Destination.DestinationDefinition.Name,
	})
	defer uploadJobCreationStat.RecordDuration()()

	uploadStartAfter := r.uploadStartAfterTime()
	err = r.createUploadJobsFromStagingFiles(ctx, warehouse, stagingFilesList, priority, uploadStartAfter)
	if err != nil {
		return err
	}
	setLastProcessedMarker(warehouse, uploadStartAfter)

	return nil
}

func setLastProcessedMarker(warehouse model.Warehouse, lastProcessedTime time.Time) {
	lastProcessedMarkerMapLock.Lock()
	defer lastProcessedMarkerMapLock.Unlock()
	lastProcessedMarkerMap[warehouse.Identifier] = lastProcessedTime.Unix()
	lastProcessedMarkerExp.Set(warehouse.Identifier, lastProcessedTime)
}

func (r *Router) uploadStartAfterTime() time.Time {
	if r.config.enableJitterForSyncs {
		return timeutil.Now().Add(time.Duration(rand.Intn(15)) * time.Second)
	}
	return time.Now()
}

func (r *Router) mainLoop(ctx context.Context) {
	for {
		if !r.isEnabled {
			select {
			case <-ctx.Done():
				return
			case <-time.After(r.config.mainLoopSleep):
			}
			continue
		}

		jobCreationChan := make(chan struct{}, r.config.maxParallelJobCreation)
		r.configSubscriberLock.RLock()
		wg := sync.WaitGroup{}
		wg.Add(len(r.warehouses))

		r.stats.NewTaggedStat("wh_scheduler.warehouse_length", stats.GaugeType, stats.Tags{
			warehouseutils.DestinationType: r.destType,
		}).Gauge(len(r.warehouses)) // Correlation between number of warehouses and scheduling time.
		whTotalSchedulingStats := r.stats.NewTaggedStat("wh_scheduler.total_scheduling_time", stats.TimerType, stats.Tags{
			warehouseutils.DestinationType: r.destType,
		})
		whTotalSchedulingStart := time.Now()

		for _, warehouse := range r.warehouses {
			w := warehouse
			rruntime.GoForWarehouse(func() {
				jobCreationChan <- struct{}{}
				defer func() {
					wg.Done()
					<-jobCreationChan
				}()

				r.logger.Debugf("[WH] Processing Jobs for warehouse: %s", w.Identifier)
				err := r.createJobs(ctx, w)
				if err != nil {
					r.logger.Errorf("[WH] Failed to process warehouse Jobs: %v", err)
				}
			})
		}
		r.configSubscriberLock.RUnlock()
		wg.Wait()

		whTotalSchedulingStats.Since(whTotalSchedulingStart)
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.config.mainLoopSleep):
		}
	}
}

func (r *Router) processingStats(availableWorkers int, jobStats model.UploadJobsStats) {
	// Get pending jobs
	pendingJobsStat := r.stats.NewTaggedStat("wh_processing_pending_jobs", stats.GaugeType, stats.Tags{
		"destType": r.destType,
	})
	pendingJobsStat.Gauge(int(jobStats.PendingJobs))

	availableWorkersStat := r.stats.NewTaggedStat("wh_processing_available_workers", stats.GaugeType, stats.Tags{
		"destType": r.destType,
	})
	availableWorkersStat.Gauge(availableWorkers)

	pickupLagStat := r.stats.NewTaggedStat("wh_processing_pickup_lag", stats.TimerType, stats.Tags{
		"destType": r.destType,
	})
	pickupLagStat.SendTiming(jobStats.PickupLag)

	pickupWaitTimeStat := r.stats.NewTaggedStat("wh_processing_pickup_wait_time", stats.TimerType, stats.Tags{
		"destType": r.destType,
	})
	pickupWaitTimeStat.SendTiming(jobStats.PickupWaitTime)
}

func (r *Router) getUploadsToProcess(ctx context.Context, availableWorkers int, skipIdentifiers []string) ([]*UploadJob, error) {
	uploads, err := r.uploadRepo.GetToProcess(ctx, r.destType, availableWorkers, repo.ProcessOptions{
		SkipIdentifiers:                   skipIdentifiers,
		SkipWorkspaces:                    tenantManager.DegradedWorkspaces(),
		AllowMultipleSourcesForJobsPickup: r.config.allowMultipleSourcesForJobsPickup,
	})
	if err != nil {
		return nil, err
	}

	var uploadJobs []*UploadJob
	for _, upload := range uploads {
		r.configSubscriberLock.RLock()
		if upload.WorkspaceID == "" {
			var ok bool
			upload.WorkspaceID, ok = r.workspaceBySourceIDs[upload.SourceID]
			if !ok {
				r.logger.Warnf("could not find workspace id for source id: %s", upload.SourceID)
			}
		}

		warehouse, found := lo.Find(r.warehouses, func(w model.Warehouse) bool {
			return w.Source.ID == upload.SourceID && w.Destination.ID == upload.DestinationID
		})
		r.configSubscriberLock.RUnlock()

		upload.UseRudderStorage = warehouse.GetBoolDestinationConfig("useRudderStorage")

		if !found {
			uploadJob := r.uploadJobFactory.NewUploadJob(ctx, &model.UploadJob{
				Upload: upload,
			}, nil)
			err := fmt.Errorf("unable to find source : %s or destination : %s, both or the connection between them", upload.SourceID, upload.DestinationID)
			_, _ = uploadJob.setUploadError(err, model.Aborted)
			r.logger.Errorf("%v", err)
			continue
		}

		stagingFilesList, err := r.stagingRepo.GetForUpload(ctx, upload)
		if err != nil {
			return nil, err
		}

		whManager, err := manager.New(r.destType, r.conf, r.logger, r.stats)
		if err != nil {
			return nil, err
		}
		uploadJob := r.uploadJobFactory.NewUploadJob(ctx, &model.UploadJob{
			Warehouse:    warehouse,
			Upload:       upload,
			StagingFiles: stagingFilesList,
		}, whManager)

		uploadJobs = append(uploadJobs, uploadJob)
	}

	jobsStats, err := r.uploadRepo.UploadJobsStats(ctx, r.destType, repo.ProcessOptions{
		SkipIdentifiers: skipIdentifiers,
		SkipWorkspaces:  tenantManager.DegradedWorkspaces(),
	})
	if err != nil {
		return nil, fmt.Errorf("processing stats: %w", err)
	}

	r.processingStats(availableWorkers, jobsStats)

	return uploadJobs, nil
}

func (r *Router) runUploadJobAllocator(ctx context.Context) {
loop:
	for {
		if !r.initialConfigFetched {
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(r.config.waitForConfig):
			}
			continue
		}

		availableWorkers := r.config.noOfWorkers - r.getActiveWorkerCount()
		if availableWorkers < 1 {
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(r.config.waitForWorkerSleep):
			}
			continue
		}

		inProgressNamespaces := r.getInProgressNamespaces()
		r.logger.Debugf(`Current inProgress namespace identifiers for %s: %v`, r.destType, inProgressNamespaces)

		uploadJobsToProcess, err := r.getUploadsToProcess(ctx, availableWorkers, inProgressNamespaces)
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) ||
				strings.Contains(err.Error(), "pq: canceling statement due to user request") {
				break loop
			} else {
				r.logger.Errorf(`Error executing getUploadsToProcess: %v`, err)
				panic(err)
			}
		}

		for _, uploadJob := range uploadJobsToProcess {
			r.setDestInProgress(uploadJob.warehouse, uploadJob.upload.ID)
		}

		for _, uploadJob := range uploadJobsToProcess {
			workerName := r.workerIdentifier(uploadJob.warehouse)
			r.workerChannelMapLock.RLock()
			r.workerChannelMap[workerName] <- uploadJob
			r.workerChannelMapLock.RUnlock()
		}

		select {
		case <-ctx.Done():
			break loop
		case <-time.After(r.config.uploadAllocatorSleep):
		}
	}

	r.workerChannelMapLock.RLock()
	for _, workerChannel := range r.workerChannelMap {
		close(workerChannel)
	}
	r.workerChannelMapLock.RUnlock()
}

// Enable enables a router :)
func (r *Router) Enable() {
	r.isEnabled = true
}

// Disable disables a router:)
func (r *Router) Disable() {
	r.isEnabled = false
}

func (r *Router) Shutdown() error {
	r.backgroundCancel()
	return r.backgroundWait()
}

func (r *Router) resetInProgressJobs(ctx context.Context) error {
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
		r.destType,
		true,
	)
	_, err := r.dbHandle.ExecContext(ctx, sqlStatement)
	return err
}
