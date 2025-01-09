package router

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/services/notifier"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/bcm"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
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

const defaultUploadPriority = 100

type (
	workerIdentifierMapKey = string
	jobID                  = int64
)

type Router struct {
	destType string

	db           *sqlquerywrapper.DB
	stagingRepo  *repo.StagingFiles
	uploadRepo   *repo.Uploads
	whSchemaRepo *repo.WHSchema

	triggerStore       *sync.Map
	createUploadAlways createUploadAlwaysLoader

	logger       logger.Logger
	conf         *config.Config
	statsFactory stats.Stats

	warehouses           []model.Warehouse
	workspaceBySourceIDs map[string]string
	configSubscriberLock sync.RWMutex

	workerChannelMap     map[string]chan *UploadJob
	workerChannelMapLock sync.RWMutex

	createJobMarkerMap     map[string]time.Time
	createJobMarkerMapLock sync.RWMutex

	inProgressMap     map[workerIdentifierMapKey][]jobID
	inProgressMapLock sync.RWMutex

	processingMu sync.Mutex

	scheduledTimesCache     map[string][]int
	scheduledTimesCacheLock sync.RWMutex

	activeWorkerCount atomic.Int32
	now               func() time.Time
	nowSQL            string

	backgroundGroup *errgroup.Group

	tenantManager    *multitenant.Manager
	bcManager        *bcm.BackendConfigManager
	uploadJobFactory UploadJobFactory
	notifier         *notifier.Notifier

	config struct {
		maxConcurrentUploadJobs           int
		allowMultipleSourcesForJobsPickup bool
		waitForWorkerSleep                time.Duration
		uploadAllocatorSleep              time.Duration
		uploadStatusTrackFrequency        time.Duration
		shouldPopulateHistoricIdentities  bool
		uploadFreqInS                     config.ValueLoader[int64]
		noOfWorkers                       config.ValueLoader[int]
		enableJitterForSyncs              config.ValueLoader[bool]
		maxParallelJobCreation            config.ValueLoader[int]
		mainLoopSleep                     config.ValueLoader[time.Duration]
		stagingFilesBatchSize             config.ValueLoader[int]
		warehouseSyncFreqIgnore           config.ValueLoader[bool]
		cronTrackerRetries                config.ValueLoader[int64]
		uploadBufferTimeInMin             config.ValueLoader[time.Duration]
	}

	stats struct {
		processingPendingJobsStat        stats.Gauge
		processingAvailableWorkersStat   stats.Gauge
		processingPickupLagStat          stats.Timer
		processingPickupWaitTimeStat     stats.Timer
		schedulerWarehouseLengthStat     stats.Gauge
		schedulerTotalSchedulingTimeStat stats.Timer
		cronTrackerExecTimestamp         stats.Gauge
	}
}

func New(
	reporting types.Reporting,
	destType string,
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	db *sqlquerywrapper.DB,
	notifier *notifier.Notifier,
	tenantManager *multitenant.Manager,
	controlPlaneClient *controlplane.Client,
	bcManager *bcm.BackendConfigManager,
	encodingFactory *encoding.Factory,
	triggerStore *sync.Map,
	createUploadAlways createUploadAlwaysLoader,
) *Router {
	r := &Router{}

	r.conf = conf
	r.statsFactory = statsFactory

	r.logger = logger.Child(destType)
	r.logger.Infof("WH: Warehouse Router started: %s", destType)

	r.db = db
	r.stagingRepo = repo.NewStagingFiles(db)
	r.uploadRepo = repo.NewUploads(db)
	r.whSchemaRepo = repo.NewWHSchemas(db)

	r.notifier = notifier
	r.tenantManager = tenantManager
	r.bcManager = bcManager
	r.destType = destType
	r.now = timeutil.Now
	r.triggerStore = triggerStore
	r.createJobMarkerMap = make(map[string]time.Time)
	r.createUploadAlways = createUploadAlways
	r.scheduledTimesCache = make(map[string][]int)
	r.inProgressMap = make(map[workerIdentifierMapKey][]jobID)

	r.uploadJobFactory = UploadJobFactory{
		reporting:            reporting,
		conf:                 r.conf,
		logger:               r.logger,
		statsFactory:         r.statsFactory,
		db:                   r.db,
		destinationValidator: validations.NewDestinationValidator(),
		loadFile: &loadfiles.LoadFileGenerator{
			Conf:               r.conf,
			Logger:             r.logger.Child("loadfile"),
			Notifier:           r.notifier,
			StageRepo:          r.stagingRepo,
			LoadRepo:           repo.NewLoadFiles(db),
			ControlPlaneClient: controlPlaneClient,
		},
		encodingFactory: encodingFactory,
	}
	loadfiles.WithConfig(r.uploadJobFactory.loadFile, r.conf)

	r.loadReloadableConfig(warehouseutils.WHDestNameMap[destType])
	r.loadStats()
	return r
}

func (r *Router) Start(ctx context.Context) error {
	if err := r.uploadRepo.ResetInProgress(ctx, r.destType); err != nil {
		return err
	}

	g, gCtx := errgroup.WithContext(ctx)
	r.backgroundGroup = g
	g.Go(crash.NotifyWarehouse(func() error {
		r.backendConfigSubscriber(gCtx)
		return nil
	}))
	g.Go(crash.NotifyWarehouse(func() error {
		return r.runUploadJobAllocator(gCtx)
	}))
	g.Go(crash.NotifyWarehouse(func() error {
		r.mainLoop(gCtx)
		return nil
	}))
	g.Go(crash.NotifyWarehouse(func() error {
		return r.cronTracker(gCtx)
	}))
	return g.Wait()
}

// Backend Config subscriber subscribes to backend-config and gets all the configurations that includes all sources, destinations and their latest values.
func (r *Router) backendConfigSubscriber(ctx context.Context) {
	for warehouses := range r.bcManager.Subscribe(ctx) {
		r.logger.Info(`Received updated workspace config`)

		warehouses = lo.Filter(warehouses, func(warehouse model.Warehouse, _ int) bool {
			return warehouse.Destination.DestinationDefinition.Name == r.destType
		})

		for _, warehouse := range warehouses {
			if warehouseutils.IDResolutionEnabled() && slices.Contains(warehouseutils.IdentityEnabledWarehouses, r.destType) {
				r.setupIdentityTables(ctx, warehouse)
				if r.config.shouldPopulateHistoricIdentities && warehouse.Destination.Enabled {
					// non-blocking populate historic identities
					r.populateHistoricIdentities(ctx, warehouse)
				}
			}
		}

		r.configSubscriberLock.Lock()
		r.warehouses = warehouses

		if r.workspaceBySourceIDs == nil {
			r.workspaceBySourceIDs = make(map[string]string)
		}

		for _, warehouse := range warehouses {
			r.workspaceBySourceIDs[warehouse.Source.ID] = warehouse.WorkspaceID
		}
		r.configSubscriberLock.Unlock()

		r.workerChannelMapLock.Lock()
		if r.workerChannelMap == nil {
			r.workerChannelMap = make(map[string]chan *UploadJob)
		}

		for _, warehouse := range warehouses {
			workerName := r.workerIdentifier(warehouse)
			// spawn one worker for each unique destID_namespace
			// check this commit to https://github.com/rudderlabs/rudder-server/pull/476/commits/fbfddf167aa9fc63485fe006d34e6881f5019667
			// to avoid creating goroutine for disabled sources/destinations
			if _, ok := r.workerChannelMap[workerName]; !ok {
				r.workerChannelMap[workerName] = r.initWorker()
			}
		}
		r.workerChannelMapLock.Unlock()
	}
}

// workerIdentifier get name of the worker (`destID_namespace`) to be stored in map wh.workerChannelMap
func (r *Router) workerIdentifier(warehouse model.Warehouse) (identifier string) {
	if r.config.allowMultipleSourcesForJobsPickup {
		return warehouse.Source.ID + "_" + warehouse.Destination.ID + "_" + warehouse.Namespace
	}
	return warehouse.Destination.ID + "_" + warehouse.Namespace
}

func (r *Router) initWorker() chan *UploadJob {
	workerChan := make(chan *UploadJob, 1000)

	for i := 0; i < r.config.maxConcurrentUploadJobs; i++ {
		r.backgroundGroup.Go(func() error {
			for uploadJob := range workerChan {
				r.incrementActiveWorkers()

				err := uploadJob.run()
				if err != nil {
					r.logger.Errorf("[WH] Failed in handle Upload jobs for worker: %+v", err)
				}

				r.removeDestInProgress(uploadJob.warehouse, uploadJob.upload.ID)

				r.decrementActiveWorkers()
			}
			return nil
		})
	}
	return workerChan
}

func (r *Router) incrementActiveWorkers() {
	r.activeWorkerCount.Add(1)
}

func (r *Router) decrementActiveWorkers() {
	r.activeWorkerCount.Add(-1)
}

func (r *Router) getActiveWorkerCount() int {
	return int(r.activeWorkerCount.Load())
}

func (r *Router) setDestInProgress(warehouse model.Warehouse, jobID int64) {
	r.inProgressMapLock.Lock()
	defer r.inProgressMapLock.Unlock()

	identifier := r.workerIdentifier(warehouse)
	r.inProgressMap[identifier] = append(r.inProgressMap[identifier], jobID)
}

func (r *Router) removeDestInProgress(warehouse model.Warehouse, jobID int64) {
	r.inProgressMapLock.Lock()
	defer r.inProgressMapLock.Unlock()

	identifier := r.workerIdentifier(warehouse)

	if idx, inProgress := r.checkInProgressMap(jobID, identifier); inProgress {
		r.inProgressMap[identifier] = append(r.inProgressMap[identifier][:idx], r.inProgressMap[identifier][idx+1:]...)
	}
}

func (r *Router) isUploadJobInProgress(warehouse model.Warehouse, jobID int64) (int, bool) {
	r.inProgressMapLock.RLock()
	defer r.inProgressMapLock.RUnlock()

	return r.checkInProgressMap(jobID, r.workerIdentifier(warehouse))
}

func (r *Router) getInProgressNamespaces() []string {
	r.inProgressMapLock.RLock()
	defer r.inProgressMapLock.RUnlock()

	var identifiers []string
	for k, v := range r.inProgressMap {
		if len(v) >= r.config.maxConcurrentUploadJobs {
			identifiers = append(identifiers, k)
		}
	}
	return identifiers
}

func (r *Router) checkInProgressMap(jobID int64, identifier string) (int, bool) {
	for idx, id := range r.inProgressMap[identifier] {
		if jobID == id {
			return idx, true
		}
	}
	return 0, false
}

func (r *Router) runUploadJobAllocator(ctx context.Context) error {
	defer func() {
		r.workerChannelMapLock.RLock()
		for _, workerChannel := range r.workerChannelMap {
			close(workerChannel)
		}
		r.workerChannelMapLock.RUnlock()
	}()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-r.bcManager.InitialConfigFetched:
			r.logger.Debugf("Initial config fetched in runUploadJobAllocator for %s", r.destType)
		}

		availableWorkers := r.config.noOfWorkers.Load() - r.getActiveWorkerCount()
		if availableWorkers < 1 {
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(r.config.waitForWorkerSleep):
			}
			continue
		}

		r.processingMu.Lock()
		inProgressNamespaces := r.getInProgressNamespaces()
		r.logger.Debugf(`Current inProgress namespace identifiers for %s: %v`, r.destType, inProgressNamespaces)

		uploadJobsToProcess, err := r.uploadsToProcess(ctx, availableWorkers, inProgressNamespaces)
		if err != nil && ctx.Err() == nil {
			r.logger.Errorn("Error getting uploads to process", logger.NewErrorField(err))
			r.processingMu.Unlock()
			return err
		}
		for _, uploadJob := range uploadJobsToProcess {
			r.setDestInProgress(uploadJob.warehouse, uploadJob.upload.ID)
		}
		r.processingMu.Unlock()

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
	return nil
}

func (r *Router) uploadsToProcess(ctx context.Context, availableWorkers int, skipIdentifiers []string) ([]*UploadJob, error) {
	uploads, err := r.uploadRepo.GetToProcess(ctx, r.destType, availableWorkers, repo.ProcessOptions{
		SkipIdentifiers:                   skipIdentifiers,
		SkipWorkspaces:                    r.tenantManager.DegradedWorkspaces(),
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

		upload.UseRudderStorage = warehouse.GetBoolDestinationConfig(model.UseRudderStorageSetting)

		if !found {
			uploadJob := r.uploadJobFactory.NewUploadJob(ctx, &model.UploadJob{
				Upload: upload,
			}, nil)

			err := fmt.Errorf("unable to find source : %s or destination : %s, both or the connection between them", upload.SourceID, upload.DestinationID)

			_, _ = uploadJob.setUploadError(err, model.Aborted)

			r.logger.Errorf("%v", err)
			continue
		}

		stagingFilesList, err := r.stagingRepo.GetForUploadID(ctx, upload.ID)
		if err != nil {
			return nil, err
		}

		whManager, err := manager.New(r.destType, r.conf, r.logger, r.statsFactory)
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
		SkipWorkspaces:  r.tenantManager.DegradedWorkspaces(),
	})
	if err != nil {
		return nil, fmt.Errorf("processing stats: %w", err)
	}

	r.processingStats(availableWorkers, jobsStats)

	return uploadJobs, nil
}

func (r *Router) processingStats(availableWorkers int, jobStats model.UploadJobsStats) {
	r.stats.processingPendingJobsStat.Gauge(int(jobStats.PendingJobs))
	r.stats.processingAvailableWorkersStat.Gauge(availableWorkers)
	r.stats.processingPickupLagStat.SendTiming(jobStats.PickupLag)
	r.stats.processingPickupWaitTimeStat.SendTiming(jobStats.PickupWaitTime)
}

func (r *Router) mainLoop(ctx context.Context) {
	for {
		jobCreationChan := make(chan struct{}, r.config.maxParallelJobCreation.Load())

		r.configSubscriberLock.RLock()

		wg := sync.WaitGroup{}
		wg.Add(len(r.warehouses))

		r.stats.schedulerWarehouseLengthStat.Gauge(len(r.warehouses))

		schedulingStartTime := r.now()

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

		r.stats.schedulerTotalSchedulingTimeStat.Since(schedulingStartTime)

		select {
		case <-ctx.Done():
			return
		case <-time.After(r.config.mainLoopSleep.Load()):
		}
	}
}

func (r *Router) createJobs(ctx context.Context, warehouse model.Warehouse) (err error) {
	if err := r.canCreateUpload(ctx, warehouse); err != nil {
		r.statsFactory.NewTaggedStat("wh_scheduler.upload_sync_skipped", stats.CountType, stats.Tags{
			"workspaceId":   warehouse.WorkspaceID,
			"destinationID": warehouse.Destination.ID,
			"destType":      warehouse.Destination.DestinationDefinition.Name,
		}).Count(1)

		r.logger.Debugw("Skipping upload loop since upload freq not exceeded", logfield.Error, err.Error())

		return nil
	}

	priority, err := r.handlePriorityForWaitingUploads(ctx, warehouse)
	if err != nil {
		return fmt.Errorf("handling priority for waiting uploads: %w", err)
	}

	stagingFilesFetchStart := r.now()
	stagingFilesList, err := r.stagingRepo.Pending(ctx, warehouse.Source.ID, warehouse.Destination.ID)
	if err != nil {
		return fmt.Errorf("pending staging files for %q: %w", warehouse.Identifier, err)
	}

	stagingFilesFetchStat := r.statsFactory.NewTaggedStat("wh_scheduler.pending_staging_files", stats.TimerType, stats.Tags{
		"workspaceId":   warehouse.WorkspaceID,
		"destinationID": warehouse.Destination.ID,
		"destType":      warehouse.Destination.DestinationDefinition.Name,
	})
	stagingFilesFetchStat.Since(stagingFilesFetchStart)

	if len(stagingFilesList) == 0 {
		r.logger.Debugf("[WH]: Found no pending staging files for %s", warehouse.Identifier)
		return nil
	}

	uploadJobCreationStat := r.statsFactory.NewTaggedStat("wh_scheduler.create_upload_jobs", stats.TimerType, stats.Tags{
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

	r.updateCreateJobMarker(warehouse, uploadStartAfter)

	return nil
}

func (r *Router) handlePriorityForWaitingUploads(ctx context.Context, warehouse model.Warehouse) (int, error) {
	latestInfo, err := r.uploadRepo.GetLatestUploadInfo(
		ctx,
		warehouse.Source.ID,
		warehouse.Destination.ID,
	)
	if err != nil {
		if errors.Is(err, model.ErrNoUploadsFound) {
			return defaultUploadPriority, nil
		}
		return 0, fmt.Errorf("getting latest upload info: %w", err)
	}

	if latestInfo.Status != model.Waiting {
		return defaultUploadPriority, nil
	}

	r.processingMu.Lock()
	defer r.processingMu.Unlock()

	// If it is present do nothing else delete it
	if _, inProgress := r.isUploadJobInProgress(warehouse, latestInfo.ID); !inProgress {
		if err := r.uploadRepo.DeleteWaiting(ctx, latestInfo.ID); err != nil {
			return 0, fmt.Errorf("deleting waiting upload: %w", err)
		}
	}
	return latestInfo.Priority, nil
}

func (r *Router) uploadStartAfterTime() time.Time {
	if r.config.enableJitterForSyncs.Load() {
		return r.now().Add(time.Duration(rand.Intn(15)) * time.Second)
	}
	return r.now()
}

func (r *Router) createUploadJobsFromStagingFiles(ctx context.Context, warehouse model.Warehouse, stagingFiles []*model.StagingFile, priority int, uploadStartAfter time.Time) error {
	// count := 0
	// Process staging files in batches of stagingFilesBatchSize
	// E.g. If there are 1000 pending staging files and stagingFilesBatchSize is 100,
	// Then we create 10 new entries in wh_uploads table each with 100 staging files
	_, uploadTriggered := r.triggerStore.Load(warehouse.Identifier)
	if uploadTriggered {
		priority = 50
	}

	batches := service.StageFileBatching(stagingFiles, r.config.stagingFilesBatchSize.Load())
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
		r.triggerStore.Delete(warehouse.Identifier)
	}

	return nil
}

func (r *Router) uploadFrequencyExceeded(warehouse model.Warehouse, syncFrequency string) bool {
	freqInS := r.uploadFreqInS(syncFrequency)

	r.createJobMarkerMapLock.RLock()
	lastCreatedAt, ok := r.createJobMarkerMap[warehouse.Identifier]
	r.createJobMarkerMapLock.RUnlock()

	if !ok {
		return true
	}
	return r.now().Sub(lastCreatedAt) > time.Duration(freqInS)*time.Second
}

func (r *Router) uploadFreqInS(syncFrequency string) int64 {
	freqInMin, err := strconv.ParseInt(syncFrequency, 10, 64)
	if err != nil {
		return r.config.uploadFreqInS.Load()
	}
	return freqInMin * 60
}

func (r *Router) updateCreateJobMarker(warehouse model.Warehouse, lastProcessedTime time.Time) {
	r.createJobMarkerMapLock.Lock()
	defer r.createJobMarkerMapLock.Unlock()

	r.createJobMarkerMap[warehouse.Identifier] = lastProcessedTime
}

func (r *Router) loadReloadableConfig(whName string) {
	r.config.maxConcurrentUploadJobs = r.conf.GetIntVar(1, 1, fmt.Sprintf(`Warehouse.%v.maxConcurrentUploadJobs`, whName))
	r.config.waitForWorkerSleep = r.conf.GetDurationVar(5, time.Second, "Warehouse.waitForWorkerSleep", "Warehouse.waitForWorkerSleepInS")
	r.config.uploadAllocatorSleep = r.conf.GetDurationVar(5, time.Second, "Warehouse.uploadAllocatorSleep", "Warehouse.uploadAllocatorSleepInS")
	r.config.uploadStatusTrackFrequency = r.conf.GetDurationVar(30, time.Minute, "Warehouse.uploadStatusTrackFrequency", "Warehouse.uploadStatusTrackFrequencyInMin")
	r.config.allowMultipleSourcesForJobsPickup = r.conf.GetBoolVar(false, fmt.Sprintf(`Warehouse.%v.allowMultipleSourcesForJobsPickup`, whName))
	r.config.shouldPopulateHistoricIdentities = r.conf.GetBoolVar(false, "Warehouse.populateHistoricIdentities")
	r.config.uploadFreqInS = r.conf.GetReloadableInt64Var(1800, 1, "Warehouse.uploadFreqInS")
	r.config.noOfWorkers = r.conf.GetReloadableIntVar(8, 1, fmt.Sprintf(`Warehouse.%v.noOfWorkers`, whName), "Warehouse.noOfWorkers")
	r.config.maxParallelJobCreation = r.conf.GetReloadableIntVar(8, 1, "Warehouse.maxParallelJobCreation")
	r.config.mainLoopSleep = r.conf.GetReloadableDurationVar(5, time.Second, "Warehouse.mainLoopSleep", "Warehouse.mainLoopSleepInS")
	r.config.stagingFilesBatchSize = r.conf.GetReloadableIntVar(960, 1, "Warehouse.stagingFilesBatchSize")
	r.config.enableJitterForSyncs = r.conf.GetReloadableBoolVar(false, "Warehouse.enableJitterForSyncs")
	r.config.warehouseSyncFreqIgnore = r.conf.GetReloadableBoolVar(false, "Warehouse.warehouseSyncFreqIgnore")
	r.config.cronTrackerRetries = r.conf.GetReloadableInt64Var(5, 1, "Warehouse.cronTrackerRetries")
	r.config.uploadBufferTimeInMin = r.conf.GetReloadableDurationVar(180, time.Minute, "Warehouse.uploadBufferTimeInMin")
}

func (r *Router) loadStats() {
	tags := stats.Tags{"module": moduleName, "destType": r.destType}
	r.stats.processingPendingJobsStat = r.statsFactory.NewTaggedStat("wh_processing_pending_jobs", stats.GaugeType, tags)
	r.stats.processingAvailableWorkersStat = r.statsFactory.NewTaggedStat("wh_processing_available_workers", stats.GaugeType, tags)
	r.stats.processingPickupLagStat = r.statsFactory.NewTaggedStat("wh_processing_pickup_lag", stats.TimerType, tags)
	r.stats.processingPickupWaitTimeStat = r.statsFactory.NewTaggedStat("wh_processing_pickup_wait_time", stats.TimerType, tags)
	r.stats.schedulerWarehouseLengthStat = r.statsFactory.NewTaggedStat("wh_scheduler_warehouse_length", stats.GaugeType, tags)
	r.stats.schedulerTotalSchedulingTimeStat = r.statsFactory.NewTaggedStat("wh_scheduler_total_scheduling_time", stats.TimerType, tags)
	r.stats.cronTrackerExecTimestamp = r.statsFactory.NewTaggedStat("warehouse_cron_tracker_timestamp_seconds", stats.GaugeType, tags)
}

func (r *Router) copyWarehouses() []model.Warehouse {
	r.configSubscriberLock.RLock()
	defer r.configSubscriberLock.RUnlock()

	warehouses := make([]model.Warehouse, len(r.warehouses))
	copy(warehouses, r.warehouses)
	return warehouses
}

func (r *Router) getNowSQL() string {
	if r.nowSQL != "" {
		return r.nowSQL
	}
	return "NOW()"
}
