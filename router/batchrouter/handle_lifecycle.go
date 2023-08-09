package batchrouter

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/isolation"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// Setup initializes the batch router
func (brt *Handle) Setup(
	destType string,
	backendConfig backendconfig.BackendConfig,
	jobsDB, errorDB jobsdb.JobsDB,
	reporting types.Reporting,
	transientSources transientsource.Service,
	rsourcesService rsources.JobService,
	debugger destinationdebugger.DestinationDebugger,
) {
	brt.destType = destType
	brt.backendConfig = backendConfig
	brt.logger = logger.NewLogger().Child("batchrouter").Child(destType)

	brt.netHandle = &http.Client{
		Transport: &http.Transport{},
		Timeout:   config.GetDuration("BatchRouter.httpTimeout", 10, time.Second),
	}
	brt.jobsDB = jobsDB
	brt.errorDB = errorDB
	brt.reporting = reporting
	brt.fileManagerFactory = filemanager.New
	brt.transientSources = transientSources
	brt.rsourcesService = rsourcesService
	if brt.warehouseClient == nil {
		brt.warehouseClient = client.NewWarehouse(misc.GetWarehouseURL(), client.WithTimeout(
			config.GetDuration("WarehouseClient.timeout", 30, time.Second),
		))
	}
	brt.debugger = debugger
	brt.Diagnostics = diagnostics.Diagnostics
	if brt.adaptiveLimit == nil {
		brt.adaptiveLimit = func(limit int64) int64 { return limit }
	}
	defaultIsolationMode := isolation.ModeDestination
	if config.IsSet("WORKSPACE_NAMESPACE") {
		defaultIsolationMode = isolation.ModeWorkspace
	}
	isolationMode := config.GetString("BatchRouter.isolationMode", string(defaultIsolationMode))
	var err error
	if brt.isolationStrategy, err = isolation.GetStrategy(isolation.Mode(isolationMode), destType, func(destinationID string) bool {
		brt.configSubscriberMu.RLock()
		defer brt.configSubscriberMu.RUnlock()
		_, ok := brt.destinationsMap[destinationID]
		return ok
	}); err != nil {
		panic(fmt.Errorf("resolving isolation strategy for mode %q: %w", isolationMode, err))
	}
	config.RegisterIntConfigVariable(10000, &brt.maxEventsInABatch, false, 1, []string{"BatchRouter." + brt.destType + "." + "maxEventsInABatch", "BatchRouter.maxEventsInABatch"}...)
	config.RegisterIntConfigVariable(10000, &brt.maxPayloadSizeInBytes, false, 1, []string{"BatchRouter." + brt.destType + "." + "maxPayloadSizeInBytes", "BatchRouter.maxPayloadSizeInBytes"}...)
	config.RegisterIntConfigVariable(128, &brt.maxFailedCountForJob, true, 1, []string{"BatchRouter." + brt.destType + "." + "maxFailedCountForJob", "BatchRouter." + "maxFailedCountForJob"}...)
	config.RegisterDurationConfigVariable(30, &brt.asyncUploadTimeout, true, time.Minute, []string{"BatchRouter." + brt.destType + "." + "asyncUploadTimeout", "BatchRouter." + "asyncUploadTimeout"}...)
	config.RegisterDurationConfigVariable(180, &brt.retryTimeWindow, true, time.Minute, []string{"BatchRouter." + brt.destType + "." + "retryTimeWindow", "BatchRouter." + brt.destType + "." + "retryTimeWindowInMins", "BatchRouter." + "retryTimeWindow", "BatchRouter." + "retryTimeWindowInMins"}...)
	config.RegisterBoolConfigVariable(types.DefaultReportingEnabled, &brt.reportingEnabled, false, "Reporting.enabled")
	config.RegisterIntConfigVariable(100000, &brt.jobQueryBatchSize, true, 1, []string{"BatchRouter." + brt.destType + "." + "jobQueryBatchSize", "BatchRouter.jobQueryBatchSize"}...)
	config.RegisterDurationConfigVariable(10, &brt.pollStatusLoopSleep, true, time.Second, []string{"BatchRouter." + brt.destType + "." + "pollStatusLoopSleep", "BatchRouter.pollStatusLoopSleep"}...)
	config.RegisterInt64ConfigVariable(1*bytesize.GB, &brt.payloadLimit, true, 1, []string{"BatchRouter." + brt.destType + "." + "PayloadLimit", "BatchRouter.PayloadLimit"}...)
	config.RegisterDurationConfigVariable(600, &brt.jobsDBCommandTimeout, true, time.Second, []string{"JobsDB.BatchRouter.CommandRequestTimeout", "JobsDB.CommandRequestTimeout"}...)
	config.RegisterDurationConfigVariable(600, &brt.jobdDBQueryRequestTimeout, true, time.Second, []string{"JobsDB.BatchRouter.QueryRequestTimeout", "JobsDB.QueryRequestTimeout"}...)
	config.RegisterIntConfigVariable(2, &brt.jobdDBMaxRetries, true, 1, []string{"JobsDB.BatchRouter.MaxRetries", "JobsDB.MaxRetries"}...)
	config.RegisterDurationConfigVariable(2, &brt.minIdleSleep, true, time.Second, []string{"BatchRouter.minIdleSleep"}...)
	config.RegisterDurationConfigVariable(30, &brt.uploadFreq, true, time.Second, []string{"BatchRouter.uploadFreqInS", "BatchRouter.uploadFreq"}...)
	config.RegisterBoolConfigVariable(false, &brt.forceHonorUploadFrequency, true, "BatchRouter.forceHonorUploadFrequency")
	config.RegisterBoolConfigVariable(false, &brt.disableEgress, false, "disableEgress")
	config.RegisterStringConfigVariable("", &brt.toAbortDestinationIDs, true, "BatchRouter.toAbortDestinationIDs")
	config.RegisterDurationConfigVariable(3, &brt.warehouseServiceMaxRetryTime, true, time.Hour, []string{"BatchRouter.warehouseServiceMaxRetryTime", "BatchRouter.warehouseServiceMaxRetryTimeinHr"}...)
	brt.transformerURL = config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	config.RegisterStringConfigVariable("", &brt.datePrefixOverride, true, "BatchRouter.datePrefixOverride")
	config.RegisterStringConfigVariable("", &brt.customDatePrefix, true, "BatchRouter.customDatePrefix")

	ctx, cancel := context.WithCancel(context.Background())
	brt.backgroundGroup, brt.backgroundCtx = errgroup.WithContext(ctx)
	brt.backgroundCancel = cancel
	brt.backgroundWait = brt.backgroundGroup.Wait

	brt.backendConfigInitializedOnce = sync.Once{}
	brt.backendConfigInitialized = make(chan bool)

	brt.destinationsMap = map[string]*router_utils.DestinationWithSources{}
	brt.connectionWHNamespaceMap = map[string]string{}
	brt.encounteredMergeRuleMap = map[string]map[string]bool{}
	brt.uploadIntervalMap = map[string]time.Duration{}
	brt.lastExecTimes = map[string]time.Time{}
	brt.dateFormatProvider = &storageDateFormatProvider{dateFormatsCache: make(map[string]string)}
	var diagnosisTickerTime time.Duration
	config.RegisterDurationConfigVariable(600, &diagnosisTickerTime, false, time.Second, []string{"Diagnostics.batchRouterTimePeriod", "Diagnostics.batchRouterTimePeriodInS"}...)
	brt.diagnosisTicker = time.NewTicker(diagnosisTickerTime)
	brt.uploadedRawDataJobsCache = make(map[string]map[string]bool)

	var limiterGroup sync.WaitGroup
	limiterStatsPeriod := config.GetDuration("BatchRouter.Limiter.statsPeriod", 15, time.Second)
	brt.limiter.read = kitsync.NewLimiter(ctx, &limiterGroup, "brt_read",
		getBatchRouterConfigInt("Limiter.read.limit", brt.destType, 20),
		stats.Default,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("BatchRouter.Limiter.read.dynamicPeriod", 1, time.Second)),
		kitsync.WithLimiterTags(map[string]string{"destType": brt.destType}),
		kitsync.WithLimiterStatsTriggerFunc(func() <-chan time.Time {
			return time.After(limiterStatsPeriod)
		}),
	)
	brt.limiter.process = kitsync.NewLimiter(ctx, &limiterGroup, "brt_process",
		getBatchRouterConfigInt("Limiter.process.limit", brt.destType, 20),
		stats.Default,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("BatchRouter.Limiter.process.dynamicPeriod", 1, time.Second)),
		kitsync.WithLimiterTags(map[string]string{"destType": brt.destType}),
		kitsync.WithLimiterStatsTriggerFunc(func() <-chan time.Time {
			return time.After(limiterStatsPeriod)
		}),
	)
	brt.limiter.upload = kitsync.NewLimiter(ctx, &limiterGroup, "brt_upload",
		getBatchRouterConfigInt("Limiter.upload.limit", brt.destType, 50),
		stats.Default,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("BatchRouter.Limiter.upload.dynamicPeriod", 1, time.Second)),
		kitsync.WithLimiterTags(map[string]string{"destType": brt.destType}),
		kitsync.WithLimiterStatsTriggerFunc(func() <-chan time.Time {
			return time.After(limiterStatsPeriod)
		}),
	)

	brt.logger.Infof("BRT: Batch Router started: %s", destType)

	// waiting for reporting client setup
	if brt.reporting != nil && brt.reportingEnabled {
		// error is ignored as context.TODO() is passed, err is not expected.
		_ = brt.reporting.WaitForSetup(context.TODO(), types.CoreReportingClient)
	}

	brt.crashRecover()

	// periodically publish a zero counter for ensuring that stuck processing pipeline alert
	// can always detect a stuck batch router
	brt.backgroundGroup.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(15 * time.Second):
				stats.Default.NewTaggedStat(`pipeline_processed_events`, stats.CountType, stats.Tags{
					"module":   "batch_router",
					"destType": brt.destType,
					"state":    jobsdb.Executing.State,
					"code":     "0",
				}).Count(0)
			}
		}
	})

	brt.backgroundGroup.Go(func() error {
		limiterGroup.Wait()
		return nil
	})

	brt.backgroundGroup.Go(misc.WithBugsnag(func() error {
		brt.collectMetrics(brt.backgroundCtx)
		return nil
	}))

	if slices.Contains(asyncDestinations, brt.destType) {
		brt.startAsyncDestinationManager()
	}

	brt.backgroundGroup.Go(misc.WithBugsnag(func() error {
		brt.backendConfigSubscriber()
		return nil
	}))
}

func (brt *Handle) startAsyncDestinationManager() {
	asyncStatTags := map[string]string{
		"module":   "batch_router",
		"destType": brt.destType,
	}
	brt.asyncPollTimeStat = stats.Default.NewTaggedStat("async_poll_time", stats.TimerType, asyncStatTags)
	brt.asyncFailedJobsTimeStat = stats.Default.NewTaggedStat("async_failed_job_poll_time", stats.TimerType, asyncStatTags)
	brt.asyncSuccessfulJobCount = stats.Default.NewTaggedStat("async_successful_job_count", stats.CountType, asyncStatTags)
	brt.asyncFailedJobCount = stats.Default.NewTaggedStat("async_failed_job_count", stats.CountType, asyncStatTags)
	brt.asyncAbortedJobCount = stats.Default.NewTaggedStat("async_aborted_job_count", stats.CountType, asyncStatTags)

	brt.asyncDestinationStruct = make(map[string]*common.AsyncDestinationStruct)

	brt.backgroundGroup.Go(misc.WithBugsnag(func() error {
		brt.pollAsyncStatus(brt.backgroundCtx)
		return nil
	}))

	brt.backgroundGroup.Go(misc.WithBugsnag(func() error {
		brt.asyncUploadWorker(brt.backgroundCtx)
		return nil
	}))
}

// Start starts the batch router's main loop
func (brt *Handle) Start() {
	ctx := brt.backgroundCtx
	brt.backgroundGroup.Go(misc.WithBugsnag(func() error {
		<-brt.backendConfigInitialized
		brt.mainLoop(ctx)
		return nil
	}))
}

// Shutdown stops the batch router
func (brt *Handle) Shutdown() {
	brt.backgroundCancel()
	_ = brt.backgroundWait()
}

func (brt *Handle) initAsyncDestinationStruct(destination *backendconfig.DestinationT) {
	_, ok := brt.asyncDestinationStruct[destination.ID]
	manager, err := asyncdestinationmanager.NewManager(destination, brt.backendConfig)
	if err != nil {
		brt.logger.Errorf("BRT: Error initializing async destination struct for %s destination: %v", destination.Name, err)
		destInitFailStat := stats.Default.NewTaggedStat("destination_initialization_fail", stats.CountType, map[string]string{
			"module":   "batch_router",
			"destType": destination.DestinationDefinition.Name,
		})
		destInitFailStat.Count(1)
		manager = &common.InvalidManager{}
	}
	if !ok {
		brt.asyncDestinationStruct[destination.ID] = &common.AsyncDestinationStruct{}
	}
	brt.asyncDestinationStruct[destination.ID].Destination = destination
	brt.asyncDestinationStruct[destination.ID].Manager = manager
}

func (brt *Handle) refreshDestination(destination backendconfig.DestinationT) {
	if slices.Contains(asyncDestinations, destination.DestinationDefinition.Name) {
		asyncDestStruct, ok := brt.asyncDestinationStruct[destination.ID]
		if ok && asyncDestStruct.Destination != nil &&
			asyncDestStruct.Destination.RevisionID == destination.RevisionID {
			return
		}
		brt.initAsyncDestinationStruct(&destination)
	}
}

func (brt *Handle) crashRecover() {
	if slices.Contains(objectStoreDestinations, brt.destType) {
		brt.logger.Debug("BRT: Checking for incomplete journal entries to recover from...")
		entries := brt.jobsDB.GetJournalEntries(jobsdb.RawDataDestUploadOperation)
		for _, entry := range entries {
			var object ObjectStorageDefinition
			if err := json.Unmarshal(entry.OpPayload, &object); err != nil {
				panic(err)
			}
			if len(object.Config) == 0 {
				// Backward compatibility. If old entries dont have config, just delete journal entry
				brt.jobsDB.JournalDeleteEntry(entry.OpID)
				continue
			}
			downloader, err := brt.fileManagerFactory(&filemanager.Settings{
				Provider: object.Provider,
				Config:   object.Config,
			})
			if err != nil {
				panic(err)
			}

			localTmpDirName := "/rudder-raw-data-dest-upload-crash-recovery/"
			tmpDirPath, err := misc.CreateTMPDIR()
			if err != nil {
				panic(err)
			}
			jsonPath := fmt.Sprintf("%v%v.json", tmpDirPath+localTmpDirName, fmt.Sprintf("%v.%v", time.Now().Unix(), uuid.New().String()))

			err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
			if err != nil {
				panic(err)
			}
			jsonFile, err := os.Create(jsonPath)
			if err != nil {
				panic(err)
			}

			brt.logger.Debugf("BRT: Downloading data for incomplete journal entry to recover from %s at key: %s\n", object.Provider, object.Key)

			var objKey string
			if prefix, ok := object.Config["prefix"]; ok && prefix != "" {
				objKey += fmt.Sprintf("/%s", strings.TrimSpace(prefix.(string)))
			}
			objKey += object.Key

			err = downloader.Download(context.TODO(), jsonFile, objKey)
			if err != nil {
				brt.logger.Errorf("BRT: Failed to download data for incomplete journal entry to recover from %s at key: %s with error: %v\n", object.Provider, object.Key, err)
				brt.jobsDB.JournalDeleteEntry(entry.OpID)
				continue
			}

			jsonFile.Close()
			defer os.Remove(jsonPath)
			rawf, err := os.Open(jsonPath)
			if err != nil {
				panic(err)
			}
			reader, err := gzip.NewReader(rawf)
			if err != nil {
				panic(err)
			}

			sc := bufio.NewScanner(reader)

			brt.logger.Debug("BRT: Setting go map cache for incomplete journal entry to recover from...")
			for sc.Scan() {
				lineBytes := sc.Bytes()
				eventID := gjson.GetBytes(lineBytes, "messageId").String()
				if _, ok := brt.uploadedRawDataJobsCache[object.DestinationID]; !ok {
					brt.uploadedRawDataJobsCache[object.DestinationID] = make(map[string]bool)
				}
				brt.uploadedRawDataJobsCache[object.DestinationID][eventID] = true
			}
			_ = reader.Close()
			brt.jobsDB.JournalDeleteEntry(entry.OpID)
		}
	}
}

func (brt *Handle) backendConfigSubscriber() {
	ch := brt.backendConfig.Subscribe(brt.backgroundCtx, backendconfig.TopicBackendConfig)
	initialized := func() {
		brt.backendConfigInitializedOnce.Do(func() {
			close(brt.backendConfigInitialized)
		})
	}
	defer initialized()
	for data := range ch {
		destinationsMap := map[string]*router_utils.DestinationWithSources{}
		connectionWHNamespaceMap := map[string]string{}
		uploadIntervalMap := map[string]time.Duration{}
		config := data.Data.(map[string]backendconfig.ConfigT)
		for _, wConfig := range config {
			for _, source := range wConfig.Sources {
				if len(source.Destinations) > 0 {
					for _, destination := range source.Destinations {
						if destination.DestinationDefinition.Name == brt.destType {
							if _, ok := destinationsMap[destination.ID]; !ok {
								destinationsMap[destination.ID] = &router_utils.DestinationWithSources{Destination: destination, Sources: []backendconfig.SourceT{}}
								uploadIntervalMap[destination.ID] = brt.uploadInterval(destination.Config)
							}
							destinationsMap[destination.ID].Sources = append(destinationsMap[destination.ID].Sources, source)
							brt.refreshDestination(destination)

							// initialize map to track encountered anonymousIds for a warehouse destination
							if warehouseutils.IDResolutionEnabled() && slices.Contains(warehouseutils.IdentityEnabledWarehouses, brt.destType) {
								connIdentifier := connectionIdentifier(Connection{Destination: destination, Source: source})
								warehouseConnIdentifier := warehouseConnectionIdentifier(brt.destType, connIdentifier, source, destination)
								connectionWHNamespaceMap[connIdentifier] = warehouseConnIdentifier
							}
						}
					}
				}
			}
		}
		brt.configSubscriberMu.Lock()
		brt.destinationsMap = destinationsMap
		brt.connectionWHNamespaceMap = connectionWHNamespaceMap
		brt.uploadIntervalMap = uploadIntervalMap
		initialized()
		brt.configSubscriberMu.Unlock()
	}
}
