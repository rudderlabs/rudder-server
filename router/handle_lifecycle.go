package router

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/bytesize"

	"golang.org/x/sync/errgroup"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	customDestinationManager "github.com/rudderlabs/rudder-server/router/customdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/internal/eventorder"
	"github.com/rudderlabs/rudder-server/router/internal/partition"
	"github.com/rudderlabs/rudder-server/router/isolation"
	"github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/services/rsources"
	transformerFeaturesService "github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

// Setup initializes this module
func (rt *Handle) Setup(
	destinationDefinition backendconfig.DestinationDefinitionT,
	log logger.Logger,
	config *config.Config,
	backendConfig backendconfig.BackendConfig,
	jobsDB jobsdb.JobsDB,
	errorDB jobsdb.JobsDB,
	transientSources transientsource.Service,
	rsourcesService rsources.JobService,
	transformerFeaturesService transformerFeaturesService.FeaturesService,
	debugger destinationdebugger.DestinationDebugger,
	throttlerFactory throttler.Factory,
) {
	rt.backendConfig = backendConfig
	rt.debugger = debugger
	rt.throttlerFactory = throttlerFactory

	destType := destinationDefinition.Name
	rt.logger = log.Child(destType)
	rt.logger.Info("router setup: ", destType)

	rt.transientSources = transientSources
	rt.rsourcesService = rsourcesService
	rt.transformerFeaturesService = transformerFeaturesService

	rt.jobsDB = jobsDB
	rt.errorDB = errorDB
	rt.destType = destType

	rt.drainer = routerutils.NewDrainer(
		config,
		func(destinationID string) (*routerutils.DestinationWithSources, bool) {
			rt.destinationsMapMu.RLock()
			defer rt.destinationsMapMu.RUnlock()
			dest, destFound := rt.destinationsMap[destinationID]
			return dest, destFound
		})
	rt.reloadableConfig = &reloadableConfig{}
	rt.setupReloadableVars()
	rt.crashRecover()
	rt.responseQ = make(chan workerJobStatus, rt.reloadableConfig.jobQueryBatchSize.Load())
	if rt.netHandle == nil {
		netHandle := &netHandle{disableEgress: config.GetBool("disableEgress", false)}
		netHandle.logger = rt.logger.Child("network")
		netHandle.Setup(destType, rt.netClientTimeout)
		rt.netHandle = netHandle
	}

	rt.customDestinationManager = customDestinationManager.New(destType, customDestinationManager.Opts{
		Timeout: rt.netClientTimeout,
	})
	rt.telemetry = &Diagnostic{}
	rt.telemetry.failuresMetric = make(map[string]map[string]int)
	rt.telemetry.diagnosisTicker = time.NewTicker(rt.diagnosisTickerTime)

	rt.destinationResponseHandler = NewResponseHandler(rt.logger, destinationDefinition.ResponseRules)
	if value, ok := destinationDefinition.Config["saveDestinationResponse"].(bool); ok {
		rt.saveDestinationResponse = value
	}
	rt.guaranteeUserEventOrder = getRouterConfigBool("guaranteeUserEventOrder", rt.destType, true)
	rt.noOfWorkers = getRouterConfigInt("noOfWorkers", destType, 64)
	rt.workerInputBufferSize = getRouterConfigInt("noOfJobsPerChannel", destType, 1000)

	rt.enableBatching = config.GetBoolVar(false, "Router."+rt.destType+".enableBatching")

	rt.drainConcurrencyLimit = config.GetReloadableIntVar(1, 1, "Router."+destType+".eventOrderDrainedConcurrencyLimit", "Router.eventOrderDrainedConcurrencyLimit")
	rt.eventOrderKeyThreshold = config.GetReloadableIntVar(200, 1, "Router."+destType+".eventOrderKeyThreshold", "Router.eventOrderKeyThreshold")
	rt.eventOrderDisabledStateDuration = config.GetReloadableDurationVar(20, time.Minute, "Router."+destType+".eventOrderDisabledStateDuration", "Router.eventOrderDisabledStateDuration")
	rt.eventOrderHalfEnabledStateDuration = config.GetReloadableDurationVar(10, time.Minute, "Router."+destType+".eventOrderHalfEnabledStateDuration", "Router.eventOrderHalfEnabledStateDuration")
	rt.reportJobsdbPayload = config.GetReloadableBoolVar(true, "Router."+destType+".reportJobsdbPayload", "Router.reportJobsdbPayload")

	statTags := stats.Tags{"destType": rt.destType}
	rt.tracer = stats.Default.NewTracer("router")
	rt.batchInputCountStat = stats.Default.NewTaggedStat("router_batch_num_input_jobs", stats.CountType, statTags)
	rt.batchOutputCountStat = stats.Default.NewTaggedStat("router_batch_num_output_jobs", stats.CountType, statTags)
	rt.routerTransformInputCountStat = stats.Default.NewTaggedStat("router_transform_num_input_jobs", stats.CountType, statTags)
	rt.routerTransformOutputCountStat = stats.Default.NewTaggedStat("router_transform_num_output_jobs", stats.CountType, statTags)
	rt.batchInputOutputDiffCountStat = stats.Default.NewTaggedStat("router_batch_input_output_diff_jobs", stats.CountType, statTags)
	rt.routerResponseTransformStat = stats.Default.NewTaggedStat("response_transform_latency", stats.TimerType, statTags)
	rt.throttlingErrorStat = stats.Default.NewTaggedStat("router_throttling_error", stats.CountType, statTags)
	rt.throttledStat = stats.Default.NewTaggedStat("router_throttled", stats.CountType, statTags)
	rt.transformer = transformer.NewTransformer(rt.netClientTimeout, rt.transformerTimeout,
		backendConfig, rt.reloadableConfig.oauthV2Enabled,
		rt.reloadableConfig.oauthV2ExpirationTimeDiff,
	)

	rt.oauth = oauth.NewOAuthErrorHandler(backendConfig)

	rt.isBackendConfigInitialized = false
	rt.backendConfigInitialized = make(chan bool)

	isolationMode := isolationMode(destType, config)
	var err error
	if rt.isolationStrategy, err = isolation.GetStrategy(isolationMode, rt.destType, func(destinationID string) bool {
		rt.destinationsMapMu.RLock()
		defer rt.destinationsMapMu.RUnlock()
		_, ok := rt.destinationsMap[destinationID]
		return ok
	}); err != nil {
		panic(fmt.Errorf("resolving isolation strategy for mode %q: %w", isolationMode, err))
	}

	rt.eventOrderingDisabledForWorkspace = func(workspaceID string) bool {
		return slices.Contains(config.GetStringSlice("Router.orderingDisabledWorkspaceIDs", nil), workspaceID)
	}
	rt.eventOrderingDisabledForDestination = func(destinationID string) bool {
		return slices.Contains(config.GetStringSlice("Router.orderingDisabledDestinationIDs", nil), destinationID)
	}
	rt.barrier = eventorder.NewBarrier(eventorder.WithMetadata(map[string]string{
		"destType":         rt.destType,
		"batching":         strconv.FormatBool(rt.enableBatching),
		"transformerProxy": strconv.FormatBool(rt.reloadableConfig.transformerProxy.Load()),
	}),
		eventorder.WithEventOrderKeyThreshold(rt.eventOrderKeyThreshold),
		eventorder.WithDisabledStateDuration(rt.eventOrderDisabledStateDuration),
		eventorder.WithHalfEnabledStateDuration(rt.eventOrderHalfEnabledStateDuration),
		eventorder.WithDrainConcurrencyLimit(rt.drainConcurrencyLimit),
		eventorder.WithDebugInfoProvider(rt.eventOrderDebugInfo),
		eventorder.WithOrderingDisabledCheckForBarrierKey(func(key eventorder.BarrierKey) bool {
			return rt.eventOrderingDisabledForWorkspace(key.WorkspaceID) || rt.eventOrderingDisabledForDestination(key.DestinationID)
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	rt.backgroundCtx = ctx
	rt.backgroundGroup = g
	rt.backgroundCancel = cancel
	rt.backgroundWait = g.Wait

	var limiterGroup sync.WaitGroup
	limiterStatsPeriod := config.GetDuration("Router.Limiter.statsPeriod", 15, time.Second)
	rt.limiter.pickup = kitsync.NewLimiter(ctx, &limiterGroup, "rt_pickup",
		getRouterConfigInt("Limiter.pickup.limit", rt.destType, 100),
		stats.Default,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Router.Limiter.pickup.dynamicPeriod", 1, time.Second)),
		kitsync.WithLimiterTags(map[string]string{"destType": rt.destType}),
		kitsync.WithLimiterStatsTriggerFunc(func() <-chan time.Time {
			return time.After(limiterStatsPeriod)
		}),
	)
	rt.limiter.stats.pickup = partition.NewStats()

	rt.limiter.transform = kitsync.NewLimiter(ctx, &limiterGroup, "rt_transform",
		getRouterConfigInt("Limiter.transform.limit", rt.destType, 200),
		stats.Default,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Router.Limiter.transform.dynamicPeriod", 1, time.Second)),
		kitsync.WithLimiterTags(map[string]string{"destType": rt.destType}),
		kitsync.WithLimiterStatsTriggerFunc(func() <-chan time.Time {
			return time.After(limiterStatsPeriod)
		}),
	)
	rt.limiter.stats.transform = partition.NewStats()

	rt.limiter.batch = kitsync.NewLimiter(ctx, &limiterGroup, "rt_batch",
		getRouterConfigInt("Limiter.batch.limit", rt.destType, 200),
		stats.Default,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Router.Limiter.batch.dynamicPeriod", 1, time.Second)),
		kitsync.WithLimiterTags(map[string]string{"destType": rt.destType}),
		kitsync.WithLimiterStatsTriggerFunc(func() <-chan time.Time {
			return time.After(limiterStatsPeriod)
		}),
	)
	rt.limiter.stats.batch = partition.NewStats()

	rt.limiter.process = kitsync.NewLimiter(ctx, &limiterGroup, "rt_process",
		getRouterConfigInt("Limiter.process.limit", rt.destType, 200),
		stats.Default,
		kitsync.WithLimiterDynamicPeriod(config.GetDuration("Router.Limiter.process.dynamicPeriod", 1, time.Second)),
		kitsync.WithLimiterTags(map[string]string{"destType": rt.destType}),
		kitsync.WithLimiterStatsTriggerFunc(func() <-chan time.Time {
			return time.After(limiterStatsPeriod)
		}),
	)
	rt.limiter.stats.process = partition.NewStats()

	rt.backgroundGroup.Go(func() error {
		limiterGroup.Wait()
		return nil
	})

	g.Go(crash.Wrapper(func() error {
		limiterStats := func(key string, pstats *partition.Stats) {
			allPStats := pstats.All()
			for _, pstat := range allPStats {
				statTags := stats.Tags{
					"destType": rt.destType,
				}
				stats.Default.NewTaggedStat("rt_"+key+"_limiter_stats_throughput", stats.GaugeType, statTags).Gauge(pstat.Throughput)
				stats.Default.NewTaggedStat("rt_"+key+"_limiter_stats_errors", stats.GaugeType, statTags).Gauge(pstat.Errors)
				stats.Default.NewTaggedStat("rt_"+key+"_limiter_stats_successes", stats.GaugeType, statTags).Gauge(pstat.Successes)
				stats.Default.NewTaggedStat("rt_"+key+"_limiter_stats_norm_throughput", stats.GaugeType, statTags).Gauge(pstat.NormalizedThroughput)
				stats.Default.NewTaggedStat("rt_"+key+"_limiter_stats_score", stats.GaugeType, statTags).Gauge(pstat.Score)
			}
		}
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(15 * time.Second):
				limiterStats("pickup", rt.limiter.stats.pickup)
				limiterStats("transform", rt.limiter.stats.transform)
				limiterStats("batch", rt.limiter.stats.batch)
				limiterStats("process", rt.limiter.stats.process)
			}
		}
	}))

	// periodically publish a zero counter for ensuring that stuck processing pipeline alert
	// can always detect a stuck router
	g.Go(crash.Wrapper(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(15 * time.Second):
				stats.Default.NewTaggedStat(`pipeline_processed_events`, stats.CountType, stats.Tags{
					"module":   "router",
					"destType": rt.destType,
					"state":    jobsdb.Executing.State,
					"code":     "0",
				}).Count(0)
			}
		}
	}))

	g.Go(crash.Wrapper(func() error {
		rt.collectMetrics(ctx)
		return nil
	}))

	g.Go(crash.Wrapper(func() error {
		rt.statusInsertLoop()
		return nil
	}))

	if rt.adaptiveLimit == nil {
		rt.adaptiveLimit = func(limit int64) int64 { return limit }
	}

	rruntime.Go(func() {
		rt.backendConfigSubscriber()
	})
}

func (rt *Handle) setupReloadableVars() {
	rt.reloadableConfig.jobsDBCommandTimeout = config.GetReloadableDurationVar(90, time.Second, "JobsDB.Router.CommandRequestTimeout", "JobsDB.CommandRequestTimeout")
	rt.reloadableConfig.jobdDBMaxRetries = config.GetReloadableIntVar(2, 1, "JobsDB.Router.MaxRetries", "JobsDB.MaxRetries")
	rt.reloadableConfig.noOfJobsToBatchInAWorker = config.GetReloadableIntVar(20, 1, "Router."+rt.destType+".noOfJobsToBatchInAWorker", "Router.noOfJobsToBatchInAWorker")
	rt.reloadableConfig.maxFailedCountForJob = config.GetReloadableIntVar(3, 1, "Router."+rt.destType+".maxFailedCountForJob", "Router.maxFailedCountForJob")
	rt.reloadableConfig.maxFailedCountForSourcesJob = config.GetReloadableIntVar(3, 1, "Router.RSources"+rt.destType+".maxFailedCountForJob", "Router.RSources.maxFailedCountForJob")
	rt.reloadableConfig.payloadLimit = config.GetReloadableInt64Var(100*bytesize.MB, 1, "Router."+rt.destType+".PayloadLimit", "Router.PayloadLimit")
	rt.reloadableConfig.retryTimeWindow = config.GetReloadableDurationVar(180, time.Minute, "Router."+rt.destType+".retryTimeWindow", "Router.retryTimeWindow")
	rt.reloadableConfig.sourcesRetryTimeWindow = config.GetReloadableDurationVar(1, time.Minute, "Router.RSources"+rt.destType+".retryTimeWindow", "Router.RSources.retryTimeWindow")
	rt.reloadableConfig.maxDSQuerySize = config.GetReloadableIntVar(10, 1, "Router."+rt.destType+".maxDSQuery", "Router.maxDSQuery")
	rt.reloadableConfig.savePayloadOnError = config.GetReloadableBoolVar(false, "Router."+rt.destType+".savePayloadOnError", "Router.savePayloadOnError")
	rt.reloadableConfig.transformerProxy = config.GetReloadableBoolVar(false, "Router."+rt.destType+".transformerProxy", "Router.transformerProxy")
	rt.reloadableConfig.skipRtAbortAlertForTransformation = config.GetReloadableBoolVar(false, "Router."+rt.destType+".skipRtAbortAlertForTf", "Router.skipRtAbortAlertForTf")
	rt.reloadableConfig.skipRtAbortAlertForDelivery = config.GetReloadableBoolVar(false, "Router."+rt.destType+".skipRtAbortAlertForDelivery", "Router.skipRtAbortAlertForDelivery")
	rt.reloadableConfig.jobQueryBatchSize = config.GetReloadableIntVar(10000, 1, "Router.jobQueryBatchSize")
	rt.reloadableConfig.updateStatusBatchSize = config.GetReloadableIntVar(1000, 1, "Router.updateStatusBatchSize")
	rt.reloadableConfig.readSleep = config.GetReloadableDurationVar(1000, time.Millisecond, "Router.readSleep", "Router.readSleepInMS")
	rt.reloadableConfig.jobsBatchTimeout = config.GetReloadableDurationVar(5, time.Second, "Router.jobsBatchTimeout", "Router.jobsBatchTimeoutInSec")
	rt.reloadableConfig.maxStatusUpdateWait = config.GetReloadableDurationVar(5, time.Second, "Router.maxStatusUpdateWait", "Router.maxStatusUpdateWaitInS")
	rt.reloadableConfig.minRetryBackoff = config.GetReloadableDurationVar(10, time.Second, "Router.minRetryBackoff", "Router.minRetryBackoffInS")
	rt.reloadableConfig.maxRetryBackoff = config.GetReloadableDurationVar(300, time.Second, "Router.maxRetryBackoff", "Router.maxRetryBackoffInS")
	rt.reloadableConfig.pickupFlushInterval = config.GetReloadableDurationVar(2, time.Second, "Router.pickupFlushInterval")
	rt.reloadableConfig.failingJobsPenaltySleep = config.GetReloadableDurationVar(2000, time.Millisecond, "Router.failingJobsPenaltySleep")
	rt.reloadableConfig.failingJobsPenaltyThreshold = config.GetReloadableFloat64Var(0.6, "Router.failingJobsPenaltyThreshold")
	rt.reloadableConfig.oauthV2Enabled = config.GetReloadableBoolVar(false, "Router."+rt.destType+".oauthV2Enabled", "Router.oauthV2Enabled")
	rt.reloadableConfig.oauthV2ExpirationTimeDiff = config.GetReloadableDurationVar(5, time.Minute, "Router."+rt.destType+".oauth.expirationTimeDiff", "Router.oauth.expirationTimeDiff")
	rt.diagnosisTickerTime = config.GetDurationVar(60, time.Second, "Diagnostics.routerTimePeriod", "Diagnostics.routerTimePeriodInS")
	rt.netClientTimeout = config.GetDurationVar(10, time.Second,
		"Router."+rt.destType+".httpTimeout",
		"Router."+rt.destType+".httpTimeoutInS",
		"Router.httpTimeout", "Router.httpTimeoutInS")
	rt.transformerTimeout = config.GetDurationVar(600, time.Second, "HttpClient.backendProxy.timeout", "HttpClient.routerTransformer.timeout")
}

func (rt *Handle) Start() {
	rt.logger.Infof("Starting router: %s", rt.destType)
	rt.startEnded = make(chan struct{})
	ctx := rt.backgroundCtx

	rt.backgroundGroup.Go(crash.Wrapper(func() error {
		defer close(rt.startEnded) // always close the channel
		select {
		case <-ctx.Done():
			rt.logger.Infof("Router : %s start goroutine exited", rt.destType)
			return nil
		case <-rt.backendConfigInitialized:
			// no-op, just wait
		}

		// waiting for transformer features
		rt.logger.Info("Router: Waiting for transformer features")
		select {
		case <-ctx.Done():
			return nil
		case <-rt.transformerFeaturesService.Wait():
			// proceed
		}
		rt.logger.Info("Router: Transformer features received")

		if rt.customDestinationManager != nil {
			select {
			case <-ctx.Done():
				return nil
			case <-rt.customDestinationManager.BackendConfigInitialized():
				// no-op, just wait
			}
		}

		// start the ping loop
		pool := workerpool.New(ctx, func(partition string) workerpool.Worker { return newPartitionWorker(ctx, rt, partition) }, rt.logger)
		defer pool.Shutdown()
		var mainLoopSleep time.Duration
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(mainLoopSleep):
				for _, partition := range rt.activePartitions(ctx) {
					pool.PingWorker(partition)
				}
				mainLoopSleep = rt.reloadableConfig.readSleep.Load()
			}
		}
	}))
}

func (rt *Handle) Shutdown() {
	if rt.startEnded == nil {
		// router is not started
		return
	}
	rt.logger.Infof("Shutting down router: %s", rt.destType)
	rt.backgroundCancel()

	<-rt.startEnded // wait for all workers to stop first
	rt.throttlerFactory.Shutdown()
	close(rt.responseQ) // now it is safe to close the response channel
	_ = rt.backgroundWait()
}

// statusInsertLoop will run in a separate goroutine
// Blocking method, returns when rt.responseQ channel is closed.
func (rt *Handle) statusInsertLoop() {
	statusStat := stats.Default.NewTaggedStat("router_status_loop", stats.TimerType, stats.Tags{"destType": rt.destType})
	countStat := stats.Default.NewTaggedStat("router_status_events", stats.CountType, stats.Tags{"destType": rt.destType})

	for {
		jobResponseBuffer, numJobResponses, _, isResponseQOpen := lo.BufferWithTimeout(
			rt.responseQ,
			rt.reloadableConfig.updateStatusBatchSize.Load(),
			rt.reloadableConfig.maxStatusUpdateWait.Load(),
		)
		if numJobResponses > 0 {
			start := time.Now()
			rt.commitStatusList(&jobResponseBuffer)
			countStat.Count(numJobResponses)
			statusStat.Since(start)
		}
		if !isResponseQOpen {
			rt.logger.Debugf("[%v Router] :: statusInsertLoop exiting", rt.destType)
			return
		}
	}
}

func (rt *Handle) backendConfigSubscriber() {
	ch := rt.backendConfig.Subscribe(context.TODO(), backendconfig.TopicBackendConfig)
	for configEvent := range ch {
		destinationsMap := map[string]*routerutils.DestinationWithSources{}
		connectionsMap := map[types.SourceDest]types.ConnectionWithID{}
		configData := configEvent.Data.(map[string]backendconfig.ConfigT)
		for _, wConfig := range configData {
			for i := range wConfig.Sources {
				source := &wConfig.Sources[i]
				for i := range source.Destinations {
					destination := &source.Destinations[i]
					if destination.DestinationDefinition.Name == rt.destType {
						if _, ok := destinationsMap[destination.ID]; !ok {
							destinationsMap[destination.ID] = &routerutils.DestinationWithSources{
								Destination: *destination,
								Sources:     []backendconfig.SourceT{},
							}
						}
						destinationsMap[destination.ID].Sources = append(destinationsMap[destination.ID].Sources, *source)

						rt.destinationResponseHandler = NewResponseHandler(rt.logger, destination.DestinationDefinition.ResponseRules)
						if value, ok := destination.DestinationDefinition.Config["saveDestinationResponse"].(bool); ok {
							rt.saveDestinationResponse = value
						}

						// Config key "throttlingCost" is expected to have the eventType as the first key and the call type
						// as the second key (e.g. track, identify, etc...) or default to apply the cost to all call types:
						// dDT["config"]["throttlingCost"] = `{"eventType":{"default":1,"track":2,"identify":3}}`
						if value, ok := destination.DestinationDefinition.Config["throttlingCost"].(map[string]interface{}); ok {
							m := types.NewEventTypeThrottlingCost(value)
							rt.throttlingCosts.Store(&m)
						}
					}
				}
			}
			for connectionID := range wConfig.Connections {
				connection := wConfig.Connections[connectionID]
				if dest, ok := destinationsMap[connection.DestinationID]; ok &&
					dest.Destination.DestinationDefinition.Name == rt.destType {
					connectionsMap[types.SourceDest{
						SourceID:      connection.SourceID,
						DestinationID: connection.DestinationID,
					}] = types.ConnectionWithID{
						ConnectionID: connectionID,
						Connection:   connection,
					}
				}
			}
		}
		rt.destinationsMapMu.Lock()
		rt.connectionsMap = connectionsMap
		rt.destinationsMap = destinationsMap
		rt.destinationsMapMu.Unlock()
		if !rt.isBackendConfigInitialized {
			rt.isBackendConfigInitialized = true
			rt.backendConfigInitialized <- true
		}
	}
}
