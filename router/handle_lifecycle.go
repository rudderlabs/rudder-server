package router

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	customDestinationManager "github.com/rudderlabs/rudder-server/router/customdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/internal/partition"
	"github.com/rudderlabs/rudder-server/router/isolation"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
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
	debugger destinationdebugger.DestinationDebugger,
) {
	rt.backendConfig = backendConfig
	rt.debugger = debugger

	destType := destinationDefinition.Name
	rt.logger = log.Child(destType)
	rt.logger.Info("router setup: ", destType)

	rt.transientSources = transientSources
	rt.rsourcesService = rsourcesService

	// waiting for reporting client setup
	err := rt.Reporting.WaitForSetup(context.TODO(), utilTypes.CoreReportingClient)
	if err != nil {
		return
	}

	rt.jobsDB = jobsDB
	rt.errorDB = errorDB
	rt.destType = destType

	rt.reloadableConfig = &reloadableConfig{}
	config.RegisterDurationConfigVariable(90, &rt.reloadableConfig.jobsDBCommandTimeout, true, time.Second, []string{"JobsDB.Router.CommandRequestTimeout", "JobsDB.CommandRequestTimeout"}...)
	config.RegisterIntConfigVariable(2, &rt.reloadableConfig.jobdDBMaxRetries, true, 1, []string{"JobsDB." + "Router." + "MaxRetries", "JobsDB." + "MaxRetries"}...)
	config.RegisterIntConfigVariable(20, &rt.reloadableConfig.noOfJobsToBatchInAWorker, true, 1, []string{"Router." + rt.destType + "." + "noOfJobsToBatchInAWorker", "Router." + "noOfJobsToBatchInAWorker"}...)
	config.RegisterIntConfigVariable(3, &rt.reloadableConfig.maxFailedCountForJob, true, 1, []string{"Router." + rt.destType + "." + "maxFailedCountForJob", "Router." + "maxFailedCountForJob"}...)
	config.RegisterInt64ConfigVariable(100*bytesize.MB, &rt.reloadableConfig.payloadLimit, true, 1, []string{"Router." + rt.destType + "." + "PayloadLimit", "Router." + "PayloadLimit"}...)
	config.RegisterDurationConfigVariable(3600, &rt.reloadableConfig.routerTimeout, true, time.Second, []string{"Router." + rt.destType + "." + "routerTimeout", "Router." + "routerTimeout"}...)
	config.RegisterDurationConfigVariable(180, &rt.reloadableConfig.retryTimeWindow, true, time.Minute, []string{"Router." + rt.destType + "." + "retryTimeWindow", "Router." + rt.destType + "." + "retryTimeWindowInMins", "Router." + "retryTimeWindow", "Router." + "retryTimeWindowInMins"}...)
	config.RegisterIntConfigVariable(10, &rt.reloadableConfig.maxDSQuerySize, true, 1, []string{"Router." + rt.destType + "." + "maxDSQuery", "Router." + "maxDSQuery"}...)
	config.RegisterIntConfigVariable(50, &rt.reloadableConfig.jobIteratorMaxQueries, true, 1, "Router.jobIterator.maxQueries")
	config.RegisterIntConfigVariable(10, &rt.reloadableConfig.jobIteratorDiscardedPercentageTolerance, true, 1, "Router.jobIterator.discardedPercentageTolerance")
	config.RegisterBoolConfigVariable(false, &rt.reloadableConfig.savePayloadOnError, true, []string{"Router." + rt.destType + "." + "savePayloadOnError", "Router." + "savePayloadOnError"}...)
	config.RegisterBoolConfigVariable(false, &rt.reloadableConfig.transformerProxy, true, []string{"Router." + rt.destType + "." + "transformerProxy", "Router." + "transformerProxy"}...)
	config.RegisterBoolConfigVariable(false, &rt.reloadableConfig.skipRtAbortAlertForTransformation, true, []string{"Router." + rt.destType + "." + "skipRtAbortAlertForTf", "Router.skipRtAbortAlertForTf"}...)
	config.RegisterBoolConfigVariable(false, &rt.reloadableConfig.skipRtAbortAlertForDelivery, true, []string{"Router." + rt.destType + "." + "skipRtAbortAlertForDelivery", "Router.skipRtAbortAlertForDelivery"}...)
	config.RegisterIntConfigVariable(10000, &rt.reloadableConfig.jobQueryBatchSize, true, 1, "Router.jobQueryBatchSize")
	config.RegisterIntConfigVariable(1000, &rt.reloadableConfig.updateStatusBatchSize, true, 1, "Router.updateStatusBatchSize")
	config.RegisterDurationConfigVariable(1000, &rt.reloadableConfig.readSleep, true, time.Millisecond, []string{"Router.readSleep", "Router.readSleepInMS"}...)
	config.RegisterDurationConfigVariable(5, &rt.reloadableConfig.jobsBatchTimeout, true, time.Second, []string{"Router.jobsBatchTimeout", "Router.jobsBatchTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(5, &rt.reloadableConfig.maxStatusUpdateWait, true, time.Second, []string{"Router.maxStatusUpdateWait", "Router.maxStatusUpdateWaitInS"}...)
	config.RegisterDurationConfigVariable(10, &rt.reloadableConfig.minRetryBackoff, true, time.Second, []string{"Router.minRetryBackoff", "Router.minRetryBackoffInS"}...)
	config.RegisterDurationConfigVariable(300, &rt.reloadableConfig.maxRetryBackoff, true, time.Second, []string{"Router.maxRetryBackoff", "Router.maxRetryBackoffInS"}...)
	config.RegisterStringConfigVariable("", &rt.reloadableConfig.toAbortDestinationIDs, true, "Router.toAbortDestinationIDs")
	config.RegisterDurationConfigVariable(2, &rt.reloadableConfig.pickupFlushInterval, true, time.Second, "Router.pickupFlushInterval")
	config.RegisterDurationConfigVariable(2000, &rt.reloadableConfig.failingJobsPenaltySleep, true, time.Millisecond, []string{"Router.failingJobsPenaltySleep"}...)
	config.RegisterFloat64ConfigVariable(0.6, &rt.reloadableConfig.failingJobsPenaltyThreshold, true, []string{"Router.failingJobsPenaltyThreshold"}...)

	config.RegisterDurationConfigVariable(60, &rt.diagnosisTickerTime, false, time.Second, []string{"Diagnostics.routerTimePeriod", "Diagnostics.routerTimePeriodInS"}...)

	netClientTimeoutKeys := []string{"Router." + rt.destType + "." + "httpTimeout", "Router." + rt.destType + "." + "httpTimeoutInS", "Router." + "httpTimeout", "Router." + "httpTimeoutInS"}
	config.RegisterDurationConfigVariable(10, &rt.netClientTimeout, false, time.Second, netClientTimeoutKeys...)
	config.RegisterDurationConfigVariable(600, &rt.transformerTimeout, false, time.Second, "HttpClient.backendProxy.timeout", "HttpClient.routerTransformer.timeout")
	rt.crashRecover()
	rt.responseQ = make(chan workerJobStatus, rt.reloadableConfig.jobQueryBatchSize)
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

	config.RegisterBoolConfigVariable(false, &rt.enableBatching, false, "Router."+rt.destType+"."+"enableBatching")

	rt.drainConcurrencyLimit = getRouterConfigInt("drainedConcurrencyLimit", destType, 1)
	rt.barrierConcurrencyLimit = getRouterConfigInt("barrierConcurrencyLimit", destType, 100)

	statTags := stats.Tags{"destType": rt.destType}
	rt.batchInputCountStat = stats.Default.NewTaggedStat("router_batch_num_input_jobs", stats.CountType, statTags)
	rt.batchOutputCountStat = stats.Default.NewTaggedStat("router_batch_num_output_jobs", stats.CountType, statTags)
	rt.routerTransformInputCountStat = stats.Default.NewTaggedStat("router_transform_num_input_jobs", stats.CountType, statTags)
	rt.routerTransformOutputCountStat = stats.Default.NewTaggedStat("router_transform_num_output_jobs", stats.CountType, statTags)
	rt.batchInputOutputDiffCountStat = stats.Default.NewTaggedStat("router_batch_input_output_diff_jobs", stats.CountType, statTags)
	rt.routerResponseTransformStat = stats.Default.NewTaggedStat("response_transform_latency", stats.TimerType, statTags)
	rt.throttlingErrorStat = stats.Default.NewTaggedStat("router_throttling_error", stats.CountType, statTags)
	rt.throttledStat = stats.Default.NewTaggedStat("router_throttled", stats.CountType, statTags)

	rt.transformer = transformer.NewTransformer(rt.netClientTimeout, rt.transformerTimeout)

	rt.oauth = oauth.NewOAuthErrorHandler(backendConfig)

	rt.isBackendConfigInitialized = false
	rt.backendConfigInitialized = make(chan bool)

	isolationMode := isolationMode(destType, config)
	if rt.isolationStrategy, err = isolation.GetStrategy(isolationMode, rt.destType, func(destinationID string) bool {
		rt.destinationsMapMu.RLock()
		defer rt.destinationsMapMu.RUnlock()
		_, ok := rt.destinationsMap[destinationID]
		return ok
	}); err != nil {
		panic(fmt.Errorf("resolving isolation strategy for mode %q: %w", isolationMode, err))
	}

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

	g.Go(misc.WithBugsnag(func() error {
		limiterStats := func(key string, pstats *partition.Stats) {
			allPStats := pstats.All()
			for _, pstat := range allPStats {
				statTags := stats.Tags{
					"destType":  rt.destType,
					"partition": pstat.Partition,
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
	g.Go(misc.WithBugsnag(func() error {
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

	g.Go(misc.WithBugsnag(func() error {
		rt.collectMetrics(ctx)
		return nil
	}))

	g.Go(misc.WithBugsnag(func() error {
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

func (rt *Handle) Start() {
	rt.logger.Infof("Starting router: %s", rt.destType)
	rt.startEnded = make(chan struct{})
	ctx := rt.backgroundCtx

	rt.backgroundGroup.Go(misc.WithBugsnag(func() error {
		defer close(rt.startEnded) // always close the channel
		select {
		case <-ctx.Done():
			rt.logger.Infof("Router : %s start goroutine exited", rt.destType)
			return nil
		case <-rt.backendConfigInitialized:
			// no-op, just wait
		}
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
				mainLoopSleep = rt.reloadableConfig.readSleep
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

	<-rt.startEnded     // wait for all workers to stop first
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
			rt.reloadableConfig.updateStatusBatchSize,
			rt.reloadableConfig.maxStatusUpdateWait,
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
		}
		rt.destinationsMapMu.Lock()
		rt.destinationsMap = destinationsMap
		rt.destinationsMapMu.Unlock()
		if !rt.isBackendConfigInitialized {
			rt.isBackendConfigInitialized = true
			rt.backendConfigInitialized <- true
		}
	}
}
