package router

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonparser"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	customDestinationManager "github.com/rudderlabs/rudder-server/router/customdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/internal/eventorder"
	"github.com/rudderlabs/rudder-server/router/internal/jobiterator"
	"github.com/rudderlabs/rudder-server/router/internal/partition"
	"github.com/rudderlabs/rudder-server/router/isolation"
	"github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/diagnostics"

	"github.com/rudderlabs/rudder-server/services/rmetrics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	transformerFeaturesService "github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/misc"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
)

const module = "router"

// Handle is the handle to this module.
type Handle struct {
	// external dependencies
	jobsDB                     jobsdb.JobsDB
	throttlerFactory           throttler.Factory
	backendConfig              backendconfig.BackendConfig
	Reporting                  reporter
	transientSources           transientsource.Service
	rsourcesService            rsources.JobService
	transformerFeaturesService transformerFeaturesService.FeaturesService
	debugger                   destinationdebugger.DestinationDebugger
	pendingEventsRegistry      rmetrics.PendingEventsRegistry
	adaptiveLimit              func(int64) int64

	// configuration
	reloadableConfig                   *reloadableConfig
	destType                           string
	guaranteeUserEventOrder            bool
	netClientTimeout                   time.Duration
	transformerTimeout                 time.Duration
	enableBatching                     bool
	noOfWorkers                        int
	eventOrderKeyThreshold             config.ValueLoader[int]
	eventOrderDisabledStateDuration    config.ValueLoader[time.Duration]
	eventOrderHalfEnabledStateDuration config.ValueLoader[time.Duration]
	deliveryThrottlerTimeout           config.ValueLoader[time.Duration]
	drainConcurrencyLimit              config.ValueLoader[int]
	maxNoOfJobsPerChannel              int // maximum capacity of each worker channel (hard capacity limit of the underlying go channel)
	noOfJobsPerChannel                 int // requested capacity of each worker channel (important when job buffering is being calculated using the standard method)
	saveDestinationResponse            bool
	saveDestinationResponseOverride    config.ValueLoader[bool]
	reportJobsdbPayload                config.ValueLoader[bool]

	diagnosisTickerTime time.Duration

	// state

	logger                         logger.Logger
	tracer                         stats.Tracer
	destinationResponseHandler     ResponseHandler
	telemetry                      *Diagnostic
	netHandle                      NetHandle
	customDestinationManager       customDestinationManager.DestinationManager
	transformer                    transformer.Transformer
	isOAuthDestination             bool
	destinationsMapMu              sync.RWMutex
	destinationsMap                map[string]*routerutils.DestinationWithSources // destinationID -> destination
	connectionsMap                 map[types.SourceDest]types.ConnectionWithID
	isBackendConfigInitialized     bool
	backendConfigInitialized       chan bool
	responseQ                      chan workerJobStatus
	throttlingCosts                atomic.Pointer[types.EventTypeThrottlingCost]
	batchSizeHistogramStat         stats.Measurement
	batchInputCountStat            stats.Measurement
	batchOutputCountStat           stats.Measurement
	routerTransformInputCountStat  stats.Measurement
	routerTransformOutputCountStat stats.Measurement
	batchInputOutputDiffCountStat  stats.Measurement
	routerResponseTransformStat    stats.Measurement
	processRequestsHistogramStat   stats.Measurement
	processRequestsCountStat       stats.Measurement
	processJobsHistogramStat       stats.Measurement
	processJobsCountStat           stats.Measurement
	throttlingErrorStat            stats.Measurement
	throttledStat                  stats.Measurement
	isolationStrategy              isolation.Strategy
	backgroundGroup                *errgroup.Group
	backgroundCtx                  context.Context
	backgroundCancel               context.CancelFunc
	backgroundWait                 func() error
	startEnded                     chan struct{}
	barrier                        *eventorder.Barrier

	eventOrderingDisabledForWorkspace   func(workspaceID string) bool
	eventOrderingDisabledForDestination func(destinationID string) bool

	limiter struct {
		pickup    kitsync.Limiter
		transform kitsync.Limiter
		batch     kitsync.Limiter
		process   kitsync.Limiter
		stats     struct {
			pickup    *partition.Stats
			transform *partition.Stats
			batch     *partition.Stats
			process   *partition.Stats
		}
	}

	drainer routerutils.Drainer
}

// activePartitions returns the list of active partitions, depending on the active isolation strategy
func (rt *Handle) activePartitions(ctx context.Context) []string {
	statTags := map[string]string{"destType": rt.destType}
	defer stats.Default.NewTaggedStat("rt_active_partitions_time", stats.TimerType, statTags).RecordDuration()()
	keys, err := rt.isolationStrategy.ActivePartitions(ctx, rt.jobsDB)
	if err != nil && ctx.Err() == nil {
		panic(err)
	}
	stats.Default.NewTaggedStat("rt_active_partitions", stats.GaugeType, statTags).Gauge(len(keys))
	return keys
}

// pickup picks up jobs from the jobsDB for the provided partition and returns the number of jobs picked up and whether the limits were reached or not
// picked up jobs are distributed to the workers
func (rt *Handle) pickup(ctx context.Context, partition string, workers []*worker, pickupBatchSizeGauge Gauge[int]) (pickupCount int, limitsReached bool) {
	// pickup limiter with dynamic priority
	start := time.Now()
	var discardedCount int
	limiter := rt.limiter.pickup
	limiterStats := rt.limiter.stats.pickup
	limiterEnd := limiter.BeginWithPriority("", LimiterPriorityValueFrom(limiterStats.Score(partition), 100))
	defer limiterEnd()

	defer func() {
		limiterStats.Update(partition, time.Since(start), pickupCount+discardedCount, discardedCount)
	}()

	//#JobOrder (See comment marked #JobOrder
	if rt.guaranteeUserEventOrder {
		rt.barrier.Sync()
	}

	var firstJob *jobsdb.JobT
	var lastJob *jobsdb.JobT

	jobIteratorMaxQueries := config.GetIntVar(50, 1,
		"Router."+rt.destType+"."+partition+".jobIterator.maxQueries",
		"Router."+rt.destType+".jobIterator.maxQueries",
		"Router.jobIterator.maxQueries")
	jobIteratorDiscardedPercentageTolerance := config.GetIntVar(10, 1,
		"Router."+rt.destType+"."+partition+".jobIterator.discardedPercentageTolerance",
		"Router."+rt.destType+".jobIterator.discardedPercentageTolerance",
		"Router.jobIterator.discardedPercentageTolerance")

	jobQueryBatchSize := rt.reloadableConfig.jobQueryBatchSize.Load()
	if rt.isolationStrategy.SupportsPickupQueryThrottling() {
		jobQueryBatchSize = rt.getAdaptedJobQueryBatchSize(
			jobQueryBatchSize,
			func() []throttler.PickupThrottler {
				return rt.throttlerFactory.GetActivePickupThrottlers(partition)
			},
			rt.reloadableConfig.readSleep.Load(),
			rt.reloadableConfig.maxJobQueryBatchSize.Load(),
		)
	}
	pickupBatchSizeGauge.Gauge(jobQueryBatchSize)

	iterator := jobiterator.New(
		rt.getQueryParams(partition, jobQueryBatchSize),
		rt.getJobsFn(ctx),
		jobiterator.WithDiscardedPercentageTolerance(jobIteratorDiscardedPercentageTolerance),
		jobiterator.WithMaxQueries(jobIteratorMaxQueries),
	)

	if !iterator.HasNext() {
		rt.pipelineDelayStats(partition, nil, nil)
		rt.logger.Debugn("RT: DB Read Complete. No RT Jobs to process for destination", obskit.DestinationType(rt.destType))
		limiterEnd() // exit the limiter before sleeping
		_ = misc.SleepCtx(ctx, rt.reloadableConfig.readSleep.Load())
		return 0, false
	}

	type reservedJob struct {
		slot        *reservedSlot
		job         *jobsdb.JobT
		drainReason string
		parameters  routerutils.JobParameters
	}

	var statusList []*jobsdb.JobStatusT
	var reservedJobs []reservedJob
	blockedOrderKeys := make(map[eventorder.BarrierKey]struct{})

	flushTime := time.Now()
	shouldFlush := func() bool {
		return len(statusList) > 0 && time.Since(flushTime) > rt.reloadableConfig.pickupFlushInterval.Load()
	}
	flush := func() {
		flushTime = time.Now()
		// Mark the jobs as executing
		err := misc.RetryWithNotify(context.Background(), rt.reloadableConfig.jobsDBCommandTimeout.Load(), rt.reloadableConfig.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
			return rt.jobsDB.UpdateJobStatus(ctx, statusList, []string{rt.destType}, nil)
		}, rt.sendRetryUpdateStats)
		if err != nil {
			rt.logger.Errorn("Error occurred while marking jobs statuses as executing. Panicking", obskit.DestinationType(rt.destType), obskit.Error(err))
			panic(err)
		}

		rt.logger.Debugn("[DRAIN DEBUG] counts final jobs length being processed", obskit.DestinationType(rt.destType), logger.NewIntField("jobsLength", int64(len(reservedJobs))))
		assignedTime := time.Now()
		for _, reservedJob := range reservedJobs {
			reservedJob.slot.Use(workerJob{job: reservedJob.job, assignedAt: assignedTime, drainReason: reservedJob.drainReason, parameters: &reservedJob.parameters})
		}
		pickupCount += len(reservedJobs)
		reservedJobs = nil
		statusList = nil
	}

	traces := make(map[string]stats.TraceSpan)
	defer func() {
		for _, span := range traces {
			span.End()
		}
	}()

	// Identify jobs which can be processed
	var iterationInterrupted bool
	for iterator.HasNext() {
		if ctx.Err() != nil {
			return 0, false
		}
		job := iterator.Next()

		if firstJob == nil {
			firstJob = job
		}
		lastJob = job
		var parameters routerutils.JobParameters
		if err := jsonrs.Unmarshal(job.Parameters, &parameters); err != nil {
			rt.logger.Errorn("Error occurred while unmarshalling job parameters. Panicking", obskit.Error(err))
			panic(err)
		}
		workerJobSlot, err := rt.findWorkerSlot(ctx, workers, job, parameters, blockedOrderKeys)
		if err == nil {
			traceParent := jsonparser.GetStringOrEmpty(job.Parameters, "traceparent")
			if traceParent != "" {
				if _, ok := traces[traceParent]; !ok {
					ctx := stats.InjectTraceParentIntoContext(context.Background(), traceParent)
					_, span := rt.tracer.Start(ctx, "rt.pickup", stats.SpanKindConsumer, stats.SpanWithTags(stats.Tags{
						"workspaceId":   job.WorkspaceId,
						"sourceId":      parameters.SourceID,
						"destinationId": parameters.DestinationID,
						"destType":      rt.destType,
					}))
					traces[traceParent] = span
				}
			} else {
				rt.logger.Debugn("traceParent is empty during router pickup", logger.NewIntField("jobId", job.JobID))
			}

			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				JobState:      jobsdb.Executing.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "",
				ErrorResponse: routerutils.EmptyPayload, // check
				Parameters:    routerutils.EmptyPayload,
				JobParameters: job.Parameters,
				WorkspaceId:   job.WorkspaceId,
			}
			statusList = append(statusList, &status)
			reservedJobs = append(reservedJobs, reservedJob{slot: workerJobSlot.slot, job: job, drainReason: workerJobSlot.drainReason, parameters: parameters})
			if shouldFlush() {
				flush()
			}
		} else {
			discardedJobCountStat := stats.Default.NewTaggedStat("router_iterator_stats_discarded_job_count", stats.CountType, stats.Tags{"destType": rt.destType, "partition": partition, "reason": err.Error(), "workspaceId": job.WorkspaceId})
			discardedJobCountStat.Increment()
			iterator.Discard(job)
			discardedCount++
			if rt.stopIteration(err, parameters.DestinationID) {
				discarded := iterator.Stop() // stop the iterator and count all additional jobs discarded by operator by using the same reason as the last job that was discarded
				discardedJobCountStat.Count(discarded)
				iterationInterrupted = true
				break
			}
			if rt.isolationStrategy.StopQueries(err, parameters.DestinationID) {
				iterator.StopQueries()
			}
		}
	}

	flush()
	iteratorStats := iterator.Stats()
	stats.Default.NewTaggedStat("router_iterator_stats_query_count", stats.GaugeType, stats.Tags{"destType": rt.destType, "partition": partition}).Gauge(iteratorStats.QueryCount)
	stats.Default.NewTaggedStat("router_iterator_stats_total_jobs", stats.GaugeType, stats.Tags{"destType": rt.destType, "partition": partition}).Gauge(iteratorStats.TotalJobs)
	stats.Default.NewTaggedStat("router_iterator_stats_discarded_jobs", stats.GaugeType, stats.Tags{"destType": rt.destType, "partition": partition}).Gauge(iteratorStats.DiscardedJobs)

	// the following stat (in combination with the limiter's timer stats) is used to track the pickup loop's average latency  and max processing capacity
	stats.Default.NewTaggedStat("router_pickup_total_jobs_count", stats.CountType, stats.Tags{"destType": rt.destType, "partition": partition}).Count(iteratorStats.TotalJobs)

	rt.pipelineDelayStats(partition, firstJob, lastJob)
	limitsReached = iteratorStats.LimitsReached && !iterationInterrupted

	// sleep for a while if we are discarding too many jobs, so that we don't have a loop running continuously without producing events
	eligibleForFailingJobsPenalty := iteratorStats.LimitsReached || iterationInterrupted
	discardedRatio := float64(iteratorStats.DiscardedJobs) / float64(iteratorStats.TotalJobs)
	// If the discarded ratio is greater than the penalty threshold,
	// sleep for a while to avoid having a loop running continuously without producing events
	if eligibleForFailingJobsPenalty && discardedRatio > rt.reloadableConfig.failingJobsPenaltyThreshold.Load() {
		limiterEnd() // exit the limiter before sleeping
		_ = misc.SleepCtx(ctx, rt.reloadableConfig.failingJobsPenaltySleep.Load())
	}

	return pickupCount, limitsReached
}

// getAdaptedJobQueryBatchSize returns the adapted job query batch size based on the throttling limits
func (*Handle) getAdaptedJobQueryBatchSize(input int, pickupThrottlers func() []throttler.PickupThrottler, readSleep time.Duration, maxLimit int) int {
	jobQueryBatchSize := input
	readSleepSeconds := int((readSleep + time.Second - 1) / time.Second) // rounding up to the nearest second
	// Calculate the total limit of all active throttlers:
	//
	//   - if there is a global throttler, use its limit
	//   - if there are event type specific throttlers, use the sum of their limits
	totalLimit := lo.Reduce(pickupThrottlers(), func(totalLimit int, throttler throttler.PickupThrottler, index int) int {
		switch throttler.GetEventType() {
		case "all":
			// global throttler, total limit is the limit of the first throttler
			if index == 0 {
				return int(throttler.GetLimitPerSecond())
			}
			return totalLimit
		default:
			// throttler per event type, total limit is the sum of all recently used throttler per event type limits
			if int(time.Since(throttler.GetLastUsed()).Seconds()) <= readSleepSeconds*2 {
				return totalLimit + int(throttler.GetLimitPerSecond())
			}
			return totalLimit
		}
	}, 0)
	// If throttling is enabled then we need to adapt job query batch size:
	//
	//  - we assume that we will read for more than readSleep seconds (min 1 second)
	//  - we will be setting the batch size to be min(totalLimit * readSleepSeconds, maxLimit)
	if totalLimit > 0 {
		readSleepSeconds := int((readSleep + time.Second - 1) / time.Second) // rounding up to the nearest second
		jobQueryBatchSize = totalLimit * readSleepSeconds
		if jobQueryBatchSize > maxLimit {
			return maxLimit
		}
	}
	return jobQueryBatchSize
}

func (rt *Handle) stopIteration(err error, destinationID string) bool {
	// if the context is cancelled, we can stop iteration
	if errors.Is(err, types.ErrContextCancelled) {
		return true
	}
	// if we are not guaranteeing user event order, we can stop iteration if there are no more slots available
	if !rt.guaranteeUserEventOrder && errors.Is(err, types.ErrWorkerNoSlot) {
		return true
	}
	// delegate to the isolation strategy for the final decision
	return rt.isolationStrategy.StopIteration(err, destinationID)
}

// commitStatusList commits the status of the jobs to the jobsDB
func (rt *Handle) commitStatusList(workerJobStatuses *[]workerJobStatus) {
	reportMetrics := make([]*utilTypes.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*utilTypes.ConnectionDetails)
	transformedAtMap := make(map[string]string)
	statusDetailsMap := make(map[string]*utilTypes.StatusDetail)
	routerWorkspaceJobStatusCount := make(map[string]int)
	var completedJobsList []*jobsdb.JobT
	var statusList []*jobsdb.JobStatusT
	jobIDConnectionDetailsMap := make(map[int64]jobsdb.ConnectionDetails)
	for _, workerJobStatus := range *workerJobStatuses {
		parameters := workerJobStatus.parameters
		errorCode, _ := strconv.Atoi(workerJobStatus.status.ErrorCode)
		rt.throttlerFactory.GetPickupThrottler(rt.destType, parameters.DestinationID, workerJobStatus.parameters.EventType).ResponseCodeReceived(errorCode) // send response code to throttler
		// Update metrics maps
		// REPORTING - ROUTER - START
		workspaceID := workerJobStatus.status.WorkspaceId
		eventName := jsonparser.GetStringOrEmpty(workerJobStatus.job.Parameters, "event_name")
		eventType := jsonparser.GetStringOrEmpty(workerJobStatus.job.Parameters, "event_type")
		jobIDConnectionDetailsMap[workerJobStatus.job.JobID] = jobsdb.ConnectionDetails{
			SourceID:      parameters.SourceID,
			DestinationID: parameters.DestinationID,
		}
		key := parameters.SourceID + ":" + parameters.DestinationID + ":" + parameters.SourceJobRunID + ":" + workerJobStatus.status.JobState + ":" + workerJobStatus.status.ErrorCode + ":" + eventName + ":" + eventType
		_, ok := connectionDetailsMap[key]
		if !ok {
			cd := &utilTypes.ConnectionDetails{
				SourceID:                parameters.SourceID,
				DestinationID:           parameters.DestinationID,
				SourceTaskRunID:         parameters.SourceTaskRunID,
				SourceJobID:             parameters.SourceJobID,
				SourceJobRunID:          parameters.SourceJobRunID,
				SourceDefinitionID:      parameters.SourceDefinitionID,
				DestinationDefinitionID: parameters.DestinationDefinitionID,
				SourceCategory:          parameters.SourceCategory,
			}
			connectionDetailsMap[key] = cd
			transformedAtMap[key] = parameters.TransformAt
		}
		sd, ok := statusDetailsMap[key]
		if !ok {
			sampleEvent := workerJobStatus.payload
			if rt.transientSources.Apply(parameters.SourceID) {
				sampleEvent = routerutils.EmptyPayload
			}
			sd = &utilTypes.StatusDetail{
				Status:         workerJobStatus.status.JobState,
				StatusCode:     errorCode,
				SampleResponse: string(workerJobStatus.status.ErrorResponse),
				SampleEvent:    sampleEvent,
				EventName:      eventName,
				EventType:      eventType,
				StatTags:       workerJobStatus.statTags,
			}
			statusDetailsMap[key] = sd
		}

		switch workerJobStatus.status.JobState {
		case jobsdb.Failed.State:
			if workerJobStatus.status.ErrorCode != strconv.Itoa(types.RouterUnMarshalErrorCode) {
				if workerJobStatus.status.AttemptNum == 1 {
					sd.Count++
				}
			}
		case jobsdb.Succeeded.State, jobsdb.Filtered.State:
			routerWorkspaceJobStatusCount[workspaceID]++
			sd.Count++
			completedJobsList = append(completedJobsList, workerJobStatus.job)
		case jobsdb.Aborted.State:
			routerWorkspaceJobStatusCount[workspaceID]++
			sd.Count++
			sd.FailedMessages = append(sd.FailedMessages, &utilTypes.FailedMessage{MessageID: parameters.MessageID, ReceivedAt: parameters.ParseReceivedAtTime()})
			completedJobsList = append(completedJobsList, workerJobStatus.job)
		}

		// REPORTING - ROUTER - END

		statusList = append(statusList, workerJobStatus.status)

		// tracking router errors
		if diagnostics.EnableDestinationFailuresMetric {
			if workerJobStatus.status.JobState == jobsdb.Failed.State || workerJobStatus.status.JobState == jobsdb.Aborted.State {
				var event string
				if workerJobStatus.status.JobState == jobsdb.Failed.State {
					event = diagnostics.RouterFailed
				} else {
					event = diagnostics.RouterAborted
				}

				rt.telemetry.failureMetricLock.Lock()
				if _, ok := rt.telemetry.failuresMetric[event][string(workerJobStatus.status.ErrorResponse)]; !ok {
					rt.telemetry.failuresMetric[event] = make(map[string]int)
				}
				rt.telemetry.failuresMetric[event][string(workerJobStatus.status.ErrorResponse)] += 1
				rt.telemetry.failureMetricLock.Unlock()
			}
		}
	}

	// REPORTING - ROUTER - START
	utilTypes.AssertSameKeys(connectionDetailsMap, statusDetailsMap)
	for k, cd := range connectionDetailsMap {
		var inPu string
		if transformedAtMap[k] == "processor" {
			inPu = utilTypes.DEST_TRANSFORMER
		} else {
			inPu = utilTypes.EVENT_FILTER
		}
		m := &utilTypes.PUReportedMetric{
			ConnectionDetails: *cd,
			PUDetails:         *utilTypes.CreatePUDetails(inPu, utilTypes.ROUTER, true, false),
			StatusDetail:      statusDetailsMap[k],
		}
		if m.StatusDetail.Count != 0 {
			reportMetrics = append(reportMetrics, m)
		}
	}
	// REPORTING - ROUTER - END

	if len(statusList) > 0 {
		rt.logger.Debugn("[Router] :: flushing batch of status", obskit.DestinationType(rt.destType), logger.NewIntField("batchSize", int64(rt.reloadableConfig.updateStatusBatchSize.Load())))

		sort.Slice(statusList, func(i, j int) bool {
			return statusList[i].JobID < statusList[j].JobID
		})
		// Update the status
		err := misc.RetryWithNotify(context.Background(), rt.reloadableConfig.jobsDBCommandTimeout.Load(), rt.reloadableConfig.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
			return rt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
				err := rt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{rt.destType}, nil)
				if err != nil {
					return fmt.Errorf("updating %s jobs statuses: %w", rt.destType, err)
				}

				// rsources stats
				err = rt.updateRudderSourcesStats(ctx, tx, completedJobsList, statusList)
				if err != nil {
					return err
				}
				if err = rt.Reporting.Report(ctx, reportMetrics, tx.Tx()); err != nil {
					return fmt.Errorf("reporting metrics: %w", err)
				}
				return nil
			})
		}, rt.sendRetryStoreStats)
		if err != nil {
			panic(err)
		}
		routerutils.UpdateProcessedEventsMetrics(stats.Default, module, rt.destType, statusList, jobIDConnectionDetailsMap)
		for workspace, jobCount := range routerWorkspaceJobStatusCount {
			rt.pendingEventsRegistry.DecreasePendingEvents("rt", workspace, rt.destType, float64(jobCount))
		}
	}

	if rt.guaranteeUserEventOrder {
		//#JobOrder (see other #JobOrder comment)
		for _, resp := range *workerJobStatuses {
			status := resp.status.JobState
			userID := resp.userID
			worker := resp.worker
			if status != jobsdb.Failed.State {
				orderKey := eventorder.BarrierKey{
					UserID:        userID,
					DestinationID: jsonparser.GetStringOrEmpty(resp.job.Parameters, "destination_id"),
					WorkspaceID:   resp.job.WorkspaceId,
				}
				rt.logger.Debugn("EventOrder", logger.NewIntField("workerID", int64(worker.id)), logger.NewIntField("jobID", resp.status.JobID), logger.NewStringField("key", orderKey.String()), logger.NewStringField("jobState", status))
				if err := worker.barrier.StateChanged(orderKey, resp.status.JobID, status); err != nil {
					panic(err)
				}
			}
		}
		// End #JobOrder
	}
}

func (rt *Handle) getJobsFn(parentContext context.Context) func(context.Context, jobsdb.GetQueryParams, jobsdb.MoreToken) (*jobsdb.MoreJobsResult, error) {
	return func(ctx context.Context, params jobsdb.GetQueryParams, resumeFrom jobsdb.MoreToken) (*jobsdb.MoreJobsResult, error) {
		jobs, err := misc.QueryWithRetriesAndNotify(parentContext, rt.reloadableConfig.jobsDBCommandTimeout.Load(), rt.reloadableConfig.jobdDBMaxRetries.Load(), func(ctx context.Context) (*jobsdb.MoreJobsResult, error) {
			return rt.jobsDB.GetToProcess(
				ctx,
				params,
				resumeFrom,
			)
		}, rt.sendQueryRetryStats)
		if err != nil && parentContext.Err() != nil { // parentContext.Err() != nil means we are shutting down
			return &jobsdb.MoreJobsResult{}, nil //nolint:nilerr
		}
		return jobs, err
	}
}

func (rt *Handle) getQueryParams(partition string, pickUpCount int) jobsdb.GetQueryParams {
	params := jobsdb.GetQueryParams{
		CustomValFilters: []string{rt.destType},
		PayloadSizeLimit: rt.adaptiveLimit(rt.reloadableConfig.payloadLimit.Load()),
		JobsLimit:        pickUpCount,
	}
	rt.isolationStrategy.AugmentQueryParams(partition, &params)
	return params
}

type workerJobSlot struct {
	slot        *reservedSlot
	drainReason string
}

func (rt *Handle) findWorkerSlot(ctx context.Context, workers []*worker, job *jobsdb.JobT, parameters routerutils.JobParameters, blockedOrderKeys map[eventorder.BarrierKey]struct{}) (*workerJobSlot, error) {
	if rt.backgroundCtx.Err() != nil {
		return nil, types.ErrContextCancelled
	}
	orderKey := eventorder.BarrierKey{
		UserID:        job.UserID,
		DestinationID: parameters.DestinationID,
		WorkspaceID:   job.WorkspaceId,
	}

	eventOrderingDisabled := !rt.guaranteeUserEventOrder
	if (rt.guaranteeUserEventOrder && rt.barrier.Disabled(orderKey)) ||
		(rt.eventOrderingDisabledForWorkspace(job.WorkspaceId) ||
			rt.eventOrderingDisabledForDestination(parameters.DestinationID)) {
		eventOrderingDisabled = true
		stats.Default.NewTaggedStat("router_eventorder_key_disabled", stats.CountType, stats.Tags{
			"destType":      rt.destType,
			"destinationId": parameters.DestinationID,
			"workspaceID":   job.WorkspaceId,
		}).Increment()
	}
	abortedJob, abortReason := rt.drainOrRetryLimitReached(job.CreatedAt, parameters.DestinationID, parameters.SourceJobRunID, &job.LastJobStatus) // if job's aborted, then send it to its worker right away
	if eventOrderingDisabled {
		availableWorkers := lo.Filter(workers, func(w *worker, _ int) bool { return w.AvailableSlots() > 0 })
		if len(availableWorkers) == 0 {
			return nil, types.ErrWorkerNoSlot
		}
		slot := availableWorkers[rand.Intn(len(availableWorkers))].ReserveSlot()
		if slot == nil {
			return nil, types.ErrWorkerNoSlot
		}
		if abortedJob {
			return &workerJobSlot{slot: slot, drainReason: abortReason}, nil
		}
		if rt.shouldBackoff(job) {
			slot.Release()
			return nil, types.ErrJobBackoff
		}
		if rt.shouldThrottle(ctx, job, parameters.DestinationID, parameters.EventType) {
			slot.Release()
			return nil, types.ErrDestinationThrottled
		}

		return &workerJobSlot{slot: slot}, nil
	}

	// checking if the orderKey is in blockedOrderKeys. If yes, returning nil.
	// this check is done to maintain order.
	if _, ok := blockedOrderKeys[orderKey]; ok {
		rt.logger.Debugn("[Router] :: Skipping processing of job as orderKey has earlier jobs in throttled map", obskit.DestinationType(rt.destType), logger.NewIntField("jobID", job.JobID), logger.NewStringField("orderKey", orderKey.String()))
		return nil, types.ErrJobOrderBlocked
	}

	//#JobOrder (see other #JobOrder comment)
	if !abortedJob && rt.shouldBackoff(job) { // backoff
		blockedOrderKeys[orderKey] = struct{}{}
		return nil, types.ErrJobBackoff
	}
	worker := workers[getWorkerPartition(orderKey, len(workers))]
	slot := worker.ReserveSlot()
	if slot == nil {
		blockedOrderKeys[orderKey] = struct{}{}
		return nil, types.ErrWorkerNoSlot
	}

	if enter, previousFailedJobID := worker.barrier.Enter(orderKey, job.JobID); !enter {
		previousFailedJobIDStr := "<nil>"
		if previousFailedJobID != nil {
			previousFailedJobIDStr = strconv.FormatInt(*previousFailedJobID, 10)
		}
		rt.logger.Debugn("EventOrder: job is blocked", logger.NewIntField("jobID", job.JobID), logger.NewStringField("orderKey", orderKey.String()), logger.NewStringField("previousFailedJobID", previousFailedJobIDStr))
		slot.Release()
		blockedOrderKeys[orderKey] = struct{}{}
		return nil, types.ErrBarrierExists
	}
	rt.logger.Debugn("EventOrder: job is allowed to be processed", logger.NewIntField("jobID", job.JobID), logger.NewStringField("orderKey", orderKey.String()))
	if !abortedJob && rt.shouldThrottle(ctx, job, parameters.DestinationID, parameters.EventType) {
		blockedOrderKeys[orderKey] = struct{}{}
		worker.barrier.Leave(orderKey, job.JobID)
		slot.Release()
		return nil, types.ErrDestinationThrottled
	}
	return &workerJobSlot{slot: slot, drainReason: abortReason}, nil
	//#EndJobOrder
}

// checks if job is configured to drain or if it's retry limit is reached
func (rt *Handle) drainOrRetryLimitReached(createdAt time.Time, destID, sourceJobRunID string, jobStatus *jobsdb.JobStatusT) (bool, string) {
	drain, reason := rt.drainer.Drain(createdAt, destID, sourceJobRunID)
	if drain {
		return true, reason
	}
	retryLimitReached := rt.retryLimitReached(jobStatus)
	if retryLimitReached {
		return true, "retry limit reached"
	}
	return false, ""
}

func (rt *Handle) retryLimitReached(status *jobsdb.JobStatusT) bool {
	respStatusCode, _ := strconv.Atoi(status.ErrorCode)
	switch respStatusCode {
	case types.RouterUnMarshalErrorCode: // 5xx errors
		return false
	}

	if respStatusCode < http.StatusInternalServerError {
		return false
	}

	firstAttemptedAtTime := time.Now()
	if firstAttemptedAt := jsonparser.GetStringOrEmpty(status.ErrorResponse, "firstAttemptedAt"); firstAttemptedAt != "" {
		if t, err := time.Parse(misc.RFC3339Milli, firstAttemptedAt); err == nil {
			firstAttemptedAtTime = t
		}
	}

	maxFailedCountForJob := rt.reloadableConfig.maxFailedCountForJob.Load()
	retryTimeWindow := rt.reloadableConfig.retryTimeWindow.Load()
	if jsonparser.GetStringOrEmpty(status.JobParameters, "source_job_run_id") != "" {
		maxFailedCountForJob = rt.reloadableConfig.maxFailedCountForSourcesJob.Load()
		retryTimeWindow = rt.reloadableConfig.sourcesRetryTimeWindow.Load()
	}

	return time.Since(firstAttemptedAtTime) > retryTimeWindow &&
		status.AttemptNum >= maxFailedCountForJob // retry time window exceeded
}

func (*Handle) shouldBackoff(job *jobsdb.JobT) bool {
	return job.LastJobStatus.JobState == jobsdb.Failed.State && job.LastJobStatus.AttemptNum > 0 && time.Until(job.LastJobStatus.RetryTime) > 0
}

func (rt *Handle) shouldThrottle(ctx context.Context, job *jobsdb.JobT, destinationID, eventType string) (limited bool) {
	if rt.throttlerFactory == nil {
		// throttlerFactory could be nil when throttling is disabled or misconfigured.
		// in case of misconfiguration, logging errors are emitted.
		rt.logger.Debugn("[Router] :: ThrottlerFactory is nil. Not throttling destination", obskit.DestinationType(rt.destType), obskit.DestinationID(destinationID))
		return false
	}

	throttler := rt.throttlerFactory.GetPickupThrottler(rt.destType, destinationID, eventType)
	throttlingCost := rt.getThrottlingCost(job, eventType)

	limited, err := throttler.CheckLimitReached(ctx, throttlingCost)
	if err != nil {
		// we can't throttle, let's hit the destination, worst case we get a 429
		rt.throttlingErrorStat.Count(1)
		rt.logger.Errorn("[Router] :: Throttler error", obskit.DestinationType(rt.destType), obskit.Error(err))
		return false
	}
	if limited {
		rt.throttledStat.Count(1)
		rt.logger.Debugn("[Router] :: Skipping processing of job as throttled limits exceeded", obskit.DestinationType(rt.destType), logger.NewIntField("jobID", job.JobID), logger.NewStringField("userID", job.UserID))
	}

	return limited
}

func (rt *Handle) getThrottlingCost(job *jobsdb.JobT, eventType string) (cost int64) {
	cost = 1
	if tc := rt.throttlingCosts.Load(); tc != nil {
		cost = tc.Cost(eventType)
	}

	return cost * int64(job.EventCount)
}

func (*Handle) crashRecover() {
	// NO-OP
}
