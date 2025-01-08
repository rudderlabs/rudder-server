package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	customDestinationManager "github.com/rudderlabs/rudder-server/router/customdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/internal/eventorder"
	"github.com/rudderlabs/rudder-server/router/internal/jobiterator"
	"github.com/rudderlabs/rudder-server/router/internal/partition"
	"github.com/rudderlabs/rudder-server/router/isolation"
	rtThrottler "github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/oauth"
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
	errorDB                    jobsdb.JobsDB
	throttlerFactory           rtThrottler.Factory
	backendConfig              backendconfig.BackendConfig
	Reporting                  reporter
	transientSources           transientsource.Service
	rsourcesService            rsources.JobService
	transformerFeaturesService transformerFeaturesService.FeaturesService
	debugger                   destinationdebugger.DestinationDebugger
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
	drainConcurrencyLimit              config.ValueLoader[int]
	workerInputBufferSize              int
	saveDestinationResponse            bool
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
	oauth                          oauth.Authorizer
	destinationsMapMu              sync.RWMutex
	destinationsMap                map[string]*routerutils.DestinationWithSources // destinationID -> destination
	connectionsMap                 map[types.SourceDest]types.ConnectionWithID
	isBackendConfigInitialized     bool
	backendConfigInitialized       chan bool
	responseQ                      chan workerJobStatus
	throttlingCosts                atomic.Pointer[types.EventTypeThrottlingCost]
	batchInputCountStat            stats.Measurement
	batchOutputCountStat           stats.Measurement
	routerTransformInputCountStat  stats.Measurement
	routerTransformOutputCountStat stats.Measurement
	batchInputOutputDiffCountStat  stats.Measurement
	routerResponseTransformStat    stats.Measurement
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
func (rt *Handle) pickup(ctx context.Context, partition string, workers []*worker) (pickupCount int, limitsReached bool) {
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

	iterator := jobiterator.New(
		rt.getQueryParams(partition, rt.reloadableConfig.jobQueryBatchSize.Load()),
		rt.getJobsFn(ctx),
		jobiterator.WithDiscardedPercentageTolerance(jobIteratorDiscardedPercentageTolerance),
		jobiterator.WithMaxQueries(jobIteratorMaxQueries),
	)

	if !iterator.HasNext() {
		rt.pipelineDelayStats(partition, nil, nil)
		rt.logger.Debugf("RT: DB Read Complete. No RT Jobs to process for destination: %s", rt.destType)
		limiterEnd() // exit the limiter before sleeping
		_ = misc.SleepCtx(ctx, rt.reloadableConfig.readSleep.Load())
		return 0, false
	}

	type reservedJob struct {
		slot        *workerSlot
		job         *jobsdb.JobT
		drainReason string
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
			rt.logger.Errorf("Error occurred while marking %s jobs statuses as executing. Panicking. Err: %v", rt.destType, err)
			panic(err)
		}

		rt.logger.Debugf("[DRAIN DEBUG] counts  %v final jobs length being processed %v", rt.destType, len(reservedJobs))
		assignedTime := time.Now()
		for _, reservedJob := range reservedJobs {
			reservedJob.slot.Use(workerJob{job: reservedJob.job, assignedAt: assignedTime, drainReason: reservedJob.drainReason})
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
		workerJobSlot, err := rt.findWorkerSlot(ctx, workers, job, blockedOrderKeys)
		if err == nil {
			traceParent := gjson.GetBytes(job.Parameters, "traceparent").String()
			if traceParent != "" {
				if _, ok := traces[traceParent]; !ok {
					ctx := stats.InjectTraceParentIntoContext(context.Background(), traceParent)
					_, span := rt.tracer.Start(ctx, "rt.pickup", stats.SpanKindConsumer, stats.SpanWithTags(stats.Tags{
						"workspaceId":   job.WorkspaceId,
						"sourceId":      gjson.GetBytes(job.Parameters, "source_id").String(),
						"destinationId": gjson.GetBytes(job.Parameters, "destination_id").String(),
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
			reservedJobs = append(reservedJobs, reservedJob{slot: workerJobSlot.slot, job: job, drainReason: workerJobSlot.drainReason})
			if shouldFlush() {
				flush()
			}
		} else {
			stats.Default.NewTaggedStat("router_iterator_stats_discarded_job_count", stats.CountType, stats.Tags{"destType": rt.destType, "partition": partition, "reason": err.Error(), "workspaceId": job.WorkspaceId}).Increment()
			iterator.Discard(job)
			discardedCount++
			if rt.stopIteration(err) {
				iterationInterrupted = true
				break
			}
		}
	}
	iteratorStats := iterator.Stats()
	stats.Default.NewTaggedStat("router_iterator_stats_query_count", stats.GaugeType, stats.Tags{"destType": rt.destType, "partition": partition}).Gauge(iteratorStats.QueryCount)
	stats.Default.NewTaggedStat("router_iterator_stats_total_jobs", stats.GaugeType, stats.Tags{"destType": rt.destType, "partition": partition}).Gauge(iteratorStats.TotalJobs)
	stats.Default.NewTaggedStat("router_iterator_stats_discarded_jobs", stats.GaugeType, stats.Tags{"destType": rt.destType, "partition": partition}).Gauge(iteratorStats.DiscardedJobs)

	flush()
	rt.pipelineDelayStats(partition, firstJob, lastJob)
	limitsReached = iteratorStats.LimitsReached && !iterationInterrupted
	eligibleForFailingJobsPenalty := iteratorStats.LimitsReached || iterationInterrupted
	discardedRatio := float64(iteratorStats.DiscardedJobs) / float64(iteratorStats.TotalJobs)
	// If the discarded ratio is greater than the penalty threshold,
	// sleep for a while to avoid having a loop running continuously without producing events
	if eligibleForFailingJobsPenalty && discardedRatio > rt.reloadableConfig.failingJobsPenaltyThreshold.Load() {
		limiterEnd() // exit the limiter before sleeping
		_ = misc.SleepCtx(ctx, rt.reloadableConfig.failingJobsPenaltySleep.Load())
	}

	return
}

func (rt *Handle) stopIteration(err error) bool {
	// if the context is cancelled, we can stop iteration
	if errors.Is(err, types.ErrContextCancelled) {
		return true
	}
	// if we are not guaranteeing user event order, we can stop iteration if there are no more slots available
	if !rt.guaranteeUserEventOrder && errors.Is(err, types.ErrWorkerNoSlot) {
		return true
	}
	// delegate to the isolation strategy for the final decision
	return rt.isolationStrategy.StopIteration(err)
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
	var routerAbortedJobs []*jobsdb.JobT
	jobIDConnectionDetailsMap := make(map[int64]jobsdb.ConnectionDetails)
	for _, workerJobStatus := range *workerJobStatuses {
		var parameters routerutils.JobParameters
		err := json.Unmarshal(workerJobStatus.job.Parameters, &parameters)
		if err != nil {
			rt.logger.Error("Unmarshal of job parameters failed. ", string(workerJobStatus.job.Parameters))
		}
		errorCode, _ := strconv.Atoi(workerJobStatus.status.ErrorCode)
		rt.throttlerFactory.Get(rt.destType, parameters.DestinationID).ResponseCodeReceived(errorCode) // send response code to throttler
		// Update metrics maps
		// REPORTING - ROUTER - START
		workspaceID := workerJobStatus.status.WorkspaceId
		eventName := gjson.GetBytes(workerJobStatus.job.Parameters, "event_name").String()
		eventType := gjson.GetBytes(workerJobStatus.job.Parameters, "event_type").String()
		jobIDConnectionDetailsMap[workerJobStatus.job.JobID] = jobsdb.ConnectionDetails{
			SourceID:      parameters.SourceID,
			DestinationID: parameters.DestinationID,
		}
		key := fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s", parameters.SourceID, parameters.DestinationID, parameters.SourceJobRunID, workerJobStatus.status.JobState, workerJobStatus.status.ErrorCode, eventName, eventType)
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
			routerAbortedJobs = append(routerAbortedJobs, workerJobStatus.job)
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
		rt.logger.Debugf("[%v Router] :: flushing batch of %v status", rt.destType, rt.reloadableConfig.updateStatusBatchSize)

		sort.Slice(statusList, func(i, j int) bool {
			return statusList[i].JobID < statusList[j].JobID
		})
		// Store the aborted jobs to errorDB
		if routerAbortedJobs != nil {
			err := misc.RetryWithNotify(context.Background(), rt.reloadableConfig.jobsDBCommandTimeout.Load(), rt.reloadableConfig.jobdDBMaxRetries.Load(), func(ctx context.Context) error {
				return rt.errorDB.Store(ctx, routerAbortedJobs)
			}, rt.sendRetryStoreStats)
			if err != nil {
				panic(fmt.Errorf("storing jobs into ErrorDB: %w", err))
			}
		}
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
			rmetrics.DecreasePendingEvents(
				"rt",
				workspace,
				rt.destType,
				float64(jobCount),
			)
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
					DestinationID: gjson.GetBytes(resp.job.Parameters, "destination_id").String(),
					WorkspaceID:   resp.job.WorkspaceId,
				}
				rt.logger.Debugw(
					"EventOrder",
					"worker#", worker.id,
					"jobID", resp.status.JobID,
					"key", orderKey.String(),
					"jobState", status,
				)
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
	slot        *workerSlot
	drainReason string
}

func (rt *Handle) findWorkerSlot(ctx context.Context, workers []*worker, job *jobsdb.JobT, blockedOrderKeys map[eventorder.BarrierKey]struct{}) (*workerJobSlot, error) {
	if rt.backgroundCtx.Err() != nil {
		return nil, types.ErrContextCancelled
	}

	var parameters routerutils.JobParameters
	if err := json.Unmarshal(job.Parameters, &parameters); err != nil {
		rt.logger.Errorf(`[%v Router] :: Unmarshalling parameters failed with the error %v . Returning nil worker`, err)
		return nil, types.ErrParamsUnmarshal
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
	abortedJob, abortReason := rt.drainOrRetryLimitReached(job) // if job's aborted, then send it to its worker right away
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
		if rt.shouldThrottle(ctx, job, parameters) {
			slot.Release()
			return nil, types.ErrDestinationThrottled
		}

		return &workerJobSlot{slot: slot}, nil
	}

	// checking if the orderKey is in blockedOrderKeys. If yes, returning nil.
	// this check is done to maintain order.
	if _, ok := blockedOrderKeys[orderKey]; ok {
		rt.logger.Debugf(`[%v Router] :: Skipping processing of job:%d of orderKey:%s as orderKey has earlier jobs in throttled map`, rt.destType, job.JobID, orderKey)
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
		rt.logger.Debugf("EventOrder: job %d of orderKey %s is blocked (previousFailedJobID: %s)", job.JobID, orderKey, previousFailedJobIDStr)
		slot.Release()
		blockedOrderKeys[orderKey] = struct{}{}
		return nil, types.ErrBarrierExists
	}
	rt.logger.Debugf("EventOrder: job %d of orderKey %s is allowed to be processed", job.JobID, orderKey)
	if !abortedJob && rt.shouldThrottle(ctx, job, parameters) {
		blockedOrderKeys[orderKey] = struct{}{}
		worker.barrier.Leave(orderKey, job.JobID)
		slot.Release()
		return nil, types.ErrDestinationThrottled
	}
	return &workerJobSlot{slot: slot, drainReason: abortReason}, nil
	//#EndJobOrder
}

// checks if job is configured to drain or if it's retry limit is reached
func (rt *Handle) drainOrRetryLimitReached(job *jobsdb.JobT) (bool, string) {
	drain, reason := rt.drainer.Drain(job)
	if drain {
		return true, reason
	}
	retryLimitReached := rt.retryLimitReached(&job.LastJobStatus)
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
	if firstAttemptedAt := gjson.GetBytes(status.ErrorResponse, "firstAttemptedAt").Str; firstAttemptedAt != "" {
		if t, err := time.Parse(misc.RFC3339Milli, firstAttemptedAt); err == nil {
			firstAttemptedAtTime = t
		}
	}

	maxFailedCountForJob := rt.reloadableConfig.maxFailedCountForJob.Load()
	retryTimeWindow := rt.reloadableConfig.retryTimeWindow.Load()
	if gjson.GetBytes(status.JobParameters, "source_job_run_id").Str != "" {
		maxFailedCountForJob = rt.reloadableConfig.maxFailedCountForSourcesJob.Load()
		retryTimeWindow = rt.reloadableConfig.sourcesRetryTimeWindow.Load()
	}

	return time.Since(firstAttemptedAtTime) > retryTimeWindow &&
		status.AttemptNum >= maxFailedCountForJob // retry time window exceeded
}

func (*Handle) shouldBackoff(job *jobsdb.JobT) bool {
	return job.LastJobStatus.JobState == jobsdb.Failed.State && job.LastJobStatus.AttemptNum > 0 && time.Until(job.LastJobStatus.RetryTime) > 0
}

func (rt *Handle) shouldThrottle(ctx context.Context, job *jobsdb.JobT, parameters routerutils.JobParameters) (limited bool) {
	if rt.throttlerFactory == nil {
		// throttlerFactory could be nil when throttling is disabled or misconfigured.
		// in case of misconfiguration, logging errors are emitted.
		rt.logger.Debugf(`[%v Router] :: ThrottlerFactory is nil. Not throttling destination with ID %s`,
			rt.destType, parameters.DestinationID,
		)
		return false
	}

	throttler := rt.throttlerFactory.Get(rt.destType, parameters.DestinationID)
	throttlingCost := rt.getThrottlingCost(job)

	limited, err := throttler.CheckLimitReached(ctx, parameters.DestinationID, throttlingCost)
	if err != nil {
		// we can't throttle, let's hit the destination, worst case we get a 429
		rt.throttlingErrorStat.Count(1)
		rt.logger.Errorf(`[%v Router] :: Throttler error: %v`, rt.destType, err)
		return false
	}
	if limited {
		rt.throttledStat.Count(1)
		rt.logger.Debugf(
			"[%v Router] :: Skipping processing of job:%d of user:%s as throttled limits exceeded",
			rt.destType, job.JobID, job.UserID,
		)
	}

	return limited
}

func (rt *Handle) getThrottlingCost(job *jobsdb.JobT) (cost int64) {
	cost = 1
	if tc := rt.throttlingCosts.Load(); tc != nil {
		eventType := gjson.GetBytes(job.Parameters, "event_type").String()
		cost = tc.Cost(eventType)
	}

	return cost * int64(job.EventCount)
}

func (*Handle) crashRecover() {
	// NO-OP
}

func (rt *Handle) handleOAuthDestResponse(params *HandleDestOAuthRespParams, authErrorCategory string) (int, string, string) {
	trRespStatusCode := params.trRespStCd
	trRespBody := params.trRespBody
	destinationJob := params.destinationJob

	workspaceID := destinationJob.JobMetadataArray[0].WorkspaceID
	// Check the category
	// Trigger the refresh endpoint/disable endpoint
	rudderAccountID := oauth.GetAccountId(destinationJob.Destination.Config, oauth.DeliveryAccountIdKey)
	if strings.TrimSpace(rudderAccountID) == "" {
		return trRespStatusCode, trRespBody, params.contentType
	}
	switch authErrorCategory {
	case oauth.AUTH_STATUS_INACTIVE:
		authStatusStCd := rt.updateAuthStatusToInactive(&destinationJob.Destination, workspaceID, rudderAccountID)
		authStatusMsg := gjson.Get(trRespBody, "message").Raw
		return authStatusStCd, authStatusMsg, "text/plain; charset=utf-8"
	case oauth.REFRESH_TOKEN:
		refTokenParams := &oauth.RefreshTokenParams{
			Secret:      params.secret,
			WorkspaceId: workspaceID,
			AccountId:   rudderAccountID,
			DestDefName: destinationJob.Destination.DestinationDefinition.Name,
			WorkerId:    params.workerID,
		}
		errCatStatusCode, refSecret := rt.oauth.RefreshToken(refTokenParams)
		if routerutils.IsNotEmptyString(refSecret.Err) && refSecret.Err == oauth.REF_TOKEN_INVALID_GRANT {
			// In-case the refresh token has been revoked, this error comes in
			// Even trying to refresh the token also doesn't work here. Hence, this would be more ideal to Abort Events
			// As well as to disable destination as well.
			// Alert the user in this error as well, to check if the refresh token also has been revoked & fix it
			authStatusInactiveStCode := rt.updateAuthStatusToInactive(&destinationJob.Destination, workspaceID, rudderAccountID)
			stats.Default.NewTaggedStat(oauth.REF_TOKEN_INVALID_GRANT, stats.CountType, stats.Tags{
				"destinationId": destinationJob.Destination.ID,
				"workspaceId":   refTokenParams.WorkspaceId,
				"accountId":     refTokenParams.AccountId,
				"destType":      refTokenParams.DestDefName,
				"flowType":      string(oauth.RudderFlow_Delivery),
			}).Increment()
			rt.logger.Errorf(`[OAuth request] Aborting the event as %v`, oauth.REF_TOKEN_INVALID_GRANT)
			return authStatusInactiveStCode, refSecret.ErrorMessage, "text/plain; charset=utf-8"
		}
		// Error while refreshing the token or Has an error while refreshing or sending empty access token
		if errCatStatusCode != http.StatusOK || routerutils.IsNotEmptyString(refSecret.Err) {
			return http.StatusTooManyRequests, refSecret.Err, "text/plain; charset=utf-8"
		}
		// Retry with Refreshed Token by failing with 5xx
		return http.StatusInternalServerError, trRespBody, params.contentType
	default:
		// By default, send the status code & response from transformed response directly
		return trRespStatusCode, trRespBody, params.contentType
	}
}

func (rt *Handle) updateAuthStatusToInactive(destination *backendconfig.DestinationT, workspaceID, rudderAccountId string) int {
	inactiveAuthStatusStatTags := stats.Tags{
		"id":          destination.ID,
		"destType":    destination.DestinationDefinition.Name,
		"workspaceId": workspaceID,
		"success":     "true",
		"flowType":    string(oauth.RudderFlow_Delivery),
	}
	errCatStatusCode, _ := rt.oauth.AuthStatusToggle(&oauth.AuthStatusToggleParams{
		Destination:     destination,
		WorkspaceId:     workspaceID,
		RudderAccountId: rudderAccountId,
		AuthStatus:      oauth.AuthStatusInactive,
	})
	if errCatStatusCode != http.StatusOK {
		// Error while inactivating authStatus
		inactiveAuthStatusStatTags["success"] = "false"
	}
	stats.Default.NewTaggedStat("auth_status_inactive_category_count", stats.CountType, inactiveAuthStatusStatTags).Increment()
	// Abort the jobs as the destination is disabled
	return http.StatusBadRequest
}
