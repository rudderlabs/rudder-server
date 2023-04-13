package router

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	customDestinationManager "github.com/rudderlabs/rudder-server/router/customdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/internal/eventorder"
	"github.com/rudderlabs/rudder-server/router/internal/jobiterator"
	rtThrottler "github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/oauth"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/bytesize"
	"github.com/rudderlabs/rudder-server/utils/misc"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
)

type reporter interface {
	WaitForSetup(ctx context.Context, clientName string) error
	Report(metrics []*utilTypes.PUReportedMetric, txn *sql.Tx)
}

type tenantStats interface {
	CalculateSuccessFailureCounts(workspace, destType string, isSuccess, isDrained bool)
	GetRouterPickupJobs(
		destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int,
	) map[string]int
	ReportProcLoopAddStats(stats map[string]map[string]int, tableType string)
	UpdateWorkspaceLatencyMap(destType, workspaceID string, val float64)
}

type HandleDestOAuthRespParamsT struct {
	ctx            context.Context
	destinationJob types.DestinationJobT
	workerID       int
	trRespStCd     int
	trRespBody     string
	secret         json.RawMessage
}

type DiagnosticT struct {
	requestsMetricLock sync.RWMutex
	failureMetricLock  sync.RWMutex
	diagnosisTicker    *time.Ticker
	requestsMetric     []requestMetric
	failuresMetric     map[string]map[string]int
}

// HandleT is the handle to this module.
type HandleT struct {
	responseQ                               chan jobResponseT
	jobsDB                                  jobsdb.MultiTenantJobsDB
	errorDB                                 jobsdb.JobsDB
	netHandle                               NetHandleI
	MultitenantI                            tenantStats
	destName                                string
	destinationId                           string
	workers                                 []*worker
	telemetry                               *DiagnosticT
	customDestinationManager                customDestinationManager.DestinationManager
	throttlingCosts                         atomic.Pointer[types.EventTypeThrottlingCost]
	throttlerFactory                        *rtThrottler.Factory
	guaranteeUserEventOrder                 bool
	netClientTimeout                        time.Duration
	backendProxyTimeout                     time.Duration
	jobsDBCommandTimeout                    time.Duration
	jobdDBMaxRetries                        int
	enableBatching                          bool
	transformer                             transformer.Transformer
	configSubscriberLock                    sync.RWMutex
	destinationsMap                         map[string]*routerutils.BatchDestinationT // destinationID -> destination
	logger                                  logger.Logger
	batchInputCountStat                     stats.Measurement
	batchOutputCountStat                    stats.Measurement
	routerTransformInputCountStat           stats.Measurement
	routerTransformOutputCountStat          stats.Measurement
	batchInputOutputDiffCountStat           stats.Measurement
	routerResponseTransformStat             stats.Measurement
	throttlingErrorStat                     stats.Measurement
	throttledStat                           stats.Measurement
	noOfWorkers                             int
	workerInputBufferSize                   int
	allowAbortedUserJobsCountForProcessing  int
	isBackendConfigInitialized              bool
	backendConfig                           backendconfig.BackendConfig
	backendConfigInitialized                chan bool
	maxFailedCountForJob                    int
	noOfJobsToBatchInAWorker                int
	retryTimeWindow                         time.Duration
	routerTimeout                           time.Duration
	destinationResponseHandler              ResponseHandlerI
	saveDestinationResponse                 bool
	Reporting                               reporter
	savePayloadOnError                      bool
	oauth                                   oauth.Authorizer
	transformerProxy                        bool
	skipRtAbortAlertForDelivery             bool // represents if transformation(router or batch) should be alerted via router-aborted-count alert def
	skipRtAbortAlertForTransformation       bool // represents if event delivery(via transformerProxy) should be alerted via router-aborted-count alert def
	workspaceSet                            map[string]struct{}
	sourceIDWorkspaceMap                    map[string]string
	maxDSQuerySize                          int
	jobIteratorMaxQueries                   int
	jobIteratorDiscardedPercentageTolerance int

	backgroundGroup  *errgroup.Group
	backgroundCtx    context.Context
	backgroundCancel context.CancelFunc
	backgroundWait   func() error
	startEnded       chan struct{}
	lastQueryRunTime time.Time

	payloadLimit     int64
	transientSources transientsource.Service
	rsourcesService  rsources.JobService
	debugger         destinationdebugger.DestinationDebugger
	adaptiveLimit    func(int64) int64
}

type jobResponseT struct {
	status *jobsdb.JobStatusT
	worker *worker
	userID string
	JobT   *jobsdb.JobT
}

// JobParametersT struct holds source id and destination id of a job
type JobParametersT struct {
	SourceID                string      `json:"source_id"`
	DestinationID           string      `json:"destination_id"`
	ReceivedAt              string      `json:"received_at"`
	TransformAt             string      `json:"transform_at"`
	SourceTaskRunID         string      `json:"source_task_run_id"`
	SourceJobID             string      `json:"source_job_id"`
	SourceJobRunID          string      `json:"source_job_run_id"`
	SourceDefinitionID      string      `json:"source_definition_id"`
	DestinationDefinitionID string      `json:"destination_definition_id"`
	SourceCategory          string      `json:"source_category"`
	RecordID                interface{} `json:"record_id"`
	MessageID               string      `json:"message_id"`
	WorkspaceID             string      `json:"workspaceId"`
	RudderAccountID         string      `json:"rudderAccountId"`
}

var (
	jobQueryBatchSize, updateStatusBatchSize            int
	readSleep, maxStatusUpdateWait, diagnosisTickerTime time.Duration
	minRetryBackoff, maxRetryBackoff, jobsBatchTimeout  time.Duration
	pkgLogger                                           logger.Logger
	Diagnostics                                         diagnostics.DiagnosticsI
	fixedLoopSleep                                      time.Duration
	toAbortDestinationIDs                               string
	disableEgress                                       bool
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type requestMetric struct {
	RequestRetries       int
	RequestAborted       int
	RequestSuccess       int
	RequestCompletedTime time.Duration
}

func isSuccessStatus(status int) bool {
	return status >= 200 && status < 300
}

func isJobTerminated(status int) bool {
	if status == 429 {
		return false
	}
	return status >= 200 && status < 500
}

func loadConfig() {
	config.RegisterIntConfigVariable(10000, &jobQueryBatchSize, true, 1, "Router.jobQueryBatchSize")
	config.RegisterIntConfigVariable(1000, &updateStatusBatchSize, true, 1, "Router.updateStatusBatchSize")
	config.RegisterDurationConfigVariable(1000, &readSleep, true, time.Millisecond, []string{"Router.readSleep", "Router.readSleepInMS"}...)
	config.RegisterDurationConfigVariable(5, &jobsBatchTimeout, true, time.Second, []string{"Router.jobsBatchTimeout", "Router.jobsBatchTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(5, &maxStatusUpdateWait, true, time.Second, []string{"Router.maxStatusUpdateWait", "Router.maxStatusUpdateWaitInS"}...)
	config.RegisterBoolConfigVariable(false, &disableEgress, false, "disableEgress")
	// Time period for diagnosis ticker
	config.RegisterDurationConfigVariable(60, &diagnosisTickerTime, false, time.Second, []string{"Diagnostics.routerTimePeriod", "Diagnostics.routerTimePeriodInS"}...)
	config.RegisterDurationConfigVariable(10, &minRetryBackoff, true, time.Second, []string{"Router.minRetryBackoff", "Router.minRetryBackoffInS"}...)
	config.RegisterDurationConfigVariable(300, &maxRetryBackoff, true, time.Second, []string{"Router.maxRetryBackoff", "Router.maxRetryBackoffInS"}...)
	config.RegisterDurationConfigVariable(0, &fixedLoopSleep, true, time.Millisecond, []string{"Router.fixedLoopSleep", "Router.fixedLoopSleepInMS"}...)
	config.RegisterStringConfigVariable("", &toAbortDestinationIDs, true, "Router.toAbortDestinationIDs")
}

func sendRetryStoreStats(attempt int) {
	pkgLogger.Warnf("Timeout during store jobs in router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_store_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "router"}).Count(1)
}

func sendRetryUpdateStats(attempt int) {
	pkgLogger.Warnf("Timeout during update job status in router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_update_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "router"}).Count(1)
}

func sendQueryRetryStats(attempt int) {
	pkgLogger.Warnf("Timeout during query jobs in router module, attempt %d", attempt)
	stats.Default.NewTaggedStat("jobsdb_query_timeout", stats.CountType, stats.Tags{"attempt": fmt.Sprint(attempt), "module": "router"}).Count(1)
}

func getIterableStruct(payload []byte, transformAt string) ([]integrations.PostParametersT, error) {
	var err error
	var response integrations.PostParametersT
	responseArray := make([]integrations.PostParametersT, 0)
	if transformAt == "router" {
		err = json.Unmarshal(payload, &response)
		if err != nil {
			err = json.Unmarshal(payload, &responseArray)
		} else {
			responseArray = append(responseArray, response)
		}
	} else {
		err = json.Unmarshal(payload, &response)
		if err == nil {
			responseArray = append(responseArray, response)
		}
	}

	return responseArray, err
}

type JobResponse struct {
	jobID                  int64
	destinationJob         *types.DestinationJobT
	destinationJobMetadata *types.JobMetadataT
	respStatusCode         int
	respBody               string
	errorAt                string
	status                 *jobsdb.JobStatusT
}

func durationBeforeNextAttempt(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	return time.Duration(math.Min(float64(maxRetryBackoff), float64(minRetryBackoff)*math.Exp2(float64(attempt-1))))
}

func (rt *HandleT) trackRequestMetrics(reqMetric requestMetric) {
	if diagnostics.EnableRouterMetric {
		rt.telemetry.requestsMetricLock.Lock()
		rt.telemetry.requestsMetric = append(rt.telemetry.requestsMetric, reqMetric)
		rt.telemetry.requestsMetricLock.Unlock()
	}
}

func (rt *HandleT) initWorkers() {
	rt.workers = make([]*worker, rt.noOfWorkers)

	g, _ := errgroup.WithContext(context.Background())
	for i := 0; i < rt.noOfWorkers; i++ {
		worker := &worker{
			input: make(chan workerMessageT, rt.workerInputBufferSize),
			barrier: eventorder.NewBarrier(
				eventorder.WithConcurrencyLimit(rt.allowAbortedUserJobsCountForProcessing),
				eventorder.WithMetadata(map[string]string{
					"destType":         rt.destName,
					"batching":         strconv.FormatBool(rt.enableBatching),
					"transformerProxy": strconv.FormatBool(rt.transformerProxy),
				})),
			id:                        i,
			rt:                        rt,
			deliveryTimeStat:          stats.Default.NewTaggedStat("router_delivery_time", stats.TimerType, stats.Tags{"destType": rt.destName}),
			batchTimeStat:             stats.Default.NewTaggedStat("router_batch_time", stats.TimerType, stats.Tags{"destType": rt.destName}),
			routerDeliveryLatencyStat: stats.Default.NewTaggedStat("router_delivery_latency", stats.TimerType, stats.Tags{"destType": rt.destName}),
			routerProxyStat:           stats.Default.NewTaggedStat("router_proxy_latency", stats.TimerType, stats.Tags{"destType": rt.destName}),
		}
		rt.workers[i] = worker

		g.Go(misc.WithBugsnag(func() error {
			worker.WorkerProcess()
			return nil
		}))
	}
	rt.backgroundGroup.Go(func() error {
		err := g.Wait()

		// clean up channels workers are publishing to:
		close(rt.responseQ)
		rt.logger.Debugf("[%v Router] :: closing responseQ", rt.destName)
		return err
	})
}

func (rt *HandleT) stopWorkers() {
	for _, worker := range rt.workers {
		// FIXME remove paused worker, use shutdown instead
		close(worker.input)
	}
}

func (rt *HandleT) findWorkerSlot(job *jobsdb.JobT, blockedOrderKeys map[string]struct{}) *workerSlot {
	if rt.backgroundCtx.Err() != nil {
		return nil
	}

	var parameters JobParametersT
	err := json.Unmarshal(job.Parameters, &parameters)
	if err != nil {
		rt.logger.Errorf(`[%v Router] :: Unmarshalling parameters failed with the error %v . Returning nil worker`, err)
		return nil
	}
	orderKey := jobOrderKey(job.UserID, parameters.DestinationID)

	// checking if the orderKey is in blockedOrderKeys. If yes, returning nil.
	// this check is done to maintain order.
	if _, ok := blockedOrderKeys[orderKey]; ok {
		rt.logger.Debugf(`[%v Router] :: Skipping processing of job:%d of orderKey:%s as orderKey has earlier jobs in throttled map`, rt.destName, job.JobID, orderKey)
		return nil
	}

	if !rt.guaranteeUserEventOrder {
		availableWorkers := lo.Filter(rt.workers, func(w *worker, _ int) bool { return w.AvailableSlots() > 0 })
		if len(availableWorkers) == 0 || rt.shouldThrottle(job, parameters) || rt.shouldBackoff(job) {
			return nil
		}
		return availableWorkers[rand.Intn(len(availableWorkers))].ReserveSlot() // skipcq: GSC-G404
	}

	//#JobOrder (see other #JobOrder comment)
	worker := rt.workers[rt.getWorkerPartition(orderKey)]
	if rt.shouldBackoff(job) { // backoff
		blockedOrderKeys[orderKey] = struct{}{}
		return nil
	}
	slot := worker.ReserveSlot()
	if slot == nil {
		blockedOrderKeys[orderKey] = struct{}{}
		return nil
	}

	enter, previousFailedJobID := worker.barrier.Enter(orderKey, job.JobID)
	if enter {
		rt.logger.Debugf("EventOrder: job %d of orderKey %s is allowed to be processed", job.JobID, orderKey)
		if rt.shouldThrottle(job, parameters) {
			blockedOrderKeys[orderKey] = struct{}{}
			worker.barrier.Leave(orderKey, job.JobID)
			slot.Release()
			return nil
		}
		return slot
	}
	previousFailedJobIDStr := "<nil>"
	if previousFailedJobID != nil {
		previousFailedJobIDStr = strconv.FormatInt(*previousFailedJobID, 10)
	}
	rt.logger.Debugf("EventOrder: job %d of orderKey %s is blocked (previousFailedJobID: %s)", job.JobID, orderKey, previousFailedJobIDStr)
	slot.Release()
	return nil
	//#EndJobOrder
}

func (rt *HandleT) getWorkerPartition(key string) int {
	return misc.GetHash(key) % rt.noOfWorkers
}

func (rt *HandleT) shouldThrottle(job *jobsdb.JobT, parameters JobParametersT) (
	limited bool,
) {
	if rt.throttlerFactory == nil {
		// throttlerFactory could be nil when throttling is disabled or misconfigured.
		// in case of misconfiguration, logging errors are emitted.
		rt.logger.Debugf(`[%v Router] :: ThrottlerFactory is nil. Not throttling destination with ID %s`,
			rt.destName, parameters.DestinationID,
		)
		return false
	}

	throttler := rt.throttlerFactory.Get(rt.destName, parameters.DestinationID)
	throttlingCost := rt.getThrottlingCost(job)

	limited, err := throttler.CheckLimitReached(parameters.DestinationID, throttlingCost)
	if err != nil {
		// we can't throttle, let's hit the destination, worst case we get a 429
		rt.throttlingErrorStat.Count(1)
		rt.logger.Errorf(`[%v Router] :: Throttler error: %v`, rt.destName, err)
		return false
	}
	if limited {
		rt.throttledStat.Count(1)
		rt.logger.Debugf(
			"[%v Router] :: Skipping processing of job:%d of user:%s as throttled limits exceeded",
			rt.destName, job.JobID, job.UserID,
		)
	}

	return limited
}

func (*HandleT) shouldBackoff(job *jobsdb.JobT) bool {
	return job.LastJobStatus.JobState == jobsdb.Failed.State && job.LastJobStatus.AttemptNum > 0 && time.Until(job.LastJobStatus.RetryTime) > 0
}

func (rt *HandleT) commitStatusList(responseList *[]jobResponseT) {
	reportMetrics := make([]*utilTypes.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*utilTypes.ConnectionDetails)
	transformedAtMap := make(map[string]string)
	statusDetailsMap := make(map[string]*utilTypes.StatusDetail)
	routerWorkspaceJobStatusCount := make(map[string]int)
	var completedJobsList []*jobsdb.JobT
	var statusList []*jobsdb.JobStatusT
	var routerAbortedJobs []*jobsdb.JobT
	for _, resp := range *responseList {
		var parameters JobParametersT
		err := json.Unmarshal(resp.JobT.Parameters, &parameters)
		if err != nil {
			rt.logger.Error("Unmarshal of job parameters failed. ", string(resp.JobT.Parameters))
		}
		// Update metrics maps
		// REPORTING - ROUTER - START
		workspaceID := resp.status.WorkspaceId
		eventName := gjson.GetBytes(resp.JobT.Parameters, "event_name").String()
		eventType := gjson.GetBytes(resp.JobT.Parameters, "event_type").String()
		key := fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s", parameters.SourceID, parameters.DestinationID, parameters.SourceJobRunID, resp.status.JobState, resp.status.ErrorCode, eventName, eventType)
		_, ok := connectionDetailsMap[key]
		if !ok {
			cd := utilTypes.CreateConnectionDetail(parameters.SourceID, parameters.DestinationID, parameters.SourceTaskRunID, parameters.SourceJobID, parameters.SourceJobRunID, parameters.SourceDefinitionID, parameters.DestinationDefinitionID, parameters.SourceCategory, "", "", "", 0)
			connectionDetailsMap[key] = cd
			transformedAtMap[key] = parameters.TransformAt
		}
		sd, ok := statusDetailsMap[key]
		if !ok {
			errorCode, err := strconv.Atoi(resp.status.ErrorCode)
			if err != nil {
				errorCode = 200 // TODO handle properly
			}
			sampleEvent := resp.JobT.EventPayload
			if rt.transientSources.Apply(parameters.SourceID) {
				sampleEvent = routerutils.EmptyPayload
			}
			sd = utilTypes.CreateStatusDetail(resp.status.JobState, 0, 0, errorCode, string(resp.status.ErrorResponse), sampleEvent, eventName, eventType, "")
			statusDetailsMap[key] = sd
		}

		switch resp.status.JobState {
		case jobsdb.Failed.State:
			if resp.status.ErrorCode != strconv.Itoa(types.RouterTimedOutStatusCode) && resp.status.ErrorCode != strconv.Itoa(types.RouterUnMarshalErrorCode) {
				rt.MultitenantI.CalculateSuccessFailureCounts(workspaceID, rt.destName, false, false)
				if resp.status.AttemptNum == 1 {
					sd.Count++
				}
			}
		case jobsdb.Succeeded.State:
			routerWorkspaceJobStatusCount[workspaceID]++
			sd.Count++
			rt.MultitenantI.CalculateSuccessFailureCounts(workspaceID, rt.destName, true, false)
			completedJobsList = append(completedJobsList, resp.JobT)
		case jobsdb.Aborted.State:
			routerWorkspaceJobStatusCount[workspaceID]++
			sd.Count++
			rt.MultitenantI.CalculateSuccessFailureCounts(workspaceID, rt.destName, false, true)
			routerAbortedJobs = append(routerAbortedJobs, resp.JobT)
			completedJobsList = append(completedJobsList, resp.JobT)
		}

		// REPORTING - ROUTER - END

		statusList = append(statusList, resp.status)

		// tracking router errors
		if diagnostics.EnableDestinationFailuresMetric {
			if resp.status.JobState == jobsdb.Failed.State || resp.status.JobState == jobsdb.Aborted.State {
				var event string
				if resp.status.JobState == jobsdb.Failed.State {
					event = diagnostics.RouterFailed
				} else {
					event = diagnostics.RouterAborted
				}

				rt.telemetry.failureMetricLock.Lock()
				if _, ok := rt.telemetry.failuresMetric[event][string(resp.status.ErrorResponse)]; !ok {
					rt.telemetry.failuresMetric[event] = make(map[string]int)
				}
				rt.telemetry.failuresMetric[event][string(resp.status.ErrorResponse)] += 1
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
		rt.logger.Debugf("[%v Router] :: flushing batch of %v status", rt.destName, updateStatusBatchSize)

		sort.Slice(statusList, func(i, j int) bool {
			return statusList[i].JobID < statusList[j].JobID
		})
		// Store the aborted jobs to errorDB
		if routerAbortedJobs != nil {
			err := misc.RetryWithNotify(context.Background(), rt.jobsDBCommandTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) error {
				return rt.errorDB.Store(ctx, routerAbortedJobs)
			}, sendRetryStoreStats)
			if err != nil {
				panic(fmt.Errorf("storing jobs into ErrorDB: %w", err))
			}
		}
		// Update the status
		err := misc.RetryWithNotify(context.Background(), rt.jobsDBCommandTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) error {
			return rt.jobsDB.WithUpdateSafeTx(ctx, func(tx jobsdb.UpdateSafeTx) error {
				err := rt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{rt.destName}, rt.parameterFilters())
				if err != nil {
					return fmt.Errorf("updating %s jobs statuses: %w", rt.destName, err)
				}

				// rsources stats
				err = rt.updateRudderSourcesStats(ctx, tx, completedJobsList, statusList)
				if err != nil {
					return err
				}
				rt.Reporting.Report(reportMetrics, tx.SqlTx())
				return nil
			})
		}, sendRetryStoreStats)
		if err != nil {
			panic(err)
		}
		rt.updateProcessedEventsMetrics(statusList)
		for workspace, jobCount := range routerWorkspaceJobStatusCount {
			rmetrics.DecreasePendingEvents(
				"rt",
				workspace,
				rt.destName,
				float64(jobCount),
			)
		}
	}

	if rt.guaranteeUserEventOrder {
		//#JobOrder (see other #JobOrder comment)
		for _, resp := range *responseList {
			status := resp.status.JobState
			userID := resp.userID
			worker := resp.worker
			if status != jobsdb.Failed.State {
				orderKey := jobOrderKey(userID, gjson.GetBytes(resp.JobT.Parameters, "destination_id").String())
				rt.logger.Debugf("EventOrder: [%d] job %d for key %s %s", worker.id, resp.status.JobID, orderKey, status)
				if err := worker.barrier.StateChanged(orderKey, resp.status.JobID, status); err != nil {
					panic(err)
				}
			}
		}
		// End #JobOrder
	}
}

// statusInsertLoop will run in a separate goroutine
// Blocking method, returns when rt.responseQ channel is closed.
func (rt *HandleT) statusInsertLoop() {
	statusStat := stats.Default.NewTaggedStat("router_status_loop", stats.TimerType, stats.Tags{"destType": rt.destName})
	countStat := stats.Default.NewTaggedStat("router_status_events", stats.CountType, stats.Tags{"destType": rt.destName})

	for {
		jobResponseBuffer, numJobResponses, _, isResponseQOpen := lo.BufferWithTimeout(
			rt.responseQ,
			updateStatusBatchSize,
			maxStatusUpdateWait,
		)
		if numJobResponses > 0 {
			start := time.Now()
			rt.commitStatusList(&jobResponseBuffer)
			countStat.Count(numJobResponses)
			statusStat.Since(start)
		}
		if !isResponseQOpen {
			rt.logger.Debugf("[%v Router] :: statusInsertLoop exiting", rt.destName)
			return
		}
	}
}

func (rt *HandleT) collectMetrics(ctx context.Context) {
	if !diagnostics.EnableRouterMetric {
		return
	}

	for {
		select {
		case <-ctx.Done():
			rt.logger.Debugf("[%v Router] :: collectMetrics exiting", rt.destName)
			return
		case <-rt.telemetry.diagnosisTicker.C:
		}
		rt.telemetry.requestsMetricLock.RLock()
		var diagnosisProperties map[string]interface{}
		retries := 0
		aborted := 0
		success := 0
		var compTime time.Duration
		for _, reqMetric := range rt.telemetry.requestsMetric {
			retries += reqMetric.RequestRetries
			aborted += reqMetric.RequestAborted
			success += reqMetric.RequestSuccess
			compTime += reqMetric.RequestCompletedTime
		}
		if len(rt.telemetry.requestsMetric) > 0 {
			diagnosisProperties = map[string]interface{}{
				rt.destName: map[string]interface{}{
					diagnostics.RouterAborted:       aborted,
					diagnostics.RouterRetries:       retries,
					diagnostics.RouterSuccess:       success,
					diagnostics.RouterCompletedTime: (compTime / time.Duration(len(rt.telemetry.requestsMetric))) / time.Millisecond,
				},
			}

			Diagnostics.Track(diagnostics.RouterEvents, diagnosisProperties)
		}

		rt.telemetry.requestsMetric = nil
		rt.telemetry.requestsMetricLock.RUnlock()

		// This lock will ensure we don't send out Track Request while filling up the
		// failureMetric struct
		rt.telemetry.failureMetricLock.Lock()
		for key, value := range rt.telemetry.failuresMetric {
			var err error
			stringValueBytes, err := jsonfast.Marshal(value)
			if err != nil {
				stringValueBytes = []byte{}
			}

			Diagnostics.Track(key, map[string]interface{}{
				diagnostics.RouterDestination: rt.destName,
				diagnostics.Count:             len(value),
				diagnostics.ErrorCountMap:     string(stringValueBytes),
			})
		}
		rt.telemetry.failuresMetric = make(map[string]map[string]int)
		rt.telemetry.failureMetricLock.Unlock()
	}
}

//#JobOrder (see other #JobOrder comment)
// If a job fails (say with given failed_job_id), we need to fail other jobs from that user till
//the failed_job_id succeeds. We achieve this by keeping the failed_job_id in a failedJobIDMap
//structure (mapping userID to failed_job_id). All subsequent jobs (guaranteed to be job_id >= failed_job_id)
//are put in Waiting.State in worker loop till the failed_job_id succeeds.
//However, the step of removing failed_job_id from the failedJobIDMap structure is QUITE TRICKY.
//To understand that, need to understand the complete lifecycle of a job.
//The job goes through the following data-structures in order
//   i>   generatorLoop Buffer (read from DB)
//   ii>  requestQ (no longer used - RIP)
//   iii> Worker Process
//   iv>  responseQ
//   v>   statusInsertLoop Buffer (enough jobs are buffered before updating status)
// Now, when the failed_job_id eventually succeeds in the Worker Process (iii above),
// there may be pending jobs in all the other data-structures. For example, there
//may be jobs in responseQ(iv) and statusInsertLoop(v) buffer - all those jobs will
//be in Waiting state. Similarly, there may be other jobs in requestQ and generatorLoop
//buffer.
//If the failed_job_id succeeds, and we remove the filter gate, then all the jobs in requestQ
//will pass through before the jobs in responseQ/insertStatus buffer. That will violate the
//ordering of job.
//We fix this by removing an entry from the failedJobIDMap structure only when we are guaranteed
//that all the other structures are empty. We do the following to achieve this
// A. In generatorLoop, we do not let any job pass through except failed_job_id. That ensures requestQ is empty
// B. We wait for the failed_job_id status (when succeeded) to be sync'd to disk. This along with A ensures
//    that responseQ and statusInsertLoop Buffer are empty for that userID.
// C. Finally, we want for generatorLoop buffer to be fully processed.

func (rt *HandleT) generatorLoop(ctx context.Context) {
	rt.logger.Infof("Generator started for %s and destinationID %s", rt.destName, rt.destinationId)

	timeout := time.After(10 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			rt.logger.Infof("Generator exiting for router %s", rt.destName)
			return
		case <-timeout:

			start := time.Now()
			processCount := rt.readAndProcess()
			stats.Default.NewTaggedStat("router_generator_loop", stats.TimerType, stats.Tags{"destType": rt.destName}).Since(start)
			stats.Default.NewTaggedStat("router_generator_events", stats.CountType, stats.Tags{"destType": rt.destName}).Count(processCount)

			timeElapsed := time.Since(start)
			nextTimeout := time.Second - timeElapsed
			if nextTimeout < fixedLoopSleep {
				nextTimeout = fixedLoopSleep
			}
			timeout = time.After(nextTimeout)
		}
	}
}

func (rt *HandleT) getQueryParams(pickUpCount int) jobsdb.GetQueryParamsT {
	if rt.destinationId != rt.destName {
		return jobsdb.GetQueryParamsT{
			CustomValFilters:              []string{rt.destName},
			ParameterFilters:              rt.parameterFilters(),
			IgnoreCustomValFiltersInQuery: true,
			PayloadSizeLimit:              rt.adaptiveLimit(rt.payloadLimit),
			JobsLimit:                     pickUpCount,
		}
	}
	return jobsdb.GetQueryParamsT{
		CustomValFilters: []string{rt.destName},
		PayloadSizeLimit: rt.adaptiveLimit(rt.payloadLimit),
		JobsLimit:        pickUpCount,
	}
}

func (rt *HandleT) parameterFilters() []jobsdb.ParameterFilterT {
	if rt.destinationId != rt.destName {
		return []jobsdb.ParameterFilterT{{
			Name:  "destination_id",
			Value: rt.destinationId,
		}}
	}
	return nil
}

func (rt *HandleT) readAndProcess() int {
	//#JobOrder (See comment marked #JobOrder
	if rt.guaranteeUserEventOrder {
		for idx := range rt.workers {
			rt.workers[idx].barrier.Sync()
		}
	}

	timeOut := rt.routerTimeout
	timeElapsed := time.Since(rt.lastQueryRunTime)
	if timeElapsed < timeOut {
		timeOut = timeElapsed
	}
	rt.lastQueryRunTime = time.Now()

	pickupMap := rt.MultitenantI.GetRouterPickupJobs(rt.destName, rt.noOfWorkers, timeOut, jobQueryBatchSize)
	totalPickupCount := 0
	for _, pickup := range pickupMap {
		if pickup > 0 {
			totalPickupCount += pickup
		}
	}
	iterator := jobiterator.New(
		pickupMap,
		rt.getQueryParams(totalPickupCount),
		rt.getJobsFn(),
		jobiterator.WithDiscardedPercentageTolerance(rt.jobIteratorDiscardedPercentageTolerance),
		jobiterator.WithMaxQueries(rt.jobIteratorMaxQueries),
		jobiterator.WithLegacyOrderGroupKey(!misc.UseFairPickup()),
	)

	rt.logger.Debugf("[%v Router] :: pickupMap: %+v", rt.destName, pickupMap)

	if !iterator.HasNext() {
		rt.logger.Debugf("RT: DB Read Complete. No RT Jobs to process for destination: %s", rt.destName)
		time.Sleep(readSleep)
		return 0
	}

	// List of jobs which can be processed mapped per channel
	type workerJob struct {
		slot *workerSlot
		job  *jobsdb.JobT
	}

	var statusList []*jobsdb.JobStatusT
	var workerJobs []workerJob
	blockedOrderKeys := make(map[string]struct{})

	// Identify jobs which can be processed
	for iterator.HasNext() {
		job := iterator.Next()
		if slot := rt.findWorkerSlot(job, blockedOrderKeys); slot != nil {
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
			workerJobs = append(workerJobs, workerJob{slot: slot, job: job})
		} else {
			iterator.Discard(job)
		}
	}
	iteratorStats := iterator.Stats()
	stats.Default.NewTaggedStat("router_iterator_stats_query_count", stats.GaugeType, stats.Tags{"destType": rt.destName}).Gauge(iteratorStats.QueryCount)
	stats.Default.NewTaggedStat("router_iterator_stats_total_jobs", stats.GaugeType, stats.Tags{"destType": rt.destName}).Gauge(iteratorStats.TotalJobs)
	stats.Default.NewTaggedStat("router_iterator_stats_discarded_jobs", stats.GaugeType, stats.Tags{"destType": rt.destName}).Gauge(iteratorStats.DiscardedJobs)

	// Mark the jobs as executing
	err := misc.RetryWithNotify(context.Background(), rt.jobsDBCommandTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) error {
		return rt.jobsDB.UpdateJobStatus(ctx, statusList, []string{rt.destName}, rt.parameterFilters())
	}, sendRetryUpdateStats)
	if err != nil {
		pkgLogger.Errorf("Error occurred while marking %s jobs statuses as executing. Panicking. Err: %v", rt.destName, err)
		panic(err)
	}

	rt.logger.Debugf("[DRAIN DEBUG] counts  %v final jobs length being processed %v", rt.destName, len(workerJobs))

	if len(workerJobs) == 0 {
		rt.logger.Debugf("RT: No workers found for the jobs. Sleeping. Destination: %s", rt.destName)
		time.Sleep(readSleep)
		return 0
	}

	assignedTime := time.Now()
	for _, workerJob := range workerJobs {
		workerJob.slot.Use(workerMessageT{job: workerJob.job, assignedAt: assignedTime})
	}

	return len(workerJobs)
}

func (rt *HandleT) getJobsFn() func(context.Context, map[string]int, jobsdb.GetQueryParamsT, jobsdb.MoreToken) (*jobsdb.GetAllJobsResult, error) {
	return func(ctx context.Context, pickupMap map[string]int, params jobsdb.GetQueryParamsT, resumeFrom jobsdb.MoreToken) (*jobsdb.GetAllJobsResult, error) {
		return misc.QueryWithRetriesAndNotify(context.Background(), rt.jobsDBCommandTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) (*jobsdb.GetAllJobsResult, error) {
			return rt.jobsDB.GetAllJobs(
				ctx,
				pickupMap,
				params,
				rt.maxDSQuerySize,
				resumeFrom,
			)
		}, sendQueryRetryStats)
	}
}

func (*HandleT) crashRecover() {
	// NO-OP
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("router")
	Diagnostics = diagnostics.Diagnostics
}

// Setup initializes this module
func (rt *HandleT) Setup(
	backendConfig backendconfig.BackendConfig,
	jobsDB jobsdb.MultiTenantJobsDB,
	errorDB jobsdb.JobsDB,
	destinationConfig destinationConfig,
	transientSources transientsource.Service,
	rsourcesService rsources.JobService,
	debugger destinationdebugger.DestinationDebugger,
) {
	rt.backendConfig = backendConfig
	rt.workspaceSet = make(map[string]struct{})
	rt.debugger = debugger

	destName := destinationConfig.name
	rt.logger = pkgLogger.Child(destName)
	rt.logger.Info("Router started: ", destinationConfig.destinationID)

	rt.transientSources = transientSources
	rt.rsourcesService = rsourcesService

	// waiting for reporting client setup
	err := rt.Reporting.WaitForSetup(context.TODO(), utilTypes.CoreReportingClient)
	if err != nil {
		return
	}

	rt.jobsDB = jobsDB
	rt.errorDB = errorDB
	rt.destName = destName
	rt.destinationId = destinationConfig.destinationID
	netClientTimeoutKeys := []string{"Router." + rt.destName + "." + "httpTimeout", "Router." + rt.destName + "." + "httpTimeoutInS", "Router." + "httpTimeout", "Router." + "httpTimeoutInS"}
	config.RegisterDurationConfigVariable(10, &rt.netClientTimeout, false, time.Second, netClientTimeoutKeys...)
	config.RegisterDurationConfigVariable(30, &rt.backendProxyTimeout, false, time.Second, "HttpClient.backendProxy.timeout")
	config.RegisterDurationConfigVariable(90, &rt.jobsDBCommandTimeout, true, time.Second, []string{"JobsDB.Router.CommandRequestTimeout", "JobsDB.CommandRequestTimeout"}...)
	config.RegisterIntConfigVariable(3, &rt.jobdDBMaxRetries, true, 1, []string{"JobsDB." + "Router." + "MaxRetries", "JobsDB." + "MaxRetries"}...)
	rt.crashRecover()
	rt.responseQ = make(chan jobResponseT, jobQueryBatchSize)
	if rt.netHandle == nil {
		netHandle := &NetHandleT{}
		netHandle.logger = rt.logger.Child("network")
		netHandle.Setup(destName, rt.netClientTimeout)
		rt.netHandle = netHandle
	}

	rt.customDestinationManager = customDestinationManager.New(destName, customDestinationManager.Opts{
		Timeout: rt.netClientTimeout,
	})
	rt.telemetry = &DiagnosticT{}
	rt.telemetry.failuresMetric = make(map[string]map[string]int)
	rt.telemetry.diagnosisTicker = time.NewTicker(diagnosisTickerTime)

	rt.destinationResponseHandler = New(destinationConfig.responseRules)
	if value, ok := destinationConfig.config["saveDestinationResponse"].(bool); ok {
		rt.saveDestinationResponse = value
	}
	rt.guaranteeUserEventOrder = getRouterConfigBool("guaranteeUserEventOrder", rt.destName, true)
	rt.noOfWorkers = getRouterConfigInt("noOfWorkers", destName, 64)
	rt.workerInputBufferSize = getRouterConfigInt("noOfJobsPerChannel", destName, 1000)
	maxFailedCountKeys := []string{"Router." + rt.destName + "." + "maxFailedCountForJob", "Router." + "maxFailedCountForJob"}
	retryTimeWindowKeys := []string{"Router." + rt.destName + "." + "retryTimeWindow", "Router." + rt.destName + "." + "retryTimeWindowInMins", "Router." + "retryTimeWindow", "Router." + "retryTimeWindowInMins"}
	savePayloadOnErrorKeys := []string{"Router." + rt.destName + "." + "savePayloadOnError", "Router." + "savePayloadOnError"}
	transformerProxyKeys := []string{"Router." + rt.destName + "." + "transformerProxy", "Router." + "transformerProxy"}

	batchJobCountKeys := []string{"Router." + rt.destName + "." + "noOfJobsToBatchInAWorker", "Router." + "noOfJobsToBatchInAWorker"}
	config.RegisterIntConfigVariable(20, &rt.noOfJobsToBatchInAWorker, true, 1, batchJobCountKeys...)
	config.RegisterIntConfigVariable(3, &rt.maxFailedCountForJob, true, 1, maxFailedCountKeys...)
	routerPayloadLimitKeys := []string{"Router." + rt.destName + "." + "PayloadLimit", "Router." + "PayloadLimit"}
	config.RegisterInt64ConfigVariable(100*bytesize.MB, &rt.payloadLimit, true, 1, routerPayloadLimitKeys...)
	routerTimeoutKeys := []string{"Router." + rt.destName + "." + "routerTimeout", "Router." + "routerTimeout"}
	config.RegisterDurationConfigVariable(3600, &rt.routerTimeout, true, time.Second, routerTimeoutKeys...)
	config.RegisterDurationConfigVariable(180, &rt.retryTimeWindow, true, time.Minute, retryTimeWindowKeys...)
	maxDSQuerySizeKeys := []string{"Router." + rt.destName + "." + "maxDSQuery", "Router." + "maxDSQuery"}
	config.RegisterIntConfigVariable(10, &rt.maxDSQuerySize, true, 1, maxDSQuerySizeKeys...)

	config.RegisterIntConfigVariable(50, &rt.jobIteratorMaxQueries, true, 1, "Router.jobIterator.maxQueries")
	config.RegisterIntConfigVariable(10, &rt.jobIteratorDiscardedPercentageTolerance, true, 1, "Router.jobIterator.discardedPercentageTolerance")

	config.RegisterBoolConfigVariable(false, &rt.enableBatching, false, "Router."+rt.destName+"."+"enableBatching")
	config.RegisterBoolConfigVariable(false, &rt.savePayloadOnError, true, savePayloadOnErrorKeys...)
	config.RegisterBoolConfigVariable(false, &rt.transformerProxy, true, transformerProxyKeys...)
	// START: Alert configuration
	// We want to use these configurations to control what alerts we show via router-abort-count alert definition
	rtAbortTransformationKeys := []string{"Router." + rt.destName + "." + "skipRtAbortAlertForTf", "Router.skipRtAbortAlertForTf"}
	rtAbortDeliveryKeys := []string{"Router." + rt.destName + "." + "skipRtAbortAlertForDelivery", "Router.skipRtAbortAlertForDelivery"}

	config.RegisterBoolConfigVariable(false, &rt.skipRtAbortAlertForTransformation, true, rtAbortTransformationKeys...)
	config.RegisterBoolConfigVariable(false, &rt.skipRtAbortAlertForDelivery, true, rtAbortDeliveryKeys...)
	// END: Alert configuration
	rt.allowAbortedUserJobsCountForProcessing = getRouterConfigInt("allowAbortedUserJobsCountForProcessing", destName, 1)

	statTags := stats.Tags{"destType": rt.destName}
	rt.batchInputCountStat = stats.Default.NewTaggedStat("router_batch_num_input_jobs", stats.CountType, statTags)
	rt.batchOutputCountStat = stats.Default.NewTaggedStat("router_batch_num_output_jobs", stats.CountType, statTags)
	rt.routerTransformInputCountStat = stats.Default.NewTaggedStat("router_transform_num_input_jobs", stats.CountType, statTags)
	rt.routerTransformOutputCountStat = stats.Default.NewTaggedStat("router_transform_num_output_jobs", stats.CountType, statTags)
	rt.batchInputOutputDiffCountStat = stats.Default.NewTaggedStat("router_batch_input_output_diff_jobs", stats.CountType, statTags)
	rt.routerResponseTransformStat = stats.Default.NewTaggedStat("response_transform_latency", stats.TimerType, statTags)
	rt.throttlingErrorStat = stats.Default.NewTaggedStat("router_throttling_error", stats.CountType, statTags)
	rt.throttledStat = stats.Default.NewTaggedStat("router_throttled", stats.CountType, statTags)

	rt.transformer = transformer.NewTransformer(rt.netClientTimeout, rt.backendProxyTimeout)

	rt.oauth = oauth.NewOAuthErrorHandler(backendConfig)

	rt.isBackendConfigInitialized = false
	rt.backendConfigInitialized = make(chan bool)

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	rt.backgroundCtx = ctx
	rt.backgroundGroup = g
	rt.backgroundCancel = cancel
	rt.backgroundWait = g.Wait
	rt.initWorkers()

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

func (rt *HandleT) Start() {
	rt.logger.Infof("Starting router: %s", rt.destName)
	rt.startEnded = make(chan struct{})
	ctx := rt.backgroundCtx

	rt.backgroundGroup.Go(misc.WithBugsnag(func() error {
		defer close(rt.startEnded) // always close the channel
		defer rt.stopWorkers()     // workers are started before the generatorLoop, so always stop them
		select {
		case <-ctx.Done():
			rt.logger.Infof("Router : %s start goroutine exited", rt.destName)
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
		rt.generatorLoop(ctx)
		return nil
	}))
}

func (rt *HandleT) Shutdown() {
	if rt.startEnded == nil {
		// router is not started
		return
	}
	rt.logger.Infof("Shutting down router: %s destinationId: %s", rt.destName, rt.destinationId)
	rt.backgroundCancel()

	<-rt.startEnded
	_ = rt.backgroundWait()
}

func (rt *HandleT) backendConfigSubscriber() {
	ch := rt.backendConfig.Subscribe(context.TODO(), backendconfig.TopicBackendConfig)
	for configEvent := range ch {
		rt.configSubscriberLock.Lock()
		rt.destinationsMap = map[string]*routerutils.BatchDestinationT{}
		configData := configEvent.Data.(map[string]backendconfig.ConfigT)
		rt.sourceIDWorkspaceMap = map[string]string{}
		for workspaceID, wConfig := range configData {
			for i := range wConfig.Sources {
				source := &wConfig.Sources[i]
				rt.sourceIDWorkspaceMap[source.ID] = workspaceID
				for i := range source.Destinations {
					destination := &source.Destinations[i]
					if destination.DestinationDefinition.Name == rt.destName {
						if _, ok := rt.destinationsMap[destination.ID]; !ok {
							rt.destinationsMap[destination.ID] = &routerutils.BatchDestinationT{
								Destination: *destination,
								Sources:     []backendconfig.SourceT{},
							}
						}
						if _, ok := rt.workspaceSet[workspaceID]; !ok {
							rt.workspaceSet[workspaceID] = struct{}{}
							rt.MultitenantI.UpdateWorkspaceLatencyMap(rt.destName, workspaceID, 0)
						}
						rt.destinationsMap[destination.ID].Sources = append(rt.destinationsMap[destination.ID].Sources, *source)

						rt.destinationResponseHandler = New(destination.DestinationDefinition.ResponseRules)
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
		if !rt.isBackendConfigInitialized {
			rt.isBackendConfigInitialized = true
			rt.backendConfigInitialized <- true
		}
		rt.configSubscriberLock.Unlock()
	}
}

func (rt *HandleT) HandleOAuthDestResponse(params *HandleDestOAuthRespParamsT) (int, string) {
	trRespStatusCode := params.trRespStCd
	trRespBody := params.trRespBody
	destinationJob := params.destinationJob

	if trRespStatusCode != http.StatusOK {
		var destErrOutput integrations.TransResponseT
		if destError := json.Unmarshal([]byte(trRespBody), &destErrOutput); destError != nil {
			// Errors like OOM kills of transformer, transformer down etc...
			// If destResBody comes out with a plain string, then this will occur
			return http.StatusInternalServerError, fmt.Sprintf(`{
				Error: %v,
				(trRespStCd, trRespBody): (%v, %v),
			}`, destError, trRespStatusCode, trRespBody)
		}
		workspaceID := destinationJob.JobMetadataArray[0].WorkspaceID
		var errCatStatusCode int
		// Check the category
		// Trigger the refresh endpoint/disable endpoint
		rudderAccountID := oauth.GetAccountId(destinationJob.Destination.Config, oauth.DeliveryAccountIdKey)
		if strings.TrimSpace(rudderAccountID) == "" {
			return trRespStatusCode, trRespBody
		}
		switch destErrOutput.AuthErrorCategory {
		case oauth.DISABLE_DEST:
			return rt.ExecDisableDestination(&destinationJob.Destination, workspaceID, trRespBody, rudderAccountID)
		case oauth.REFRESH_TOKEN:
			var refSecret *oauth.AuthResponse
			refTokenParams := &oauth.RefreshTokenParams{
				Secret:          params.secret,
				WorkspaceId:     workspaceID,
				AccountId:       rudderAccountID,
				DestDefName:     destinationJob.Destination.DestinationDefinition.Name,
				EventNamePrefix: "refresh_token",
				WorkerId:        params.workerID,
			}
			errCatStatusCode, refSecret = rt.oauth.RefreshToken(refTokenParams)
			refSec := *refSecret
			if routerutils.IsNotEmptyString(refSec.Err) && refSec.Err == oauth.INVALID_REFRESH_TOKEN_GRANT {
				// In-case the refresh token has been revoked, this error comes in
				// Even trying to refresh the token also doesn't work here. Hence, this would be more ideal to Abort Events
				// As well as to disable destination as well.
				// Alert the user in this error as well, to check if the refresh token also has been revoked & fix it
				disableStCd, _ := rt.ExecDisableDestination(&destinationJob.Destination, workspaceID, trRespBody, rudderAccountID)
				stats.Default.NewTaggedStat(oauth.INVALID_REFRESH_TOKEN_GRANT, stats.CountType, stats.Tags{
					"destinationId": destinationJob.Destination.ID,
					"workspaceId":   refTokenParams.WorkspaceId,
					"accountId":     refTokenParams.AccountId,
					"destType":      refTokenParams.DestDefName,
					"flowType":      string(oauth.RudderFlow_Delivery),
				}).Increment()
				rt.logger.Errorf(`[OAuth request] Aborting the event as %v`, oauth.INVALID_REFRESH_TOKEN_GRANT)
				return disableStCd, refSec.Err
			}
			// Error while refreshing the token or Has an error while refreshing or sending empty access token
			if errCatStatusCode != http.StatusOK || routerutils.IsNotEmptyString(refSec.Err) {
				return http.StatusTooManyRequests, refSec.Err
			}
			// Retry with Refreshed Token by failing with 5xx
			return http.StatusInternalServerError, trRespBody
		}
	}
	// By default, send the status code & response from transformed response directly
	return trRespStatusCode, trRespBody
}

func (rt *HandleT) ExecDisableDestination(destination *backendconfig.DestinationT, workspaceID, destResBody, rudderAccountId string) (int, string) {
	disableDestStatTags := stats.Tags{
		"id":          destination.ID,
		"destType":    destination.DestinationDefinition.Name,
		"workspaceId": workspaceID,
		"success":     "true",
		"flowType":    string(oauth.RudderFlow_Delivery),
	}
	errCatStatusCode, errCatResponse := rt.oauth.DisableDestination(destination, workspaceID, rudderAccountId)
	if errCatStatusCode != http.StatusOK {
		// Error while disabling a destination
		// High-Priority notification to rudderstack needs to be sent
		disableDestStatTags["success"] = "false"
		stats.Default.NewTaggedStat("disable_destination_category_count", stats.CountType, disableDestStatTags).Increment()
		return http.StatusBadRequest, errCatResponse
	}
	// High-Priority notification to workspace(& rudderstack) needs to be sent
	stats.Default.NewTaggedStat("disable_destination_category_count", stats.CountType, disableDestStatTags).Increment()
	// Abort the jobs as the destination is disabled
	return http.StatusBadRequest, destResBody
}

func (rt *HandleT) updateRudderSourcesStats(ctx context.Context, tx jobsdb.UpdateSafeTx, jobs []*jobsdb.JobT, jobStatuses []*jobsdb.JobStatusT) error {
	rsourcesStats := rsources.NewStatsCollector(rt.rsourcesService)
	rsourcesStats.BeginProcessing(jobs)
	rsourcesStats.JobStatusesUpdated(jobStatuses)
	err := rsourcesStats.Publish(ctx, tx.SqlTx())
	if err != nil {
		rt.logger.Errorf("publishing rsources stats: %w", err)
	}
	return err
}

func (rt *HandleT) updateProcessedEventsMetrics(statusList []*jobsdb.JobStatusT) {
	eventsPerStateAndCode := map[string]map[string]int{}
	for i := range statusList {
		state := statusList[i].JobState
		code := statusList[i].ErrorCode
		if _, ok := eventsPerStateAndCode[state]; !ok {
			eventsPerStateAndCode[state] = map[string]int{}
		}
		eventsPerStateAndCode[state][code]++
	}
	for state, codes := range eventsPerStateAndCode {
		for code, count := range codes {
			stats.Default.NewTaggedStat(`pipeline_processed_events`, stats.CountType, stats.Tags{
				"module":   "router",
				"destType": rt.destName,
				"state":    state,
				"code":     code,
			}).Count(count)
		}
	}
}

func (rt *HandleT) getThrottlingCost(job *jobsdb.JobT) (cost int64) {
	cost = 1
	if tc := rt.throttlingCosts.Load(); tc != nil {
		eventType := gjson.GetBytes(job.Parameters, "event_type").String()
		cost = tc.Cost(eventType)
	}

	return cost * int64(job.EventCount)
}

func jobOrderKey(userID, destinationID string) string {
	return fmt.Sprintf(`%s:%s`, userID, destinationID)
}
