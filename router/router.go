package router

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	customDestinationManager "github.com/rudderlabs/rudder-server/router/customdestinationmanager"
	oauth "github.com/rudderlabs/rudder-server/router/oauthResponseHandler"
	"github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/types"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/services/metric"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/transientsource"
	"github.com/rudderlabs/rudder-server/utils/bytesize"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
)

type reporter interface {
	WaitForSetup(ctx context.Context, clientName string)
	Report(metrics []*utilTypes.PUReportedMetric, txn *sql.Tx)
}

type tenantStats interface {
	CalculateSuccessFailureCounts(workspace, destType string, isSuccess, isDrained bool)
	GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64)
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

// HandleT is the handle to this module.
type HandleT struct {
	responseQ             chan jobResponseT
	jobsDB                jobsdb.MultiTenantJobsDB
	errorDB               jobsdb.JobsDB
	netHandle             NetHandleI
	MultitenantI          tenantStats
	destName              string
	workers               []*workerT
	perfStats             *misc.PerfStats
	successCount          uint64
	failCount             uint64
	failedEventsListMutex sync.RWMutex
	failedEventsList      *list.List
	failedEventsChan      chan jobsdb.JobStatusT

	eventOrderSyncMu        sync.Mutex                            // protects all maps below
	toClearFailJobIDMap     map[int][]string                      // list of userIDs to clear from failedJobIDMap for each worker
	toClearAbortedJobIDMap  map[int][]string                      // list of userIDs to clear from abortedUserIDMap for each worker
	toRemoveAbortedJobIDMap map[int]map[string]map[int64]struct{} // list of jobIDs to remove from each userID contained in the abortedUserIDMap for each worker

	requestsMetricLock                     sync.RWMutex
	failureMetricLock                      sync.RWMutex
	diagnosisTicker                        *time.Ticker
	requestsMetric                         []requestMetric
	failuresMetric                         map[string]map[string]int
	customDestinationManager               customDestinationManager.DestinationManager
	throttler                              throttler.Throttler
	guaranteeUserEventOrder                bool
	netClientTimeout                       time.Duration
	backendProxyTimeout                    time.Duration
	jobdDBQueryRequestTimeout              time.Duration
	jobsDBCommandTimeout                   time.Duration
	jobdDBMaxRetries                       int
	enableBatching                         bool
	transformer                            transformer.Transformer
	configSubscriberLock                   sync.RWMutex
	destinationsMap                        map[string]*routerutils.BatchDestinationT // destinationID -> destination
	logger                                 logger.LoggerI
	batchInputCountStat                    stats.RudderStats
	batchOutputCountStat                   stats.RudderStats
	routerTransformInputCountStat          stats.RudderStats
	routerTransformOutputCountStat         stats.RudderStats
	batchInputOutputDiffCountStat          stats.RudderStats
	routerResponseTransformStat            stats.RudderStats
	noOfWorkers                            int
	allowAbortedUserJobsCountForProcessing int
	throttledUserMap                       map[string]struct{} // used before calling findWorker. A temp storage to save <userid> whose job can be throttled.
	isBackendConfigInitialized             bool
	backendConfig                          backendconfig.BackendConfig
	backendConfigInitialized               chan bool
	maxFailedCountForJob                   int
	noOfJobsToBatchInAWorker               int
	retryTimeWindow                        time.Duration
	routerTimeout                          time.Duration
	destinationResponseHandler             ResponseHandlerI
	saveDestinationResponse                bool
	Reporting                              reporter
	savePayloadOnError                     bool
	oauth                                  oauth.Authorizer
	transformerProxy                       bool
	saveDestinationResponseOverride        bool
	workspaceSet                           map[string]struct{}
	sourceIDWorkspaceMap                   map[string]string
	maxDSQuerySize                         int

	backgroundGroup  *errgroup.Group
	backgroundCtx    context.Context
	backgroundCancel context.CancelFunc
	backgroundWait   func() error
	startEnded       chan struct{}
	lastQueryRunTime time.Time
	timeGained       float64

	payloadLimit     int64
	transientSources transientsource.Service
	rsourcesService  rsources.JobService
}

type jobResponseT struct {
	status *jobsdb.JobStatusT
	worker *workerT
	userID string
	JobT   *jobsdb.JobT
}

// JobParametersT struct holds source id and destination id of a job
type JobParametersT struct {
	SourceID                string      `json:"source_id"`
	DestinationID           string      `json:"destination_id"`
	ReceivedAt              string      `json:"received_at"`
	TransformAt             string      `json:"transform_at"`
	SourceBatchID           string      `json:"source_batch_id"`
	SourceTaskID            string      `json:"source_task_id"`
	SourceTaskRunID         string      `json:"source_task_run_id"`
	SourceJobID             string      `json:"source_job_id"`
	SourceJobRunID          string      `json:"source_job_run_id"`
	SourceDefinitionID      string      `json:"source_definition_id"`
	DestinationDefinitionID string      `json:"destination_definition_id"`
	SourceCategory          string      `json:"source_category"`
	RecordID                interface{} `json:"record_id"`
	MessageID               string      `json:"message_id"`
	WorkspaceID             string      `json:"workspaceId"`
	RudderAccountId         string      `json:"rudderAccountId"`
}

type workerMessageT struct {
	job                *jobsdb.JobT
	throttledAtTime    time.Time
	workerAssignedTime time.Time
}

// workerT a structure to define a worker for sending events to sinks
type workerT struct {
	channel                    chan workerMessageT     // the worker job channel
	workerID                   int                     // identifies the worker
	failedJobs                 int                     // counts the failed jobs of a worker till it gets reset by external channel
	sleepTime                  time.Duration           // the sleep duration for every job of the worker
	failedJobIDMap             map[string]int64        // user to failed jobId
	failedJobIDMutex           sync.RWMutex            // lock to protect structure above
	retryForJobMap             map[int64]time.Time     // jobID to next retry time map
	retryForJobMapMutex        sync.RWMutex            // lock to protect structure above
	routerJobs                 []types.RouterJobT      // slice to hold router jobs to send to destination transformer
	destinationJobs            []types.DestinationJobT // slice to hold destination jobs
	rt                         *HandleT                // handle to router
	deliveryTimeStat           stats.RudderStats
	routerDeliveryLatencyStat  stats.RudderStats
	routerProxyStat            stats.RudderStats
	batchTimeStat              stats.RudderStats
	abortedUserIDMap           map[string]map[int64]struct{} // aborted user to list of accepted jobs map
	abortedUserMutex           sync.RWMutex
	jobCountsByDestAndUser     map[string]*destJobCountsT
	throttledAtTime            time.Time
	latestAssignedTime         time.Time
	processingStartTime        time.Time
	encounteredRouterTransform bool
}

type destJobCountsT struct {
	total  int
	byUser map[string]int
}

var (
	jobQueryBatchSize, updateStatusBatchSize, noOfJobsPerChannel  int
	failedEventsCacheSize                                         int
	readSleep, minSleep, maxStatusUpdateWait, diagnosisTickerTime time.Duration
	minRetryBackoff, maxRetryBackoff, jobsBatchTimeout            time.Duration
	pkgLogger                                                     logger.LoggerI
	Diagnostics                                                   diagnostics.DiagnosticsI
	fixedLoopSleep                                                time.Duration
	toAbortDestinationIDs                                         string
	QueryFilters                                                  jobsdb.QueryFiltersT
	disableEgress                                                 bool
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
	config.RegisterIntConfigVariable(1000, &noOfJobsPerChannel, false, 1, "Router.noOfJobsPerChannel")
	config.RegisterDurationConfigVariable(5, &jobsBatchTimeout, true, time.Second, []string{"Router.jobsBatchTimeout", "Router.jobsBatchTimeoutInSec"}...)
	config.RegisterDurationConfigVariable(0, &minSleep, false, time.Second, []string{"Router.minSleep", "Router.minSleepInS"}...)
	config.RegisterDurationConfigVariable(5, &maxStatusUpdateWait, true, time.Second, []string{"Router.maxStatusUpdateWait", "Router.maxStatusUpdateWaitInS"}...)
	config.RegisterBoolConfigVariable(false, &disableEgress, false, "disableEgress")
	// Time period for diagnosis ticker
	config.RegisterDurationConfigVariable(60, &diagnosisTickerTime, false, time.Second, []string{"Diagnostics.routerTimePeriod", "Diagnostics.routerTimePeriodInS"}...)
	config.RegisterDurationConfigVariable(10, &minRetryBackoff, true, time.Second, []string{"Router.minRetryBackoff", "Router.minRetryBackoffInS"}...)
	config.RegisterDurationConfigVariable(300, &maxRetryBackoff, true, time.Second, []string{"Router.maxRetryBackoff", "Router.maxRetryBackoffInS"}...)
	config.RegisterDurationConfigVariable(0, &fixedLoopSleep, true, time.Millisecond, []string{"Router.fixedLoopSleep", "Router.fixedLoopSleepInMS"}...)
	config.RegisterIntConfigVariable(10, &failedEventsCacheSize, false, 1, "Router.failedEventsCacheSize")
	config.RegisterStringConfigVariable("", &toAbortDestinationIDs, true, "Router.toAbortDestinationIDs")
	// sources failed keys config
	config.RegisterDurationConfigVariable(48, &failedKeysExpire, true, time.Hour, "Router.failedKeysExpire")
	config.RegisterDurationConfigVariable(24, &failedKeysCleanUpSleep, true, time.Hour, "Router.failedKeysCleanUpSleep")
	failedKeysEnabled = config.GetBool("Router.failedKeysEnabled", false)
}

func (worker *workerT) trackStuckDelivery() chan struct{} {
	var d time.Duration
	if worker.rt.transformerProxy {
		d = (worker.rt.backendProxyTimeout + worker.rt.netClientTimeout) * 2
	} else {
		d = worker.rt.netClientTimeout * 2
	}

	ch := make(chan struct{}, 1)
	rruntime.Go(func() {
		select {
		case <-ch:
			// do nothing
		case <-time.After(d):
			worker.rt.logger.Infof("[%s Router] Delivery to destination exceeded the 2 * configured timeout ", worker.rt.destName)
			stat := stats.NewTaggedStat("router_delivery_exceeded_timeout", stats.CountType, stats.Tags{
				"destType": worker.rt.destName,
			})
			stat.Increment()
		}
	})
	return ch
}

func (worker *workerT) recordStatsForFailedTransforms(transformType string, transformedJobs []types.DestinationJobT) {
	for _, destJob := range transformedJobs {
		if destJob.StatusCode != http.StatusOK {
			transformFailedCountStat := stats.NewTaggedStat("router_transform_num_failed_jobs", stats.CountType, stats.Tags{
				"destType":      worker.rt.destName,
				"transformType": transformType,
				"statusCode":    strconv.Itoa(destJob.StatusCode),
				"destination":   destJob.Destination.ID,
			})
			transformFailedCountStat.Count(1)
		}
	}
}

func (worker *workerT) routerTransform(routerJobs []types.RouterJobT) []types.DestinationJobT {
	worker.rt.routerTransformInputCountStat.Count(len(routerJobs))
	destinationJobs := worker.rt.transformer.Transform(transformer.ROUTER_TRANSFORM, &types.TransformMessageT{Data: routerJobs, DestType: strings.ToLower(worker.rt.destName)})
	worker.rt.routerTransformOutputCountStat.Count(len(destinationJobs))
	worker.recordStatsForFailedTransforms("routerTransform", destinationJobs)
	return destinationJobs
}

func (worker *workerT) batch(routerJobs []types.RouterJobT) []types.DestinationJobT {
	inputJobsLength := len(routerJobs)
	worker.rt.batchInputCountStat.Count(inputJobsLength)
	destinationJobs := worker.rt.transformer.Transform(
		transformer.BATCH,
		&types.TransformMessageT{
			Data:     routerJobs,
			DestType: strings.ToLower(worker.rt.destName),
		},
	)
	worker.rt.batchOutputCountStat.Count(len(destinationJobs))
	worker.recordStatsForFailedTransforms("batch", destinationJobs)

	return destinationJobs
}

func (worker *workerT) workerProcess() {
	timeout := time.After(jobsBatchTimeout)
	for {
		select {
		case message, hasMore := <-worker.channel:
			if !hasMore {
				if len(worker.routerJobs) == 0 {
					worker.rt.logger.Debugf("[%s Router] :: Worker channel closed, processed %d jobs", worker.rt.destName, len(worker.routerJobs))
					return
				}

				if worker.rt.enableBatching {
					worker.destinationJobs = worker.batch(worker.routerJobs)
				} else {
					worker.destinationJobs = worker.routerTransform(worker.routerJobs)
				}
				worker.processDestinationJobs()
				worker.rt.logger.Debugf("[%s Router] :: Worker channel closed, processed %d jobs", worker.rt.destName, len(worker.routerJobs))
				return
			}

			job := message.job
			worker.throttledAtTime = message.throttledAtTime
			worker.rt.logger.Debugf("[%v Router] :: performing checks to send payload.", worker.rt.destName)

			userID := job.UserID

			var parameters JobParametersT
			err := json.Unmarshal(job.Parameters, &parameters)
			if err != nil {
				worker.rt.logger.Error("Unmarshal of job parameters failed. ", string(job.Parameters))
			}

			var isPrevFailedUser bool
			var previousFailedJobID int64
			if worker.rt.guaranteeUserEventOrder {
				// If there is a failed jobID from this user, we cannot pass future jobs
				worker.failedJobIDMutex.RLock()
				previousFailedJobID, isPrevFailedUser = worker.failedJobIDMap[userID]
				worker.failedJobIDMutex.RUnlock()

				// mark job as waiting if prev job from same user has not succeeded yet
				if isPrevFailedUser {
					markedAsWaiting := worker.handleJobForPrevFailedUser(job, userID, previousFailedJobID)
					if markedAsWaiting {
						worker.rt.logger.Debugf(`Decrementing in throttle map for destination:%s since job:%d is marked as waiting for user:%s`, parameters.DestinationID, job.JobID, userID)
						worker.rt.throttler.Dec(parameters.DestinationID, userID, 1, worker.throttledAtTime, throttler.ALL_LEVELS)
						continue
					}
				}
			}

			firstAttemptedAt := gjson.GetBytes(job.LastJobStatus.ErrorResponse, "firstAttemptedAt").Str

			jobMetadata := types.JobMetadataT{
				UserID:             userID,
				JobID:              job.JobID,
				SourceID:           parameters.SourceID,
				DestinationID:      parameters.DestinationID,
				AttemptNum:         job.LastJobStatus.AttemptNum,
				ReceivedAt:         parameters.ReceivedAt,
				CreatedAt:          job.CreatedAt.Format(misc.RFC3339Milli),
				FirstAttemptedAt:   firstAttemptedAt,
				TransformAt:        parameters.TransformAt,
				JobT:               job,
				WorkspaceID:        parameters.WorkspaceID,
				WorkerAssignedTime: message.workerAssignedTime,
			}

			worker.rt.configSubscriberLock.RLock()
			batchDestination, ok := worker.rt.destinationsMap[parameters.DestinationID]
			destination := batchDestination.Destination
			worker.rt.configSubscriberLock.RUnlock()
			if !ok {
				status := jobsdb.JobStatusT{
					JobID:         job.JobID,
					AttemptNum:    job.LastJobStatus.AttemptNum,
					JobState:      jobsdb.Aborted.State,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     "",
					ErrorResponse: []byte(`{"reason": "Aborted because destination is not available in the config" }`),
					Parameters:    routerutils.EmptyPayload,
					WorkspaceId:   job.WorkspaceId,
				}
				worker.rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID, JobT: job}
				continue
			}
			if authType := routerutils.GetAuthType(destination); routerutils.IsNotEmptyString(authType) && authType == "OAuth" {
				rudderAccountID := routerutils.GetRudderAccountId(&destination)
				if routerutils.IsNotEmptyString(rudderAccountID) {
					worker.rt.logger.Debugf(`[%s][FetchToken] Token Fetch Method to be called`, destination.DestinationDefinition.Name)
					// Get Access Token Information to send it as part of the event
					tokenStatusCode, accountSecretInfo := worker.rt.oauth.FetchToken(&oauth.RefreshTokenParams{
						AccountId:       rudderAccountID,
						WorkspaceId:     jobMetadata.WorkspaceID,
						DestDefName:     destination.DestinationDefinition.Name,
						EventNamePrefix: "fetch_token",
					})
					worker.rt.logger.Debugf(`[%s][FetchToken] Token Fetch Method finished (statusCode, value): (%v, %+v)`, destination.DestinationDefinition.Name, tokenStatusCode, accountSecretInfo)
					if tokenStatusCode == http.StatusOK {
						jobMetadata.Secret = accountSecretInfo.Account.Secret
					} else {
						worker.rt.logger.Errorf(`[%s][FetchToken] Error in Token Fetch statusCode: %d\t error: %s\n`, destination.DestinationDefinition.Name, tokenStatusCode, accountSecretInfo.Err)
					}
				}
			}

			worker.recordCountsByDestAndUser(destination.ID, userID)
			worker.encounteredRouterTransform = false

			if worker.rt.enableBatching {
				routerJob := types.RouterJobT{Message: job.EventPayload, JobMetadata: jobMetadata, Destination: destination}
				worker.routerJobs = append(worker.routerJobs, routerJob)
				if len(worker.routerJobs) >= worker.rt.noOfJobsToBatchInAWorker {
					worker.destinationJobs = worker.batch(worker.routerJobs)
					worker.processDestinationJobs()
				}
			} else if parameters.TransformAt == "router" {
				worker.encounteredRouterTransform = true
				routerJob := types.RouterJobT{Message: job.EventPayload, JobMetadata: jobMetadata, Destination: destination}
				worker.routerJobs = append(worker.routerJobs, routerJob)

				if len(worker.routerJobs) >= worker.rt.noOfJobsToBatchInAWorker {
					worker.destinationJobs = worker.routerTransform(worker.routerJobs)
					worker.processDestinationJobs()
				}
			} else {
				destinationJob := types.DestinationJobT{Message: job.EventPayload, JobMetadataArray: []types.JobMetadataT{jobMetadata}, Destination: destination}
				worker.destinationJobs = append(worker.destinationJobs, destinationJob)
				worker.processDestinationJobs()
			}

		case <-timeout:
			timeout = time.After(jobsBatchTimeout)

			if len(worker.routerJobs) > 0 {
				if worker.rt.enableBatching {
					worker.destinationJobs = worker.batch(worker.routerJobs)
				} else {
					worker.destinationJobs = worker.routerTransform(worker.routerJobs)
				}
				worker.processDestinationJobs()
			}
		}
	}
}

func (worker *workerT) processDestinationJobs() {
	ctx := context.TODO()
	worker.batchTimeStat.Start()

	var respContentType string
	var respStatusCode, prevRespStatusCode int
	var respBody string
	var respBodyTemp string

	var destinationResponseHandler ResponseHandlerI
	worker.rt.configSubscriberLock.RLock()
	destinationResponseHandler = worker.rt.destinationResponseHandler
	saveDestinationResponse := worker.rt.saveDestinationResponse
	worker.rt.configSubscriberLock.RUnlock()

	/*
		Batch
		[u1e1, u2e1, u1e2, u2e2, u1e3, u2e3]
		[b1, b2, b3]
		b1 will send if success
		b2 will send if b2 failed then will drop b3

		Router transform
		[u1e1, u2e1, u1e2, u2e2, u1e3, u2e3]
		200, 200, 500, 200, 200, 200

		Case 1:
		u1e1 will send - success
		u2e1 will send - success
		u1e2 will drop because transformer gave 500
		u2e2 will send - success
		u1e3 should be dropped because u1e2 should be retried
		u2e3 will send

		Case 2:
		u1e1 will send - success
		u2e1 will send - failed 5xx
		u1e2 will send
		u2e2 will drop - because request to destination failed with 5xx
		u1e3 will send
		u2e3 will drop - because request to destination failed with 5xx

		Case 3:
		u1e1 will send - success
		u2e1 will send - failed 4xx
		u1e2 will send
		u2e2 will send - because previous job is aborted
		u1e3 will send
		u2e3 will send
	*/

	failedUserIDsMap := make(map[string]struct{})
	apiCallsCount := make(map[string]*destJobCountsT)
	routerJobResponses := make([]*JobResponse, 0)

	sort.Slice(worker.destinationJobs, func(i, j int) bool {
		return worker.destinationJobs[i].JobMetadataArray[0].JobID < worker.destinationJobs[j].JobMetadataArray[0].JobID
	})

	for _, destinationJob := range worker.destinationJobs {
		var attemptedToSendTheJob bool
		respBodyArr := make([]string, 0)
		if destinationJob.StatusCode == 200 || destinationJob.StatusCode == 0 {
			if worker.canSendJobToDestination(prevRespStatusCode, failedUserIDsMap, &destinationJob) {
				diagnosisStartTime := time.Now()
				destinationID := destinationJob.JobMetadataArray[0].DestinationID

				worker.recordAPICallCount(apiCallsCount, destinationID, destinationJob.JobMetadataArray)
				transformAt := destinationJob.JobMetadataArray[0].TransformAt

				// START: request to destination endpoint
				worker.deliveryTimeStat.Start()
				workspaceID := destinationJob.JobMetadataArray[0].JobT.WorkspaceId
				deliveryLatencyStat := stats.NewTaggedStat("delivery_latency", stats.TimerType, stats.Tags{
					"module":      "router",
					"destType":    worker.rt.destName,
					"destination": misc.GetTagName(destinationJob.Destination.ID, destinationJob.Destination.Name),
					"workspaceId": workspaceID,
				})
				deliveryLatencyStat.Start()
				startedAt := time.Now()

				if worker.latestAssignedTime != destinationJob.JobMetadataArray[0].WorkerAssignedTime {
					worker.latestAssignedTime = destinationJob.JobMetadataArray[0].WorkerAssignedTime
					worker.processingStartTime = time.Now()
				}
				// TODO: remove trackStuckDelivery once we verify it is not needed,
				//			router_delivery_exceeded_timeout -> goes to zero
				ch := worker.trackStuckDelivery()

				// Assuming twice the overhead - defensive: 30% was just fine though
				// In fact, the timeout should be more than the maximum latency allowed by these workers.
				// Assuming 10s maximum latency
				elapsed := time.Since(worker.processingStartTime)
				threshold := worker.rt.routerTimeout
				if elapsed > threshold {
					respStatusCode = types.RouterTimedOutStatusCode
					respBody = fmt.Sprintf("Failed with status code %d as the jobs took more time than expected. Will be retried", types.RouterTimedOutStatusCode)
					worker.rt.logger.Debugf(
						"Will drop with %d because of time expiry %v",
						types.RouterTimedOutStatusCode, destinationJob.JobMetadataArray[0].JobID,
					)
				} else if worker.rt.customDestinationManager != nil {
					for _, destinationJobMetadata := range destinationJob.JobMetadataArray {
						if destinationID != destinationJobMetadata.DestinationID {
							panic(fmt.Errorf("different destinations are grouped together"))
						}
					}
					respStatusCode, respBody = worker.rt.customDestinationManager.SendData(destinationJob.Message, destinationID)
				} else {
					result, err := getIterableStruct(destinationJob.Message, transformAt)
					if err != nil {
						respStatusCode, respBody = types.RouterUnMarshalErrorCode, fmt.Errorf("transformer response unmarshal error: %w", err).Error()
					} else {
						for _, val := range result {
							err := integrations.ValidatePostInfo(val)
							if err != nil {
								respStatusCode, respBodyTemp = http.StatusBadRequest, fmt.Sprintf(`400 GetPostInfoFailed with error: %s`, err.Error())
								respBodyArr = append(respBodyArr, respBodyTemp)
							} else {
								// stat start
								pkgLogger.Debugf(`responseTransform status :%v, %s`, worker.rt.transformerProxy, worker.rt.destName)
								// transformer proxy start
								if worker.rt.transformerProxy {
									jobID := destinationJob.JobMetadataArray[0].JobID
									pkgLogger.Debugf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Request started`, worker.rt.destName, jobID)
									proxyReqparams := &transformer.ProxyRequestParams{
										DestName:     worker.rt.destName,
										JobID:        jobID,
										ResponseData: val,
									}
									rtlTime := time.Now()
									respStatusCode, respBodyTemp, respContentType = worker.rt.transformer.ProxyRequest(ctx, proxyReqparams)
									worker.routerProxyStat.SendTiming(time.Since(rtlTime))
									pkgLogger.Debugf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Request ended`, worker.rt.destName, jobID)
									authType := routerutils.GetAuthType(destinationJob.Destination)
									if routerutils.IsNotEmptyString(authType) && authType == "OAuth" {
										pkgLogger.Debugf(`Sending for OAuth destination`)
										// Token from header of the request
										respStatusCode, respBodyTemp = worker.rt.HandleOAuthDestResponse(&HandleDestOAuthRespParamsT{
											ctx:            ctx,
											destinationJob: destinationJob,
											workerID:       worker.workerID,
											trRespStCd:     respStatusCode,
											trRespBody:     respBodyTemp,
											secret:         destinationJob.JobMetadataArray[0].Secret,
										})
									}
								} else {
									sendCtx, cancel := context.WithTimeout(ctx, worker.rt.netClientTimeout)
									rdlTime := time.Now()
									resp := worker.rt.netHandle.SendPost(sendCtx, val)
									cancel()
									respStatusCode, respBodyTemp, respContentType = resp.StatusCode, string(resp.ResponseBody), resp.ResponseContentType
									// stat end
									worker.routerDeliveryLatencyStat.SendTiming(time.Since(rdlTime))
								}
								// transformer proxy end
								if isSuccessStatus(respStatusCode) {
									respBodyArr = append(respBodyArr, respBodyTemp)
								} else {
									respBodyArr = []string{respBodyTemp}
									break
								}
							}
						}
						respBody = strings.Join(respBodyArr, " ")
						if worker.rt.transformerProxy {
							stats.NewTaggedStat("transformer_proxy.input_events_count", stats.CountType, stats.Tags{
								"destType":      worker.rt.destName,
								"destinationId": destinationJob.Destination.ID,
								"workspace":     workspaceID,
								"workspaceId":   workspaceID,
							}).Count(len(result))

							pkgLogger.Debugf(`[TransformerProxy] (Dest-%v) {Job - %v} Input Router Events: %v, Out router events: %v`, worker.rt.destName,
								destinationJob.JobMetadataArray[0].JobID,
								len(result),
								len(respBodyArr),
							)

							stats.NewTaggedStat("transformer_proxy.output_events_count", stats.CountType, stats.Tags{
								"destType":      worker.rt.destName,
								"destinationId": destinationJob.Destination.ID,
								"workspace":     workspaceID,
								"workspaceId":   workspaceID,
							}).Count(len(respBodyArr))
						}
					}
				}
				ch <- struct{}{}
				timeTaken := time.Since(startedAt)
				if respStatusCode != types.RouterTimedOutStatusCode && respStatusCode != types.RouterUnMarshalErrorCode {
					worker.rt.MultitenantI.UpdateWorkspaceLatencyMap(worker.rt.destName, workspaceID, float64(timeTaken)/float64(time.Second))
				}

				// Using response status code and body to get response code rudder router logic is based on.
				// Works when transformer proxy in disabled
				if !worker.rt.transformerProxy && destinationResponseHandler != nil {
					respStatusCode = destinationResponseHandler.IsSuccessStatus(respStatusCode, respBody)
				}

				attemptedToSendTheJob = true

				worker.deliveryTimeStat.End()
				deliveryLatencyStat.End()

				// END: request to destination endpoint

				if isSuccessStatus(respStatusCode) && !worker.rt.saveDestinationResponseOverride {
					if saveDestinationResponse {
						if !getRouterConfigBool("saveDestinationResponse", worker.rt.destName, true) {
							respBody = ""
						}
					} else {
						respBody = ""
					}
				}

				worker.updateReqMetrics(respStatusCode, &diagnosisStartTime)
			} else {
				respStatusCode = http.StatusInternalServerError
				if !worker.rt.enableBatching {
					respBody = "skipping sending to destination because previous job (of user) in batch is failed."
				}
			}
		} else {
			respStatusCode = destinationJob.StatusCode
			respBody = destinationJob.Error
		}

		prevRespStatusCode = respStatusCode

		if !isJobTerminated(respStatusCode) {
			for _, metadata := range destinationJob.JobMetadataArray {
				failedUserIDsMap[metadata.UserID] = struct{}{}
			}
		}

		// assigning the destinationJob to a local variable (_destinationJob), so that
		// elements in routerJobResponses have pointer to the right job.
		_destinationJob := destinationJob

		for _, destinationJobMetadata := range _destinationJob.JobMetadataArray {
			_destinationJobMetadata := destinationJobMetadata
			// assigning the destinationJobMetadata to a local variable (_destinationJobMetadata), so that
			// elements in routerJobResponses have pointer to the right destinationJobMetadata.

			routerJobResponses = append(routerJobResponses, &JobResponse{
				jobID:                  destinationJobMetadata.JobID,
				destinationJob:         &_destinationJob,
				destinationJobMetadata: &_destinationJobMetadata,
				respStatusCode:         respStatusCode,
				respBody:               respBody,
				attemptedToSendTheJob:  attemptedToSendTheJob,
			})
		}
	}

	sort.Slice(routerJobResponses, func(i, j int) bool {
		return routerJobResponses[i].jobID < routerJobResponses[j].jobID
	})

	// Struct to hold unique users in the batch (worker.destinationJobs)
	userToJobIDMap := make(map[string]int64)

	for _, routerJobResponse := range routerJobResponses {
		destinationJobMetadata := routerJobResponse.destinationJobMetadata
		destinationJob := routerJobResponse.destinationJob
		attemptedToSendTheJob := routerJobResponse.attemptedToSendTheJob
		attemptNum := destinationJobMetadata.AttemptNum
		respStatusCode = routerJobResponse.respStatusCode
		status := jobsdb.JobStatusT{
			JobID:       destinationJobMetadata.JobID,
			AttemptNum:  attemptNum,
			ExecTime:    time.Now(),
			RetryTime:   time.Now(),
			Parameters:  routerutils.EmptyPayload,
			WorkspaceId: destinationJobMetadata.WorkspaceID,
		}

		routerJobResponse.status = &status

		if !isJobTerminated(respStatusCode) {
			if prevFailedJobID, ok := userToJobIDMap[destinationJobMetadata.UserID]; ok {
				// This means more than two jobs of the same user are in the batch & the batch job is failed
				// Only one job is marked failed and the rest are marked waiting
				// Job order logic requires that at any point of time, we should have only one failed job per user
				// This is introduced to ensure the above statement
				resp := misc.UpdateJSONWithNewKeyVal(routerutils.EmptyPayload, "blocking_id", prevFailedJobID)
				resp = misc.UpdateJSONWithNewKeyVal(resp, "user_id", destinationJobMetadata.UserID)
				resp = misc.UpdateJSONWithNewKeyVal(resp, "moreinfo", "attempted to send in a batch")

				status.JobState = jobsdb.Waiting.State
				status.ErrorResponse = resp
				worker.rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: destinationJobMetadata.UserID, JobT: destinationJobMetadata.JobT}
				continue
			}
			userToJobIDMap[destinationJobMetadata.UserID] = destinationJobMetadata.JobID
		}

		if attemptedToSendTheJob {
			status.AttemptNum++
		}

		status.ErrorResponse = routerutils.EmptyPayload
		status.ErrorCode = strconv.Itoa(respStatusCode)

		worker.postStatusOnResponseQ(respStatusCode, routerJobResponse.respBody, destinationJob.Message, respContentType, destinationJobMetadata, &status)

		worker.sendEventDeliveryStat(destinationJobMetadata, &status, &destinationJob.Destination)

		if attemptedToSendTheJob {
			worker.sendRouterResponseCountStat(&status, &destinationJob.Destination)
		}
	}

	// NOTE: Sending live events to config backend after the status objects are built completely.
	destLiveEventSentMap := make(map[*types.DestinationJobT]struct{})
	for _, routerJobResponse := range routerJobResponses {
		// Sending only one destination live event for every destinationJob, if it was attemptedToSendTheJob
		if _, ok := destLiveEventSentMap[routerJobResponse.destinationJob]; !ok && routerJobResponse.attemptedToSendTheJob {
			payload := routerJobResponse.destinationJob.Message
			if routerJobResponse.destinationJob.Message == nil {
				payload = routerJobResponse.destinationJobMetadata.JobT.EventPayload
			}
			sourcesIDs := make([]string, 0)
			for _, metadata := range routerJobResponse.destinationJob.JobMetadataArray {
				if !misc.ContainsString(sourcesIDs, metadata.SourceID) {
					sourcesIDs = append(sourcesIDs, metadata.SourceID)
				}
			}
			worker.sendDestinationResponseToConfigBackend(payload, routerJobResponse.destinationJobMetadata, routerJobResponse.status, sourcesIDs)
			destLiveEventSentMap[routerJobResponse.destinationJob] = struct{}{}
		}
	}

	worker.decrementInThrottleMap(apiCallsCount)
	worker.batchTimeStat.End()

	// routerJobs/destinationJobs are processed. Clearing the queues.
	worker.routerJobs = make([]types.RouterJobT, 0)
	worker.destinationJobs = make([]types.DestinationJobT, 0)
	worker.jobCountsByDestAndUser = make(map[string]*destJobCountsT)
}

func (worker *workerT) canSendJobToDestination(prevRespStatusCode int, failedUserIDsMap map[string]struct{}, destinationJob *types.DestinationJobT) bool {
	if prevRespStatusCode == 0 {
		return true
	}

	if !worker.rt.guaranteeUserEventOrder {
		// if guaranteeUserEventOrder is false, letting the next jobs pass
		return true
	}

	// If batching is enabled, we send the request only if the previous one succeeds
	if worker.rt.enableBatching {
		return isSuccessStatus(prevRespStatusCode)
	}

	// If the destinationJob has come through router transform,
	// drop the request if it is of a failed user, else send
	for i := range destinationJob.JobMetadataArray {
		if _, ok := failedUserIDsMap[destinationJob.JobMetadataArray[i].UserID]; ok {
			return false
		}
	}

	return true
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
	attemptedToSendTheJob  bool
	status                 *jobsdb.JobStatusT
}

func (worker *workerT) recordCountsByDestAndUser(destID, userID string) {
	if _, ok := worker.jobCountsByDestAndUser[destID]; !ok {
		worker.jobCountsByDestAndUser[destID] = &destJobCountsT{byUser: make(map[string]int)}
	}
	worker.jobCountsByDestAndUser[destID].total++
	if worker.rt.throttler.IsUserLevelEnabled() {
		if _, ok := worker.jobCountsByDestAndUser[destID].byUser[userID]; !ok {
			worker.jobCountsByDestAndUser[destID].byUser[userID] = 0
		}
		worker.jobCountsByDestAndUser[destID].byUser[userID]++
	}
}

func (worker *workerT) recordAPICallCount(apiCallsCount map[string]*destJobCountsT, destinationID string, jobMetadata []types.JobMetadataT) {
	if _, ok := apiCallsCount[destinationID]; !ok {
		apiCallsCount[destinationID] = &destJobCountsT{byUser: make(map[string]int)}
	}
	apiCallsCount[destinationID].total++
	if worker.rt.throttler.IsUserLevelEnabled() {
		var userIDs []string
		for _, metadata := range jobMetadata {
			userIDs = append(userIDs, metadata.UserID)
		}
		for _, userID := range misc.Unique(userIDs) {
			if _, ok := apiCallsCount[destinationID].byUser[userID]; !ok {
				apiCallsCount[destinationID].byUser[userID] = 0
			}
			apiCallsCount[destinationID].byUser[userID]++
		}
	}
}

// decrements counts in throttle map by the diff between api calls made to destinations and initial incremented ones in throttler
func (worker *workerT) decrementInThrottleMap(apiCallsCount map[string]*destJobCountsT) {
	for destID, incrementedMapByDestID := range worker.jobCountsByDestAndUser {
		if worker.rt.throttler.IsUserLevelEnabled() {
			for userID, incrementedCountByUserID := range incrementedMapByDestID.byUser {
				var sentCountByUserID int
				var ok bool
				if sentCountByUserID, ok = apiCallsCount[destID].byUser[userID]; !ok {
					sentCountByUserID = 0
				}
				diff := int64(incrementedCountByUserID - sentCountByUserID)
				if diff > 0 {
					// decrement only half to account for api call to be made again in router transform
					if worker.encounteredRouterTransform {
						diff = diff / 2
					}
					pkgLogger.Debugf(`Decrementing user level throttle map by %d for dest:%s, user:%s`, diff, destID, userID)
					worker.rt.throttler.Dec(destID, userID, diff, worker.throttledAtTime, throttler.USER_LEVEL)
				}
			}
		}
		if worker.rt.throttler.IsDestLevelEnabled() {
			var sentMapByDestID *destJobCountsT
			var ok bool
			if sentMapByDestID, ok = apiCallsCount[destID]; !ok {
				sentMapByDestID = &destJobCountsT{}
			}
			diff := int64(incrementedMapByDestID.total - sentMapByDestID.total)
			if diff > 0 {
				// decrement only half to account for api call to be made again in router transform
				if worker.encounteredRouterTransform {
					diff = diff / 2
				}
				pkgLogger.Debugf(`Decrementing destination level throttle map by %d for dest:%s`, diff, destID)
				worker.rt.throttler.Dec(destID, "", diff, worker.throttledAtTime, throttler.DESTINATION_LEVEL)
			}
		}
	}
}

func (worker *workerT) updateReqMetrics(respStatusCode int, diagnosisStartTime *time.Time) {
	var reqMetric requestMetric

	if isSuccessStatus(respStatusCode) {
		reqMetric.RequestSuccess = reqMetric.RequestSuccess + 1
	} else {
		reqMetric.RequestRetries = reqMetric.RequestRetries + 1
	}
	reqMetric.RequestCompletedTime = time.Since(*diagnosisStartTime)
	worker.rt.trackRequestMetrics(reqMetric)
}

func (worker *workerT) updateAbortedMetrics(destinationID, statusCode string) {
	eventsAbortedStat := stats.NewTaggedStat(`router_aborted_events`, stats.CountType, stats.Tags{
		"destType":       worker.rt.destName,
		"respStatusCode": statusCode,
		"destId":         destinationID,
	})
	eventsAbortedStat.Increment()
}

func (worker *workerT) postStatusOnResponseQ(respStatusCode int, respBody string, payload json.RawMessage,
	respContentType string, destinationJobMetadata *types.JobMetadataT, status *jobsdb.JobStatusT,
) {
	// Enhancing status.ErrorResponse with firstAttemptedAt
	firstAttemptedAtTime := time.Now()
	if destinationJobMetadata.FirstAttemptedAt != "" {
		t, err := time.Parse(misc.RFC3339Milli, destinationJobMetadata.FirstAttemptedAt)
		if err == nil {
			firstAttemptedAtTime = t
		}
	}

	status.ErrorResponse = routerutils.EnhanceJSON(status.ErrorResponse, "firstAttemptedAt", firstAttemptedAtTime.Format(misc.RFC3339Milli))
	status.ErrorResponse = routerutils.EnhanceJSON(status.ErrorResponse, "response", respBody)
	status.ErrorResponse = routerutils.EnhanceJSON(status.ErrorResponse, "content-type", respContentType)

	if isSuccessStatus(respStatusCode) {
		atomic.AddUint64(&worker.rt.successCount, 1)
		status.JobState = jobsdb.Succeeded.State
		worker.rt.logger.Debugf("[%v Router] :: sending success status to response", worker.rt.destName)
		worker.rt.responseQ <- jobResponseT{status: status, worker: worker, userID: destinationJobMetadata.UserID, JobT: destinationJobMetadata.JobT}

		// Deleting jobID from retryForJobMap. jobID goes into retryForJobMap if it is failed with 5xx or 429.
		// It's safe to delete from the map, even if jobID is not present.
		worker.retryForJobMapMutex.Lock()
		delete(worker.retryForJobMap, destinationJobMetadata.JobID)
		worker.retryForJobMapMutex.Unlock()
	} else {
		// Saving payload to DB only
		// 1. if job failed and
		// 2. if router job undergoes batching or dest transform.
		if payload != nil && (worker.rt.enableBatching || destinationJobMetadata.TransformAt == "router") {
			if worker.rt.savePayloadOnError {
				status.ErrorResponse = routerutils.EnhanceJSON(status.ErrorResponse, "payload", string(payload))
			}
		}
		// the job failed
		worker.rt.logger.Debugf("[%v Router] :: Job failed to send, analyzing...", worker.rt.destName)
		worker.failedJobs++
		atomic.AddUint64(&worker.rt.failCount, 1)

		status.JobState = jobsdb.Failed.State

		worker.rt.failedEventsChan <- *status

		if respStatusCode >= 500 {
			timeElapsed := time.Since(firstAttemptedAtTime)
			if respStatusCode != types.RouterTimedOutStatusCode && respStatusCode != types.RouterUnMarshalErrorCode {
				if timeElapsed > worker.rt.retryTimeWindow && status.AttemptNum >= worker.rt.maxFailedCountForJob {
					status.JobState = jobsdb.Aborted.State
					worker.retryForJobMapMutex.Lock()
					delete(worker.retryForJobMap, destinationJobMetadata.JobID)
					worker.retryForJobMapMutex.Unlock()
				} else {
					worker.retryForJobMapMutex.Lock()
					worker.retryForJobMap[destinationJobMetadata.JobID] = time.Now().Add(durationBeforeNextAttempt(status.AttemptNum))
					worker.retryForJobMapMutex.Unlock()
				}
			}
		} else if respStatusCode == 429 {
			worker.retryForJobMapMutex.Lock()
			worker.retryForJobMap[destinationJobMetadata.JobID] = time.Now().Add(durationBeforeNextAttempt(status.AttemptNum))
			worker.retryForJobMapMutex.Unlock()
		} else {
			status.JobState = jobsdb.Aborted.State
		}

		if status.JobState == jobsdb.Aborted.State {
			worker.updateAbortedMetrics(destinationJobMetadata.DestinationID, status.ErrorCode)
			destinationJobMetadata.JobT.Parameters = misc.UpdateJSONWithNewKeyVal(destinationJobMetadata.JobT.Parameters, "stage", "router")
			destinationJobMetadata.JobT.Parameters = misc.UpdateJSONWithNewKeyVal(destinationJobMetadata.JobT.Parameters, "reason", status.ErrorResponse) // NOTE: Old key used was "error_response"
		}

		if worker.rt.guaranteeUserEventOrder {
			if status.JobState == jobsdb.Failed.State {
				//#JobOrder (see other #JobOrder comment)
				worker.failedJobIDMutex.Lock()
				_, isPrevFailedUser := worker.failedJobIDMap[destinationJobMetadata.UserID]
				if !isPrevFailedUser && destinationJobMetadata.UserID != "" {
					worker.rt.logger.Debugf("[%v Router] :: userId %v failed for the first time adding to map", worker.rt.destName, destinationJobMetadata.UserID)
					worker.failedJobIDMap[destinationJobMetadata.UserID] = destinationJobMetadata.JobID
				}
				worker.failedJobIDMutex.Unlock()
			} else if status.JobState == jobsdb.Aborted.State {
				// Job is aborted.
				// So, adding the user to aborted map, if not already present.
				// If user is present in the aborted map, decrementing the count.
				// This map is used to limit the pickup of aborted user's job.
				worker.abortedUserMutex.Lock()
				worker.rt.logger.Debugf("[%v Router] :: adding userID to abortedUserMap : %s", worker.rt.destName, destinationJobMetadata.UserID)
				if _, ok := worker.abortedUserIDMap[destinationJobMetadata.UserID]; !ok {
					// this will enable the concurrency limiter
					worker.abortedUserIDMap[destinationJobMetadata.UserID] = map[int64]struct{}{}
				}
				worker.abortedUserMutex.Unlock()
			}
		}
		worker.rt.logger.Debugf("[%v Router] :: sending failed/aborted state as response", worker.rt.destName)
		worker.rt.responseQ <- jobResponseT{status: status, worker: worker, userID: destinationJobMetadata.UserID, JobT: destinationJobMetadata.JobT}
	}
}

func (worker *workerT) sendRouterResponseCountStat(status *jobsdb.JobStatusT, destination *backendconfig.DestinationT) {
	destinationTag := misc.GetTagName(destination.ID, destination.Name)
	routerResponseStat := stats.NewTaggedStat("router_response_counts", stats.CountType, stats.Tags{
		"destType":       worker.rt.destName,
		"respStatusCode": status.ErrorCode,
		"destination":    destinationTag,
		"attempt_number": strconv.Itoa(status.AttemptNum),
		"workspaceId":    status.WorkspaceId,
	})
	routerResponseStat.Count(1)
}

func (worker *workerT) sendEventDeliveryStat(destinationJobMetadata *types.JobMetadataT, status *jobsdb.JobStatusT, destination *backendconfig.DestinationT) {
	destinationTag := misc.GetTagName(destination.ID, destination.Name)
	if status.JobState == jobsdb.Succeeded.State {
		eventsDeliveredStat := stats.NewTaggedStat("event_delivery", stats.CountType, stats.Tags{
			"module":         "router",
			"destType":       worker.rt.destName,
			"destination":    destinationTag,
			"attempt_number": strconv.Itoa(status.AttemptNum),
			"workspaceId":    status.WorkspaceId,
		})
		eventsDeliveredStat.Count(1)
		if destinationJobMetadata.ReceivedAt != "" {
			receivedTime, err := time.Parse(misc.RFC3339Milli, destinationJobMetadata.ReceivedAt)
			if err == nil {
				eventsDeliveryTimeStat := stats.NewTaggedStat(
					"event_delivery_time", stats.TimerType, map[string]string{
						"module":         "router",
						"destType":       worker.rt.destName,
						"destination":    destinationTag,
						"attempt_number": strconv.Itoa(status.AttemptNum),
						"workspaceId":    status.WorkspaceId,
					})

				eventsDeliveryTimeStat.SendTiming(time.Since(receivedTime))
			}
		}
	}
}

func (*workerT) sendDestinationResponseToConfigBackend(payload json.RawMessage, destinationJobMetadata *types.JobMetadataT, status *jobsdb.JobStatusT, sourceIDs []string) {
	// Sending destination response to config backend
	if status.ErrorCode != fmt.Sprint(types.RouterUnMarshalErrorCode) && status.ErrorCode != fmt.Sprint(types.RouterTimedOutStatusCode) {
		deliveryStatus := destinationdebugger.DeliveryStatusT{
			DestinationID: destinationJobMetadata.DestinationID,
			SourceID:      strings.Join(sourceIDs, ","),
			Payload:       payload,
			AttemptNum:    status.AttemptNum,
			JobState:      status.JobState,
			ErrorCode:     status.ErrorCode,
			ErrorResponse: status.ErrorResponse,
			SentAt:        status.ExecTime.Format(misc.RFC3339Milli),
			EventName:     gjson.GetBytes(destinationJobMetadata.JobT.Parameters, "event_name").String(),
			EventType:     gjson.GetBytes(destinationJobMetadata.JobT.Parameters, "event_type").String(),
		}
		destinationdebugger.RecordEventDeliveryStatus(destinationJobMetadata.DestinationID, &deliveryStatus)
	}
}

func (worker *workerT) handleJobForPrevFailedUser(job *jobsdb.JobT, userID string, previousFailedJobID int64) (markedAsWaiting bool) {
	// job is behind in queue of failed job from same user
	if previousFailedJobID < job.JobID {
		worker.rt.logger.Debugf("[%v Router] :: skipping processing job for userID: %v since prev failed job exists, prev id %v, current id %v", worker.rt.destName, userID, previousFailedJobID, job.JobID)
		resp := misc.UpdateJSONWithNewKeyVal(routerutils.EmptyPayload, "blocking_id", previousFailedJobID)
		resp = misc.UpdateJSONWithNewKeyVal(resp, "user_id", userID)
		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			JobState:      jobsdb.Waiting.State,
			ErrorResponse: resp, // check
			Parameters:    routerutils.EmptyPayload,
			WorkspaceId:   job.WorkspaceId,
		}
		worker.rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID, JobT: job}
		return true
	}
	if previousFailedJobID != job.JobID {
		panic(fmt.Errorf("previousFailedJobID:%d != job.JobID:%d", previousFailedJobID, job.JobID))
	}
	return false
}

func durationBeforeNextAttempt(attempt int) (d time.Duration) {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = minRetryBackoff
	b.MaxInterval = maxRetryBackoff
	b.RandomizationFactor = 0
	b.MaxElapsedTime = 0
	b.Multiplier = 2
	b.Reset()
	for index := 0; index < attempt; index++ {
		d = b.NextBackOff()
	}
	return
}

func (rt *HandleT) addToFailedList(jobStatus *jobsdb.JobStatusT) {
	rt.failedEventsListMutex.Lock()
	defer rt.failedEventsListMutex.Unlock()
	if rt.failedEventsList.Len() == failedEventsCacheSize {
		firstEnqueuedStatus := rt.failedEventsList.Back()
		rt.failedEventsList.Remove(firstEnqueuedStatus)
	}
	rt.failedEventsList.PushFront(jobStatus)
}

func (rt *HandleT) readFailedJobStatusChan() {
	for jobStatus := range rt.failedEventsChan {
		rt.addToFailedList(&jobStatus)
	}
}

func (rt *HandleT) trackRequestMetrics(reqMetric requestMetric) {
	if diagnostics.EnableRouterMetric {
		rt.requestsMetricLock.Lock()
		rt.requestsMetric = append(rt.requestsMetric, reqMetric)
		rt.requestsMetricLock.Unlock()
	}
}

func (rt *HandleT) initWorkers() {
	rt.workers = make([]*workerT, rt.noOfWorkers)

	g, _ := errgroup.WithContext(context.Background())
	for i := 0; i < rt.noOfWorkers; i++ {
		worker := &workerT{
			channel:                   make(chan workerMessageT, noOfJobsPerChannel),
			failedJobIDMap:            make(map[string]int64),
			retryForJobMap:            make(map[int64]time.Time),
			workerID:                  i,
			failedJobs:                0,
			sleepTime:                 minSleep,
			routerJobs:                make([]types.RouterJobT, 0),
			destinationJobs:           make([]types.DestinationJobT, 0),
			rt:                        rt,
			deliveryTimeStat:          stats.NewTaggedStat("router_delivery_time", stats.TimerType, stats.Tags{"destType": rt.destName}),
			batchTimeStat:             stats.NewTaggedStat("router_batch_time", stats.TimerType, stats.Tags{"destType": rt.destName}),
			routerDeliveryLatencyStat: stats.NewTaggedStat("router_delivery_latency", stats.TimerType, stats.Tags{"destType": rt.destName}),
			routerProxyStat:           stats.NewTaggedStat("router_proxy_latency", stats.TimerType, stats.Tags{"destType": rt.destName}),
			abortedUserIDMap:          make(map[string]map[int64]struct{}),
			jobCountsByDestAndUser:    make(map[string]*destJobCountsT),
		}
		rt.workers[i] = worker

		g.Go(misc.WithBugsnag(func() error {
			worker.workerProcess()
			return nil
		}))
	}
	rt.backgroundGroup.Go(func() error {
		err := g.Wait()

		// clean up channels workers are publishing to:
		close(rt.responseQ)
		rt.logger.Debugf("[%v Router] :: closing responseQ", rt.destName)
		close(rt.failedEventsChan)
		rt.logger.Debugf("[%v Router] :: closing failedEventsChan", rt.destName)

		return err
	})
}

func (rt *HandleT) stopWorkers() {
	for _, worker := range rt.workers {
		// FIXME remove paused worker, use shutdown instead
		close(worker.channel)
	}
}

func (rt *HandleT) findWorker(job *jobsdb.JobT, throttledAtTime time.Time) (toSendWorker *workerT) {
	if rt.backgroundCtx.Err() != nil {
		return nil
	}

	// checking if this job can be throttled
	var parameters JobParametersT
	userID := job.UserID

	// checking if the user is in throttledMap. If yes, returning nil.
	// this check is done to maintain order.
	if _, ok := rt.throttledUserMap[userID]; ok {
		rt.logger.Debugf(`[%v Router] :: Skipping processing of job:%d of user:%s as user has earlier jobs in throttled map`, rt.destName, job.JobID, userID)
		return nil
	}

	err := json.Unmarshal(job.Parameters, &parameters)
	if err != nil {
		rt.logger.Errorf(`[%v Router] :: Unmarshalling parameters failed with the error %v . Returning nil worker`, err)
		return nil
	}

	if rt.shouldThrottle(parameters.DestinationID, userID, throttledAtTime) {
		rt.throttledUserMap[userID] = struct{}{}
		rt.logger.Debugf(`[%v Router] :: Skipping processing of job:%d of user:%s as throttled limits exceeded`, rt.destName, job.JobID, userID)
		return nil
	}

	if !rt.guaranteeUserEventOrder {
		// if guaranteeUserEventOrder is false, assigning worker randomly and returning here.
		return rt.workers[rand.Intn(rt.noOfWorkers)]
	}

	index := rt.getWorkerPartition(userID)

	worker := rt.workers[index]

	//#JobOrder (see other #JobOrder comment)
	worker.failedJobIDMutex.RLock()
	defer worker.failedJobIDMutex.RUnlock()
	blockJobID, found := worker.failedJobIDMap[userID]
	if !found {
		// not a failed user
		// checking if he is an aborted user,
		// if yes returning worker only for 1 job
		worker.abortedUserMutex.Lock()
		defer worker.abortedUserMutex.Unlock()
		if runningJobIDs, ok := worker.abortedUserIDMap[userID]; ok {
			count := len(runningJobIDs)
			_, hasJobID := runningJobIDs[job.JobID]

			if !hasJobID && count >= rt.allowAbortedUserJobsCountForProcessing {
				rt.logger.Debugf("[%v Router] :: allowed jobs count(%d) >= allowAbortedUserJobsCountForProcessing(%d) for userID %s. returning nil worker", rt.destName, count, rt.allowAbortedUserJobsCountForProcessing, userID)
				return nil
			}

			rt.logger.Debugf("[%v Router] :: userID found in abortedUserIDtoJobMap: %s. Allowing jobID: %d. returning worker", rt.destName, userID, job.JobID)
			// adding job to abortedUserIDMap after all checks of backoff, throttle etc are made
			// We don't need lock inside this defer func, because we already hold the lock above and this
			// defer is called before defer Unlock
			defer func() {
				if toSendWorker != nil {
					toSendWorker.abortedUserIDMap[userID][job.JobID] = struct{}{}
				}
			}()
		}
		toSendWorker = worker
	} else {
		// This job can only be higher than blocking
		// We only let the blocking job pass
		if job.JobID < blockJobID {
			panic(fmt.Errorf("job.JobID:%d < blockJobID:%d", job.JobID, blockJobID))
		}
		if job.JobID == blockJobID {
			toSendWorker = worker
		}
	}

	// checking if this job can be backoff
	if toSendWorker != nil {
		if toSendWorker.canBackoff(job) {
			toSendWorker = nil
		}
	}

	return toSendWorker
	//#EndJobOrder
}

func (worker *workerT) canBackoff(job *jobsdb.JobT) (shouldBackoff bool) {
	// if the same job has failed before, check for next retry time
	worker.retryForJobMapMutex.RLock()
	defer worker.retryForJobMapMutex.RUnlock()
	if nextRetryTime, ok := worker.retryForJobMap[job.JobID]; ok && time.Until(nextRetryTime) > 0 {
		worker.rt.logger.Debugf("[%v Router] :: Less than next retry time: %v", worker.rt.destName, nextRetryTime)
		return true
	}
	return false
}

func (rt *HandleT) getWorkerPartition(userID string) int {
	return misc.GetHash(userID) % rt.noOfWorkers
}

func (rt *HandleT) shouldThrottle(destID, userID string, throttledAtTime time.Time) (canBeThrottled bool) {
	if !rt.throttler.IsEnabled() {
		return false
	}

	// No need of locks here, because this is used only by a single goroutine (generatorLoop)
	limitReached := rt.throttler.CheckLimitReached(destID, userID, throttledAtTime)
	if !limitReached {
		rt.throttler.Inc(destID, userID, throttledAtTime)
	}
	return limitReached
}

// ResetSleep  this makes the workers reset their sleep
func (rt *HandleT) ResetSleep() {
	for _, w := range rt.workers {
		w.sleepTime = minSleep
	}
}

func (rt *HandleT) commitStatusList(responseList *[]jobResponseT) {
	reportMetrics := make([]*utilTypes.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*utilTypes.ConnectionDetails)
	transformedAtMap := make(map[string]string)
	statusDetailsMap := make(map[string]*utilTypes.StatusDetail)
	routerWorkspaceJobStatusCount := make(map[string]int)
	jobRunIDAbortedEventsMap := make(map[string][]*FailedEventRowT)
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
		key := fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s", parameters.SourceID, parameters.DestinationID, parameters.SourceBatchID, resp.status.JobState, resp.status.ErrorCode, eventName, eventType)
		_, ok := connectionDetailsMap[key]
		if !ok {
			cd := utilTypes.CreateConnectionDetail(parameters.SourceID, parameters.DestinationID, parameters.SourceBatchID, parameters.SourceTaskID, parameters.SourceTaskRunID, parameters.SourceJobID, parameters.SourceJobRunID, parameters.SourceDefinitionID, parameters.DestinationDefinitionID, parameters.SourceCategory)
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
			sd = utilTypes.CreateStatusDetail(resp.status.JobState, 0, errorCode, string(resp.status.ErrorResponse), sampleEvent, eventName, eventType)
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
			routerWorkspaceJobStatusCount[workspaceID] += 1
			sd.Count++
			rt.MultitenantI.CalculateSuccessFailureCounts(workspaceID, rt.destName, true, false)
			completedJobsList = append(completedJobsList, resp.JobT)
		case jobsdb.Aborted.State:
			routerWorkspaceJobStatusCount[workspaceID] += 1
			sd.Count++
			rt.MultitenantI.CalculateSuccessFailureCounts(workspaceID, rt.destName, false, true)
			routerAbortedJobs = append(routerAbortedJobs, resp.JobT)
			PrepareJobRunIdAbortedEventsMap(resp.JobT.Parameters, jobRunIDAbortedEventsMap)
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

				rt.failureMetricLock.Lock()
				if _, ok := rt.failuresMetric[event][string(resp.status.ErrorResponse)]; !ok {
					rt.failuresMetric[event] = make(map[string]int)
				}
				rt.failuresMetric[event][string(resp.status.ErrorResponse)] += 1
				rt.failureMetricLock.Unlock()
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

	defer func() {
		for workspace := range routerWorkspaceJobStatusCount {
			metric.DecreasePendingEvents("rt", workspace, rt.destName, float64(routerWorkspaceJobStatusCount[workspace]))
		}
	}()

	if len(statusList) > 0 {
		rt.logger.Debugf("[%v Router] :: flushing batch of %v status", rt.destName, updateStatusBatchSize)

		sort.Slice(statusList, func(i, j int) bool {
			return statusList[i].JobID < statusList[j].JobID
		})
		// Store the aborted jobs to errorDB
		if routerAbortedJobs != nil {
			err := misc.RetryWith(context.Background(), rt.jobsDBCommandTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) error {
				return rt.errorDB.Store(ctx, routerAbortedJobs)
			})
			if err != nil {
				panic(fmt.Errorf("storing jobs into ErrorDB: %w", err))
			}
		}
		// Update the status
		err := misc.RetryWith(context.Background(), rt.jobsDBCommandTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) error {
			return rt.jobsDB.WithUpdateSafeTx(func(tx jobsdb.UpdateSafeTx) error {
				err := rt.jobsDB.UpdateJobStatusInTx(ctx, tx, statusList, []string{rt.destName}, nil)
				if err != nil {
					return fmt.Errorf("updating %s jobs statuses: %w", rt.destName, err)
				}

				// rsources stats
				err = rt.updateRudderSourcesStats(ctx, tx, completedJobsList, statusList)
				if err != nil {
					return err
				}

				// Save msgids of aborted jobs
				if len(jobRunIDAbortedEventsMap) > 0 {
					GetFailedEventsManager().SaveFailedRecordIDs(jobRunIDAbortedEventsMap, tx.Tx())
				}
				rt.Reporting.Report(reportMetrics, tx.Tx())
				return nil
			})
		})
		if err != nil {
			panic(err)
		}
		rt.updateProcessedEventsMetrics(statusList)
	}

	if rt.guaranteeUserEventOrder {
		//#JobOrder (see other #JobOrder comment)
		for _, resp := range *responseList {
			status := resp.status.JobState
			userID := resp.userID
			worker := resp.worker
			rt.eventOrderSyncMu.Lock()

			if status == jobsdb.Succeeded.State || status == jobsdb.Aborted.State {
				worker.failedJobIDMutex.RLock()
				lastJobID, ok := worker.failedJobIDMap[userID]
				worker.failedJobIDMutex.RUnlock()
				if ok && lastJobID == resp.status.JobID {
					rt.logger.Debugf("[%v Router] :: clearing failedJobIDMap for userID: %v", rt.destName, userID)
					_, ok := rt.toClearFailJobIDMap[worker.workerID]
					if !ok {
						rt.toClearFailJobIDMap[worker.workerID] = make([]string, 0)
					}
					rt.toClearFailJobIDMap[worker.workerID] = append(rt.toClearFailJobIDMap[worker.workerID], userID)
				}
			}

			// aborted jobIDs
			worker.abortedUserMutex.RLock()
			if runningJobs, ok := worker.abortedUserIDMap[userID]; ok {
				if status == jobsdb.Succeeded.State {
					// clear the map in the next generatorLoop
					_, ok := rt.toClearAbortedJobIDMap[worker.workerID]
					if !ok {
						rt.toClearAbortedJobIDMap[worker.workerID] = make([]string, 0)
					}
					rt.toClearAbortedJobIDMap[worker.workerID] = append(rt.toClearAbortedJobIDMap[worker.workerID], userID)
				} else { // any other state
					if _, ok = runningJobs[resp.status.JobID]; ok {
						// remove the jobID from the concurrency map during the next generatorLoop
						_, ok := rt.toRemoveAbortedJobIDMap[worker.workerID]
						if !ok {
							rt.toRemoveAbortedJobIDMap[worker.workerID] = map[string]map[int64]struct{}{}
						}
						if _, ok := rt.toRemoveAbortedJobIDMap[worker.workerID][userID]; !ok {
							rt.toRemoveAbortedJobIDMap[worker.workerID][userID] = make(map[int64]struct{})
						}
						rt.toRemoveAbortedJobIDMap[worker.workerID][userID][resp.status.JobID] = struct{}{}
					}
				}
			}
			worker.abortedUserMutex.RUnlock()

			rt.eventOrderSyncMu.Unlock()
		}
		// End #JobOrder
	}
}

// statusInsertLoop will run in a separate goroutine
// Blocking method, returns when rt.responseQ channel is closed.
func (rt *HandleT) statusInsertLoop() {
	var responseList []jobResponseT

	// Wait for the responses from statusQ
	lastUpdate := time.Now()

	statusStat := stats.NewTaggedStat("router_status_loop", stats.TimerType, stats.Tags{"destType": rt.destName})
	countStat := stats.NewTaggedStat("router_status_events", stats.CountType, stats.Tags{"destType": rt.destName})
	timeout := time.After(maxStatusUpdateWait)

	for {
		rt.perfStats.Start()
		select {
		case jobStatus, hasMore := <-rt.responseQ:
			if !hasMore {
				if len(responseList) == 0 {
					rt.logger.Debugf("[%v Router] :: statusInsertLoop exiting", rt.destName)
					return
				}

				statusStat.Start()
				rt.commitStatusList(&responseList)
				countStat.Count(len(responseList))
				responseList = nil
				statusStat.End()

				rt.perfStats.End(0)
				rt.logger.Debugf("[%v Router] :: statusInsertLoop exiting", rt.destName)
				return
			}
			rt.logger.Debugf(
				"[%v Router] :: Got back status error %v and state %v for job %v",
				rt.destName,
				jobStatus.status.ErrorCode,
				jobStatus.status.JobState,
				jobStatus.status.JobID,
			)
			responseList = append(responseList, jobStatus)
			rt.perfStats.End(1)
		case <-timeout:
			timeout = time.After(maxStatusUpdateWait)
			rt.perfStats.End(0)
			// Ideally should sleep for duration maxStatusUpdateWait-(time.Now()-lastUpdate)
			// but approx is good enough at the cost of reduced computation.
		}
		if len(responseList) >= updateStatusBatchSize || time.Since(lastUpdate) > maxStatusUpdateWait {
			statusStat.Start()
			rt.commitStatusList(&responseList)
			countStat.Count(len(responseList))
			responseList = nil
			lastUpdate = time.Now()
			statusStat.End()
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
		case <-rt.diagnosisTicker.C:
		}
		rt.requestsMetricLock.RLock()
		var diagnosisProperties map[string]interface{}
		retries := 0
		aborted := 0
		success := 0
		var compTime time.Duration
		for _, reqMetric := range rt.requestsMetric {
			retries += reqMetric.RequestRetries
			aborted += reqMetric.RequestAborted
			success += reqMetric.RequestSuccess
			compTime += reqMetric.RequestCompletedTime
		}
		if len(rt.requestsMetric) > 0 {
			diagnosisProperties = map[string]interface{}{
				rt.destName: map[string]interface{}{
					diagnostics.RouterAborted:       aborted,
					diagnostics.RouterRetries:       retries,
					diagnostics.RouterSuccess:       success,
					diagnostics.RouterCompletedTime: (compTime / time.Duration(len(rt.requestsMetric))) / time.Millisecond,
				},
			}

			Diagnostics.Track(diagnostics.RouterEvents, diagnosisProperties)
		}

		rt.requestsMetric = nil
		rt.requestsMetricLock.RUnlock()

		// This lock will ensure we don't send out Track Request while filling up the
		// failureMetric struct
		rt.failureMetricLock.Lock()
		for key, value := range rt.failuresMetric {
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
		rt.failuresMetric = make(map[string]map[string]int)
		rt.failureMetricLock.Unlock()
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
	rt.logger.Info("Generator started")

	generatorStat := stats.NewTaggedStat("router_generator_loop", stats.TimerType, stats.Tags{"destType": rt.destName})
	countStat := stats.NewTaggedStat("router_generator_events", stats.CountType, stats.Tags{"destType": rt.destName})

	timeout := time.After(10 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			rt.logger.Infof("Generator exiting for router %s", rt.destName)
			return
		case <-timeout:
			timeout = time.After(10 * time.Millisecond)

			generatorStat.Start()

			processCount := rt.readAndProcess()

			countStat.Count(processCount)
			generatorStat.End()

			timeToSleep := 0 * time.Nanosecond
			timeElapsed := time.Since(rt.lastQueryRunTime)
			if timeElapsed < time.Second {
				timeToSleep = time.Second - timeElapsed
			}
			if timeToSleep < fixedLoopSleep {
				timeToSleep = fixedLoopSleep
			}
			time.Sleep(timeToSleep)
		}
	}
}

func (rt *HandleT) readAndProcess() int {
	if rt.guaranteeUserEventOrder {
		//#JobOrder (See comment marked #JobOrder
		rt.eventOrderSyncMu.Lock()

		// clearing failed job ids
		for idx := range rt.toClearFailJobIDMap {
			wrk := rt.workers[idx]
			wrk.failedJobIDMutex.Lock()
			for _, userID := range rt.toClearFailJobIDMap[idx] {
				delete(wrk.failedJobIDMap, userID)
			}
			wrk.failedJobIDMutex.Unlock()
		}
		rt.toClearFailJobIDMap = make(map[int][]string)

		// clearing aborted job concurrency maps (disabling concurrency limiter for aborted jobs)
		for idx := range rt.toClearAbortedJobIDMap {
			wrk := rt.workers[idx]
			wrk.abortedUserMutex.Lock()
			for _, userID := range rt.toClearAbortedJobIDMap[idx] {
				delete(wrk.abortedUserIDMap, userID)
			}
			wrk.abortedUserMutex.Unlock()
		}
		rt.toClearAbortedJobIDMap = make(map[int][]string)

		// removing jobs from aborted job concurrency maps (letting room for new jobs to be picked up)
		for idx := range rt.toRemoveAbortedJobIDMap {
			wrk := rt.workers[idx]
			wrk.abortedUserMutex.Lock()
			for userID, jobIDs := range rt.toRemoveAbortedJobIDMap[idx] {
				abortedJobs, ok := wrk.abortedUserIDMap[userID]
				if ok {
					for jobID := range jobIDs {
						delete(abortedJobs, jobID)
					}
				}
			}
			wrk.abortedUserMutex.Unlock()
		}
		rt.toRemoveAbortedJobIDMap = make(map[int]map[string]map[int64]struct{})

		rt.eventOrderSyncMu.Unlock()
		// End of #JobOrder
	}

	timeOut := rt.routerTimeout

	timeElapsed := time.Since(rt.lastQueryRunTime)

	if timeElapsed < timeOut {
		timeOut = timeElapsed
	}

	rt.lastQueryRunTime = time.Now()

	pickupMap, latenciesUsed := rt.MultitenantI.GetRouterPickupJobs(rt.destName, rt.noOfWorkers, timeOut, jobQueryBatchSize, rt.timeGained)
	totalPickupCount := 0
	for _, pickup := range pickupMap {
		if pickup > 0 {
			totalPickupCount += pickup
		}
	}
	rt.timeGained = 0
	rt.logger.Debugf("[%v Router] :: pickupMap: %+v", rt.destName, pickupMap)
	combinedList, err := jobsdb.QueryJobsWithRetries(context.Background(), rt.jobdDBQueryRequestTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) ([]*jobsdb.JobT, error) {
		return rt.jobsDB.GetAllJobs(
			ctx,
			pickupMap,
			jobsdb.GetQueryParamsT{
				CustomValFilters: []string{rt.destName},
				PayloadSizeLimit: rt.payloadLimit,
				JobsLimit:        totalPickupCount,
			},
			rt.maxDSQuerySize,
		)
	})
	if err != nil {
		rt.logger.Errorf("[%v Router] :: Error getting jobs from DB: %v", rt.destName, err)
		panic(err)
	}

	if len(combinedList) == 0 {
		rt.logger.Debugf("RT: DB Read Complete. No RT Jobs to process for destination: %s", rt.destName)
		time.Sleep(readSleep)
		return 0
	}

	sort.Slice(combinedList, func(i, j int) bool {
		return combinedList[i].JobID < combinedList[j].JobID
	})

	if len(combinedList) > 0 {
		rt.logger.Debugf("[%v Router] :: router is enabled", rt.destName)
		rt.logger.Debugf("[%v Router] ===== len to be processed==== : %v", rt.destName, len(combinedList))
	}

	// List of jobs which can be processed mapped per channel
	type workerJobT struct {
		worker *workerT
		job    *jobsdb.JobT
	}

	var statusList []*jobsdb.JobStatusT
	var drainList []*jobsdb.JobStatusT
	var drainJobList []*jobsdb.JobT
	drainStatsbyDest := make(map[string]*routerutils.DrainStats)

	var toProcess []workerJobT

	rt.throttledUserMap = make(map[string]struct{})
	throttledAtTime := time.Now()
	connectionDetailsMap := make(map[string]*utilTypes.ConnectionDetails)
	statusDetailsMap := make(map[string]*utilTypes.StatusDetail)
	transformedAtMap := make(map[string]string)
	// Identify jobs which can be processed
	for _, job := range combinedList {
		destID := destinationID(job)
		rt.configSubscriberLock.RLock()
		drain, reason := routerutils.ToBeDrained(job, destID, toAbortDestinationIDs, rt.destinationsMap)
		rt.configSubscriberLock.RUnlock()
		if drain {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				JobState:      jobsdb.Aborted.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     strconv.Itoa(routerutils.DRAIN_ERROR_CODE),
				Parameters:    routerutils.EmptyPayload,
				ErrorResponse: routerutils.EnhanceJSON(routerutils.EmptyPayload, "reason", reason),
				WorkspaceId:   job.WorkspaceId,
			}
			// Enhancing job parameter with the drain reason.
			job.Parameters = routerutils.EnhanceJSON(job.Parameters, "stage", "router")
			job.Parameters = routerutils.EnhanceJSON(job.Parameters, "reason", reason)
			drainList = append(drainList, &status)
			drainJobList = append(drainJobList, job)
			if _, ok := drainStatsbyDest[destID]; !ok {
				drainStatsbyDest[destID] = &routerutils.DrainStats{
					Count:     0,
					Reasons:   []string{},
					Workspace: job.WorkspaceId,
				}
			}
			drainStatsbyDest[destID].Count = drainStatsbyDest[destID].Count + 1
			if !misc.ContainsString(drainStatsbyDest[destID].Reasons, reason) {
				drainStatsbyDest[destID].Reasons = append(drainStatsbyDest[destID].Reasons, reason)
			}

			rt.timeGained += latenciesUsed[job.WorkspaceId]

			rt.MultitenantI.CalculateSuccessFailureCounts(job.WorkspaceId, rt.destName, false, true)

			// REPORTING - ROUTER - START
			var parameters JobParametersT
			err := json.Unmarshal(job.Parameters, &parameters)
			if err != nil {
				rt.logger.Error("Unmarshal of job parameters failed. ", string(job.Parameters))
				continue
			}

			eventName := gjson.GetBytes(job.Parameters, "event_name").String()
			eventType := gjson.GetBytes(job.Parameters, "event_type").String()

			key := fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s", parameters.SourceID, parameters.DestinationID, parameters.SourceBatchID, status.JobState, status.ErrorCode, eventName, eventType)
			_, ok := connectionDetailsMap[key]
			if !ok {
				cd := utilTypes.CreateConnectionDetail(parameters.SourceID, parameters.DestinationID, parameters.SourceBatchID, parameters.SourceTaskID, parameters.SourceTaskRunID, parameters.SourceJobID, parameters.SourceJobRunID, parameters.SourceDefinitionID, parameters.DestinationDefinitionID, parameters.SourceCategory)
				connectionDetailsMap[key] = cd
				transformedAtMap[key] = parameters.TransformAt
			}

			sd, ok := statusDetailsMap[key]
			if !ok {
				sampleEvent := job.EventPayload
				if rt.transientSources.Apply(parameters.SourceID) {
					sampleEvent = routerutils.EmptyPayload
				}
				sd = utilTypes.CreateStatusDetail(status.JobState, 0, routerutils.DRAIN_ERROR_CODE, string(status.ErrorResponse), sampleEvent, eventName, eventType)
				statusDetailsMap[key] = sd
			}
			sd.Count++
			// REPORTING - ROUTER - END
			continue
		}
		w := rt.findWorker(job, throttledAtTime)
		if w != nil {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				JobState:      jobsdb.Executing.State,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "",
				ErrorResponse: routerutils.EmptyPayload, // check
				Parameters:    routerutils.EmptyPayload,
				WorkspaceId:   job.WorkspaceId,
			}
			statusList = append(statusList, &status)
			toProcess = append(toProcess, workerJobT{worker: w, job: job})
		}
	}
	rt.throttledUserMap = nil

	// Mark the jobs as executing
	err = misc.RetryWith(context.Background(), rt.jobsDBCommandTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) error {
		return rt.jobsDB.UpdateJobStatus(ctx, statusList, []string{rt.destName}, nil)
	})
	if err != nil {
		pkgLogger.Errorf("Error occurred while marking %s jobs statuses as executing. Panicking. Err: %v", rt.destName, err)
		panic(err)
	}

	// Mark the jobs as aborted
	if len(drainList) > 0 {
		err := misc.RetryWith(context.Background(), rt.jobsDBCommandTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) error {
			return rt.errorDB.Store(ctx, drainJobList)
		})
		if err != nil {
			pkgLogger.Errorf("Error occurred while storing %s jobs into ErrorDB. Panicking. Err: %v", rt.destName, err)
			panic(err)
		}
		// REPORTING - ROUTER - START
		utilTypes.AssertSameKeys(connectionDetailsMap, statusDetailsMap)
		reportMetrics := make([]*utilTypes.PUReportedMetric, 0)
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

		err = misc.RetryWith(context.Background(), rt.jobsDBCommandTimeout, rt.jobdDBMaxRetries, func(ctx context.Context) error {
			return rt.jobsDB.WithUpdateSafeTx(func(tx jobsdb.UpdateSafeTx) error {
				err := rt.jobsDB.UpdateJobStatusInTx(ctx, tx, drainList, []string{rt.destName}, nil)
				if err != nil {
					return fmt.Errorf("marking %s job statuses as aborted: %w", rt.destName, err)
				}
				// rsources stats
				err = rt.updateRudderSourcesStats(ctx, tx, drainJobList, drainList)
				if err != nil {
					return err
				}

				rt.Reporting.Report(reportMetrics, tx.Tx())
				return nil
			})
		})
		if err != nil {
			panic(err)
		}
		rt.updateProcessedEventsMetrics(drainList)
		for destID, destDrainStat := range drainStatsbyDest {
			drainedJobsStat := stats.NewTaggedStat(`drained_events`, stats.CountType, stats.Tags{
				"destType":    rt.destName,
				"destId":      destID,
				"module":      "router",
				"reasons":     strings.Join(destDrainStat.Reasons, ", "),
				"workspaceId": destDrainStat.Workspace,
			})
			drainedJobsStat.Count(destDrainStat.Count)
			metric.DecreasePendingEvents("rt", destDrainStat.Workspace, rt.destName, float64(drainStatsbyDest[destID].Count))
		}

		if rt.guaranteeUserEventOrder {
			for _, drainedJob := range drainJobList {
				// cleanup failedJobIDMap for drained jobs
				drainedUserID := drainedJob.UserID
				partition := rt.getWorkerPartition(drainedUserID)
				drainWorker := rt.workers[partition]

				drainWorker.failedJobIDMutex.Lock()
				lastJobID, ok := drainWorker.failedJobIDMap[drainedUserID]
				if ok && lastJobID == drainedJob.JobID {
					delete(drainWorker.failedJobIDMap, drainedUserID)
				}
				drainWorker.failedJobIDMutex.Unlock()
			}
		}
	}
	rt.logger.Debugf("[DRAIN DEBUG] counts  %v final jobs length being processed %v", rt.destName, len(toProcess))

	if len(toProcess) == 0 {
		rt.logger.Debugf("RT: No workers found for the jobs. Sleeping. Destination: %s", rt.destName)
		time.Sleep(readSleep)
		return 0
	}

	workerAssignedTime := time.Now()
	// Send the jobs to the jobQ
	for _, wrkJob := range toProcess {
		wrkJob.worker.channel <- workerMessageT{job: wrkJob.job, throttledAtTime: throttledAtTime, workerAssignedTime: workerAssignedTime}
	}

	return len(toProcess)
}

func destinationID(job *jobsdb.JobT) string {
	return gjson.GetBytes(job.Parameters, "destination_id").String()
}

func (*HandleT) crashRecover() {
	// NO-OP
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("router")
	QueryFilters = jobsdb.QueryFiltersT{CustomVal: true}
	Diagnostics = diagnostics.Diagnostics
}

// Setup initializes this module
func (rt *HandleT) Setup(backendConfig backendconfig.BackendConfig, jobsDB jobsdb.MultiTenantJobsDB, errorDB jobsdb.JobsDB, destinationDefinition backendconfig.DestinationDefinitionT, transientSources transientsource.Service, rsourcesService rsources.JobService) {
	rt.backendConfig = backendConfig
	rt.workspaceSet = make(map[string]struct{})

	destName := destinationDefinition.Name
	rt.logger = pkgLogger.Child(destName)
	rt.logger.Info("Router started: ", destName)

	rt.transientSources = transientSources
	rt.rsourcesService = rsourcesService

	// waiting for reporting client setup
	rt.Reporting.WaitForSetup(context.TODO(), utilTypes.CORE_REPORTING_CLIENT)

	rt.diagnosisTicker = time.NewTicker(diagnosisTickerTime)
	rt.jobsDB = jobsDB
	rt.errorDB = errorDB
	rt.destName = destName
	netClientTimeoutKeys := []string{"Router." + rt.destName + "." + "httpTimeout", "Router." + rt.destName + "." + "httpTimeoutInS", "Router." + "httpTimeout", "Router." + "httpTimeoutInS"}
	config.RegisterDurationConfigVariable(10, &rt.netClientTimeout, false, time.Second, netClientTimeoutKeys...)
	config.RegisterDurationConfigVariable(30, &rt.backendProxyTimeout, false, time.Second, "HttpClient.backendProxy.timeout")
	config.RegisterDurationConfigVariable(60, &rt.jobdDBQueryRequestTimeout, true, time.Second, []string{"JobsDB.Router.QueryRequestTimeout", "JobsDB.QueryRequestTimeout"}...)
	config.RegisterDurationConfigVariable(90, &rt.jobsDBCommandTimeout, true, time.Second, []string{"JobsDB.Router.CommandRequestTimeout", "JobsDB.CommandRequestTimeout"}...)
	config.RegisterIntConfigVariable(3, &rt.jobdDBMaxRetries, true, 1, []string{"JobsDB." + "Router." + "MaxRetries", "JobsDB." + "MaxRetries"}...)
	rt.crashRecover()
	rt.responseQ = make(chan jobResponseT, jobQueryBatchSize)
	rt.toClearFailJobIDMap = make(map[int][]string)
	rt.toClearAbortedJobIDMap = make(map[int][]string)
	rt.toRemoveAbortedJobIDMap = make(map[int]map[string]map[int64]struct{})

	rt.failedEventsList = list.New()
	rt.failedEventsChan = make(chan jobsdb.JobStatusT)

	if rt.netHandle == nil {
		netHandle := &NetHandleT{}
		netHandle.logger = rt.logger.Child("network")
		netHandle.Setup(destName, rt.netClientTimeout)
		rt.netHandle = netHandle
	}

	rt.perfStats = &misc.PerfStats{}
	rt.perfStats.Setup("StatsUpdate:" + destName)
	rt.customDestinationManager = customDestinationManager.New(destName, customDestinationManager.Opts{
		Timeout: rt.netClientTimeout,
	})
	rt.failuresMetric = make(map[string]map[string]int)

	rt.destinationResponseHandler = New(destinationDefinition.ResponseRules)
	if value, ok := destinationDefinition.Config["saveDestinationResponse"].(bool); ok {
		rt.saveDestinationResponse = value
	}
	rt.guaranteeUserEventOrder = getRouterConfigBool("guaranteeUserEventOrder", rt.destName, true)
	rt.noOfWorkers = getRouterConfigInt("noOfWorkers", destName, 64)
	maxFailedCountKeys := []string{"Router." + rt.destName + "." + "maxFailedCountForJob", "Router." + "maxFailedCountForJob"}
	retryTimeWindowKeys := []string{"Router." + rt.destName + "." + "retryTimeWindow", "Router." + rt.destName + "." + "retryTimeWindowInMins", "Router." + "retryTimeWindow", "Router." + "retryTimeWindowInMins"}
	savePayloadOnErrorKeys := []string{"Router." + rt.destName + "." + "savePayloadOnError", "Router." + "savePayloadOnError"}
	transformerProxyKeys := []string{"Router." + rt.destName + "." + "transformerProxy", "Router." + "transformerProxy"}
	saveDestinationResponseOverrideKeys := []string{"Router." + rt.destName + "." + "saveDestinationResponseOverride", "Router." + "saveDestinationResponseOverride"}
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
	config.RegisterBoolConfigVariable(false, &rt.enableBatching, false, "Router."+rt.destName+"."+"enableBatching")
	config.RegisterBoolConfigVariable(false, &rt.savePayloadOnError, true, savePayloadOnErrorKeys...)
	config.RegisterBoolConfigVariable(false, &rt.transformerProxy, true, transformerProxyKeys...)
	config.RegisterBoolConfigVariable(false, &rt.saveDestinationResponseOverride, true, saveDestinationResponseOverrideKeys...)
	rt.allowAbortedUserJobsCountForProcessing = getRouterConfigInt("allowAbortedUserJobsCountForProcessing", destName, 1)

	rt.batchInputCountStat = stats.NewTaggedStat("router_batch_num_input_jobs", stats.CountType, stats.Tags{
		"destType": rt.destName,
	})
	rt.batchOutputCountStat = stats.NewTaggedStat("router_batch_num_output_jobs", stats.CountType, stats.Tags{
		"destType": rt.destName,
	})

	rt.routerTransformInputCountStat = stats.NewTaggedStat("router_transform_num_input_jobs", stats.CountType, stats.Tags{
		"destType": rt.destName,
	})
	rt.routerTransformOutputCountStat = stats.NewTaggedStat("router_transform_num_output_jobs", stats.CountType, stats.Tags{
		"destType": rt.destName,
	})

	rt.batchInputOutputDiffCountStat = stats.NewTaggedStat("router_batch_input_output_diff_jobs", stats.CountType, stats.Tags{
		"destType": rt.destName,
	})

	rt.routerResponseTransformStat = stats.NewTaggedStat("response_transform_latency", stats.TimerType, stats.Tags{"destType": rt.destName})

	rt.transformer = transformer.NewTransformer(rt.netClientTimeout, rt.backendProxyTimeout)

	rt.oauth = oauth.NewOAuthErrorHandler(backendConfig)
	rt.oauth.Setup()

	var t throttler.HandleT
	t.SetUp(rt.destName)
	rt.throttler = &t

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
		rt.readFailedJobStatusChan()
		return nil
	}))
	g.Go(misc.WithBugsnag(func() error {
		rt.statusInsertLoop()
		return nil
	}))

	rruntime.Go(func() {
		rt.backendConfigSubscriber()
	})
	adminInstance.registerRouter(destName, rt)
}

func (rt *HandleT) Start() {
	rt.logger.Infof("Starting router: %s", rt.destName)
	rt.startEnded = make(chan struct{})
	ctx := rt.backgroundCtx

	rt.backgroundGroup.Go(func() error {
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
	})
}

func (rt *HandleT) Shutdown() {
	if rt.startEnded == nil {
		// router is not started
		return
	}
	rt.logger.Infof("Shutting down router: %s", rt.destName)
	rt.backgroundCancel()

	<-rt.startEnded
	_ = rt.backgroundWait()
}

func (rt *HandleT) backendConfigSubscriber() {
	ch := rt.backendConfig.Subscribe(context.TODO(), backendconfig.TopicBackendConfig)
	for configEvent := range ch {
		rt.configSubscriberLock.Lock()
		rt.destinationsMap = map[string]*routerutils.BatchDestinationT{}
		allSources := configEvent.Data.(backendconfig.ConfigT)
		rt.sourceIDWorkspaceMap = map[string]string{}
		for i := range allSources.Sources {
			source := &allSources.Sources[i]
			workspaceID := source.WorkspaceID
			rt.sourceIDWorkspaceMap[source.ID] = source.WorkspaceID
			if _, ok := rt.workspaceSet[workspaceID]; !ok {
				rt.workspaceSet[workspaceID] = struct{}{}
				rt.MultitenantI.UpdateWorkspaceLatencyMap(rt.destName, workspaceID, 0)
			}
			if len(source.Destinations) > 0 {
				for i := range source.Destinations {
					destination := &source.Destinations[i]
					if destination.DestinationDefinition.Name == rt.destName {
						if _, ok := rt.destinationsMap[destination.ID]; !ok {
							rt.destinationsMap[destination.ID] = &routerutils.BatchDestinationT{Destination: *destination, Sources: []backendconfig.SourceT{}}
						}
						rt.destinationsMap[destination.ID].Sources = append(rt.destinationsMap[destination.ID].Sources, *source)

						rt.destinationResponseHandler = New(destination.DestinationDefinition.ResponseRules)
						if value, ok := destination.DestinationDefinition.Config["saveDestinationResponse"].(bool); ok {
							rt.saveDestinationResponse = value
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
		// destErrDetailed := destErrOutput.Output
		// Check the category
		// Trigger the refresh endpoint/disable endpoint
		rudderAccountID := routerutils.GetRudderAccountId(&destinationJob.Destination)
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
				stats.NewTaggedStat(oauth.INVALID_REFRESH_TOKEN_GRANT, stats.CountType, stats.Tags{
					"destinationId": destinationJob.Destination.ID,
					"workspaceId":   refTokenParams.WorkspaceId,
					"accountId":     refTokenParams.AccountId,
					"destType":      refTokenParams.DestDefName,
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

func (rt *HandleT) ExecDisableDestination(destination *backendconfig.DestinationT, workspaceId, destResBody, rudderAccountId string) (int, string) {
	disableDestStatTags := stats.Tags{
		"id":          destination.ID,
		"destType":    destination.DestinationDefinition.Name,
		"workspaceId": workspaceId,
		"success":     "true",
	}
	errCatStatusCode, errCatResponse := rt.oauth.DisableDestination(destination, workspaceId, rudderAccountId)
	if errCatStatusCode != http.StatusOK {
		// Error while disabling a destination
		// High-Priority notification to rudderstack needs to be sent
		disableDestStatTags["success"] = "false"
		stats.NewTaggedStat("disable_destination_category_count", stats.CountType, disableDestStatTags).Increment()
		return http.StatusBadRequest, errCatResponse
	}
	// High-Priority notification to workspace(& rudderstack) needs to be sent
	stats.NewTaggedStat("disable_destination_category_count", stats.CountType, disableDestStatTags).Increment()
	// Abort the jobs as the destination is disabled
	return http.StatusBadRequest, destResBody
}

func PrepareJobRunIdAbortedEventsMap(parameters json.RawMessage, jobRunIDAbortedEventsMap map[string][]*FailedEventRowT) {
	taskRunID := gjson.GetBytes(parameters, "source_task_run_id").String()
	destinationID := gjson.GetBytes(parameters, "destination_id").String()
	recordID := json.RawMessage(gjson.GetBytes(parameters, "record_id").Raw)
	if taskRunID == "" {
		return
	}
	if _, ok := jobRunIDAbortedEventsMap[taskRunID]; !ok {
		jobRunIDAbortedEventsMap[taskRunID] = []*FailedEventRowT{}
	}
	jobRunIDAbortedEventsMap[taskRunID] = append(jobRunIDAbortedEventsMap[taskRunID], &FailedEventRowT{DestinationID: destinationID, RecordID: recordID})
}

func (rt *HandleT) updateRudderSourcesStats(ctx context.Context, tx jobsdb.UpdateSafeTx, jobs []*jobsdb.JobT, jobStatuses []*jobsdb.JobStatusT) error {
	rsourcesStats := rsources.NewStatsCollector(rt.rsourcesService)
	rsourcesStats.BeginProcessing(jobs)
	rsourcesStats.JobStatusesUpdated(jobStatuses)
	err := rsourcesStats.Publish(ctx, tx.Tx())
	if err != nil {
		rt.logger.Errorf("publishing rsources stats: %w", err)
	}
	return err
}

func (rt *HandleT) updateProcessedEventsMetrics(statusList []*jobsdb.JobStatusT) {
	eventsPerState := map[string]int{}
	for i := range statusList {
		state := statusList[i].JobState
		eventsPerState[state]++
	}
	for state, count := range eventsPerState {
		stats.NewTaggedStat(`pipeline_processed_events`, stats.CountType, stats.Tags{
			"module":   "router",
			"destType": rt.destName,
			"state":    state,
		}).Count(count)
	}
}
