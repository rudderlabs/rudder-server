package router

import (
	"container/list"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/customdestinationmanager"
	customDestinationManager "github.com/rudderlabs/rudder-server/router/customdestinationmanager"
	"github.com/rudderlabs/rudder-server/router/drain"
	"github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/router/types"
	"github.com/rudderlabs/rudder-server/services/diagnostics"
	"github.com/rudderlabs/rudder-server/utils"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
	"github.com/thoas/go-funk"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/debugger/destination"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type PauseT struct {
	respChannel   chan bool
	wg            *sync.WaitGroup
	waitForResume bool
}

//HandleT is the handle to this module.
type HandleT struct {
	pausingWorkers                         bool
	paused                                 bool
	pauseLock                              sync.Mutex
	generatorPauseChannel                  chan *PauseT
	generatorResumeChannel                 chan bool
	statusLoopPauseChannel                 chan *PauseT
	statusLoopResumeChannel                chan bool
	requestQ                               chan *jobsdb.JobT
	responseQ                              chan jobResponseT
	jobsDB                                 *jobsdb.HandleT
	errorDB                                jobsdb.JobsDB
	netHandle                              *NetHandleT
	destName                               string
	workers                                []*workerT
	perfStats                              *misc.PerfStats
	successCount                           uint64
	failCount                              uint64
	failedEventsListMutex                  sync.RWMutex
	failedEventsList                       *list.List
	failedEventsChan                       chan jobsdb.JobStatusT
	isEnabled                              bool
	toClearFailJobIDMutex                  sync.Mutex
	toClearFailJobIDMap                    map[int][]string
	requestsMetricLock                     sync.RWMutex
	failureMetricLock                      sync.RWMutex
	diagnosisTicker                        *time.Ticker
	requestsMetric                         []requestMetric
	failuresMetric                         map[string][]failureMetric
	customDestinationManager               customdestinationmanager.DestinationManager
	throttler                              throttler.Throttler
	throttlerMutex                         sync.RWMutex
	guaranteeUserEventOrder                bool
	netClientTimeout                       time.Duration
	enableBatching                         bool
	transformer                            transformer.Transformer
	configSubscriberLock                   sync.RWMutex
	destinationsMap                        map[string]backendconfig.DestinationT // destinationID -> destination
	logger                                 logger.LoggerI
	batchInputCountStat                    stats.RudderStats
	batchOutputCountStat                   stats.RudderStats
	batchInputOutputDiffCountStat          stats.RudderStats
	eventsAbortedStat                      stats.RudderStats
	drainedJobsStat                        stats.RudderStats
	noOfWorkers                            int
	allowAbortedUserJobsCountForProcessing int
	throttledUserMap                       map[string]struct{} // used before calling findWorker. A temp storage to save <userid> whose job can be throttled.
	isBackendConfigInitialized             bool
	backendConfigInitialized               chan bool
	maxFailedCountForJob                   int
	retryTimeWindow                        time.Duration
	destinationResponseHandler             ResponseHandlerI
	saveDestinationResponse                bool
	drainJobHandler                        drain.DrainI
	reporting                              utilTypes.ReportingI
	reportingEnabled                       bool
}

type jobResponseT struct {
	status *jobsdb.JobStatusT
	worker *workerT
	userID string
	JobT   *jobsdb.JobT
}

//JobParametersT struct holds source id and destination id of a job
type JobParametersT struct {
	SourceID        string `json:"source_id"`
	DestinationID   string `json:"destination_id"`
	ReceivedAt      string `json:"received_at"`
	TransformAt     string `json:"transform_at"`
	SourceBatchID   string `json:"source_batch_id"`
	SourceTaskID    string `json:"source_task_id"`
	SourceTaskRunID string `json:"source_task_run_id"`
	SourceJobID     string `json:"source_job_id"`
	SourceJobRunID  string `json:"source_job_run_id"`
}

type workerMessageT struct {
	job             *jobsdb.JobT
	throttledAtTime time.Time
}

// workerT a structure to define a worker for sending events to sinks
type workerT struct {
	pauseChannel               chan *PauseT
	resumeChannel              chan bool
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
	batchTimeStat              stats.RudderStats
	abortedUserIDMap           map[string]int // aborted user to count of jobs allowed map
	abortedUserMutex           sync.RWMutex
	jobCountsByDestAndUser     map[string]*destJobCountsT
	throttledAtTime            time.Time
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
	noOfJobsToBatchInAWorker                                      int
	pkgLogger                                                     logger.LoggerI
	Diagnostics                                                   diagnostics.DiagnosticsI = diagnostics.Diagnostics
	fixedLoopSleep                                                time.Duration
	toAbortDestinationIDs                                         string
)

type requestMetric struct {
	RequestRetries       int
	RequestAborted       int
	RequestSuccess       int
	RequestCompletedTime time.Duration
}

type failureMetric struct {
	RouterDestination string          `json:"router_destination"`
	UserId            string          `json:"user_id"`
	RouterAttemptNum  int             `json:"router_attempt_num"`
	ErrorCode         string          `json:"error_code"`
	ErrorResponse     json.RawMessage `json:"error_response"`
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
	config.RegisterDurationConfigVariable(time.Duration(1000), &readSleep, true, time.Millisecond, "Router.readSleepInMS")
	noOfJobsPerChannel = config.GetInt("Router.noOfJobsPerChannel", 1000)
	noOfJobsToBatchInAWorker = config.GetInt("Router.noOfJobsToBatchInAWorker", 20)
	config.RegisterDurationConfigVariable(time.Duration(5), &jobsBatchTimeout, true, time.Second, "Router.jobsBatchTimeoutInSec")
	minSleep = config.GetDuration("Router.minSleepInS", time.Duration(0)) * time.Second
	config.RegisterDurationConfigVariable(time.Duration(5), &maxStatusUpdateWait, true, time.Second, "Router.maxStatusUpdateWaitInS")

	// Time period for diagnosis ticker
	diagnosisTickerTime = config.GetDuration("Diagnostics.routerTimePeriodInS", 60) * time.Second
	config.RegisterDurationConfigVariable(time.Duration(10), &minRetryBackoff, true, time.Second, "Router.minRetryBackoffInS")
	config.RegisterDurationConfigVariable(time.Duration(300), &maxRetryBackoff, true, time.Second, "Router.maxRetryBackoffInS")
	config.RegisterDurationConfigVariable(time.Duration(0), &fixedLoopSleep, true, time.Millisecond, "Router.fixedLoopSleepInMS")
	failedEventsCacheSize = config.GetInt("Router.failedEventsCacheSize", 10)
	config.RegisterStringConfigVariable("", &toAbortDestinationIDs, true, "Router.toAbortDestinationIDs")
}

func (worker *workerT) trackStuckDelivery() chan struct{} {
	ch := make(chan struct{}, 1)
	rruntime.Go(func() {
		select {
		case <-ch:
			// do nothing
		case <-time.After(worker.rt.netClientTimeout * 2):
			worker.rt.logger.Infof("[%s Router] Delivery to destination exceeded the 2 * configured timeout ", worker.rt.destName)
			stat := stats.NewTaggedStat("router_delivery_exceeded_timeout", stats.CountType, stats.Tags{
				"destType": worker.rt.destName,
			})
			stat.Increment()
		}
	})
	return ch
}

func (worker *workerT) routerTransform(routerJobs []types.RouterJobT) []types.DestinationJobT {
	destinationJobs := worker.rt.transformer.Transform(transformer.ROUTER_TRANSFORM, &types.TransformMessageT{Data: routerJobs, DestType: strings.ToLower(worker.rt.destName)})
	return destinationJobs
}

func (worker *workerT) batch(routerJobs []types.RouterJobT) []types.DestinationJobT {

	inputJobsLength := len(routerJobs)
	worker.rt.batchInputCountStat.Count(inputJobsLength)

	destinationJobs := worker.rt.transformer.Transform(transformer.BATCH, &types.TransformMessageT{Data: routerJobs, DestType: strings.ToLower(worker.rt.destName)})
	worker.rt.batchOutputCountStat.Count(len(destinationJobs))

	var totalJobMetadataCount int
	for _, destinationJob := range destinationJobs {
		totalJobMetadataCount += len(destinationJob.JobMetadataArray)
	}

	if inputJobsLength != totalJobMetadataCount {
		worker.rt.batchInputOutputDiffCountStat.Count(inputJobsLength - totalJobMetadataCount)

		worker.rt.logger.Errorf("[%v Router] :: Total input jobs count:%d did not match total job metadata count:%d returned from batch transformer", worker.rt.destName, inputJobsLength, totalJobMetadataCount)
		jobIDs := make([]string, len(routerJobs))
		for idx, routerJob := range routerJobs {
			jobIDs[idx] = fmt.Sprintf("%v", routerJob.JobMetadata.JobID)
		}
		worker.rt.logger.Errorf("[%v Router] :: Job ids : %s", worker.rt.destName, strings.Join(jobIDs, ", "))
	}

	return destinationJobs
}

func (worker *workerT) workerProcess() {

	timeout := time.After(jobsBatchTimeout)
	for {
		select {
		case pause := <-worker.pauseChannel:
			//Drain the channel
			for len(worker.channel) > 0 {
				<-worker.channel
			}

			//Clear buffers
			worker.routerJobs = make([]types.RouterJobT, 0)
			worker.destinationJobs = make([]types.DestinationJobT, 0)
			worker.jobCountsByDestAndUser = make(map[string]*destJobCountsT)

			pkgLogger.Infof("Router worker %d is paused. Dest type: %s", worker.workerID, worker.rt.destName)
			pause.wg.Done()
			pause.respChannel <- true
			if pause.waitForResume {
				<-worker.resumeChannel
				pkgLogger.Infof("Router worker %d is resumed. Dest type: %s", worker.workerID, worker.rt.destName)
			}
		case message := <-worker.channel:
			if worker.rt.pausingWorkers {
				continue
			}

			job := message.job
			worker.throttledAtTime = message.throttledAtTime
			worker.rt.logger.Debugf("[%v Router] :: performing checks to send payload to %s. Payload: ", worker.rt.destName, job.EventPayload)

			userID := job.UserID

			var parameters JobParametersT
			err := json.Unmarshal(job.Parameters, &parameters)
			if err != nil {
				worker.rt.logger.Error("Unmarshal of job parameters failed. ", string(job.Parameters))
			}

			var isPrevFailedUser bool
			var previousFailedJobID int64
			if worker.rt.guaranteeUserEventOrder {
				//If there is a failed jobID from this user, we cannot pass future jobs
				worker.failedJobIDMutex.RLock()
				previousFailedJobID, isPrevFailedUser = worker.failedJobIDMap[userID]
				worker.failedJobIDMutex.RUnlock()

				// mark job as waiting if prev job from same user has not succeeded yet
				if isPrevFailedUser {
					markedAsWaiting := worker.handleJobForPrevFailedUser(job, parameters, userID, previousFailedJobID)
					if markedAsWaiting {
						worker.rt.logger.Debugf(`Decrementing in throttle map for destination:%s since job:%d is marked as waiting for user:%s`, parameters.DestinationID, job.JobID, userID)
						worker.rt.throttler.Dec(parameters.DestinationID, userID, 1, worker.throttledAtTime, throttler.ALL_LEVELS)
						continue
					}
				}
			}

			firstAttemptedAt := gjson.GetBytes(job.LastJobStatus.ErrorResponse, "firstAttemptedAt").Str

			jobMetadata := types.JobMetadataT{
				UserID:           userID,
				JobID:            job.JobID,
				SourceID:         parameters.SourceID,
				DestinationID:    parameters.DestinationID,
				AttemptNum:       job.LastJobStatus.AttemptNum,
				ReceivedAt:       parameters.ReceivedAt,
				CreatedAt:        job.CreatedAt.Format(misc.RFC3339Milli),
				FirstAttemptedAt: firstAttemptedAt,
				TransformAt:      parameters.TransformAt,
				JobT:             job}

			worker.rt.configSubscriberLock.RLock()
			destination := worker.rt.destinationsMap[parameters.DestinationID]
			worker.rt.configSubscriberLock.RUnlock()

			worker.recordCountsByDestAndUser(destination.ID, userID)
			worker.encounteredRouterTransform = false
			if worker.rt.enableBatching {
				routerJob := types.RouterJobT{Message: job.EventPayload, JobMetadata: jobMetadata, Destination: destination}
				worker.routerJobs = append(worker.routerJobs, routerJob)

				if len(worker.routerJobs) >= noOfJobsToBatchInAWorker {
					worker.destinationJobs = worker.batch(worker.routerJobs)
					worker.processDestinationJobs()
				}
			} else if parameters.TransformAt == "router" {
				worker.encounteredRouterTransform = true
				routerJob := types.RouterJobT{Message: job.EventPayload, JobMetadata: jobMetadata, Destination: destination}
				worker.routerJobs = append(worker.routerJobs, routerJob)

				if len(worker.routerJobs) >= noOfJobsToBatchInAWorker {
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
			if worker.rt.pausingWorkers {
				continue
			}

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
	worker.handleWorkerDestinationJobs()
	//routerJobs/destinationJobs are processed. Clearing the queues.
	worker.routerJobs = make([]types.RouterJobT, 0)
	worker.destinationJobs = make([]types.DestinationJobT, 0)
	worker.jobCountsByDestAndUser = make(map[string]*destJobCountsT)
}

func (worker *workerT) canSendJobToDestination(prevRespStatusCode int, failedUserIDsMap map[string]struct{}, destinationJob types.DestinationJobT) bool {
	if prevRespStatusCode == 0 {
		return true
	}

	if !worker.rt.guaranteeUserEventOrder {
		//if guaranteeUserEventOrder is false, letting the next jobs pass
		return true
	}

	//If batching is enabled, we send the request only if the previous one succeeds
	if worker.rt.enableBatching {
		return isSuccessStatus(prevRespStatusCode)
	}

	//If the destinationJob has come through router transform,
	//drop the request if it is of a failed user, else send
	for _, metadata := range destinationJob.JobMetadataArray {
		if _, ok := failedUserIDsMap[metadata.UserID]; ok {
			return false
		}
	}

	return true
}

//rawMsg passed must be a valid JSON
func (worker *workerT) enhanceResponse(rawMsg []byte, key, val string) []byte {
	resp, err := sjson.SetBytes(rawMsg, key, val)
	if err != nil {
		return []byte(`{}`)
	}

	return resp
}

func getIterableStruct(payload []byte, transformAt string) []integrations.PostParametersT {
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
	if err != nil {
		panic(fmt.Errorf("setting Payload through sjson failed with err %v", err))
	}
	return responseArray
}

func (worker *workerT) handleWorkerDestinationJobs() {
	worker.batchTimeStat.Start()

	var respStatusCode, prevRespStatusCode int
	var respBody string
	var respBodyTemp string
	handledJobMetadatas := make(map[int64]*types.JobMetadataT)

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
	for _, destinationJob := range worker.destinationJobs {
		var attemptedToSendTheJob bool
		respBodyArr := make([]string, 0)
		if destinationJob.StatusCode == 200 || destinationJob.StatusCode == 0 {
			if worker.canSendJobToDestination(prevRespStatusCode, failedUserIDsMap, destinationJob) {
				diagnosisStartTime := time.Now()
				sourceID := destinationJob.JobMetadataArray[0].SourceID
				destinationID := destinationJob.JobMetadataArray[0].DestinationID

				worker.recordAPICallCount(apiCallsCount, destinationID, destinationJob.JobMetadataArray)
				transformAt := destinationJob.JobMetadataArray[0].TransformAt

				// START: request to destination endpoint
				worker.deliveryTimeStat.Start()
				deliveryLatencyStat := stats.NewTaggedStat("delivery_latency", stats.TimerType, stats.Tags{
					"module":      "router",
					"destType":    worker.rt.destName,
					"destination": misc.GetTagName(destinationJob.Destination.ID, destinationJob.Destination.Name),
				})
				deliveryLatencyStat.Start()

				ch := worker.trackStuckDelivery()
				if worker.rt.customDestinationManager != nil {
					for _, destinationJobMetadata := range destinationJob.JobMetadataArray {
						if sourceID != destinationJobMetadata.SourceID {
							panic(fmt.Errorf("different sources are grouped together"))
						}
						if destinationID != destinationJobMetadata.DestinationID {
							panic(fmt.Errorf("different destinations are grouped together"))
						}
					}
					respStatusCode, respBody = worker.rt.customDestinationManager.SendData(destinationJob.Message, sourceID, destinationID)
				} else {
					result := getIterableStruct(destinationJob.Message, transformAt)
					for _, val := range result {
						err := integrations.ValidatePostInfo(val)
						if err != nil {
							respStatusCode, respBodyTemp = 400, fmt.Sprintf(`400 GetPostInfoFailed with error: %s`, err.Error())
							respBodyArr = append(respBodyArr, respBodyTemp)
						} else {
							respStatusCode, respBodyTemp = worker.rt.netHandle.sendPost(val)
							if isSuccessStatus(respStatusCode) {
								respBodyArr = append(respBodyArr, respBodyTemp)
							} else {
								respBodyArr = []string{respBodyTemp}
								break
							}
						}
					}
					respBody = strings.Join(respBodyArr, " ")
				}
				ch <- struct{}{}

				//Using reponse status code and body to get response code rudder router logic is based on.
				if destinationResponseHandler != nil {
					respStatusCode = destinationResponseHandler.IsSuccessStatus(respStatusCode, respBody)
				}

				prevRespStatusCode = respStatusCode
				attemptedToSendTheJob = true

				worker.deliveryTimeStat.End()
				deliveryLatencyStat.End()
				// END: request to destination endpoint

				if isSuccessStatus(respStatusCode) {
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
				respStatusCode = 500
				respBody = "skipping sending to destination because previous job (of user) in batch is failed."
			}
		} else {
			respStatusCode = destinationJob.StatusCode
			respBody = destinationJob.Error
		}

		if !isJobTerminated(respStatusCode) {
			for _, metadata := range destinationJob.JobMetadataArray {
				failedUserIDsMap[metadata.UserID] = struct{}{}
			}
		}

		for _, destinationJobMetadata := range destinationJob.JobMetadataArray {
			handledJobMetadatas[destinationJobMetadata.JobID] = &destinationJobMetadata

			attemptNum := destinationJobMetadata.AttemptNum
			if attemptedToSendTheJob {
				attemptNum++
			}

			status := jobsdb.JobStatusT{
				JobID:         destinationJobMetadata.JobID,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				AttemptNum:    attemptNum,
				ErrorCode:     strconv.Itoa(respStatusCode),
				ErrorResponse: []byte(`{}`),
			}

			worker.postStatusOnResponseQ(respStatusCode, respBody, destinationJob.Message, &destinationJobMetadata, &status)

			worker.sendEventDeliveryStat(&destinationJobMetadata, &status, &destinationJob.Destination)

			if attemptedToSendTheJob {
				worker.sendRouterResponseCountStat(&destinationJobMetadata, &status, &destinationJob.Destination)
			}
			payload := destinationJob.Message
			if destinationJob.Message == nil {
				payload = destinationJobMetadata.JobT.EventPayload
			}
			worker.sendDestinationResponseToConfigBackend(payload, &destinationJobMetadata, &status)
		}
	}

	worker.decrementInThrottleMap(apiCallsCount)

	//if batching/routerTransform is enabled, we need to make sure that all the routerJobs status are written to DB.
	//if in any case transformer doesn't send all the job ids back, setting their statuses as failed
	for _, routerJob := range worker.routerJobs {
		if _, ok := handledJobMetadatas[routerJob.JobMetadata.JobID]; !ok {
			status := jobsdb.JobStatusT{
				JobID:         routerJob.JobMetadata.JobID,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				AttemptNum:    routerJob.JobMetadata.AttemptNum,
				ErrorCode:     strconv.Itoa(500),
				ErrorResponse: []byte(`{}`),
			}

			worker.postStatusOnResponseQ(500, "transformer failed to handle this job", nil, &routerJob.JobMetadata, &status)
		}
	}

	worker.batchTimeStat.End()
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
		for _, userID := range funk.UniqString(userIDs) {
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
	worker.rt.eventsAbortedStat = stats.NewTaggedStat(`router_aborted_events`, stats.CountType, stats.Tags{
		"destType":       worker.rt.destName,
		"respStatusCode": statusCode,
		"destId":         destinationID,
	})
	worker.rt.eventsAbortedStat.Increment()
}

func (worker *workerT) postStatusOnResponseQ(respStatusCode int, respBody string, payload json.RawMessage, destinationJobMetadata *types.JobMetadataT, status *jobsdb.JobStatusT) {
	//Enhancing status.ErrorResponse with firstAttemptedAt
	firstAttemptedAtTime := time.Now()
	if destinationJobMetadata.FirstAttemptedAt != "" {
		t, err := time.Parse(misc.RFC3339Milli, destinationJobMetadata.FirstAttemptedAt)
		if err == nil {
			firstAttemptedAtTime = t
		}

	}

	status.ErrorResponse = worker.enhanceResponse(status.ErrorResponse, "firstAttemptedAt", firstAttemptedAtTime.Format(misc.RFC3339Milli))
	if respBody != "" {
		status.ErrorResponse = worker.enhanceResponse(status.ErrorResponse, "response", respBody)
	}

	if isSuccessStatus(respStatusCode) {
		atomic.AddUint64(&worker.rt.successCount, 1)
		status.JobState = jobsdb.Succeeded.State
		worker.rt.logger.Debugf("[%v Router] :: sending success status to response", worker.rt.destName)
		worker.rt.responseQ <- jobResponseT{status: status, worker: worker, userID: destinationJobMetadata.UserID, JobT: destinationJobMetadata.JobT}

		if worker.rt.guaranteeUserEventOrder {
			//Removing the user from aborted user map
			worker.abortedUserMutex.Lock()
			delete(worker.abortedUserIDMap, destinationJobMetadata.UserID)
			worker.abortedUserMutex.Unlock()
		}

		//Deleting jobID from retryForJobMap. jobID goes into retryForJobMap if it is failed with 5xx or 429.
		//Its safe to delete from the map, even if jobID is not present.
		worker.retryForJobMapMutex.Lock()
		delete(worker.retryForJobMap, destinationJobMetadata.JobID)
		worker.retryForJobMapMutex.Unlock()
	} else {
		//Saving payload to DB only
		//1. if job failed and
		//2. if router job undergoes batching or dest transform.
		if payload != nil && (worker.rt.enableBatching || destinationJobMetadata.TransformAt == "router") {
			status.ErrorResponse = worker.enhanceResponse(status.ErrorResponse, "payload", string(payload))
		}
		// the job failed
		worker.rt.logger.Debugf("[%v Router] :: Job failed to send, analyzing...", worker.rt.destName)
		worker.failedJobs++
		atomic.AddUint64(&worker.rt.failCount, 1)

		//addToFailedMap is used to decide whether the jobID has to be added to the failedJobIDMap.
		//If the job is aborted then there is no point in adding it to the failedJobIDMap.
		addToFailedMap := true

		status.JobState = jobsdb.Failed.State

		worker.rt.failedEventsChan <- *status

		if respStatusCode >= 500 {
			timeElapsed := time.Since(firstAttemptedAtTime)
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
		} else if respStatusCode == 429 {
			worker.retryForJobMapMutex.Lock()
			worker.retryForJobMap[destinationJobMetadata.JobID] = time.Now().Add(durationBeforeNextAttempt(status.AttemptNum))
			worker.retryForJobMapMutex.Unlock()
		} else {
			status.JobState = jobsdb.Aborted.State
		}

		if status.JobState == jobsdb.Aborted.State {
			addToFailedMap = false
			worker.updateAbortedMetrics(destinationJobMetadata.DestinationID, status.ErrorCode)
			destinationJobMetadata.JobT.Parameters = misc.UpdateJSONWithNewKeyVal(destinationJobMetadata.JobT.Parameters, "stage", "router")
			destinationJobMetadata.JobT.Parameters = misc.UpdateJSONWithNewKeyVal(destinationJobMetadata.JobT.Parameters, "error_response", status.ErrorResponse)
		}

		if worker.rt.guaranteeUserEventOrder {
			if addToFailedMap {
				//#JobOrder (see other #JobOrder comment)
				worker.failedJobIDMutex.RLock()
				_, isPrevFailedUser := worker.failedJobIDMap[destinationJobMetadata.UserID]
				worker.failedJobIDMutex.RUnlock()
				if !isPrevFailedUser && destinationJobMetadata.UserID != "" {
					worker.rt.logger.Errorf("[%v Router] :: userId %v failed for the first time adding to map", worker.rt.destName, destinationJobMetadata.UserID)
					worker.failedJobIDMutex.Lock()
					worker.failedJobIDMap[destinationJobMetadata.UserID] = destinationJobMetadata.JobID
					worker.failedJobIDMutex.Unlock()
				}
			} else {
				//Job is aborted.
				//So, adding the user to aborted map, if not already present.
				//If user is present in the aborted map, decrementing the count.
				//This map is used to limit the pick up of aborted users's job.
				worker.abortedUserMutex.Lock()
				worker.rt.logger.Debugf("[%v Router] :: adding userID to abortedUserMap : %s", worker.rt.destName, destinationJobMetadata.UserID)
				count, ok := worker.abortedUserIDMap[destinationJobMetadata.UserID]
				if !ok {
					worker.abortedUserIDMap[destinationJobMetadata.UserID] = 0
				} else {
					//Decrementing the count.
					//This is necessary to let other jobs of the same user to get a worker.
					count--
					worker.abortedUserIDMap[destinationJobMetadata.UserID] = count
				}

				worker.abortedUserMutex.Unlock()
			}
		}
		worker.rt.logger.Debugf("[%v Router] :: sending failed/aborted state as response", worker.rt.destName)
		worker.rt.responseQ <- jobResponseT{status: status, worker: worker, userID: destinationJobMetadata.UserID, JobT: destinationJobMetadata.JobT}
	}
}

func (worker *workerT) sendRouterResponseCountStat(destinationJobMetadata *types.JobMetadataT, status *jobsdb.JobStatusT, destination *backendconfig.DestinationT) {
	destinationTag := misc.GetTagName(destination.ID, destination.Name)
	routerResponseStat := stats.NewTaggedStat("router_response_counts", stats.CountType, stats.Tags{
		"destType":       worker.rt.destName,
		"respStatusCode": status.ErrorCode,
		"destination":    destinationTag,
		"attempt_number": strconv.Itoa(status.AttemptNum),
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
					})

				eventsDeliveryTimeStat.SendTiming(time.Since(receivedTime))
			}
		}
	}
}

func (worker *workerT) sendDestinationResponseToConfigBackend(payload json.RawMessage, destinationJobMetadata *types.JobMetadataT, status *jobsdb.JobStatusT) {
	//Sending destination response to config backend
	if destinationdebugger.HasUploadEnabled(destinationJobMetadata.DestinationID) {
		deliveryStatus := destinationdebugger.DeliveryStatusT{
			DestinationID: destinationJobMetadata.DestinationID,
			SourceID:      destinationJobMetadata.SourceID,
			Payload:       payload,
			AttemptNum:    status.AttemptNum,
			JobState:      status.JobState,
			ErrorCode:     status.ErrorCode,
			ErrorResponse: status.ErrorResponse,
		}
		destinationdebugger.RecordEventDeliveryStatus(destinationJobMetadata.DestinationID, &deliveryStatus)
	}
}

func (worker *workerT) handleJobForPrevFailedUser(job *jobsdb.JobT, parameters JobParametersT, userID string, previousFailedJobID int64) (markedAsWaiting bool) {
	// job is behind in queue of failed job from same user
	if previousFailedJobID < job.JobID {
		worker.rt.logger.Debugf("[%v Router] :: skipping processing job for userID: %v since prev failed job exists, prev id %v, current id %v", worker.rt.destName, userID, previousFailedJobID, job.JobID)
		resp := fmt.Sprintf(`{"blocking_id":"%v", "user_id":"%s"}`, previousFailedJobID, userID)
		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			JobState:      jobsdb.Waiting.State,
			ErrorResponse: []byte(resp), // check
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

func (rt *HandleT) addToFailedList(jobStatus jobsdb.JobStatusT) {
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
		rt.addToFailedList(jobStatus)
	}
}

func (rt *HandleT) trackRequestMetrics(reqMetric requestMetric) {
	if diagnostics.EnableRouterMetric {
		rt.requestsMetricLock.Lock()
		if rt.requestsMetric == nil {
			var requestsMetric []requestMetric
			rt.requestsMetric = append(requestsMetric, reqMetric)
		} else {
			rt.requestsMetric = append(rt.requestsMetric, reqMetric)
		}
		rt.requestsMetricLock.Unlock()
	}
}

func (rt *HandleT) initWorkers() {
	rt.workers = make([]*workerT, rt.noOfWorkers)
	for i := 0; i < rt.noOfWorkers; i++ {
		worker := &workerT{
			pauseChannel:           make(chan *PauseT),
			resumeChannel:          make(chan bool),
			channel:                make(chan workerMessageT, noOfJobsPerChannel),
			failedJobIDMap:         make(map[string]int64),
			retryForJobMap:         make(map[int64]time.Time),
			workerID:               i,
			failedJobs:             0,
			sleepTime:              minSleep,
			routerJobs:             make([]types.RouterJobT, 0),
			destinationJobs:        make([]types.DestinationJobT, 0),
			rt:                     rt,
			deliveryTimeStat:       stats.NewTaggedStat("router_delivery_time", stats.TimerType, stats.Tags{"destType": rt.destName}),
			batchTimeStat:          stats.NewTaggedStat("router_batch_time", stats.TimerType, stats.Tags{"destType": rt.destName}),
			abortedUserIDMap:       make(map[string]int),
			jobCountsByDestAndUser: make(map[string]*destJobCountsT),
		}
		rt.workers[i] = worker
		rruntime.Go(func() {
			worker.workerProcess()
		})
	}
}

func (rt *HandleT) findWorker(job *jobsdb.JobT, throttledAtTime time.Time) (toSendWorker *workerT) {

	if !rt.guaranteeUserEventOrder {
		//if guaranteeUserEventOrder is false, assigning worker randomly and returning here.
		return rt.workers[rand.Intn(rt.noOfWorkers)]
	}

	userID := job.UserID

	//checking if the user is in throttledMap. If yes, returning nil.
	//this check is done to maintain order.
	if _, ok := rt.throttledUserMap[userID]; ok {
		rt.logger.Debugf(`[%v Router] :: Skipping processing of job:%d of user:%s as user has earlier jobs in throttled map`, rt.destName, job.JobID, userID)
		return nil
	}

	index := int(math.Abs(float64(misc.GetHash(userID) % rt.noOfWorkers)))

	worker := rt.workers[index]

	//#JobOrder (see other #JobOrder comment)
	worker.failedJobIDMutex.RLock()
	defer worker.failedJobIDMutex.RUnlock()
	blockJobID, found := worker.failedJobIDMap[userID]
	if !found {
		//not a failed user
		//checking if he is an aborted user,
		//if yes returning worker only for 1 job
		worker.abortedUserMutex.Lock()
		defer worker.abortedUserMutex.Unlock()
		if count, ok := worker.abortedUserIDMap[userID]; ok {
			if count >= rt.allowAbortedUserJobsCountForProcessing {
				rt.logger.Debugf("[%v Router] :: allowed jobs count(%d) >= allowAbortedUserJobsCountForProcessing(%d) for userID %s. returing nil worker", rt.destName, count, rt.allowAbortedUserJobsCountForProcessing, userID)
				return nil
			}

			rt.logger.Debugf("[%v Router] :: userID found in abortedUserIDtoJobMap: %s. Allowing jobID: %d. returing worker", rt.destName, userID, job.JobID)
			// incrementing abortedUserIDMap after all checks of backoff, throttle etc are made
			// We don't need lock inside this defer func, because we already hold the lock above and this
			// defer is called before defer Unlock
			defer func() {
				if toSendWorker != nil {
					toSendWorker.abortedUserIDMap[userID] = toSendWorker.abortedUserIDMap[userID] + 1
				}
			}()
		}
		toSendWorker = worker
	} else {
		//This job can only be higher than blocking
		//We only let the blocking job pass
		if job.JobID < blockJobID {
			panic(fmt.Errorf("job.JobID:%d < blockJobID:%d", job.JobID, blockJobID))
		}
		if job.JobID == blockJobID {
			toSendWorker = worker
		}
	}

	//checking if this job can be backedoff
	if toSendWorker != nil {
		if toSendWorker.canBackoff(job, userID) {
			return nil
		}
	}

	//checking if this job can be throttled
	if toSendWorker != nil {
		var parameters JobParametersT
		err := json.Unmarshal(job.Parameters, &parameters)
		if err == nil && rt.canThrottle(parameters.DestinationID, userID, throttledAtTime) {
			rt.throttledUserMap[userID] = struct{}{}
			rt.logger.Debugf(`[%v Router] :: Skipping processing of job:%d of user:%s as throttled limits exceeded`, rt.destName, job.JobID, userID)
			return nil
		}
	}

	return toSendWorker
	//#EndJobOrder
}

func (worker *workerT) canBackoff(job *jobsdb.JobT, userID string) (shouldBackoff bool) {
	// if the same job has failed before, check for next retry time
	worker.retryForJobMapMutex.RLock()
	defer worker.retryForJobMapMutex.RUnlock()
	if nextRetryTime, ok := worker.retryForJobMap[job.JobID]; ok && time.Until(nextRetryTime) > 0 {
		worker.rt.logger.Debugf("[%v Router] :: Less than next retry time: %v", worker.rt.destName, nextRetryTime)
		return true
	}
	return false
}

func (rt *HandleT) canThrottle(destID string, userID string, throttledAtTime time.Time) (canBeThrottled bool) {
	if !rt.throttler.IsEnabled() {
		return false
	}

	//No need of locks here, because this is used only by a single goroutine (generatorLoop)
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

//Enable enables a router :)
func (rt *HandleT) Enable() {
	rt.isEnabled = true
}

//Disable disables a router:)
func (rt *HandleT) Disable() {
	rt.isEnabled = false
}

func (rt *HandleT) commitStatusList(responseList *[]jobResponseT) {
	reportMetrics := make([]*utilTypes.PUReportedMetric, 0)
	connectionDetailsMap := make(map[string]*utilTypes.ConnectionDetails)
	statusDetailsMap := make(map[string]*utilTypes.StatusDetail)

	var statusList []*jobsdb.JobStatusT
	var routerAbortedJobs []*jobsdb.JobT
	for _, resp := range *responseList {
		//Update metrics maps
		//REPORTING - ROUTER - START
		if rt.reporting != nil && rt.reportingEnabled {
			var parameters JobParametersT
			err := json.Unmarshal(resp.JobT.Parameters, &parameters)
			if err != nil {
				rt.logger.Error("Unmarshal of job parameters failed. ", string(resp.JobT.Parameters))
			}
			key := fmt.Sprintf("%s:%s:%s:%s:%s", parameters.SourceID, parameters.DestinationID, parameters.SourceBatchID, resp.status.JobState, resp.status.ErrorCode)
			cd, ok := connectionDetailsMap[key]
			if !ok {
				cd = utilTypes.CreateConnectionDetail(parameters.SourceID, parameters.DestinationID, parameters.SourceBatchID, parameters.SourceTaskID, parameters.SourceTaskRunID, parameters.SourceJobID, parameters.SourceJobRunID)
				connectionDetailsMap[key] = cd
			}
			sd, ok := statusDetailsMap[key]
			if !ok {
				errorCode, err := strconv.Atoi(resp.status.ErrorCode)
				if err != nil {
					errorCode = 200 //TODO handle properly
				}
				sd = utilTypes.CreateStatusDetail(resp.status.JobState, 0, errorCode, string(resp.status.ErrorResponse), resp.JobT.EventPayload)
				statusDetailsMap[key] = sd
			}
			if resp.status.JobState == jobsdb.Failed.State && resp.status.AttemptNum == 1 {
				sd.Count++
			}
			if resp.status.JobState != jobsdb.Failed.State {
				sd.Count++
			}
		}
		//REPORTING - ROUTER - END

		statusList = append(statusList, resp.status)

		if resp.status.JobState == jobsdb.Aborted.State {
			routerAbortedJobs = append(routerAbortedJobs, resp.JobT)
		}

		//tracking router errors
		if diagnostics.EnableDestinationFailuresMetric {
			if resp.status.JobState == jobsdb.Failed.State || resp.status.JobState == jobsdb.Aborted.State {
				var event string
				if resp.status.JobState == jobsdb.Failed.State {
					event = diagnostics.RouterFailed
				} else {
					event = diagnostics.RouterAborted
				}

				rt.failureMetricLock.Lock()
				failureMetricVal := failureMetric{RouterDestination: rt.destName, UserId: resp.userID, RouterAttemptNum: resp.status.AttemptNum, ErrorCode: resp.status.ErrorCode, ErrorResponse: resp.status.ErrorResponse}
				rt.failuresMetric[event] = append(rt.failuresMetric[event], failureMetricVal)
				rt.failureMetricLock.Unlock()
			}
		}
	}

	//REPORTING - ROUTER - START
	if rt.reporting != nil && rt.reportingEnabled {
		utilTypes.AssertSameKeys(connectionDetailsMap, statusDetailsMap)
		for k, cd := range connectionDetailsMap {
			m := &utilTypes.PUReportedMetric{
				ConnectionDetails: *cd,
				PUDetails:         *utilTypes.CreatePUDetails(utilTypes.DEST_TRANSFORMER, utilTypes.ROUTER, true, false),
				StatusDetail:      statusDetailsMap[k],
			}
			if m.StatusDetail.Count != 0 {
				reportMetrics = append(reportMetrics, m)
			}
		}
	}
	//REPORTING - ROUTER - END

	if len(statusList) > 0 {
		rt.logger.Debugf("[%v Router] :: flushing batch of %v status", rt.destName, updateStatusBatchSize)

		sort.Slice(statusList, func(i, j int) bool {
			return statusList[i].JobID < statusList[j].JobID
		})
		//Store the aborted jobs to errorDB
		if routerAbortedJobs != nil {
			rt.errorDB.Store(routerAbortedJobs)
		}
		//Update the status
		txn := rt.jobsDB.BeginGlobalTransaction()
		rt.jobsDB.AcquireUpdateJobStatusLocks()
		err := rt.jobsDB.UpdateJobStatusInTxn(txn, statusList, []string{rt.destName}, nil)
		if err != nil {
			rt.logger.Errorf("[Router] :: Error occurred while updating %s jobs statuses. Panicking. Err: %v", rt.destName, err)
			panic(err)
		}
		if rt.reporting != nil && rt.reportingEnabled {
			rt.reporting.Report(reportMetrics, txn)
		}
		rt.jobsDB.CommitTransaction(txn)
		rt.jobsDB.ReleaseUpdateJobStatusLocks()
	}

	if rt.guaranteeUserEventOrder {
		//#JobOrder (see other #JobOrder comment)
		for _, resp := range *responseList {
			status := resp.status.JobState
			userID := resp.userID
			worker := resp.worker
			if status == jobsdb.Succeeded.State || status == jobsdb.Aborted.State {
				worker.failedJobIDMutex.RLock()
				lastJobID, ok := worker.failedJobIDMap[userID]
				worker.failedJobIDMutex.RUnlock()
				if ok && lastJobID == resp.status.JobID {
					rt.toClearFailJobIDMutex.Lock()
					rt.logger.Debugf("[%v Router] :: clearing failedJobIDMap for userID: %v", rt.destName, userID)
					_, ok := rt.toClearFailJobIDMap[worker.workerID]
					if !ok {
						rt.toClearFailJobIDMap[worker.workerID] = make([]string, 0)
					}
					rt.toClearFailJobIDMap[worker.workerID] = append(rt.toClearFailJobIDMap[worker.workerID], userID)
					rt.toClearFailJobIDMutex.Unlock()
				}
			}
		}
		//End #JobOrder
	}
}

func (rt *HandleT) statusInsertLoop() {

	var responseList []jobResponseT

	//Wait for the responses from statusQ
	lastUpdate := time.Now()

	statusStat := stats.NewTaggedStat("router_status_loop", stats.TimerType, stats.Tags{"destType": rt.destName})
	countStat := stats.NewTaggedStat("router_status_events", stats.CountType, stats.Tags{"destType": rt.destName})
	timeout := time.After(maxStatusUpdateWait)
	for {
		rt.perfStats.Start()
		select {
		case pause := <-rt.statusLoopPauseChannel:
			//Commit the buffer to disc
			pkgLogger.Infof("[Router] flushing statuses to disc. Dest type: %s", rt.destName)
			statusStat.Start()
			rt.commitStatusList(&responseList)
			responseList = nil
			lastUpdate = time.Now()
			countStat.Count(len(responseList))
			statusStat.End()
			pkgLogger.Infof("statusInsertLoop loop is paused. Dest type: %s", rt.destName)
			pause.respChannel <- true
			<-rt.statusLoopResumeChannel
			pkgLogger.Infof("statusInsertLoop loop is resumed. Dest type: %s", rt.destName)
		case jobStatus := <-rt.responseQ:
			rt.logger.Debugf("[%v Router] :: Got back status error %v and state %v for job %v", rt.destName, jobStatus.status.ErrorCode,
				jobStatus.status.JobState, jobStatus.status.JobID)
			responseList = append(responseList, jobStatus)
			rt.perfStats.End(1)
		case <-timeout:
			timeout = time.After(maxStatusUpdateWait)
			rt.perfStats.End(0)
			//Ideally should sleep for duration maxStatusUpdateWait-(time.Now()-lastUpdate)
			//but approx is good enough at the cost of reduced computation.
		}
		if len(responseList) >= updateStatusBatchSize || time.Since(lastUpdate) > maxStatusUpdateWait {
			statusStat.Start()
			rt.commitStatusList(&responseList)
			responseList = nil
			lastUpdate = time.Now()
			countStat.Count(len(responseList))
			statusStat.End()
		}
	}

}

func (rt *HandleT) collectMetrics() {
	if diagnostics.EnableRouterMetric {
		for range rt.diagnosisTicker.C {
			rt.requestsMetricLock.RLock()
			var diagnosisProperties map[string]interface{}
			retries := 0
			aborted := 0
			success := 0
			var compTime time.Duration
			for _, reqMetric := range rt.requestsMetric {
				retries = retries + reqMetric.RequestRetries
				aborted = aborted + reqMetric.RequestAborted
				success = success + reqMetric.RequestSuccess
				compTime = compTime + reqMetric.RequestCompletedTime
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

			//This lock will ensure we dont send out Track Request while filling up the
			//failureMetric struct
			rt.failureMetricLock.RLock()
			for key, values := range rt.failuresMetric {
				var stringValue string
				var err error
				errorMap := make(map[string]int)
				for _, value := range values {
					errorMap[string(value.ErrorResponse)] = errorMap[string(value.ErrorResponse)] + 1
				}
				for k, v := range errorMap {
					stringValue, err = sjson.Set(stringValue, k, v)
					if err != nil {
						stringValue = ""
					}
				}
				Diagnostics.Track(key, map[string]interface{}{
					diagnostics.RouterDestination: rt.destName,
					diagnostics.Count:             len(values),
					diagnostics.ErrorCountMap:     stringValue,
				})
			}
			rt.failuresMetric = make(map[string][]failureMetric)
			rt.failureMetricLock.RUnlock()
		}
	}
}

//#JobOrder (see other #JobOrder comment)
//If a job fails (say with given failed_job_id), we need to fail other jobs from that user till
//the failed_job_id succeeds. We achieve this by keeping the failed_job_id in a failedJobIDMap
//structure (mapping userID to failed_job_id). All subsequent jobs (guaranteed to be job_id >= failed_job_id)
//are put in Waiting.State in worker loop till the failed_job_id succeeds.
//However, the step of removing failed_job_id from the failedJobIDMap structure is QUITE TRICKY.
//To understand that, need to understand the complete lifecycle of a job.
//The job goes through the following data-structures in order
//   i>   generatorLoop Buffer (read from DB)
//   ii>  requestQ
//   iii> Worker Process
//   iv>  responseQ
//   v>   statusInsertLoop Buffer (enough jobs are buffered before updating status)
// Now, when the failed_job_id eventually suceeds in the Worker Process (iii above),
// there may be pending jobs in all the other data-structures. For example, there
//may be jobs in responseQ(iv) and statusInsertLoop(v) buffer - all those jobs will
//be in Waiting state. Similarly, there may be other jobs in requestQ and generatorLoop
//buffer.
//If the failed_job_id succeeds and we remove the filter gate, then all the jobs in requestQ
//will pass through before the jobs in responseQ/insertStatus buffer. That will violate the
//ordering of job.
//We fix this by removing an entry from the failedJobIDMap structure only when we are guaranteed
//that all the other structures are empty. We do the following to ahieve this
// A. In generatorLoop, we do not let any job pass through except failed_job_id. That ensures requestQ is empty
// B. We wait for the failed_job_id status (when succeeded) to be sync'd to disk. This along with A ensures
//    that responseQ and statusInsertLoop Buffer are empty for that userID.
// C. Finally, we want for generatorLoop buffer to be fully processed.

func (rt *HandleT) generatorLoop() {

	rt.logger.Info("Generator started")

	generatorStat := stats.NewTaggedStat("router_generator_loop", stats.TimerType, stats.Tags{"destType": rt.destName})
	countStat := stats.NewTaggedStat("router_generator_events", stats.CountType, stats.Tags{"destType": rt.destName})

	timeout := time.After(10 * time.Millisecond)
	for {
		select {
		case pause := <-rt.generatorPauseChannel:
			pkgLogger.Infof("Generator loop is paused. Dest type: %s", rt.destName)
			pause.respChannel <- true
			<-rt.generatorResumeChannel
			pkgLogger.Infof("Generator loop is resumed. Dest type: %s", rt.destName)
		case <-timeout:
			timeout = time.After(10 * time.Millisecond)
			if rt.pausingWorkers {
				time.Sleep(time.Second)
				continue
			}
			generatorStat.Start()

			if rt.guaranteeUserEventOrder {
				//#JobOrder (See comment marked #JobOrder
				rt.toClearFailJobIDMutex.Lock()
				for idx := range rt.toClearFailJobIDMap {
					wrk := rt.workers[idx]
					wrk.failedJobIDMutex.Lock()
					for _, userID := range rt.toClearFailJobIDMap[idx] {
						delete(wrk.failedJobIDMap, userID)
					}
					wrk.failedJobIDMutex.Unlock()
				}
				rt.toClearFailJobIDMap = make(map[int][]string)
				rt.toClearFailJobIDMutex.Unlock()
				//End of #JobOrder
			}

			toQuery := jobQueryBatchSize
			retryList := rt.jobsDB.GetToRetry(jobsdb.GetQueryParamsT{CustomValFilters: []string{rt.destName}, Count: toQuery})
			toQuery -= len(retryList)
			throttledList := rt.jobsDB.GetThrottled(jobsdb.GetQueryParamsT{CustomValFilters: []string{rt.destName}, Count: toQuery})
			toQuery -= len(throttledList)
			waitList := rt.jobsDB.GetWaiting(jobsdb.GetQueryParamsT{CustomValFilters: []string{rt.destName}, Count: toQuery}) //Jobs send to waiting state
			toQuery -= len(waitList)
			unprocessedList := rt.jobsDB.GetUnprocessed(jobsdb.GetQueryParamsT{CustomValFilters: []string{rt.destName}, Count: toQuery})

			combinedList := append(waitList, append(unprocessedList, append(throttledList, retryList...)...)...)

			if len(combinedList) == 0 {
				rt.logger.Debugf("RT: DB Read Complete. No RT Jobs to process for destination: %s", rt.destName)
				time.Sleep(readSleep)
				continue
			}

			rt.logger.Debugf("RT: %s: DB Read Complete. retryList: %v, waitList: %v unprocessedList: %v, total: %v", rt.destName, len(retryList), len(waitList), len(unprocessedList), len(combinedList))

			sort.Slice(combinedList, func(i, j int) bool {
				return combinedList[i].JobID < combinedList[j].JobID
			})

			if len(combinedList) > 0 {
				rt.logger.Debugf("[%v Router] :: router is enabled", rt.destName)
				rt.logger.Debugf("[%v Router] ===== len to be processed==== : %v", rt.destName, len(combinedList))
			}

			//List of jobs wich can be processed mapped per channel
			type workerJobT struct {
				worker *workerT
				job    *jobsdb.JobT
			}

			var statusList []*jobsdb.JobStatusT
			var drainList []*jobsdb.JobStatusT
			drainCountByDest := make(map[string]int)

			var toProcess []workerJobT

			rt.throttledUserMap = make(map[string]struct{})
			throttledAtTime := time.Now()
			//Identify jobs which can be processed
			for _, job := range combinedList {
				destID := destinationID(job)
				if rt.isToBeDrained(job, destID) {
					status := jobsdb.JobStatusT{
						JobID:         job.JobID,
						AttemptNum:    job.LastJobStatus.AttemptNum,
						JobState:      jobsdb.Aborted.State,
						ExecTime:      time.Now(),
						RetryTime:     time.Now(),
						ErrorCode:     "",
						ErrorResponse: []byte(`{"reason": "Job aborted since destination was disabled or confifgured to be aborted via ENV" }`),
					}
					drainList = append(drainList, &status)
					if _, ok := drainCountByDest[destID]; !ok {
						drainCountByDest[destID] = 0
					}
					drainCountByDest[destID] = drainCountByDest[destID] + 1
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
						ErrorResponse: []byte(`{}`), // check
					}
					statusList = append(statusList, &status)
					toProcess = append(toProcess, workerJobT{worker: w, job: job})
				}
			}
			rt.throttledUserMap = nil

			//Mark the jobs as executing
			err := rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destName}, nil)
			if err != nil {
				pkgLogger.Errorf("Error occurred while marking %s jobs statuses as executing. Panicking. Err: %v", rt.destName, err)
				panic(err)
			}
			//Mark the jobs as aborted
			if len(drainList) > 0 {
				err = rt.jobsDB.UpdateJobStatus(drainList, []string{rt.destName}, nil)
				if err != nil {
					pkgLogger.Errorf("Error occurred while marking %s jobs statuses as aborted. Panicking. Err: %v", rt.destName, err)
					panic(err)
				}
				for destID, count := range drainCountByDest {
					rt.drainedJobsStat = stats.NewTaggedStat(`router_drained_events`, stats.CountType, stats.Tags{
						"destType": rt.destName,
						"destId":   destID,
					})
					rt.drainedJobsStat.Count(count)
				}
			}

			//Send the jobs to the jobQ
			for _, wrkJob := range toProcess {
				wrkJob.worker.channel <- workerMessageT{job: wrkJob.job, throttledAtTime: throttledAtTime}
			}

			if len(toProcess) == 0 {
				rt.logger.Debugf("RT: No workers found for the jobs. Sleeping. Destination: %s", rt.destName)
				time.Sleep(readSleep)
				continue
			}

			countStat.Count(len(combinedList))
			generatorStat.End()
			time.Sleep(fixedLoopSleep) // adding sleep here to reduce cpu load on postgres when we have less rps
		}
	}
}

func destinationID(job *jobsdb.JobT) string {
	return gjson.GetBytes(job.Parameters, "destination_id").String()
}

func (rt *HandleT) isToBeDrained(job *jobsdb.JobT, destID string) bool {
	if d, ok := rt.destinationsMap[destID]; ok && !d.Enabled {
		return true
	}
	if toAbortDestinationIDs != "" {
		abortIDs := strings.Split(toAbortDestinationIDs, ",")
		return misc.ContainsString(abortIDs, destID)
	}
	return false
}

func (rt *HandleT) crashRecover() {
	rt.jobsDB.DeleteExecuting(jobsdb.GetQueryParamsT{CustomValFilters: []string{rt.destName}, Count: -1})
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("router")
}

//Setup initializes this module
func (rt *HandleT) Setup(jobsDB *jobsdb.HandleT, errorDB jobsdb.JobsDB, destinationDefinition backendconfig.DestinationDefinitionT, reporting utilTypes.ReportingI) {

	rt.generatorPauseChannel = make(chan *PauseT)
	rt.generatorResumeChannel = make(chan bool)
	rt.statusLoopPauseChannel = make(chan *PauseT)
	rt.statusLoopResumeChannel = make(chan bool)
	rt.reporting = reporting
	rt.reportingEnabled = config.GetBool("Reporting.enabled", true)
	destName := destinationDefinition.Name
	rt.logger = pkgLogger.Child(destName)
	rt.logger.Info("Router started: ", destName)

	//waiting for reporting client setup
	if rt.reporting != nil {
		rt.reporting.WaitForSetup(utilTypes.CORE_REPORTING_CLIENT)
	}

	rt.diagnosisTicker = time.NewTicker(diagnosisTickerTime)
	rt.jobsDB = jobsDB
	rt.errorDB = errorDB
	rt.destName = destName
	rt.netClientTimeout = getRouterConfigDuration("httpTimeoutInS", destName, 30) * time.Second
	rt.crashRecover()
	rt.requestQ = make(chan *jobsdb.JobT, jobQueryBatchSize)
	rt.responseQ = make(chan jobResponseT, jobQueryBatchSize)
	rt.toClearFailJobIDMap = make(map[int][]string)
	rt.failedEventsList = list.New()
	rt.failedEventsChan = make(chan jobsdb.JobStatusT)
	rt.isEnabled = true
	rt.netHandle = &NetHandleT{}
	rt.netHandle.logger = rt.logger.Child("network")
	rt.netHandle.Setup(destName, rt.netClientTimeout)
	rt.perfStats = &misc.PerfStats{}
	rt.perfStats.Setup("StatsUpdate:" + destName)
	rt.customDestinationManager = customDestinationManager.New(destName)
	rt.failuresMetric = make(map[string][]failureMetric)

	rt.destinationResponseHandler = New(destinationDefinition.ResponseRules)
	if value, ok := destinationDefinition.Config["saveDestinationResponse"].(bool); ok {
		rt.saveDestinationResponse = value
	}

	rt.guaranteeUserEventOrder = getRouterConfigBool("guaranteeUserEventOrder", rt.destName, true)
	rt.noOfWorkers = getRouterConfigInt("noOfWorkers", destName, 64)
	maxFailedCountKeys := []string{"Router." + rt.destName + "." + "maxFailedCountForJob", "Router." + "maxFailedCountForJob"}
	retryTimeWindowKeys := []string{"Router." + rt.destName + "." + "retryTimeWindowInMins", "Router." + "retryTimeWindowInMins"}
	config.RegisterIntConfigVariable(3, &rt.maxFailedCountForJob, true, 1, maxFailedCountKeys...)
	config.RegisterDurationConfigVariable(180, &rt.retryTimeWindow, true, time.Minute, retryTimeWindowKeys...)
	rt.drainJobHandler = drain.Setup(rt.jobsDB)
	rt.enableBatching = getRouterConfigBool("enableBatching", rt.destName, false)

	rt.allowAbortedUserJobsCountForProcessing = getRouterConfigInt("allowAbortedUserJobsCountForProcessing", destName, 1)

	rt.batchInputCountStat = stats.NewTaggedStat("router_batch_num_input_jobs", stats.CountType, stats.Tags{
		"destType": rt.destName,
	})
	rt.batchOutputCountStat = stats.NewTaggedStat("router_batch_num_output_jobs", stats.CountType, stats.Tags{
		"destType": rt.destName,
	})
	rt.batchInputOutputDiffCountStat = stats.NewTaggedStat("router_batch_input_output_diff_jobs", stats.CountType, stats.Tags{
		"destType": rt.destName,
	})

	rt.transformer = transformer.NewTransformer()
	rt.transformer.Setup()

	var throttler throttler.HandleT
	throttler.SetUp(rt.destName)
	rt.throttler = &throttler

	rt.isBackendConfigInitialized = false
	rt.backendConfigInitialized = make(chan bool)

	rt.initWorkers()
	rruntime.Go(func() {
		rt.collectMetrics()
	})
	rruntime.Go(func() {
		rt.readFailedJobStatusChan()
	})
	rruntime.Go(func() {
		rt.statusInsertLoop()
	})
	rruntime.Go(func() {
		<-rt.backendConfigInitialized
		rt.generatorLoop()
	})
	rruntime.Go(func() {
		rt.backendConfigSubscriber()
	})
	adminInstance.registerRouter(destName, rt)

	rm, err := GetRoutersManager()
	if err != nil {
		panic("Routers manager is nil. Shouldn't happen. Go Debug")
	}
	rm.AddRouter(rt)
}

func (rt *HandleT) backendConfigSubscriber() {
	ch := make(chan utils.DataEvent)
	backendconfig.Subscribe(ch, backendconfig.TopicBackendConfig)
	for {
		config := <-ch
		rt.configSubscriberLock.Lock()
		rt.destinationsMap = map[string]backendconfig.DestinationT{}
		allSources := config.Data.(backendconfig.ConfigT)
		for _, source := range allSources.Sources {
			if len(source.Destinations) > 0 {
				for _, destination := range source.Destinations {
					if destination.DestinationDefinition.Name == rt.destName {
						rt.destinationsMap[destination.ID] = destination
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

//Pause will pause the router
//To completely pause the router, we should follow the steps in order
//1. pause generator
//2. drain all the worker channels
//3. drain status insert loop queue
func (rt *HandleT) Pause() {
	rt.pauseLock.Lock()
	defer rt.pauseLock.Unlock()

	if rt.paused {
		return
	}

	//Pre Pause workers
	//Ideally this is not necessary.
	//But when generatorLoop is blocked on worker channels,
	//then pausing generatorLoop takes time.
	//Pre-pausing workers will help unblock generator.
	//To prevent generator to push again to workers, using pausingWorkers flag
	rt.pausingWorkers = true
	var wg sync.WaitGroup
	for _, worker := range rt.workers {
		_worker := worker
		wg.Add(1)
		rruntime.Go(func() {
			respChannel := make(chan bool)
			_worker.pauseChannel <- &PauseT{respChannel: respChannel, wg: &wg, waitForResume: false}
			<-respChannel
		})
	}
	wg.Wait()

	//Pause generator
	respChannel := make(chan bool)
	rt.generatorPauseChannel <- &PauseT{respChannel: respChannel}
	<-respChannel

	//Pause workers
	for _, worker := range rt.workers {
		_worker := worker
		wg.Add(1)
		rruntime.Go(func() {
			respChannel := make(chan bool)
			_worker.pauseChannel <- &PauseT{respChannel: respChannel, wg: &wg, waitForResume: true}
			<-respChannel
		})
	}
	wg.Wait()
	rt.pausingWorkers = false

	//Pause statusInsertLoop
	respChannel = make(chan bool)
	rt.statusLoopPauseChannel <- &PauseT{respChannel: respChannel}
	<-respChannel

	//Clean up dangling statuses and in memory maps.
	//Delete dangling executing
	rt.crashRecover()
	rt.toClearFailJobIDMap = make(map[int][]string)
	for _, worker := range rt.workers {
		worker.failedJobIDMap = make(map[string]int64)
		worker.retryForJobMap = make(map[int64]time.Time)
		worker.abortedUserIDMap = make(map[string]int)
	}

	rt.paused = true
}

//Resume will resume the router
//Resuming all the router components in the reverse order in which they were paused.
//1. resume status insert loop queue
//2. resume all the worker channels
//3. resume generator
func (rt *HandleT) Resume() {
	rt.pauseLock.Lock()
	defer rt.pauseLock.Unlock()

	if !rt.paused {
		return
	}

	//Resume statusInsertLoop
	rt.statusLoopResumeChannel <- true

	//Resume workers
	for _, worker := range rt.workers {
		worker.resumeChannel <- true
	}

	//Resume generator
	rt.generatorResumeChannel <- true

	rt.paused = false
}
