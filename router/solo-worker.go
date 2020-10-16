package router

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	destinationdebugger "github.com/rudderlabs/rudder-server/services/destination-debugger"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// workerT a structure to define a worker for sending events to sinks
type workerT struct {
	channel          chan *jobsdb.JobT   // the worker job channel
	workerID         int                 // identifies the worker
	failedJobs       int                 // counts the failed jobs of a worker till it gets reset by external channel
	sleepTime        time.Duration       //the sleep duration for every job of the worker
	failedJobIDMap   map[string]int64    //user to failed jobId
	failedJobIDMutex sync.RWMutex        //lock to protect structure above
	retryForJobMap   map[int64]time.Time //jobID to next retry time map
}

func (worker *workerT) PostJobOnWorker(job *jobsdb.JobT) {
	worker.channel <- job
}

func (worker *workerT) GetFailedJobIDMutex() *sync.RWMutex {
	return &worker.failedJobIDMutex
}

func (worker *workerT) GetFailedJobIDMap() map[string]int64 {
	return worker.failedJobIDMap
}

func (worker *workerT) WorkerProcess(rt *HandleT) {
	batchTimeStat := stats.NewStat(
		fmt.Sprintf("router.%s_batch_time", rt.destName), stats.TimerType)

	for {
		job := <-worker.channel

		batchTimeStat.Start()

		logger.Debugf("[%v Router] :: performing checks to send payload to %s. Payload: ", rt.destName, job.EventPayload)

		userID := job.UserID

		var paramaters JobParametersT
		err := json.Unmarshal(job.Parameters, &paramaters)
		if err != nil {
			logger.Error("Unmarshal of job parameters failed. ", string(job.Parameters))
		}

		var isPrevFailedUser bool
		var previousFailedJobID int64
		if rt.keepOrderOnFailure {
			//If there is a failed jobID from this user, we cannot pass future jobs
			worker.failedJobIDMutex.RLock()
			previousFailedJobID, isPrevFailedUser = worker.failedJobIDMap[userID]
			worker.failedJobIDMutex.RUnlock()

			// mark job as waiting if prev job from same user has not succeeded yet
			if isPrevFailedUser {
				markedAsWaiting := rt.handleJobForPrevFailedUser(job, paramaters, userID, worker, previousFailedJobID)
				if markedAsWaiting {
					continue
				}
			}
		}

		// mark job as failed (without incrementing attempts) if same job has failed before and backoff duration not elapsed
		shouldBackoff := rt.handleBackoff(job, userID, worker)
		if shouldBackoff {
			continue
		}

		// mark job as throttled if either dest level event limit reached or dest user level limit reached
		hasBeenThrottled := rt.handleThrottle(job, paramaters, userID, worker, isPrevFailedUser)
		if hasBeenThrottled {
			continue
		}

		workerVersion := getRouterConfigInt("workerVersion", rt.destName, 0)
		if workerVersion == 1 {
		} else {
			rt.handleSequential(job, worker)
		}

		batchTimeStat.End()
	}
}

func (rt *HandleT) handleSequential(job *jobsdb.JobT, worker *workerT) {
	deliveryTimeStat := stats.NewStat(
		fmt.Sprintf("router.%s_delivery_time", rt.destName), stats.TimerType)
	retryAttemptsStat := stats.NewStat(
		fmt.Sprintf("router.%s_retry_attempts", rt.destName), stats.CountType)

	var respStatusCode, attempts int
	var respBody string
	// START: request to destination endpoint

	logger.Debugf("[%v Router] :: trying to send payload. Attempt no. %v of max attempts %v", rt.destName, attempts, ser)

	diagnosisStartTime := time.Now()
	deliveryTimeStat.Start()

	if job.LastJobStatus.AttemptNum > 0 {
		retryAttemptsStat.Increment()
	}

	var paramaters JobParametersT
	err := json.Unmarshal(job.Parameters, &paramaters)
	if err != nil {
		logger.Error("Unmarshal of job parameters failed. ", string(job.Parameters))
	}

	if rt.customDestinationManager != nil {
		respStatusCode, _, respBody = rt.customDestinationManager.SendData(job.EventPayload, paramaters.SourceID, paramaters.DestinationID)
	} else {
		ch := rt.trackStuckDelivery()
		respStatusCode, _, respBody = rt.netHandle.sendPost(job.EventPayload)
		ch <- struct{}{}
	}

	deliveryTimeStat.End()

	// END: request to destination endpoint
	rt.handleDestinationResponse(worker, job, diagnosisStartTime, respStatusCode, respBody)
}

func (rt *HandleT) handleDestinationResponse(worker *workerT, job *jobsdb.JobT, diagnosisStartTime time.Time, respStatusCode int, respBody string) {
	eventsAbortedStat := stats.NewStat(
		fmt.Sprintf("router.%s_events_aborted", rt.destName), stats.CountType)

	userID := job.UserID

	_, isPrevFailedUser := worker.failedJobIDMap[userID]

	var paramaters JobParametersT
	err := json.Unmarshal(job.Parameters, &paramaters)
	if err != nil {
		logger.Error("Unmarshal of job parameters failed. ", string(job.Parameters))
	}

	var reqMetric requestMetric
	status := jobsdb.JobStatusT{
		JobID:         job.JobID,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		AttemptNum:    job.LastJobStatus.AttemptNum + 1,
		ErrorCode:     strconv.Itoa(respStatusCode),
		ErrorResponse: []byte(`{}`),
	}

	routerResponseStat := stats.GetRouterStat("router_response_counts", stats.CountType, rt.destName, respStatusCode)
	routerResponseStat.Increment()

	if isSuccessStatus(respStatusCode) {
		atomic.AddUint64(&rt.successCount, 1)
		status.JobState = jobsdb.Succeeded.State
		reqMetric.RequestSuccess = reqMetric.RequestSuccess + 1
		reqMetric.RequestCompletedTime = time.Now().Sub(diagnosisStartTime)
		logger.Debugf("[%v Router] :: sending success status to response", rt.destName)
		rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
	} else {
		// the job failed
		logger.Debugf("[%v Router] :: Job failed to send, analyzing...", rt.destName)
		worker.failedJobs++
		atomic.AddUint64(&rt.failCount, 1)
		status.ErrorResponse = []byte(fmt.Sprintf(`{"reason": %v}`, strconv.Quote(respBody)))

		//addToFailedMap is used to decide whethere the jobID has to be added to the failedJobIDMap.
		//If the job is aborted then there is no point in adding it to the failedJobIDMap.
		addToFailedMap := true

		status.JobState = jobsdb.Failed.State

		// TODO: 429 should mark this throttled?
		if respStatusCode >= 500 || respStatusCode == 429 {
			// TODO: timeElapsed should be ideally from first attempt
			timeElapsed := time.Now().Sub(job.CreatedAt)
			if timeElapsed > retryTimeWindow && status.AttemptNum >= maxFailedCountForJob {
				status.JobState = jobsdb.Aborted.State
				addToFailedMap = false
				delete(worker.retryForJobMap, job.JobID)
			} else {
				worker.retryForJobMap[job.JobID] = time.Now().Add(durationBeforeNextAttempt(status.AttemptNum))
			}
		} else {
			if status.AttemptNum >= maxFailedCountForJob {
				status.JobState = jobsdb.Aborted.State
				addToFailedMap = false
				eventsAbortedStat.Increment()
			}
		}

		if addToFailedMap {
			//#JobOrder (see other #JobOrder comment)
			if rt.keepOrderOnFailure && !isPrevFailedUser && userID != "" {
				logger.Errorf("[%v Router] :: userId %v failed for the first time adding to map", rt.destName, userID)
				worker.failedJobIDMutex.Lock()
				worker.failedJobIDMap[userID] = job.JobID
				worker.failedJobIDMutex.Unlock()
			}
		}
		reqMetric.RequestRetries = reqMetric.RequestRetries + 1
		reqMetric.RequestCompletedTime = time.Now().Sub(diagnosisStartTime)
		if status.JobState == jobsdb.Aborted.State {
			reqMetric.RequestAborted = reqMetric.RequestAborted + 1
		}
		logger.Debugf("[%v Router] :: sending failed/aborted state as response", rt.destName)
		rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
	}

	if paramaters.ReceivedAt != "" {
		if status.JobState == jobsdb.Succeeded.State {
			receivedTime, err := time.Parse(misc.RFC3339Milli, paramaters.ReceivedAt)
			if err == nil {
				eventsDeliveryTimeStat := stats.NewTaggedStat(
					"event_delivery_time", stats.TimerType, map[string]string{
						"module":   "router",
						"destType": rt.destName,
						"id":       paramaters.DestinationID,
					})

				eventsDeliveryTimeStat.SendTiming(time.Now().Sub(receivedTime))
			}
		}
	}
	//Sending destination response to config backend
	if destinationdebugger.HasUploadEnabled(paramaters.DestinationID) {
		deliveryStatus := destinationdebugger.DeliveryStatusT{
			DestinationID: paramaters.DestinationID,
			SourceID:      paramaters.SourceID,
			Payload:       job.EventPayload,
			AttemptNum:    status.AttemptNum,
			JobState:      status.JobState,
			ErrorCode:     status.ErrorCode,
			ErrorResponse: status.ErrorResponse,
		}
		destinationdebugger.RecordEventDeliveryStatus(paramaters.DestinationID, &deliveryStatus)
	}
}
