package router

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/rudderlabs/rudder-server/router/throttler"
	"github.com/rudderlabs/rudder-server/services/diagnostics"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/customdestinationmanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type WorkerFactory interface {
	CreateWorkers(rt *HandleT)
	GetWorker(idx int) WorkerInterface
	ResetSleep()
}

type WorkerInterface interface {
	PostJobOnWorker(job *jobsdb.JobT)
	WorkerProcess(rt *HandleT)
	GetFailedJobIDMutex() *sync.RWMutex
	GetFailedJobIDMap() map[string]int64
}

//HandleT is the handle to this module.
type HandleT struct {
	requestQ                 chan *jobsdb.JobT
	responseQ                chan jobResponseT
	jobsDB                   *jobsdb.HandleT
	netHandle                *NetHandleT
	destName                 string
	workerFactory            WorkerFactory
	perfStats                *misc.PerfStats
	successCount             uint64
	failCount                uint64
	isEnabled                bool
	toClearFailJobIDMutex    sync.Mutex
	toClearFailJobIDMap      map[int][]string
	requestsMetricLock       sync.RWMutex
	diagnosisTicker          *time.Ticker
	requestsMetric           []requestMetric
	customDestinationManager customdestinationmanager.DestinationManager
	throttler                *throttler.HandleT
	throttlerMutex           sync.RWMutex
	keepOrderOnFailure       bool
	netClientTimeout         time.Duration
}

type jobResponseT struct {
	status *jobsdb.JobStatusT
	worker *workerT
	userID string
}

//JobParametersT struct holds source id and destination id of a job
type JobParametersT struct {
	SourceID      string `json:"source_id"`
	DestinationID string `json:"destination_id"`
	ReceivedAt    string `json:"received_at"`
}

var (
	jobQueryBatchSize, updateStatusBatchSize, noOfWorkers, noOfJobsPerChannel, ser int
	maxFailedCountForJob                                                           int
	readSleep, minSleep, maxSleep, maxStatusUpdateWait, diagnosisTickerTime        time.Duration
	randomWorkerAssign                                                             bool
	testSinkURL                                                                    string
	retryTimeWindow, minRetryBackoff, maxRetryBackoff                              time.Duration
	workerRequestBatchTimeout                                                      time.Duration
)

var userOrderingRequiredMap = map[string]bool{
	"GA": true,
	"AM": true, // make it false to disable
}

type requestMetric struct {
	RequestRetries       int
	RequestAborted       int
	RequestSuccess       int
	RequestCompletedTime time.Duration
}

func isSuccessStatus(status int) bool {
	return status >= 200 && status < 300
}

func loadConfig() {
	jobQueryBatchSize = config.GetInt("Router.jobQueryBatchSize", 10000)
	updateStatusBatchSize = config.GetInt("Router.updateStatusBatchSize", 1000)
	readSleep = config.GetDuration("Router.readSleepInMS", time.Duration(1000)) * time.Millisecond
	noOfWorkers = config.GetInt("Router.noOfWorkers", 64)
	noOfJobsPerChannel = config.GetInt("Router.noOfJobsPerChannel", 1000)
	ser = config.GetInt("Router.ser", 3)
	maxSleep = config.GetDuration("Router.maxSleepInS", time.Duration(60)) * time.Second
	minSleep = config.GetDuration("Router.minSleepInS", time.Duration(0)) * time.Second
	maxStatusUpdateWait = config.GetDuration("Router.maxStatusUpdateWaitInS", time.Duration(5)) * time.Second
	randomWorkerAssign = config.GetBool("Router.randomWorkerAssign", false)
	maxFailedCountForJob = config.GetInt("Router.maxFailedCountForJob", 3)
	testSinkURL = config.GetEnv("TEST_SINK_URL", "http://localhost:8181")
	// Time period for diagnosis ticker
	diagnosisTickerTime = config.GetDuration("Diagnostics.routerTimePeriodInS", 60) * time.Second
	retryTimeWindow = config.GetDuration("Router.retryTimeWindowInMins", time.Duration(180)) * time.Minute
	minRetryBackoff = config.GetDuration("Router.minRetryBackoffInS", time.Duration(10)) * time.Second
	maxRetryBackoff = config.GetDuration("Router.maxRetryBackoffInS", time.Duration(300)) * time.Second
	workerRequestBatchTimeout = config.GetDuration("Router.workerRequestBatchTimeoutInS", 5) * time.Second
}

func (rt *HandleT) trackStuckDelivery() chan struct{} {
	ch := make(chan struct{}, 1)
	rruntime.Go(func() {
		select {
		case _ = <-ch:
			// do nothing
		case <-time.After(rt.netClientTimeout * 2):
			logger.Infof("[%s Router] Delivery to destination exceeded the 2 * configured timeout ", rt.destName)
			stat := stats.NewTaggedStat("router_delivery_exceeded_timeout", stats.CountType, map[string]string{
				"destType": rt.destName,
			})
			stat.Increment()
		}
	})
	return ch
}

func (rt *HandleT) handleJobForPrevFailedUser(job *jobsdb.JobT, paramaters JobParametersT, userID string, worker *workerT, previousFailedJobID int64) (markedAsWaiting bool) {
	// job is behind in queue of failed job from same user
	if previousFailedJobID < job.JobID {
		logger.Debugf("[%v Router] :: skipping processing job for userID: %v since prev failed job exists, prev id %v, current id %v", rt.destName, userID, previousFailedJobID, job.JobID)
		resp := fmt.Sprintf(`{"blocking_id":"%v", "user_id":"%s"}`, previousFailedJobID, userID)
		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			JobState:      jobsdb.Waiting.State,
			ErrorResponse: []byte(resp), // check
		}
		rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
		return true
	}
	if previousFailedJobID != job.JobID {
		panic(fmt.Errorf("previousFailedJobID:%d != job.JobID:%d", previousFailedJobID, job.JobID))
	}
	return false
}

func (rt *HandleT) handleBackoff(job *jobsdb.JobT, userID string, worker *workerT) (shouldBackoff bool) {
	// if the same job has failed before, check for next retry time
	if nextRetryTime, ok := worker.retryForJobMap[job.JobID]; ok && nextRetryTime.Sub(time.Now()) > 0 {
		logger.Debugf("[%v Router] :: Less than next retry time: %v", rt.destName, nextRetryTime)
		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			JobState:      jobsdb.Failed.State,
			ErrorResponse: []byte(fmt.Sprintf(`{"Error": "Less than next retry time: %v"}`, nextRetryTime)), // check
		}
		rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
		return true
	}
	return false
}

func (rt *HandleT) handleThrottle(job *jobsdb.JobT, paramaters JobParametersT, userID string, worker *workerT, isPrevFailedUser bool) (hasBeenThrottled bool) {
	if !rt.throttler.IsEnabled() {
		return false
	}
	rt.throttlerMutex.Lock()
	toThrottle := rt.throttler.LimitReached(paramaters.DestinationID, userID)
	rt.throttlerMutex.Unlock()
	if toThrottle {
		// block other jobs of same user if userEventOrdering is required.
		if rt.keepOrderOnFailure && !isPrevFailedUser && userID != "" {
			logger.Errorf("[%v Router] :: Request Failed for userID: %v. Adding user to failed users map to preserve ordering.", rt.destName, userID)
			worker.failedJobIDMutex.Lock()
			worker.failedJobIDMap[userID] = job.JobID
			worker.failedJobIDMutex.Unlock()
		}
		logger.Debugf("[%v Router] :: throttling %v for destinationID: %v", rt.destName, job.JobID, paramaters.DestinationID)
		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			JobState:      jobsdb.Throttled.State,
			ErrorResponse: []byte(`{}`), // check
		}
		rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
		return true
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

func (rt *HandleT) findWorker(job *jobsdb.JobT) WorkerInterface {

	userID := job.UserID

	var index int
	if randomWorkerAssign {
		index = rand.Intn(noOfWorkers)
	} else {
		index = int(math.Abs(float64(misc.GetHash(userID) % noOfWorkers)))
	}

	worker := rt.workerFactory.GetWorker(index)

	//#JobOrder (see other #JobOrder comment)
	failedJobIDMutex := worker.GetFailedJobIDMutex()
	failedJobIDMutex.RLock()
	defer failedJobIDMutex.RUnlock()
	blockJobID, found := worker.GetFailedJobIDMap()[userID]
	if !found {
		return worker
	}
	//This job can only be higher than blocking
	//We only let the blocking job pass
	if job.JobID < blockJobID {
		panic(fmt.Errorf("job.JobID:%d < blockJobID:%d", job.JobID, blockJobID))
	}
	if job.JobID == blockJobID {
		return worker
	}
	return nil
	//#EndJobOrder
}

// ResetSleep  this makes the workers reset their sleep
func (rt *HandleT) ResetSleep() {
	rt.workerFactory.ResetSleep()
}

//Enable enables a router :)
func (rt *HandleT) Enable() {
	rt.isEnabled = true
}

//Disable disables a router:)
func (rt *HandleT) Disable() {
	rt.isEnabled = false
}

func (rt *HandleT) statusInsertLoop() {

	var responseList []jobResponseT
	//Wait for the responses from statusQ
	lastUpdate := time.Now()

	statusStat := stats.NewStat("router.status_loop", stats.TimerType)
	countStat := stats.NewStat("router.status_events", stats.CountType)

	for {
		rt.perfStats.Start()
		select {
		case jobStatus := <-rt.responseQ:
			logger.Debugf("[%v Router] :: Got back status error %v and state %v for job %v", rt.destName, jobStatus.status.ErrorCode,
				jobStatus.status.JobState, jobStatus.status.JobID)
			responseList = append(responseList, jobStatus)
			rt.perfStats.End(1)
		case <-time.After(maxStatusUpdateWait):
			rt.perfStats.End(0)
			//Ideally should sleep for duration maxStatusUpdateWait-(time.Now()-lastUpdate)
			//but approx is good enough at the cost of reduced computation.
		}

		if len(responseList) >= updateStatusBatchSize || time.Since(lastUpdate) > maxStatusUpdateWait {
			statusStat.Start()
			var statusList []*jobsdb.JobStatusT
			for _, resp := range responseList {
				statusList = append(statusList, resp.status)

				//tracking router errors
				if diagnostics.EnableDestinationFailuresMetric {
					if resp.status.JobState == jobsdb.Failed.State || resp.status.JobState == jobsdb.Aborted.State {
						var event string
						if resp.status.JobState == jobsdb.Failed.State {
							event = diagnostics.RouterFailed
						} else {
							event = diagnostics.RouterAborted
						}
						diagnostics.Track(event, map[string]interface{}{
							diagnostics.RouterDestination: rt.destName,
							diagnostics.UserID:            resp.userID,
							diagnostics.RouterAttemptNum:  resp.status.AttemptNum,
							diagnostics.ErrorCode:         resp.status.ErrorCode,
							diagnostics.ErrorResponse:     resp.status.ErrorResponse,
						})
					}
				}
			}

			if len(statusList) > 0 {
				logger.Debugf("[%v Router] :: flushing batch of %v status", rt.destName, updateStatusBatchSize)

				sort.Slice(statusList, func(i, j int) bool {
					return statusList[i].JobID < statusList[j].JobID
				})
				//Update the status
				rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destName}, nil)
			}

			//#JobOrder (see other #JobOrder comment)
			for _, resp := range responseList {
				status := resp.status.JobState
				userID := resp.userID
				worker := resp.worker
				if status == jobsdb.Succeeded.State || status == jobsdb.Aborted.State {
					worker.failedJobIDMutex.RLock()
					lastJobID, ok := worker.failedJobIDMap[userID]
					worker.failedJobIDMutex.RUnlock()
					if ok && lastJobID == resp.status.JobID {
						rt.toClearFailJobIDMutex.Lock()
						logger.Debugf("[%v Router] :: clearing failedJobIDMap for userID: %v", rt.destName, userID)
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
			responseList = nil
			lastUpdate = time.Now()
			countStat.Count(len(responseList))
			statusStat.End()
		}
	}

}

func (rt *HandleT) collectMetrics() {
	if diagnostics.EnableRouterMetric {
		for {
			select {
			case _ = <-rt.diagnosisTicker.C:
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

					diagnostics.Track(diagnostics.RouterEvents, diagnosisProperties)
				}

				rt.requestsMetric = nil
				rt.requestsMetricLock.RUnlock()
			}
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

	logger.Info("Generator started")

	generatorStat := stats.NewStat("router.generator_loop", stats.TimerType)
	countStat := stats.NewStat("router.generator_events", stats.CountType)

	for {
		generatorStat.Start()

		//#JobOrder (See comment marked #JobOrder
		rt.toClearFailJobIDMutex.Lock()
		for idx := range rt.toClearFailJobIDMap {
			wrk := rt.workerFactory.GetWorker(idx)
			failedJobIDMutex := wrk.GetFailedJobIDMutex()
			failedJobIDMutex.Lock()
			for _, userID := range rt.toClearFailJobIDMap[idx] {
				delete(wrk.GetFailedJobIDMap(), userID)
			}
			failedJobIDMutex.Unlock()
		}
		rt.toClearFailJobIDMap = make(map[int][]string)
		rt.toClearFailJobIDMutex.Unlock()
		//End of #JobOrder

		toQuery := jobQueryBatchSize
		retryList := rt.jobsDB.GetToRetry([]string{rt.destName}, toQuery, nil)
		toQuery -= len(retryList)
		throttledList := rt.jobsDB.GetThrottled([]string{rt.destName}, toQuery, nil)
		toQuery -= len(throttledList)
		waitList := rt.jobsDB.GetWaiting([]string{rt.destName}, toQuery, nil) //Jobs send to waiting state
		toQuery -= len(waitList)
		unprocessedList := rt.jobsDB.GetUnprocessed([]string{rt.destName}, toQuery, nil)

		combinedList := append(waitList, append(unprocessedList, append(throttledList, retryList...)...)...)

		if len(combinedList) == 0 {
			logger.Debugf("RT: DB Read Complete. No RT Jobs to process for destination: %s", rt.destName)
			time.Sleep(readSleep)
			continue
		}

		logger.Debugf("RT: %s: DB Read Complete. retryList: %v, waitList: %v unprocessedList: %v, total: %v", rt.destName, len(retryList), len(waitList), len(unprocessedList), len(combinedList))

		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})

		if len(combinedList) > 0 {
			logger.Debugf("[%v Router] :: router is enabled", rt.destName)
			logger.Debugf("[%v Router] ===== len to be processed==== : %v", rt.destName, len(combinedList))
		}

		//List of jobs wich can be processed mapped per channel
		type workerJobT struct {
			worker WorkerInterface
			job    *jobsdb.JobT
		}

		var statusList []*jobsdb.JobStatusT
		var toProcess []workerJobT

		//Identify jobs which can be processed
		for _, job := range combinedList {
			w := rt.findWorker(job)
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

		//Mark the jobs as executing
		rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destName}, nil)

		//Send the jobs to the jobQ
		for _, wrkJob := range toProcess {
			wrkJob.worker.PostJobOnWorker(wrkJob.job)
		}

		countStat.Count(len(combinedList))
		generatorStat.End()
	}
}

func (rt *HandleT) crashRecover() {
	rt.jobsDB.DeleteExecuting([]string{rt.destName}, -1, nil)
}

func (rt *HandleT) setUserEventsOrderingRequirement() {
	// user event ordering is required by default unless specified
	required := true
	if _, ok := userOrderingRequiredMap[rt.destName]; ok {
		required = userOrderingRequiredMap[rt.destName]
	}
	rt.keepOrderOnFailure = config.GetBool(fmt.Sprintf(`Router.%s.keepOrderOnFailure`, rt.destName), required)
}

func init() {
	loadConfig()
}

//Setup initializes this module
func (rt *HandleT) Setup(jobsDB *jobsdb.HandleT, destName string) {
	logger.Info("Router started")
	rt.diagnosisTicker = time.NewTicker(diagnosisTickerTime)
	rt.jobsDB = jobsDB
	rt.destName = destName
	rt.netClientTimeout = getRouterConfigDuration("httpTimeoutInS", destName, 30) * time.Second
	rt.crashRecover()
	rt.requestQ = make(chan *jobsdb.JobT, jobQueryBatchSize)
	rt.responseQ = make(chan jobResponseT, jobQueryBatchSize)
	rt.toClearFailJobIDMap = make(map[int][]string)
	rt.isEnabled = true
	rt.netHandle = &NetHandleT{}
	rt.netHandle.Setup(destName, rt.netClientTimeout)
	rt.perfStats = &misc.PerfStats{}
	rt.perfStats.Setup("StatsUpdate:" + destName)
	rt.customDestinationManager = customdestinationmanager.New(destName)
	rt.setUserEventsOrderingRequirement()

	var throttler throttler.HandleT
	throttler.SetUp(rt.destName)
	rt.throttler = &throttler

	workerVersion := getRouterConfigString("workerVersion", rt.destName, "solo")
	if workerVersion == "batch" {
		rt.workerFactory = &BatchWorkerFactory{}
	} else {
		rt.workerFactory = &SoloWorkerFactory{}
	}
	rt.workerFactory.CreateWorkers(rt)

	rruntime.Go(func() {
		rt.collectMetrics()
	})
	rruntime.Go(func() {
		rt.statusInsertLoop()
	})
	rruntime.Go(func() {
		rt.generatorLoop()
	})
	adminInstance.registerRouter(destName, rt)
}
