package router

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/services/stats"
)

//HandleT is the handle to this module.
type HandleT struct {
	requestQ              chan *jobsdb.JobT
	responseQ             chan jobResponseT
	jobsDB                *jobsdb.HandleT
	netHandle             *NetHandleT
	destID                string
	workers               []*workerT
	perfStats             *misc.PerfStats
	successCount          uint64
	failCount             uint64
	isEnabled             bool
	toClearFailJobIDMutex sync.Mutex
	toClearFailJobIDMap   map[int][]string
}

type jobResponseT struct {
	status *jobsdb.JobStatusT
	worker *workerT
	userID string
}

// workerT a structure to define a worker for sending events to sinks
type workerT struct {
	channel          chan *jobsdb.JobT // the worker job channel
	workerID         int               // identifies the worker
	failedJobs       int               // counts the failed jobs of a worker till it gets reset by external channel
	sleepTime        time.Duration     //the sleep duration for every job of the worker
	failedJobIDMap   map[string]int64  //user to failed jobId
	failedJobIDMutex sync.RWMutex      //lock to protect structure above
}

var (
	jobQueryBatchSize, updateStatusBatchSize, noOfWorkers, noOfJobsPerChannel, ser int
	maxFailedCountForJob                                                           int
	readSleep, minSleep, maxSleep, maxStatusUpdateWait                             time.Duration
	randomWorkerAssign, useTestSink, keepOrderOnFailure                            bool
	testSinkURL                                                                    string
)

func loadConfig() {
	jobQueryBatchSize = config.GetInt("Router.jobQueryBatchSize", 10000)
	updateStatusBatchSize = config.GetInt("Router.updateStatusBatchSize", 1000)
	readSleep = config.GetDuration("Router.readSleepInMS", time.Duration(10)) * time.Millisecond
	noOfWorkers = config.GetInt("Router.noOfWorkers", 8)
	noOfJobsPerChannel = config.GetInt("Router.noOfJobsPerChannel", 1000)
	ser = config.GetInt("Router.ser", 3)
	maxSleep = config.GetDuration("Router.maxSleepInS", time.Duration(60)) * time.Second
	minSleep = config.GetDuration("Router.minSleepInS", time.Duration(0)) * time.Second
	maxStatusUpdateWait = config.GetDuration("Router.maxStatusUpdateWaitInS", time.Duration(5)) * time.Second
	randomWorkerAssign = config.GetBool("Router.randomWorkerAssign", false)
	keepOrderOnFailure = config.GetBool("Router.keepOrderOnFailure", true)
	useTestSink = config.GetBool("Router.useTestSink", false)
	maxFailedCountForJob = config.GetInt("Router.maxFailedCountForJob", 8)
	testSinkURL = config.GetEnv("TEST_SINK_URL", "http://localhost:8181")
}

func (rt *HandleT) workerProcess(worker *workerT) {

	networkDelayStat := stats.NewStat(
		fmt.Sprintf("router.%s_worker_network", rt.destID), stats.TimerType)
	workerDurationStat := stats.NewStat(
		fmt.Sprintf("router.%s_worker_duration", rt.destID), stats.TimerType)
	numRetriesStat := stats.NewStat(
		fmt.Sprintf("router.%s_worker_num_retries", rt.destID), stats.CountType)
	eventsDeliveredStat := stats.NewStat("router.events_delivered", stats.CountType)

	for {
		job := <-worker.channel
		var respStatusCode, attempts int
		var respStatus, respBody string
		workerDurationStat.Start()
		logger.Debug("Router :: trying to send payload to GA", respBody)

		postInfo := integrations.GetPostInfo(job.EventPayload)
		userID := postInfo.UserID
		misc.Assert(userID != "")

		//If sink is not enabled mark all jobs as waiting
		if !rt.isEnabled {
			logger.Debug("Router is disabled")
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "",
				JobState:      jobsdb.WaitingState,
				ErrorResponse: []byte(`{"reason":"Router Disabled"}`), // check
			}
			rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
			continue
		}

		//If there is a failed jobID from this user, we cannot pass future jobs
		worker.failedJobIDMutex.RLock()
		previousFailedJobID, isPrevFailedUser := worker.failedJobIDMap[userID]
		worker.failedJobIDMutex.RUnlock()

		if isPrevFailedUser && previousFailedJobID < job.JobID {
			logger.Debugf("Router :: prev id %v, current id %v", previousFailedJobID, job.JobID)
			resp := fmt.Sprintf(`{"blocking_id":"%v", "user_id":"%s"}`, previousFailedJobID, userID)
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     respStatus,
				JobState:      jobsdb.WaitingState,
				ErrorResponse: []byte(resp), // check
			}
			rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
			continue
		}

		if isPrevFailedUser {
			misc.Assert(previousFailedJobID == job.JobID)
		}

		//We can execute thoe job
		for attempts = 0; attempts < ser; attempts++ {
			logger.Debugf("Router :: trying to send payload %v of %v", attempts, ser)

			networkDelayStat.Start()
			respStatusCode, respStatus, respBody = rt.netHandle.sendPost(job.EventPayload)
			networkDelayStat.End()

			if useTestSink {
				//Internal test. No reason to sleep
				break
			}
			if respStatusCode != http.StatusOK {
				//400 series error are client errors. Can't continue
				if respStatusCode >= http.StatusBadRequest && respStatusCode <= http.StatusUnavailableForLegalReasons {
					break
				}
				//Wait before the next retry
				worker.sleepTime = 2*worker.sleepTime + 1 //+1 handles 0 sleepTime
				if worker.sleepTime > maxSleep {
					worker.sleepTime = maxSleep
				}
				logger.Debugf("Router :: worker %v sleeping for  %v ",
					worker.workerID, worker.sleepTime)
				time.Sleep(worker.sleepTime * time.Second)
				continue
			} else {
				atomic.AddUint64(&rt.successCount, 1)
				//Divide the sleep
				worker.sleepTime = worker.sleepTime / 2
				if worker.sleepTime < minSleep {
					worker.sleepTime = minSleep
				}
				logger.Debugf("Router :: sleep for worker %v decreased to %v",
					worker.workerID, worker.sleepTime)
				break
			}
		}

		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     respStatus,
			ErrorResponse: []byte(`{}`),
		}

		if respStatusCode == http.StatusOK {
			//#JobOrder (see other #JobOrder comment)
			numRetriesStat.Count(job.LastJobStatus.AttemptNum)
			eventsDeliveredStat.Increment()
			status.AttemptNum = job.LastJobStatus.AttemptNum
			status.JobState = jobsdb.SucceededState
			logger.Debug("Router :: sending success status to response")
			rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
		} else {
			// the job failed
			logger.Debug("Router :: Job failed to send, analyzing...")
			worker.failedJobs++
			atomic.AddUint64(&rt.failCount, 1)

			//#JobOrder (see other #JobOrder comment)
			if !isPrevFailedUser && keepOrderOnFailure {
				logger.Errorf("Router :: userId %v failed for the first time adding to map", userID)
				worker.failedJobIDMutex.Lock()
				worker.failedJobIDMap[userID] = job.JobID
				worker.failedJobIDMutex.Unlock()
			}

			switch {
			case len(worker.failedJobIDMap) > 5:
				//Lot of jobs are failing in this worker. Likely the sink is down
				//We still mark the job failed but don't increment the AttemptNum
				//This is a heuristic. Will fix it with Sayan's idea
				status.JobState = jobsdb.FailedState
				status.AttemptNum = job.LastJobStatus.AttemptNum
				break
			case status.AttemptNum >= maxFailedCountForJob:
				//The job has failed enough number of times so mark it aborted
				//The reason for doing this is to filter out jobs with bad payload
				//which can never succeed.
				//However, there is a risk that if sink is down, a set of jobs can
				//reach maxCountFailure. In practice though, when sink goes down
				//lot of jobs will fail and all will get retried in batch with
				//doubling sleep in between. That case will be handled in case above
				logger.Debug("Router :: Aborting the job and deleting from user map")
				status.JobState = jobsdb.AbortedState
				status.AttemptNum = job.LastJobStatus.AttemptNum
				break
			default:
				status.JobState = jobsdb.FailedState
				status.AttemptNum = job.LastJobStatus.AttemptNum
				break
			}
			logger.Debug("Router :: sending waiting state as response")
			rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
		}
		workerDurationStat.End()
	}
}

func (rt *HandleT) initWorkers() {
	rt.workers = make([]*workerT, noOfWorkers)
	for i := 0; i < noOfWorkers; i++ {
		logger.Info("Worker Started", i)
		var worker *workerT
		worker = &workerT{
			channel:        make(chan *jobsdb.JobT, noOfJobsPerChannel),
			failedJobIDMap: make(map[string]int64),
			workerID:       i,
			failedJobs:     0,
			sleepTime:      minSleep}
		rt.workers[i] = worker
		go rt.workerProcess(worker)
	}
}

func getHash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func (rt *HandleT) findWorker(job *jobsdb.JobT) *workerT {

	postInfo := integrations.GetPostInfo(job.EventPayload)

	var index int
	if randomWorkerAssign {
		index = rand.Intn(noOfWorkers)
	} else {
		index = int(math.Abs(float64(getHash(postInfo.UserID) % noOfWorkers)))
	}

	worker := rt.workers[index]
	misc.Assert(worker != nil)

	//#JobOrder (see other #JobOrder comment)
	worker.failedJobIDMutex.RLock()
	defer worker.failedJobIDMutex.RUnlock()
	blockJobID, found := worker.failedJobIDMap[postInfo.UserID]
	if !found {
		return worker
	}
	//This job can only be higher than blocking
	//We only let the blocking job pass
	misc.Assert(job.JobID >= blockJobID)
	if job.JobID == blockJobID {
		return worker
	}
	return nil
	//#EndJobOrder
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
			logger.Debugf("Router :: Got back status error %v and state %v for job %v", jobStatus.status.ErrorCode,
				jobStatus.status.JobState, jobStatus.status.JobID)
			responseList = append(responseList, jobStatus)
			rt.perfStats.End(1)
		case <-time.After(maxStatusUpdateWait):
			rt.perfStats.End(0)
			//Ideally should sleep for duration maxStatusUpdateWait-(time.Now()-lastUpdate)
			//but approx is good enough at the cost of reduced computation.
		}

		if len(responseList) >= updateStatusBatchSize || time.Since(lastUpdate) > maxStatusUpdateWait {
			rt.perfStats.Print()
			statusStat.Start()
			var statusList []*jobsdb.JobStatusT
			for _, resp := range responseList {
				statusList = append(statusList, resp.status)
			}

			if len(statusList) > 0 {
				logger.Debugf("Router :: flushing batch of %v status", updateStatusBatchSize)

				sort.Slice(statusList, func(i, j int) bool {
					return statusList[i].JobID < statusList[j].JobID
				})
				//Update the status
				rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destID})
			}

			//#JobOrder (see other #JobOrder comment)
			for _, resp := range responseList {
				status := resp.status.JobState
				userID := resp.userID
				worker := resp.worker
				if status == jobsdb.SucceededState || status == jobsdb.AbortedState {
					worker.failedJobIDMutex.RLock()
					lastJobID, ok := worker.failedJobIDMap[userID]
					worker.failedJobIDMutex.RUnlock()
					if ok && lastJobID == resp.status.JobID {
						rt.toClearFailJobIDMutex.Lock()
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

//#JobOrder (see other #JobOrder comment)
//If a job fails (say with given failed_job_id), we need to fail other jobs from that user till
//the failed_job_id succeeds. We achieve this by keeping the failed_job_id in a failedJobIDMap
//structure (mapping userID to failed_job_id). All subsequent jobs (guaranteed to be job_id >= failed_job_id)
//are put in WaitingState in worker loop till the failed_job_id succeeds.
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
		if !rt.isEnabled {
			time.Sleep(1000)
			continue
		}
		generatorStat.Start()

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

		toQuery := jobQueryBatchSize
		retryList := rt.jobsDB.GetToRetry([]string{rt.destID}, toQuery)
		toQuery -= len(retryList)
		waitList := rt.jobsDB.GetWaiting([]string{rt.destID}, toQuery) //Jobs send to waiting state
		toQuery -= len(waitList)
		unprocessedList := rt.jobsDB.GetUnprocessed([]string{rt.destID}, toQuery)
		if len(waitList)+len(unprocessedList)+len(retryList) == 0 {
			time.Sleep(readSleep)
			continue
		}

		combinedList := append(waitList, append(unprocessedList, retryList...)...)

		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})

		if len(combinedList) > 0 {
			logger.Debug("Router :: router is enabled")
			logger.Debug("Router ===== len to be processed==== :", len(combinedList))
		}

		//List of jobs wich can be processed mapped per channel
		type workerJobT struct {
			worker *workerT
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
					JobState:      jobsdb.ExecutingState,
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
		rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destID})

		//Send the jobs to the jobQ
		for _, wrkJob := range toProcess {
			wrkJob.worker.channel <- wrkJob.job
		}

		countStat.Count(len(combinedList))
		generatorStat.End()
	}
}

func (rt *HandleT) crashRecover() {

	for {
		execList := rt.jobsDB.GetExecuting([]string{rt.destID}, jobQueryBatchSize)

		if len(execList) == 0 {
			break
		}
		logger.Debug("Router crash recovering", len(execList))

		var statusList []*jobsdb.JobStatusT

		for _, job := range execList {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				JobState:      jobsdb.FailedState,
				ErrorCode:     "",
				ErrorResponse: []byte(`{}`), // check
			}
			statusList = append(statusList, &status)
		}
		rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destID})
	}
}

func (rt *HandleT) printStatsLoop() {
	for {
		time.Sleep(60 * time.Second)
		logger.Info("Network Success/Fail", rt.successCount, rt.failCount)
		logger.Info("++++++++++++++++++++++++++++++")
		logger.Info(rt.toClearFailJobIDMap)
		logger.Info("++++++++++++++++++++++++++++++")
		for _, w := range rt.workers {
			logger.Info("--------------------------------", w.workerID)
			logger.Info(w.failedJobIDMap)
			logger.Info("--------------------------------", w.workerID)
		}
	}
}

func init() {
	config.Initialize()
	loadConfig()
}

//Setup initializes this module
func (rt *HandleT) Setup(jobsDB *jobsdb.HandleT, destID string) {
	logger.Info("Router started")
	rt.jobsDB = jobsDB
	rt.destID = destID
	rt.crashRecover()
	rt.requestQ = make(chan *jobsdb.JobT, jobQueryBatchSize)
	rt.responseQ = make(chan jobResponseT, jobQueryBatchSize)
	rt.toClearFailJobIDMap = make(map[int][]string)
	rt.isEnabled = true
	rt.netHandle = &NetHandleT{}
	rt.netHandle.Setup(destID)
	rt.perfStats = &misc.PerfStats{}
	rt.perfStats.Setup("StatsUpdate:" + destID)

	rt.initWorkers()
	go rt.printStatsLoop()
	go rt.statusInsertLoop()
	go rt.generatorLoop()
}
