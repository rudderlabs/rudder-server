package router

import (
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/integrations"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/misc"
)

//HandleT is the handle to this module.
type HandleT struct {
	requestQ               chan *jobsdb.JobT
	responseQ              chan jobResponseT
	jobsDB                 *jobsdb.HandleT
	netHandle              *NetHandleT
	destID                 string
	workers                []*workerT
	perfStats              *misc.PerfStats
	successCount           uint64
	failCount              uint64
	isEnabled              bool
	lastFailedToClearMutex sync.Mutex
	lastFailedToClear      map[*workerT][]string
}

type jobResponseT struct {
	status *jobsdb.JobStatusT
	worker *workerT
	userID string
}

// workerT a structure to define a worker for sending events to sinks
type workerT struct {
	channel              chan *jobsdb.JobT // the worker job channel
	workerID             int               // identifies the worker
	failedJobs           int               // counts the failed jobs of a worker till it gets reset by external channel
	sleepTime            time.Duration     //the sleep duration for every job of the worker
	lastFailedJobID      map[string]int64  //user to failed jobId
	lastFailedJobIDMutex sync.RWMutex      //lock to protect structure above
}

var (
	jobQueryBatchSize, updateStatusBatchSize, noOfWorkers, noOfJobsPerChannel, ser int
	maxFailedCountForJob                                                           int
	readSleep, minSleep, maxSleep, maxStatusUpdateWait                             time.Duration
	randomWorkerAssign, useTestSink                                                bool
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
	useTestSink = config.GetBool("Router.useTestSink", false)
	maxFailedCountForJob = config.GetInt("Router.maxFailedCountForJob", 8)
	testSinkURL = config.GetEnv("TEST_SINK_URL", "http://localhost:8181")
}

func (rt *HandleT) workerProcess(worker *workerT) {
	for {
		job := <-worker.channel
		var respStatusCode, attempts int
		var respStatus, respBody string

		log.Println("Router :: trying to send payload to GA", respBody)

		postInfo := integrations.GetPostInfo(job.EventPayload)
		userID := postInfo.UserID
		misc.Assert(userID != "")

		//If sink is not enabled mark all jobs as waiting
		if !rt.isEnabled {
			log.Println("Router is disabled")
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
		worker.lastFailedJobIDMutex.RLock()
		previousFailedJobID, isPrevFailedUser := worker.lastFailedJobID[userID]
		worker.lastFailedJobIDMutex.RUnlock()

		if isPrevFailedUser && previousFailedJobID < job.JobID {
			log.Printf("Router :: prev id %v, current id %v", previousFailedJobID, job.JobID)
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
			log.Printf("Router :: trying to send payload %v of %v", attempts, ser)
			respStatusCode, respStatus, respBody = rt.netHandle.sendPost(job.EventPayload)
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
				log.Printf("Router :: worker %v sleeping for  %v ",
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
				log.Printf("Router :: sleep for worker %v decreased to %v",
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
			//The job succeded for this user. If it was marked
			//failed earlier, we ned to remove it but we cannot do
			//it here. There may be other jobs ahead of this job
			//which have been put to WaitingState but are still
			//in the responseQ and haven't made to DBk. If we
			//remove this blocking JobID from lastFailedJob now
			//then the blocking wall in generateLoop() will get
			//removed and jobs with higher jobID than the ones
			//pending in responseQ will get passed.
			//To prevent this, we remove blocking JobID from
			//lastFailedJob only when we are sure the blocking JobID
			//status has made to DB. That ensures all ahead of it
			//have made to DB too.
			status.AttemptNum = job.LastJobStatus.AttemptNum
			status.JobState = jobsdb.SucceededState
			log.Println("Router :: sending success status to response")
			rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
		} else {
			// the job failed
			log.Println("Router :: Job failed to send, analyzing...")
			worker.failedJobs++
			atomic.AddUint64(&rt.failCount, 1)

			//Add it to fail map
			if !isPrevFailedUser {
				log.Printf("Router :: userId %v failed for the first time adding to map", userID)
				worker.lastFailedJobIDMutex.Lock()
				worker.lastFailedJobID[userID] = job.JobID
				worker.lastFailedJobIDMutex.Unlock()
			}

			switch {
			case len(worker.lastFailedJobID) > 5:
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
				log.Println("Router :: Aborting the job and deleting from user map")
				status.JobState = jobsdb.AbortedState
				status.AttemptNum = job.LastJobStatus.AttemptNum
				break
			default:
				status.JobState = jobsdb.FailedState
				status.AttemptNum = job.LastJobStatus.AttemptNum
				break
			}
			log.Println("Router :: sending waiting state as response")
			rt.responseQ <- jobResponseT{status: &status, worker: worker, userID: userID}
		}
	}
}

func (rt *HandleT) initWorkers() {
	rt.workers = make([]*workerT, noOfWorkers)
	for i := 0; i < noOfWorkers; i++ {
		fmt.Println("Worker Started", i)
		var worker *workerT
		worker = &workerT{
			channel:         make(chan *jobsdb.JobT, noOfJobsPerChannel),
			lastFailedJobID: make(map[string]int64),
			workerID:        i,
			failedJobs:      0,
			sleepTime:       minSleep}
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

	var w *workerT

	postInfo := integrations.GetPostInfo(job.EventPayload)

	var index int
	if randomWorkerAssign {
		index = rand.Intn(noOfWorkers)
	} else {
		index = int(math.Abs(float64(getHash(postInfo.UserID) % noOfWorkers)))
	}
	// log.Printf("userId: %s index: %d", userID, index)
	for _, worker := range rt.workers {
		if worker.workerID == index {
			w = worker
			break
		}
	}
	misc.Assert(w != nil)

	//#JobOrder (see other #JobOrder comment)
	//We need to check if this worker has blocked this userID. This is
	//improtant to keep the order of jobs. If a worker has blocked a
	//particularly userID/jobID, there may still unprocessed jobs in the worker
	//channel from that userID while generateLoop() may have found
	//the blocking jobID and other (higher jobIDs) from the DB. Jobs in
	//channel will eventually get rejected (to WaitingState) but the other jobs
	//that get added later in channel behind the blocking jobID will pass through
	//if blocking JobID succeeds next. To prevent that, we don't let any other job
	//from the userID go into the channel
	w.lastFailedJobIDMutex.RLock()
	defer w.lastFailedJobIDMutex.RUnlock()
	blockJobID, found := w.lastFailedJobID[postInfo.UserID]
	if !found {
		return w
	} else {
		//This job can only be higher than blocking
		//We only let the blocking job pass
		misc.Assert(job.JobID >= blockJobID)
		if job.JobID == blockJobID {
			return w
		} else {
			return nil
		}
	}
}

// MakeSleepToZero this makes the workers reset their sleep
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
	for {
		rt.perfStats.Start()
		select {
		case jobStatus := <-rt.responseQ:
			log.Printf("Router :: Got back status error %v and state %v for job %v", jobStatus.status.ErrorCode,
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
			var statusList []*jobsdb.JobStatusT
			for _, resp := range responseList {
				statusList = append(statusList, resp.status)
			}

			if len(statusList) > 0 {
				log.Printf("Router :: flushing batch of %v status", updateStatusBatchSize)

				sort.Slice(statusList, func(i, j int) bool {
					return statusList[i].JobID < statusList[j].JobID
				})
				//Update the status
				rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destID})
			}

			for _, resp := range responseList {
				status := resp.status.JobState
				userID := resp.userID
				worker := resp.worker
				if status == jobsdb.SucceededState || status == jobsdb.AbortedState {
					worker.lastFailedJobIDMutex.RLock()
					lastJobID, ok := worker.lastFailedJobID[userID]
					if ok && lastJobID == resp.status.JobID {
						//#JobOrder (see other #JobOrder comment)
						//The blocking jobID which had last failed
						//has succeeded and made it to status update.
						//At this point we are sure there are no pending
						//jobs in requestQ or responseQ from that userID
						//We can't still remove the gate. There may be
						//pending jobs in mainLoops buffer. The last step
						//is to keep it in lastFailedToClear buffer from
						//which it is deleted
						rt.lastFailedToClearMutex.Lock()
						_, ok := rt.lastFailedToClear[worker]
						if !ok {
							rt.lastFailedToClear[worker] = make([]string, 0)
						}
						rt.lastFailedToClear[worker] = append(rt.lastFailedToClear[worker], userID)
						rt.lastFailedToClearMutex.Unlock()
					}
					worker.lastFailedJobIDMutex.RUnlock()
				}
			}
			responseList = nil
			lastUpdate = time.Now()
		}
	}

}

func (rt *HandleT) generatorLoop() {

	fmt.Println("Generator started")

	for {
		if !rt.isEnabled {
			continue
		}

		//#JobOrder
		rt.lastFailedToClearMutex.Lock()
		for wrk := range rt.lastFailedToClear {
			wrk.lastFailedJobIDMutex.Lock()
			for _, userID := range rt.lastFailedToClear[wrk] {
				delete(wrk.lastFailedJobID, userID)
			}
			wrk.lastFailedJobIDMutex.Unlock()
		}
		//Reset the structure
		rt.lastFailedToClear = make(map[*workerT][]string)
		rt.lastFailedToClearMutex.Unlock()

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
			log.Println("Router :: router is enabled")
			log.Println("Router ===== len to be processed==== :", len(combinedList))
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
				toProcess = append(toProcess, workerJobT{worker :w, job: job})
			}
		}

		//Mark the jobs as executing
		rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destID})

		//Send the jobs to the jobQ
		for _, wrkJob := range toProcess {
			wrkJob.worker.channel <- wrkJob.job
		}
	}
}

func (rt *HandleT) crashRecover() {

	for {
		execList := rt.jobsDB.GetExecuting([]string{rt.destID}, jobQueryBatchSize)

		if len(execList) == 0 {
			break
		}
		log.Println("Router crash recovering", len(execList))
		fmt.Println("Router crash recovering", len(execList))

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
		time.Sleep(5 * time.Second)
		//fmt.Println("Network Success/Fail", rt.successCount, rt.failCount)
	}
}

func init() {
	config.Initialize()
	loadConfig()
}

//Setup initializes this module
func (rt *HandleT) Setup(jobsDB *jobsdb.HandleT, destID string) {
	fmt.Println("Router started")
	rt.jobsDB = jobsDB
	rt.destID = destID
	rt.crashRecover()
	rt.requestQ = make(chan *jobsdb.JobT, jobQueryBatchSize)
	rt.responseQ = make(chan jobResponseT, jobQueryBatchSize)
	rt.lastFailedToClear = make(map[*workerT][]string)
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
