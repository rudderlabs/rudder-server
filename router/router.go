package router

import (
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/integrations"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/misc"
)

//HandleT is the handle to this module.
type HandleT struct {
	requestQ          chan *jobsdb.JobT
	responseQ         chan *jobsdb.JobStatusT
	jobsDB            *jobsdb.HandleT
	netHandle         *NetHandleT
	destID            string
	workers           []*Worker
	perfStats         *misc.PerfStats
	successCount      uint64
	failCount         uint64
	Enabled           bool
	IsSinkAPIKeyError bool
	IsSinkDown        bool
}

// Worker a structure to define a worker for sending events to sinks
type Worker struct {
	channel         chan *jobsdb.JobT // the worker job channel
	workerID        int               // identifies the worker
	failedJobs      int               // counts the failed jobs of a worker till it gets reset by external channel
	sleepTime       time.Duration     //the sleep duration for every job of the worker
	userToFailedJob map[string]int64  //user to failed jobId
}

var (
	jobQueryBatchSize, updateStatusBatchSize, noOfWorkers, noOfJobsPerChannel, ser, maxFailedCountForJob int
	readSleep, maxSleep, maxStatusUpdateWait                                                             time.Duration
	randomWorkerAssign, useTestSink                                                                      bool
)

func loadConfig() {
	jobQueryBatchSize = config.GetInt("Router.jobQueryBatchSize", 10000)
	updateStatusBatchSize = config.GetInt("Router.updateStatusBatchSize", 1000)
	readSleep = config.GetDuration("Router.readSleepInMS", time.Duration(10)) * time.Millisecond
	noOfWorkers = config.GetInt("Router.noOfWorkers", 8)
	noOfJobsPerChannel = config.GetInt("Router.noOfJobsPerChannel", 1000)
	ser = config.GetInt("Router.ser", 3)
	maxSleep = config.GetDuration("Router.maxSleepInS", time.Duration(5)) * time.Second
	maxStatusUpdateWait = config.GetDuration("Router.maxStatusUpdateWaitInS", time.Duration(5)) * time.Second
	randomWorkerAssign = config.GetBool("Router.randomWorkerAssign", false)
	useTestSink = config.GetBool("Router.useTestSink", false)
	maxFailedCountForJob = config.GetInt("Router.maxFailedCountForJob", 8)
}

func (rt *HandleT) workerProcess(worker *Worker) {
	for {
		job := <-worker.channel
		var respStatusCode, attempts int
		var respStatus, body string

		log.Println("Router :: trying to send payload to GA")
		//fmt.Printf("Router :: isSinkAPIKeyError %v isSinkDown %v", rt.isSinkAPIKeyError, rt.isSinkDown)
		if rt.IsSinkAPIKeyError || rt.IsSinkDown {
			log.Println("Router :: api error or sink down")
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "",
				JobState:      jobsdb.WaitingState,
				ErrorResponse: []byte(`{}`), // check
			}
			rt.responseQ <- &status
			continue
		}

		postInfo := integrations.GetPostInfo(job.EventPayload)
		userID := postInfo.UserID

		if userID == "" {
			// shouldn't be, still check and continue
			log.Println("Router :: Failed to get userID for job")
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     respStatus,
				JobState:      jobsdb.FailedState,
				ErrorResponse: []byte(`{}`), // check
			}
			rt.responseQ <- &status
			continue
		}

		// check if an earlier job for this userId is still undelivered
		previousFailedJobID, isUserFound := worker.userToFailedJob[userID]
		if isUserFound {
			log.Println("Router :: user of this job has an earlier job stuck with id: ", previousFailedJobID)
			if previousFailedJobID < job.JobID {
				log.Printf("Router :: prev id %v, current id %v", previousFailedJobID, job.JobID)
				status := jobsdb.JobStatusT{
					JobID:         job.JobID,
					AttemptNum:    job.LastJobStatus.AttemptNum + 1,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     respStatus,
					JobState:      jobsdb.WaitingState,
					ErrorResponse: []byte(`{}`), // check
				}
				rt.responseQ <- &status
				continue
			}
		}

		// tryout send for ser times, for succesful user jobs
		for attempts = 0; attempts < ser; attempts++ {
			log.Printf("Router :: trying to send payload %v of %v", attempts, ser)

			// ToDo: handle error in network send gracefully!!

			if respStatusCode, respStatus, body = rt.netHandle.sendPost(job.EventPayload); respStatusCode != http.StatusOK {

				// the sleep may have gone to zero, to start things off again, assign it to 1
				if worker.sleepTime < 1 {
					worker.sleepTime = 1
				}

				if respStatusCode >= http.StatusBadRequest && respStatusCode <= http.StatusUnavailableForLegalReasons {
					// won't continue in case of these error codes (client error)
					break
				}
				// increasing sleep after every failure
				log.Printf("Router :: worker %v sleeping for  %v ", worker.workerID, worker.sleepTime)
				time.Sleep(worker.sleepTime * time.Second)

				if worker.sleepTime < maxSleep {
					worker.sleepTime = 2 * worker.sleepTime
					log.Printf("Router :: sleep for worker %v increased to %v", worker.workerID, worker.sleepTime)
				}
				continue

			} else {
				atomic.AddUint64(&rt.successCount, 1)
				// success
				worker.sleepTime = worker.sleepTime / 2
				log.Printf("Router :: sleep for worker %v decreased to %v", worker.workerID, worker.sleepTime)
				break
			}
		}

		log.Printf("Router :: code: %v, status: %v, body: %v", respStatusCode, respStatus, body)

		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum + 1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     respStatus,
			ErrorResponse: []byte(`{}`), // check
		}

		if respStatusCode == http.StatusOK {
			if previousFailedJobID == job.JobID {
				delete(worker.userToFailedJob, userID)
			}
			status.JobState = jobsdb.SucceededState
			log.Println("Router :: sending success status to response")
			rt.responseQ <- &status
		} else {
			// the job failed
			log.Println("Router :: Job failed to send, analyzing...")
			worker.failedJobs++
			atomic.AddUint64(&rt.failCount, 1)

			// store the failed job first in the map
			_, okToUse := worker.userToFailedJob[userID]
			if !okToUse {
				log.Printf("Router :: userId %v failed for the first time, adding the failed job to map", userID)
				worker.userToFailedJob[userID] = job.JobID
				//status.JobState = jobsdb.FailedState
			}

			// have seen enough for this job, abort
			// max count is 8 by default, so maxSleep * maxFailedCountForJob ~ 2 mins,
			// so, even if sink is down/timing out in between this, it will get handled
			// else can stop the sink from enabled destination.
			if status.AttemptNum > maxFailedCountForJob {
				if okToUse {
					log.Println("Router :: Aborting the job and deleting from user map")
					delete(worker.userToFailedJob, userID)
				}
				status.JobState = jobsdb.AbortedState //Abort the job
				rt.responseQ <- &status
				continue
			}

			// can be client error (payload, etc) or server error or timeout
			if respStatusCode == http.StatusUnauthorized {
				// api key error
				// process the error
				log.Println("Router :: unauthorized, stopping the router")
				rt.IsSinkAPIKeyError = true
			}

			log.Println("Router :: sending waiting state as response")
			status.JobState = jobsdb.WaitingState
			rt.responseQ <- &status

			//log.Println("sending failed status to response with done")
		}

	}
}

func (rt *HandleT) initWorkers() {
	rt.workers = make([]*Worker, noOfWorkers)
	for i := 0; i < noOfWorkers; i++ {
		var worker *Worker
		workerChannel := make(chan *jobsdb.JobT, noOfJobsPerChannel)
		workerUserTOJobMap := make(map[string]int64)
		worker = &Worker{channel: workerChannel, userToFailedJob: workerUserTOJobMap, workerID: i, failedJobs: 0, sleepTime: 1}
		rt.workers[i] = worker
		go rt.workerProcess(worker)

	}
}

func getHash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32())
}

func (rt *HandleT) findWorker(job *jobsdb.JobT) *Worker {
	var w *Worker

	// also insert a find a free worker logic

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
	return w
}

func (rt *HandleT) assignJobToWorkers(job *jobsdb.JobT) {
	w := rt.findWorker(job)
	w.channel <- job
	log.Println("Router :: job pushed to channel of ", w.workerID)
}

// MakeSleepToZero this makes the workers reset their sleep
func (rt *HandleT) MakeSleepToZero() {
	for _, w := range rt.workers {
		if w.sleepTime > maxSleep {
			w.sleepTime = 0
			w.failedJobs = 0
		}
	}

}

func (rt *HandleT) statusInsertLoop() {

	var statusList []*jobsdb.JobStatusT
	respCount := 0
	//Wait for the responses from statusQ
	lastUpdate := time.Now()
	for {
		rt.perfStats.Start()
		select {
		case status := <-rt.responseQ:
			log.Printf("Router :: Got back status error %v and state %v for job %v", status.ErrorCode, status.JobState, status.JobID)
			statusList = append(statusList, status)
			respCount++
			rt.perfStats.End(1)
		case <-time.After(maxStatusUpdateWait):
			rt.perfStats.End(0)
			//Ideally should sleep for duration maxStatusUpdateWait-(time.Now()-lastUpdate)
			//but approx is good enough at the cost of reduced computation.
		}

		if respCount >= updateStatusBatchSize || time.Since(lastUpdate) > maxStatusUpdateWait {
			rt.perfStats.Print()
			if respCount > 0 {
				log.Printf("Router :: flushing batch of %v status", updateStatusBatchSize)
				//Update the status
				rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destID})
				respCount = 0
				statusList = nil
				lastUpdate = time.Now()
			}
		}
	}

}

func (rt *HandleT) generatorLoop() {

	fmt.Println("Generator started")

	for {
		// could use mutex, though not particularly a race condition
		if !rt.Enabled || rt.IsSinkAPIKeyError {
			continue
		}
		toQuery := jobQueryBatchSize
		waitList := rt.jobsDB.GetWaiting([]string{rt.destID}, toQuery) //Jobs send to waiting state
		toQuery -= len(waitList)
		retryList := rt.jobsDB.GetToRetry([]string{rt.destID}, toQuery)
		toQuery -= len(retryList)
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

		var statusList []*jobsdb.JobStatusT

		//Mark all the jobs as executing so that next loop doesn't pick them up
		for _, job := range combinedList {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
				JobState:      jobsdb.ExecutingState,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "",
				ErrorResponse: []byte(`{}`), // check
			}
			statusList = append(statusList, &status)
		}
		rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destID})

		//Send the jobs to the jobQ
		for _, job := range combinedList {
			rt.assignJobToWorkers(job)
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
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
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

//Setup initializes this module
func (rt *HandleT) Setup(jobsDB *jobsdb.HandleT, destID string) {
	loadConfig()
	fmt.Println("Router started")
	rt.jobsDB = jobsDB
	rt.destID = destID
	rt.crashRecover()
	rt.requestQ = make(chan *jobsdb.JobT, jobQueryBatchSize)
	rt.responseQ = make(chan *jobsdb.JobStatusT, jobQueryBatchSize)
	rt.Enabled = true
	rt.IsSinkAPIKeyError = false
	rt.IsSinkDown = false
	rt.netHandle = &NetHandleT{}
	rt.netHandle.Setup(destID)

	rt.perfStats = &misc.PerfStats{}
	rt.perfStats.Setup("StatsUpdate:" + destID)

	rt.initWorkers()
	go rt.printStatsLoop()
	go rt.statusInsertLoop()
	go rt.generatorLoop()
}
