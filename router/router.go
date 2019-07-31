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
	requestQ     chan *jobsdb.JobT
	responseQ    chan *jobsdb.JobStatusT
	jobsDB       *jobsdb.HandleT
	netHandle    *NetHandleT
	destID       string
	workers      []*Worker
	perfStats    *misc.PerfStats
	successCount uint64
	failCount    uint64
	isEnabled    bool
}

// Worker a structure to define a worker for sending events to sinks
type Worker struct {
	channel                chan *jobsdb.JobT // the worker job channel
	workerID               int               // identifies the worker
	failedJobs             int               // counts the failed jobs of a worker till it gets reset by external channel
	sleepTime              time.Duration     //the sleep duration for every job of the worker
	userLastFailedJobIDMap map[string]int64  //user to failed jobId
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

func (rt *HandleT) workerProcess(worker *Worker) {
	for {
		job := <-worker.channel
		var respStatusCode, attempts int
		var respStatus, respBody string

		log.Println("Router :: trying to send payload to GA")

		//If sink is not enabled OR is not working, mark all jobs
		//as waiting
		if !rt.isEnabled {
			log.Println("Router is disabled")
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
		misc.Assert(userID != "")

		//If there is a failed jobID from this user, we cannot pass future jobs
		previousFailedJobID, isPrevFailedUser := worker.userLastFailedJobIDMap[userID]
		if isPrevFailedUser && previousFailedJobID < job.JobID {
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

		//We can try to execute the job
		for attempts = 0; attempts < ser; attempts++ {
			log.Printf("Router :: trying to send payload %v of %v", attempts, ser)

			respStatusCode, respStatus, respBody = rt.netHandle.sendPost(job.EventPayload)
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
				if worker.sleepTime > minSleep {
					log.Printf("Router :: sleep for worker %v decreased to %v",
						worker.workerID, worker.sleepTime)
					worker.sleepTime = worker.sleepTime / 2
				}
				break
			}
		}

		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum + 1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     respStatus,
			ErrorResponse: []byte(respBody),
		}

		if respStatusCode == http.StatusOK {
			//The job succeded for this user so remove the field
			if isPrevFailedUser {
				misc.Assert(previousFailedJobID == job.JobID)
				delete(worker.userLastFailedJobIDMap, userID)
			}
			status.JobState = jobsdb.SucceededState
			log.Println("Router :: sending success status to response")
			rt.responseQ <- &status
		} else {
			// the job failed
			log.Println("Router :: Job failed to send, analyzing...")
			worker.failedJobs++
			atomic.AddUint64(&rt.failCount, 1)

			if !isPrevFailedUser {
				//Regular failure. Mark worker as done
				log.Printf("Router :: userId %v failed for the first time adding to map", userID)
				worker.userLastFailedJobIDMap[userID] = job.JobID
			}

			switch {

			case len(worker.userLastFailedJobIDMap) > int(0.05*float64(noOfJobsPerChannel)):
				//Lot of jobs are failing in this wrker. Likely the sink is down
				//so we mark future jobs as waiting
				status.JobState = jobsdb.WaitingState
				break
			case status.AttemptNum > maxFailedCountForJob:
				//The job has failed enough number of times so mark it done
				//The reason for doing this is to filter out jobs with bad payload
				//which can never succeed.
				//However, there is a risk that if sink is down, a set of jobs can
				//reach maxCountFailure. In practice though, when sink goes down
				//lot of jobs will fail and all will get retried in batch with
				//doubling sleep in between. That case will be handled in case above
				if isPrevFailedUser {
					log.Println("Router :: Aborting the job and deleting from user map")
					delete(worker.userLastFailedJobIDMap, userID)
				}
				status.JobState = jobsdb.AbortedState
				break
			default:
				status.JobState = jobsdb.FailedState
				break
			}
			log.Println("Router :: sending waiting state as response")
			rt.responseQ <- &status
		}
	}
}

func (rt *HandleT) initWorkers() {
	rt.workers = make([]*Worker, noOfWorkers)
	for i := 0; i < noOfWorkers; i++ {
		fmt.Println("Worker Started", i)
		var worker *Worker
		worker = &Worker{
			channel:                make(chan *jobsdb.JobT, noOfJobsPerChannel),
			userLastFailedJobIDMap: make(map[string]int64),
			workerID:               i,
			failedJobs:             0,
			sleepTime:              minSleep}
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

//Enable enables a router :)
func (rt *HandleT) Enable() {
	rt.isEnabled = true
}

//Disable disables a router:)
func (rt *HandleT) Disable() {
	rt.isEnabled = false
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
		if !rt.isEnabled {
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
	rt.responseQ = make(chan *jobsdb.JobStatusT, jobQueryBatchSize)
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
