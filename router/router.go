package router

import (
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/misc"
	"github.com/tidwall/gjson"
)

//HandleT is the handle to this module.
type HandleT struct {
	requestQ  chan *jobsdb.JobT
	responseQ chan *jobsdb.JobStatusT
	jobsDB    *jobsdb.HandleT
	netHandle *NetHandleT
	destID    string
	workers   []*Worker
	perfStats *misc.PerfStats
}

// Worker a structure to define a worker for sending events to sinks
type Worker struct {
	channel    chan *jobsdb.JobT // the worker job channel
	workerID   int               // identifies the worker
	failedJobs int               // counts the failed jobs of a worker till it gets reset by external channel
	sleepTime  time.Duration     //the sleep duration for every job of the worker
}

var (
	jobQueryBatchSize, updateStatusBatchSize, noOfWorkers, noOfJobsPerChannel, ser int
	readSleep, maxSleep, maxStatusUpdateWait                                       time.Duration
	userIDPath                                                                     string
)

func loadConfig() {
	jobQueryBatchSize = config.GetInt("Router.jobQueryBatchSize")
	updateStatusBatchSize = config.GetInt("Router.updateStatusBatchSize")
	readSleep = config.GetDuration("Router.readSleepInS") * time.Second
	noOfWorkers = config.GetInt("Router.noOfWorkers")
	noOfJobsPerChannel = config.GetInt("Router.noOfJobsPerChannel")
	ser = config.GetInt("Router.ser")
	maxSleep = config.GetDuration("Router.maxSleepInS") * time.Second
	maxStatusUpdateWait = config.GetDuration("Router.maxStatusUpdateWaitInS") * time.Second
	userIDPath = config.GetString("Router.userIDPath") //"batch.#.message.context.traits.anonymous_id" // need to change this after transformation module
}

func (rt *HandleT) workerProcess(worker *Worker) {
	for {
		job := <-worker.channel
		var respStatusCode, attempts int
		var respStatus, body string

		log.Println("trying to send payload to GA")

		// tryout send for ser times
		for attempts = 0; attempts < ser; attempts++ {
			log.Printf("trying to send payload %v of %v", attempts, ser)

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
				log.Printf("worker %v sleeping for  %v ", worker.workerID, worker.sleepTime)
				time.Sleep(worker.sleepTime * time.Second)

				if worker.sleepTime < maxSleep {
					worker.sleepTime = 2 * worker.sleepTime
					log.Printf("sleep for worker %v increased to %v", worker.workerID, worker.sleepTime)
				}
				continue

			} else {
				// success
				worker.sleepTime = worker.sleepTime / 2
				log.Printf("sleep for worker %v decreased to %v", worker.workerID, worker.sleepTime)
				break
			}
		}

		log.Printf("code: %v, status: %v, body: %v", respStatusCode, respStatus, body)

		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			AttemptNum:    job.LastJobStatus.AttemptNum + attempts + 1,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     respStatus,
			ErrorResponse: []byte(`{}`), // check
		}

		if respStatusCode == http.StatusOK {
			status.JobState = "succeeded"
			log.Println("sending success status to response")
		} else {
			// the job failed
			worker.failedJobs++
			status.JobState = "failed"
			log.Println("sending failed status to response with done")
		}

		rt.responseQ <- &status

	}
}

func (rt *HandleT) initWorkers() {
	rt.workers = make([]*Worker, noOfWorkers)
	for i := 0; i < noOfWorkers; i++ {
		var worker *Worker
		workerChannel := make(chan *jobsdb.JobT, noOfJobsPerChannel)
		worker = &Worker{channel: workerChannel, workerID: i, failedJobs: 0, sleepTime: 1}
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
	// get userid from job.payload.
	// also insert a find a free worker logic
	userIDArray := gjson.GetBytes(job.EventPayload, userIDPath).Array()
	userID := userIDArray[0].String()

	// log.Println(userID)
	index := int(math.Abs(float64(getHash(userID) % noOfWorkers)))
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
	log.Println("job pushed to channel of ", w.workerID)
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
			log.Printf("flushing batch of %v status", updateStatusBatchSize)
			//Update the status
			rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destID})
			respCount = 0
			statusList = nil
			lastUpdate = time.Now()
		}
	}

}

func (rt *HandleT) generatorLoop() {

	fmt.Println("Generator started")

	for {
		toQuery := jobQueryBatchSize
		retryList := rt.jobsDB.GetToRetry([]string{rt.destID}, toQuery)
		toQuery -= len(retryList)
		unprocessedList := rt.jobsDB.GetUnprocessed([]string{rt.destID}, toQuery)

		if len(unprocessedList)+len(retryList) == 0 {
			time.Sleep(readSleep)
			continue
		}

		combinedList := append(unprocessedList, retryList...)

		sort.Slice(combinedList, func(i, j int) bool {
			return combinedList[i].JobID < combinedList[j].JobID
		})

		var statusList []*jobsdb.JobStatusT

		//Mark all the jobs as executing so that next loop doesn't pick them up
		for _, job := range combinedList {
			status := jobsdb.JobStatusT{
				JobID:         job.JobID,
				AttemptNum:    job.LastJobStatus.AttemptNum + 1,
				JobState:      "executing",
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
				JobState:      "failed",
				ErrorCode:     "",
				ErrorResponse: []byte(`{}`), // check
			}
			statusList = append(statusList, &status)
		}
		rt.jobsDB.UpdateJobStatus(statusList, []string{rt.destID})
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
	rt.netHandle = &NetHandleT{}
	rt.netHandle.Setup(destID)

	rt.perfStats = &misc.PerfStats{}
	rt.perfStats.Setup("StatsUpdate")

	rt.initWorkers()

	go rt.statusInsertLoop()
	go rt.generatorLoop()
}
