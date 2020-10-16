package router

import (
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

// batchWorkerT a structure to define a worker for sending events to sinks
type batchWorkerT struct {
	channel          chan []*jobsdb.JobT // the worker job channel
	workerID         int                 // identifies the worker
	failedJobs       int                 // counts the failed jobs of a worker till it gets reset by external channel
	sleepTime        time.Duration       //the sleep duration for every job of the worker
	failedJobIDMap   map[string]int64    //user to failed jobId
	failedJobIDMutex sync.RWMutex        //lock to protect structure above
	retryForJobMap   map[int64]time.Time //jobID to next retry time map
	jobRequestQ      chan *jobsdb.JobT
}

func (worker *batchWorkerT) PostJobOnWorker(job *jobsdb.JobT) {
	worker.jobRequestQ <- job
}

func (worker *batchWorkerT) WorkerProcess(rt *HandleT) {
}

func (worker *batchWorkerT) GetFailedJobIDMutex() *sync.RWMutex {
	return &worker.failedJobIDMutex
}

func (worker *batchWorkerT) GetFailedJobIDMap() map[string]int64 {
	return worker.failedJobIDMap
}
