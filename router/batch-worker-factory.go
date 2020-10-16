package router

import (
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type BatchWorkerFactory struct {
	workers []*batchWorkerT
}

func (factory *BatchWorkerFactory) CreateWorkers(rt *HandleT) {
	factory.workers = make([]*batchWorkerT, noOfWorkers)
	for i := 0; i < noOfWorkers; i++ {
		logger.Info("Worker Started", i)
		var worker *batchWorkerT
		worker = &batchWorkerT{
			channel:        make(chan []*jobsdb.JobT, noOfJobsPerChannel),
			failedJobIDMap: make(map[string]int64),
			retryForJobMap: make(map[int64]time.Time),
			workerID:       i,
			failedJobs:     0,
			sleepTime:      minSleep,
			jobRequestQ:    make(chan *jobsdb.JobT, noOfJobsPerChannel)}
		factory.workers[i] = worker
		rruntime.Go(func() {
			worker.WorkerProcess(rt)
		})

		//workerJobBatchers are used when workerVersion >= 1
		rruntime.Go(func() {
			workerJobBatcher(worker)
		})
	}
}

func (factory *BatchWorkerFactory) GetWorker(idx int) WorkerInterface {
	return factory.workers[idx]
}

// ResetSleep  this makes the workers reset their sleep
func (factory *BatchWorkerFactory) ResetSleep() {
	for _, w := range factory.workers {
		w.sleepTime = minSleep
	}
}

func workerJobBatcher(worker *batchWorkerT) {
	var workerJobBatchBuffer = make([]*jobsdb.JobT, 0)
	timeout := time.After(workerRequestBatchTimeout)
	for {
		select {
		case workerJobRequest := <-worker.jobRequestQ:
			//Append to request buffer
			workerJobBatchBuffer = append(workerJobBatchBuffer, workerJobRequest)
			if len(workerJobBatchBuffer) == 100 { //TODO make it configurable
				worker.channel <- workerJobBatchBuffer
				workerJobBatchBuffer = nil
				workerJobBatchBuffer = make([]*jobsdb.JobT, 0)
			}
		case <-timeout:
			timeout = time.After(workerRequestBatchTimeout)
			if len(workerJobBatchBuffer) > 0 {
				worker.channel <- workerJobBatchBuffer
				workerJobBatchBuffer = nil
				workerJobBatchBuffer = make([]*jobsdb.JobT, 0)
			}
		}
	}
}
