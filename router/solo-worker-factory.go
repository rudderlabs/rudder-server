package router

import (
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type SoloWorkerFactory struct {
	workers []*workerT
}

func (factory *SoloWorkerFactory) CreateWorkers(rt *HandleT) {
	factory.workers = make([]*workerT, noOfWorkers)
	for i := 0; i < noOfWorkers; i++ {
		logger.Info("Worker Started", i)
		var worker *workerT
		worker = &workerT{
			channel:        make(chan *jobsdb.JobT, noOfJobsPerChannel),
			failedJobIDMap: make(map[string]int64),
			retryForJobMap: make(map[int64]time.Time),
			workerID:       i,
			failedJobs:     0,
			sleepTime:      minSleep}
		factory.workers[i] = worker
		rruntime.Go(func() {
			worker.WorkerProcess(rt)
		})
	}
}

func (factory *SoloWorkerFactory) GetWorker(idx int) WorkerInterface {
	return factory.workers[idx]
}

// ResetSleep  this makes the workers reset their sleep
func (factory *SoloWorkerFactory) ResetSleep() {
	for _, w := range factory.workers {
		w.sleepTime = minSleep
	}
}
