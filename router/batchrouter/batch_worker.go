package batchrouter

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

// BatchWorker implements the workerpool.Worker interface for processing batches of jobs
type BatchWorker struct {
	partition string
	jobs      []*ConnectionJob
	jobBuffer *JobBuffer
}

// Work processes the current batch of jobs
func (bw *BatchWorker) Work() bool {
	if len(bw.jobs) == 0 {
		return false
	}

	// Group jobs by connection (source-destination pair)
	connectionJobs := make(map[string][]*ConnectionJob)
	for _, job := range bw.jobs {
		key := fmt.Sprintf("%s:%s", job.sourceID, job.destID)
		connectionJobs[key] = append(connectionJobs[key], job)
	}

	// Reset jobs array for next batch
	bw.jobs = make([]*ConnectionJob, 0, bw.jobBuffer.brt.maxEventsInABatch)

	// Create a connection worker factory that includes the jobs
	workerJobs := connectionJobs
	pool := workerpool.New(context.Background(), func(partition string) workerpool.Worker {
		parts := strings.Split(partition, ":")
		sourceID := parts[0]
		destID := parts[1]
		return &ConnectionWorker{
			sourceID:  sourceID,
			destID:    destID,
			jobs:      workerJobs[fmt.Sprintf("%s:%s", sourceID, destID)],
			jobBuffer: bw.jobBuffer,
		}
	}, bw.jobBuffer.brt.logger)
	defer pool.Shutdown()

	// Start workers for each connection
	for key := range connectionJobs {
		pool.PingWorker(key)
	}

	return true
}

// SleepDurations returns the min and max sleep durations for the worker when idle
func (bw *BatchWorker) SleepDurations() (min, max time.Duration) {
	return time.Millisecond * 100, time.Second * 5
}

// Stop is a no-op since this worker doesn't have internal goroutines
func (bw *BatchWorker) Stop() {
	// No-op
}
