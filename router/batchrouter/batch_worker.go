package batchrouter

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

// BatchWorker processes batches of jobs by grouping them by connection (source-destination pair)
// and delegating the actual processing to ConnectionWorkers. It implements the workerpool.Worker
// interface to integrate with the worker pool system.
type BatchWorker struct {
	partition string           // Unique identifier for this worker
	jobs      []*ConnectionJob // Current batch of jobs to process
	jobBuffer *JobBuffer       // Reference to the parent job buffer
}

// Work processes the current batch of jobs by:
// 1. Grouping jobs by connection (source-destination pair)
// 2. Creating a connection worker pool
// 3. Delegating processing to connection workers
// Returns true if any jobs were processed, false otherwise.
func (bw *BatchWorker) Work() bool {
	if len(bw.jobs) == 0 {
		return false
	}

	// Group jobs by connection for parallel processing
	connectionJobs := bw.groupJobsByConnection()

	// Reset jobs array for next batch, preserving capacity
	bw.jobs = make([]*ConnectionJob, 0, bw.jobBuffer.brt.maxEventsInABatch)

	// Process jobs using connection workers
	bw.processWithConnectionWorkers(connectionJobs)

	return true
}

// groupJobsByConnection organizes jobs by their source-destination connection
func (bw *BatchWorker) groupJobsByConnection() map[string][]*ConnectionJob {
	connectionJobs := make(map[string][]*ConnectionJob)
	for _, job := range bw.jobs {
		key := getSourceDestKey(job.sourceID, job.destID)
		connectionJobs[key] = append(connectionJobs[key], job)
	}
	return connectionJobs
}

// processWithConnectionWorkers creates a worker pool and processes jobs for each connection
func (bw *BatchWorker) processWithConnectionWorkers(connectionJobs map[string][]*ConnectionJob) {
	// Create a connection worker pool
	pool := workerpool.New(context.Background(),
		func(partition string) workerpool.Worker {
			sourceID, destID := parseConnectionKey(partition)
			return &ConnectionWorker{
				sourceID:  sourceID,
				destID:    destID,
				jobs:      connectionJobs[partition],
				jobBuffer: bw.jobBuffer,
			}
		},
		bw.jobBuffer.brt.logger,
	)
	defer pool.Shutdown()

	// Start workers for each connection
	for key := range connectionJobs {
		pool.PingWorker(key)
	}
}

// parseConnectionKey splits a connection key into source and destination IDs
func parseConnectionKey(key string) (sourceID, destID string) {
	parts := strings.Split(key, ":")
	if len(parts) != 2 {
		panic(fmt.Sprintf("invalid connection key format: %s", key))
	}
	return parts[0], parts[1]
}

// SleepDurations returns the min and max sleep durations for the worker when idle.
// These values determine how long the worker should wait before checking for new work
// when there are no jobs to process.
func (bw *BatchWorker) SleepDurations() (min, max time.Duration) {
	return time.Millisecond * 100, time.Second * 5
}

// Stop cleans up any resources used by the worker.
// Currently a no-op as BatchWorker doesn't maintain any persistent resources.
func (bw *BatchWorker) Stop() {
	// No cleanup needed
}
