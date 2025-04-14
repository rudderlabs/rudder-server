package batchrouter

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
)

// BatchWorker processes batches of jobs by grouping them by connection (source-destination pair)
// and delegating the actual processing to ConnectionWorkers. It implements the workerpool.Worker
// interface to integrate with the worker pool system.
type BatchWorker struct {
	partition string
	logger    logger.Logger
	brt       *Handle
	getJobs   func(string) []*ConnectionJob // Function to get jobs for this partition

	mu      sync.RWMutex
	workers map[string]*ConnectionWorker
}

// NewBatchWorker creates a new batch worker for the given partition
func NewBatchWorker(partition string, logger logger.Logger, brt *Handle, getJobs func(string) []*ConnectionJob) *BatchWorker {
	return &BatchWorker{
		partition: partition,
		logger:    logger.Child("batch-worker").With("partition", partition),
		brt:       brt,
		getJobs:   getJobs,
		workers:   make(map[string]*ConnectionWorker),
	}
}

// Work processes any available jobs in the shared job queue.
// Returns true if any jobs were processed.
func (bw *BatchWorker) Work() bool {
	// Get jobs from the shared queue
	jobs := bw.getJobs(bw.partition)
	if len(jobs) == 0 {
		bw.logger.Debugf("No jobs to process")
		return false
	}

	// Group jobs by connection
	jobsByConnection := make(map[string][]*ConnectionJob)
	for _, job := range jobs {
		connKey := fmt.Sprintf("%s:%s", job.sourceID, job.destID)
		jobsByConnection[connKey] = append(jobsByConnection[connKey], job)
	}

	// Process each connection's jobs with its own worker
	var wg sync.WaitGroup
	for connKey, connJobs := range jobsByConnection {
		wg.Add(1)
		worker := bw.getOrCreateWorker(connKey, connJobs)
		go func(w *ConnectionWorker) {
			defer wg.Done()
			w.Work()
		}(worker)
	}
	wg.Wait()

	return true
}

// SleepDurations returns the min and max sleep durations for the worker when idle
func (bw *BatchWorker) SleepDurations() (min, max time.Duration) {
	return time.Millisecond * 100, time.Second * 5
}

// Stop cleans up any resources used by the worker
func (bw *BatchWorker) Stop() {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	for _, worker := range bw.workers {
		worker.Stop()
	}
	bw.workers = make(map[string]*ConnectionWorker)
}

// getOrCreateWorker returns an existing ConnectionWorker for the given connection key
// or creates a new one if it doesn't exist
func (bw *BatchWorker) getOrCreateWorker(connKey string, jobs []*ConnectionJob) *ConnectionWorker {
	bw.mu.Lock()
	defer bw.mu.Unlock()

	worker, exists := bw.workers[connKey]
	if !exists {
		sourceID, destID := parseConnectionKey(connKey)
		worker = &ConnectionWorker{
			sourceID: sourceID,
			destID:   destID,
			brt:      bw.brt,
		}
		bw.workers[connKey] = worker
	}
	worker.jobs = jobs
	return worker
}

// parseConnectionKey splits a connection key into sourceID and destID.
// Returns empty strings if the key format is invalid.
func parseConnectionKey(connKey string) (sourceID, destID string) {
	parts := strings.Split(connKey, ":")
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}
