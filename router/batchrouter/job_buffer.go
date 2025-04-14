package batchrouter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
)

// JobBuffer manages job buffering and batch processing for source-destination pairs.
// It handles the buffering of jobs through channels and manages batch workers that
// process these jobs in configurable batch sizes.
type JobBuffer struct {
	// Core components
	brt        *Handle
	workerPool workerpool.WorkerPool

	// Channel management
	sourceDestMap map[string]chan *jobsdb.JobT
	mu            sync.RWMutex

	// Worker management
	batchWorkers   map[string]*BatchWorker
	batchWorkersMu sync.RWMutex
}

// ConnectionJob represents a job with its connection details for batch processing
type ConnectionJob struct {
	job      *jobsdb.JobT // The actual job to be processed
	sourceID string       // Source identifier
	destID   string       // Destination identifier
}

// NewJobBuffer creates and initializes a new JobBuffer instance
func NewJobBuffer(brt *Handle) *JobBuffer {
	if brt == nil {
		panic("batch router handle cannot be nil")
	}

	jb := &JobBuffer{
		brt:           brt,
		sourceDestMap: make(map[string]chan *jobsdb.JobT),
		batchWorkers:  make(map[string]*BatchWorker),
	}

	// Initialize the worker pool with a worker factory
	jb.workerPool = workerpool.New(context.Background(), jb.createBatchWorker, brt.logger)

	return jb
}

// createBatchWorker is a factory function for creating new batch workers
func (jb *JobBuffer) createBatchWorker(partition string) workerpool.Worker {
	return &BatchWorker{
		partition: partition,
		jobs:      make([]*ConnectionJob, 0, jb.brt.maxEventsInABatch),
		jobBuffer: jb,
	}
}

// createStat creates a new stat with consistent source and destination tags
func (jb *JobBuffer) createStat(name, statType, sourceID, destID string) stats.Measurement {
	return stats.Default.NewTaggedStat(name, statType, stats.Tags{
		"destType": jb.brt.destType,
		"sourceId": sourceID,
		"destId":   destID,
	})
}

// getSourceDestKey generates a unique key for a source-destination pair
func getSourceDestKey(sourceID, destID string) string {
	return fmt.Sprintf("%s:%s", sourceID, destID)
}

// getBatchWorker returns or creates a batch worker for a source-destination pair
func (jb *JobBuffer) getBatchWorker(sourceID, destID string) *BatchWorker {
	key := getSourceDestKey(sourceID, destID)

	jb.batchWorkersMu.Lock()
	defer jb.batchWorkersMu.Unlock()

	worker, exists := jb.batchWorkers[key]
	if !exists {
		worker = &BatchWorker{
			partition: key,
			jobs:      make([]*ConnectionJob, 0, jb.brt.maxEventsInABatch),
			jobBuffer: jb,
		}
		jb.batchWorkers[key] = worker
	}
	return worker
}

// getJobChannel returns or creates a buffered channel for a source-destination pair
func (jb *JobBuffer) getJobChannel(sourceID, destID string) chan *jobsdb.JobT {
	key := getSourceDestKey(sourceID, destID)

	// Fast path: check if channel exists
	jb.mu.RLock()
	ch, exists := jb.sourceDestMap[key]
	jb.mu.RUnlock()
	if exists {
		return ch
	}

	// Slow path: create new channel
	jb.mu.Lock()
	defer jb.mu.Unlock()

	// Double-check to avoid race conditions
	if ch, exists = jb.sourceDestMap[key]; exists {
		return ch
	}

	// Determine buffer size
	bufferSize := jb.brt.maxEventsInABatch
	if customSize := jb.brt.conf.GetIntVar(0, 0, "BatchRouter."+jb.brt.destType+".channelBufferSize", "BatchRouter.channelBufferSize"); customSize > 0 {
		bufferSize = customSize
	}

	// Create and initialize new channel
	ch = make(chan *jobsdb.JobT, bufferSize)
	jb.sourceDestMap[key] = ch

	// Initialize the batch worker before starting the consumer
	_ = jb.getBatchWorker(sourceID, destID)

	// Start consumer for this channel
	go jb.startJobConsumer(sourceID, destID, ch)

	return ch
}

// startJobConsumer processes jobs from the channel and manages batch processing
func (jb *JobBuffer) startJobConsumer(sourceID, destID string, ch chan *jobsdb.JobT) {
	key := getSourceDestKey(sourceID, destID)
	var lastJobTime time.Time
	worker := jb.getBatchWorker(sourceID, destID)

	for job := range ch {
		jb.batchWorkersMu.Lock()
		worker.jobs = append(worker.jobs, &ConnectionJob{
			job:      job,
			sourceID: sourceID,
			destID:   destID,
		})
		currentBatchSize := len(worker.jobs)
		jb.batchWorkersMu.Unlock()

		if lastJobTime.IsZero() {
			lastJobTime = time.Now()
		}

		// Trigger batch processing when thresholds are met
		if currentBatchSize >= jb.brt.maxEventsInABatch || time.Since(lastJobTime) >= jb.brt.uploadFreq.Load() {
			jb.workerPool.PingWorker(key)
			lastJobTime = time.Time{} // Reset timer after ping
		}
	}

	// Process any remaining jobs when channel is closed
	jb.batchWorkersMu.Lock()
	if len(worker.jobs) > 0 {
		jb.workerPool.PingWorker(key)
	}
	jb.batchWorkersMu.Unlock()
}

// AddJob adds a job to the appropriate buffer channel
func (jb *JobBuffer) AddJob(sourceID, destID string, job *jobsdb.JobT) {
	ch := jb.getJobChannel(sourceID, destID)

	// Try non-blocking send first
	select {
	case ch <- job:
		return
	default:
		// Channel is full, log warning and do a blocking send
		jb.brt.logger.Warnf("Buffer channel full for %s:%s, job %d will be processed in next batch",
			sourceID, destID, job.JobID)
		ch <- job
	}
}

// Stop gracefully stops all job consumers and cleans up resources
func (jb *JobBuffer) Stop() {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	// Close all channels to signal consumers to stop
	for key, ch := range jb.sourceDestMap {
		close(ch)
		delete(jb.sourceDestMap, key)
	}

	// Shutdown the worker pool
	jb.workerPool.Shutdown()
}
