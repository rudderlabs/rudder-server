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

// JobBuffer provides buffering capabilities for jobs based on source-destination pairs
type JobBuffer struct {
	sourceDestMap map[string]chan *jobsdb.JobT
	uploadTimers  map[string]*time.Timer
	mu            sync.RWMutex
	brt           *Handle

	workerPool     workerpool.WorkerPool
	batchWorkers   map[string]*BatchWorker
	batchWorkersMu sync.RWMutex
}

// ConnectionJob represents a job with its connection details
type ConnectionJob struct {
	job      *jobsdb.JobT
	sourceID string
	destID   string
}

// NewJobBuffer creates a new JobBuffer instance
func NewJobBuffer(brt *Handle) *JobBuffer {
	jb := &JobBuffer{
		sourceDestMap: make(map[string]chan *jobsdb.JobT),
		uploadTimers:  make(map[string]*time.Timer),
		brt:           brt,
		batchWorkers:  make(map[string]*BatchWorker),
	}

	// Initialize the worker pool
	jb.workerPool = workerpool.New(context.Background(), func(partition string) workerpool.Worker {
		return &BatchWorker{
			partition: partition,
			jobs:      make([]*ConnectionJob, 0, brt.maxEventsInABatch),
			jobBuffer: jb,
		}
	}, brt.logger)

	return jb
}

// createStat creates a new stat with consistent source and destination tags
func (jb *JobBuffer) createStat(name, statType, sourceID, destID string) stats.Measurement {
	return stats.Default.NewTaggedStat(name, statType, stats.Tags{
		"destType": jb.brt.destType,
		"sourceId": sourceID,
		"destId":   destID,
	})
}

// Key format: "sourceID:destinationID"
func getSourceDestKey(sourceID, destID string) string {
	return fmt.Sprintf("%s:%s", sourceID, destID)
}

// getJobChannel returns or creates a channel for a source-destination pair
func (jb *JobBuffer) getJobChannel(sourceID, destID string) chan *jobsdb.JobT {
	jb.mu.RLock()
	ch, exists := jb.sourceDestMap[getSourceDestKey(sourceID, destID)]
	jb.mu.RUnlock()

	if !exists {
		jb.mu.Lock()
		// Double-check to avoid race conditions
		if ch, exists = jb.sourceDestMap[getSourceDestKey(sourceID, destID)]; !exists {
			// Match buffer size to batch size for efficiency
			bufferSize := jb.brt.maxEventsInABatch
			if customSize := jb.brt.conf.GetIntVar(0, 0, "BatchRouter."+jb.brt.destType+".channelBufferSize", "BatchRouter.channelBufferSize"); customSize > 0 {
				bufferSize = customSize
			}

			ch = make(chan *jobsdb.JobT, bufferSize)
			jb.sourceDestMap[getSourceDestKey(sourceID, destID)] = ch

			// Start consumer for this channel
			go jb.startJobConsumer(sourceID, destID, ch)
		}
		jb.mu.Unlock()
	}

	return ch
}

// startJobConsumer starts a consumer for a source-destination pair
func (jb *JobBuffer) startJobConsumer(sourceID, destID string, ch chan *jobsdb.JobT) {
	key := getSourceDestKey(sourceID, destID)
	var lastJobTime time.Time

	for job := range ch {
		jb.batchWorkersMu.Lock()
		worker := jb.batchWorkers[key]
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

		// Ping worker when either:
		// 1. We've reached max batch size
		// 2. Upload frequency time has elapsed since first job
		if currentBatchSize >= jb.brt.maxEventsInABatch || time.Since(lastJobTime) >= jb.brt.uploadFreq.Load() {
			jb.workerPool.PingWorker(key)
			lastJobTime = time.Time{} // Reset timer after ping
		}
	}

	// Process any remaining jobs when channel is closed
	jb.batchWorkersMu.Lock()
	if worker := jb.batchWorkers[key]; len(worker.jobs) > 0 {
		jb.workerPool.PingWorker(key)
	}
	jb.batchWorkersMu.Unlock()
}

// Stop gracefully stops all job consumers and cleans up resources
func (jb *JobBuffer) Stop() {
	jb.mu.Lock()
	defer jb.mu.Unlock()

	// Close all channels to signal consumers to stop
	for key, ch := range jb.sourceDestMap {
		close(ch)
		delete(jb.sourceDestMap, key)

		// Cancel timers
		if timer, ok := jb.uploadTimers[key]; ok {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			delete(jb.uploadTimers, key)
		}
	}

	// Shutdown the worker pool
	jb.workerPool.Shutdown()
}

// AddJob adds a job to the appropriate buffer channel
func (jb *JobBuffer) AddJob(sourceID, destID string, job *jobsdb.JobT) {
	// Get or create channel and send job (non-blocking check)
	ch := jb.getJobChannel(sourceID, destID)

	// Try to send the job to the channel, but don't block if it's full
	select {
	case ch <- job:
		// Job added successfully
	default:
		// Channel is full, log warning
		jb.brt.logger.Warnf("Buffer channel full for %s:%s, job %d will be processed in next batch",
			sourceID, destID, job.JobID)
		// Force add to the channel (blocking)
		ch <- job
	}
}
