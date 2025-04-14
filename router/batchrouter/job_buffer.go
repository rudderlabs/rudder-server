package batchrouter

import (
	"context"
	"fmt"
	"sync"
	"time"

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

	// Job queues for each partition
	jobQueues   map[string][]*ConnectionJob
	queuesMutex sync.RWMutex
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
		jobQueues:     make(map[string][]*ConnectionJob),
	}

	// Initialize the worker pool with a worker factory
	jb.workerPool = workerpool.New(context.Background(), jb.createBatchWorker, brt.logger)

	return jb
}

// createBatchWorker is a factory function for creating new batch workers
func (jb *JobBuffer) createBatchWorker(partition string) workerpool.Worker {
	return NewBatchWorker(partition, jb.brt.logger, jb.brt, jb.getJobsForPartition)
}

// getSourceDestKey generates a unique key for a source-destination pair
func getSourceDestKey(sourceID, destID string) string {
	return fmt.Sprintf("%s:%s", sourceID, destID)
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

	// Start consumer for this channel
	go jb.startJobConsumer(sourceID, destID, ch)

	return ch
}

// getJobsForPartition atomically gets and clears all jobs for a partition
func (jb *JobBuffer) getJobsForPartition(partition string) []*ConnectionJob {
	jb.queuesMutex.Lock()
	defer jb.queuesMutex.Unlock()

	jobs := jb.jobQueues[partition]
	jb.jobQueues[partition] = nil // Clear the queue
	return jobs
}

// addJobToPartition adds a job to the specified partition's queue
func (jb *JobBuffer) addJobToPartition(partition string, job *ConnectionJob) {
	jb.queuesMutex.Lock()
	jb.jobQueues[partition] = append(jb.jobQueues[partition], job)
	jb.queuesMutex.Unlock()
}

// startJobConsumer processes jobs from the channel and manages batch processing
func (jb *JobBuffer) startJobConsumer(sourceID, destID string, ch chan *jobsdb.JobT) {
	key := getSourceDestKey(sourceID, destID)
	var lastJobTime time.Time
	var jobCount int

	// Create a timer channel for upload frequency checks
	uploadFreqTimer := time.NewTimer(jb.brt.uploadFreq.Load())
	defer uploadFreqTimer.Stop()

	for {
		select {
		case job, ok := <-ch:
			if !ok {
				// Channel closed, exit
				return
			}

			// Add job to the partition's queue
			jb.addJobToPartition(key, &ConnectionJob{
				job:      job,
				sourceID: sourceID,
				destID:   destID,
			})
			jobCount++

			if lastJobTime.IsZero() {
				lastJobTime = time.Now()
				// Reset timer when we get our first job
				if !uploadFreqTimer.Stop() {
					<-uploadFreqTimer.C
				}
				uploadFreqTimer.Reset(jb.brt.uploadFreq.Load())
			}

			// Trigger batch processing if max batch size reached
			if jobCount >= jb.brt.maxEventsInABatch {
				jb.workerPool.PingWorker(key)
				lastJobTime = time.Time{} // Reset timer
				jobCount = 0
				if !uploadFreqTimer.Stop() {
					<-uploadFreqTimer.C
				}
				uploadFreqTimer.Reset(jb.brt.uploadFreq.Load())
			}

		case <-uploadFreqTimer.C:
			if jobCount > 0 {
				jb.brt.logger.Infof("Upload frequency threshold reached for connection %s:%s with %d jobs", sourceID, destID, jobCount)
				jb.workerPool.PingWorker(key)
				lastJobTime = time.Time{} // Reset timer
				jobCount = 0
			}
			uploadFreqTimer.Reset(jb.brt.uploadFreq.Load())
		}
	}
}

// AddJob adds a job to the appropriate buffer channel
func (jb *JobBuffer) AddJob(sourceID, destID string, job *jobsdb.JobT) {
	jb.getJobChannel(sourceID, destID) <- job
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
