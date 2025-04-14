package batchrouter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
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
	ctx        context.Context
	cancel     context.CancelFunc

	// Channel management
	sourceDestMap map[string]chan *jobsdb.JobT
	mu            sync.RWMutex

	// Job queues for each partition
	jobQueues   map[string][]*ConnectionJob
	queuesMutex sync.RWMutex

	// Consumer management
	consumerPool  workerpool.WorkerPool
	workerLimiter kitsync.Limiter

	// Active consumer tracking
	activeConsumers sync.Map
}

// ConnectionJob represents a job with its connection details for batch processing
type ConnectionJob struct {
	job      *jobsdb.JobT // The actual job to be processed
	sourceID string       // Source identifier
	destID   string       // Destination identifier
}

// NewJobBuffer creates and initializes a new JobBuffer instance
func NewJobBuffer(brt *Handle) *JobBuffer {
	ctx, cancel := context.WithCancel(context.Background())
	maxConsumers := brt.conf.GetInt("BatchRouter.maxConsumers", 100)

	// Create a WaitGroup for the limiter
	var limiterGroup sync.WaitGroup

	jb := &JobBuffer{
		brt:           brt,
		sourceDestMap: make(map[string]chan *jobsdb.JobT),
		jobQueues:     make(map[string][]*ConnectionJob),
		ctx:           ctx,
		cancel:        cancel,
		workerLimiter: kitsync.NewLimiter(ctx, &limiterGroup, "batch_router_consumer_workers", maxConsumers, stats.Default),
	}

	// Initialize the batch worker pool
	jb.workerPool = workerpool.New(ctx, jb.createBatchWorker, brt.logger)

	// Initialize the consumer worker pool with a shorter idle timeout to keep workers active
	jb.consumerPool = workerpool.New(ctx, jb.createConsumerWorker, brt.logger,
		workerpool.WithIdleTimeout(brt.uploadFreq.Load()/2), // Set idle timeout to half the upload frequency
	)

	// Start a background goroutine to keep consumer workers active
	go jb.keepConsumersActive()

	return jb
}

// keepConsumersActive ensures consumer workers stay active by periodically pinging them
func (jb *JobBuffer) keepConsumersActive() {
	ticker := time.NewTicker(jb.brt.uploadFreq.Load() / 4) // Ping at 1/4th of upload frequency
	defer ticker.Stop()

	for {
		select {
		case <-jb.ctx.Done():
			return
		case <-ticker.C:
			// Get all active consumer keys
			jb.mu.RLock()
			keys := make([]string, 0, len(jb.sourceDestMap))
			for key := range jb.sourceDestMap {
				keys = append(keys, key)
			}
			jb.mu.RUnlock()

			// Ping each consumer worker
			for _, key := range keys {
				jb.consumerPool.PingWorker(key)
			}
		}
	}
}

// createBatchWorker is a factory function for creating new batch workers
func (jb *JobBuffer) createBatchWorker(partition string) workerpool.Worker {
	return NewBatchWorker(partition, jb.brt.logger, jb.brt, jb.getJobsForPartition)
}

// createConsumerWorker is a factory function for creating new consumer workers
func (jb *JobBuffer) createConsumerWorker(key string) workerpool.Worker {
	// Try to acquire a slot from the limiter
	release := jb.workerLimiter.Begin(key)

	// Store the release function
	jb.activeConsumers.Store(key, release)

	sourceID, destID := parseConnectionKey(key)
	ch := jb.getOrCreateJobChannel(sourceID, destID)

	callbacks := ConsumerCallbacks{
		AddJobToPartition: jb.addJobToPartition,
		PingBatchWorker:   jb.workerPool.PingWorker,
		GetUploadFreq: func() time.Duration {
			return jb.brt.uploadFreq.Load()
		},
		GetMaxBatchSize: func() int {
			return jb.brt.maxEventsInABatch
		},
	}

	return NewConsumerWorker(sourceID, destID, ch, jb.ctx, jb.brt.logger, callbacks)
}

// getSourceDestKey generates a unique key for a source-destination pair
func getSourceDestKey(sourceID, destID string) string {
	return fmt.Sprintf("%s:%s", sourceID, destID)
}

// getOrCreateJobChannel returns or creates a buffered channel for a source-destination pair
func (jb *JobBuffer) getOrCreateJobChannel(sourceID, destID string) chan *jobsdb.JobT {
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

// AddJob adds a job to the appropriate buffer channel and ensures a consumer exists
func (jb *JobBuffer) AddJob(sourceID, destID string, job *jobsdb.JobT) {
	key := getSourceDestKey(sourceID, destID)
	ch := jb.getOrCreateJobChannel(sourceID, destID)

	// Ensure the consumer is active
	jb.consumerPool.PingWorker(key)

	// Send the job to the channel
	ch <- job
}

// Stop gracefully stops all job consumers and cleans up resources
func (jb *JobBuffer) Stop() {
	// Signal all workers to stop
	jb.cancel()

	jb.mu.Lock()
	defer jb.mu.Unlock()

	// Close all channels to signal consumers to stop
	for key, ch := range jb.sourceDestMap {
		close(ch)
		delete(jb.sourceDestMap, key)
	}

	// Release all limiter slots
	jb.activeConsumers.Range(func(key, value interface{}) bool {
		if release, ok := value.(func()); ok {
			release()
		}
		return true
	})

	// Shutdown the worker pools
	jb.workerPool.Shutdown()
	jb.consumerPool.Shutdown()
}
