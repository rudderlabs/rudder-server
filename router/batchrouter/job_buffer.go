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
	consumerPool    workerpool.WorkerPool
	workerLimiter   kitsync.Limiter
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

	// Initialize the consumer worker pool
	jb.consumerPool = workerpool.New(ctx, jb.createConsumerWorker, brt.logger)

	return jb
}

// createBatchWorker is a factory function for creating new batch workers
func (jb *JobBuffer) createBatchWorker(partition string) workerpool.Worker {
	return NewBatchWorker(partition, jb.brt.logger, jb.brt, jb.getJobsForPartition)
}

// createConsumerWorker is a factory function for creating new consumer workers
func (jb *JobBuffer) createConsumerWorker(key string) workerpool.Worker {
	sourceID, destID := ParseConnectionKey(key)
	if sourceID == "" || destID == "" {
		jb.brt.logger.Errorf("Invalid connection key format: %s", key)
		return nil
	}

	jobsChan := make(chan *jobsdb.JobT, 1000)
	jb.mu.Lock()
	jb.sourceDestMap[key] = jobsChan
	jb.mu.Unlock()

	callbacks := &ConsumerCallbacks{
		GetUploadFrequency: func() time.Duration {
			return jb.brt.uploadFreq.Load()
		},
		GetMaxBatchSize: func() int {
			return jb.brt.maxEventsInABatch
		},
		ProcessJobs: func(ctx context.Context, jobs []*jobsdb.JobT) error {
			// Process jobs for this destination using the batch router's destination processor
			destWithSources, ok := jb.brt.destinationsMap[destID]
			if !ok {
				return fmt.Errorf("destination not found: %s", destID)
			}
			destJobs := &DestinationJobs{
				jobs:            jobs,
				destWithSources: *destWithSources,
			}
			// Process jobs using the batch router's worker
			worker := newWorker(destID, jb.brt.logger, jb.brt)
			worker.routeJobsToBuffer(destJobs)
			return nil
		},
		ReleaseWorker: func(sourceID, destID string) {
			key := fmt.Sprintf("%s:%s", sourceID, destID)
			jb.mu.Lock()
			delete(jb.sourceDestMap, key)
			jb.mu.Unlock()
			jb.activeConsumers.Delete(key)
		},
		PartitionJobs: func(jobs []*jobsdb.JobT) [][]*jobsdb.JobT {
			// Group jobs into batches based on size and other criteria
			var batches [][]*jobsdb.JobT
			currentBatch := make([]*jobsdb.JobT, 0, jb.brt.maxEventsInABatch)

			for _, job := range jobs {
				currentBatch = append(currentBatch, job)
				if len(currentBatch) >= jb.brt.maxEventsInABatch {
					batches = append(batches, currentBatch)
					currentBatch = make([]*jobsdb.JobT, 0, jb.brt.maxEventsInABatch)
				}
			}
			if len(currentBatch) > 0 {
				batches = append(batches, currentBatch)
			}
			return batches
		},
	}

	worker := NewConsumerWorker(sourceID, destID, jobsChan, jb.ctx, jb.brt.logger, callbacks)
	jb.activeConsumers.Store(key, worker)
	return worker
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

// AddJob adds a job to the appropriate buffer channel and ensures a consumer exists
func (jb *JobBuffer) AddJob(sourceID, destID string, job *jobsdb.JobT) {
	key := getSourceDestKey(sourceID, destID)
	ch := jb.getOrCreateJobChannel(sourceID, destID)

	// Ensure the consumer is active
	jb.consumerPool.PingWorker(key)

	// Send the job to the channel
	ch <- job
}

// Shutdown gracefully shuts down the job buffer
func (jb *JobBuffer) Shutdown() {
	jb.cancel()
	jb.workerPool.Shutdown()
	jb.consumerPool.Shutdown()
}
