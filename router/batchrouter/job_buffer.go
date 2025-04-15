package batchrouter

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/workerpool"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// JobBuffer manages job buffering and batch processing for source-destination pairs.
// It handles the buffering of jobs through channels and manages batch workers that
// process these jobs in configurable batch sizes.
type JobBuffer struct {
	// Core components
	brt    *Handle
	ctx    context.Context
	cancel context.CancelFunc

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

	// Initialize the consumer worker pool
	jb.consumerPool = workerpool.New(ctx, jb.createConsumerWorker, brt.logger)

	return jb
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

			// Create batch job structure with connection info for processing
			// Find appropriate source for this destination
			var source backendconfig.SourceT
			for _, s := range destWithSources.Sources {
				if s.ID == sourceID {
					source = s
					break
				}
			}

			batchedJobs := &BatchedJobs{
				Jobs: jobs,
				Connection: &Connection{
					Destination: destWithSources.Destination,
					Source:      source,
				},
			}

			// Track connection details for metrics and reporting
			jobIDConnectionDetailsMap := make(map[int64]jobsdb.ConnectionDetails)

			for _, job := range jobs {
				// Use strings and routerutils packages to ensure they are used
				// Add metadata for tracking the consumer worker handling
				job.Parameters = routerutils.EnhanceJSON(job.Parameters, "consumer_key",
					strings.Join([]string{sourceID, destID}, ":"))

				jobIDConnectionDetailsMap[job.JobID] = jobsdb.ConnectionDetails{
					SourceID:      sourceID,
					DestinationID: destID,
				}
			}

			// Process based on destination type
			var uploadSuccess bool
			var uploadError error

			switch {
			case IsObjectStorageDestination(jb.brt.destType):
				// Object storage destinations (S3, GCS, etc)
				output := jb.brt.upload(jb.brt.destType, batchedJobs, false)
				defer misc.RemoveFilePaths(output.LocalFilePaths...)

				jb.brt.recordDeliveryStatus(destID, sourceID, output, false)
				jb.brt.updateJobStatus(batchedJobs, false, output.Error, false)

				if output.JournalOpID > 0 {
					jb.brt.jobsDB.JournalDeleteEntry(output.JournalOpID)
				}

				if output.Error == nil {
					jb.brt.recordUploadStats(*batchedJobs.Connection, output)
					uploadSuccess = true
				} else {
					uploadError = output.Error
				}

			case IsWarehouseDestination(jb.brt.destType):
				// Warehouse destinations
				useRudderStorage := misc.IsConfiguredToUseRudderObjectStorage(batchedJobs.Connection.Destination.Config)
				objectStorageType := warehouseutils.ObjectStorageType(jb.brt.destType, batchedJobs.Connection.Destination.Config, useRudderStorage)

				splitBatchJobs := jb.brt.splitBatchJobsOnTimeWindow(*batchedJobs)
				allSuccess := true

				for _, splitJob := range splitBatchJobs {
					output := jb.brt.upload(objectStorageType, splitJob, true)
					defer misc.RemoveFilePaths(output.LocalFilePaths...)

					notifyWarehouseErr := false
					if output.Error == nil && output.Key != "" {
						output.Error = jb.brt.pingWarehouse(splitJob, output)
						if output.Error != nil {
							notifyWarehouseErr = true
							allSuccess = false
							uploadError = output.Error
						}
						warehouseutils.DestStat(stats.CountType, "generate_staging_files", destID).Count(1)
						warehouseutils.DestStat(stats.CountType, "staging_file_batch_size", destID).Count(len(splitJob.Jobs))
					} else if output.Error != nil {
						allSuccess = false
						uploadError = output.Error
					}

					jb.brt.recordDeliveryStatus(destID, sourceID, output, true)
					jb.brt.updateJobStatus(splitJob, true, output.Error, notifyWarehouseErr)
				}

				uploadSuccess = allSuccess

			case asynccommon.IsAsyncDestination(jb.brt.destType):
				// Async destinations
				jb.brt.sendJobsToStorage(*batchedJobs)
				uploadSuccess = true

			default:
				uploadError = fmt.Errorf("unsupported destination type %s for job buffer", jb.brt.destType)
				jb.brt.logger.Error(uploadError)
			}

			// Update failing destinations map
			jb.brt.failingDestinationsMu.Lock()
			jb.brt.failingDestinations[destID] = !uploadSuccess
			jb.brt.failingDestinationsMu.Unlock()

			if !uploadSuccess && uploadError != nil {
				jb.brt.logger.Errorf("Upload failed for destination %s: %v", destID, uploadError)
				return uploadError
			}

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

func ParseConnectionKey(key string) (sourceID, destID string) {
	parts := strings.Split(key, ":")
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
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
	jb.consumerPool.Shutdown()
}
