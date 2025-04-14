package batchrouter

import (
	"context"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

// ConsumerWorker handles job consumption for a specific source-destination pair.
// It implements the workerpool.Worker interface and manages the buffering and batching
// of jobs before they are processed by the batch worker.
type ConsumerWorker struct {
	sourceID string
	destID   string
	ch       chan *jobsdb.JobT
	ctx      context.Context
	logger   logger.Logger

	// Callbacks for job processing
	addJobToPartition func(partition string, job *ConnectionJob)
	pingBatchWorker   func(partition string)
	getUploadFreq     func() time.Duration
	getMaxBatchSize   func() int

	// Batch state
	currentBatchSize int
	batchMutex       sync.Mutex
	isStarted        bool
	startOnce        sync.Once
}

// NewConsumerWorker creates a new consumer worker for a source-destination pair
func NewConsumerWorker(
	sourceID string,
	destID string,
	ch chan *jobsdb.JobT,
	ctx context.Context,
	logger logger.Logger,
	callbacks ConsumerCallbacks,
) *ConsumerWorker {
	cw := &ConsumerWorker{
		sourceID:          sourceID,
		destID:            destID,
		ch:                ch,
		ctx:               ctx,
		logger:            logger.Child("consumer-worker").With("sourceID", sourceID, "destID", destID),
		addJobToPartition: callbacks.AddJobToPartition,
		pingBatchWorker:   callbacks.PingBatchWorker,
		getUploadFreq:     callbacks.GetUploadFreq,
		getMaxBatchSize:   callbacks.GetMaxBatchSize,
	}

	// Start the batch timer immediately
	cw.startBatchTimer()
	return cw
}

// ConsumerCallbacks contains callback functions needed by the consumer worker
type ConsumerCallbacks struct {
	AddJobToPartition func(partition string, job *ConnectionJob)
	PingBatchWorker   func(partition string)
	GetUploadFreq     func() time.Duration
	GetMaxBatchSize   func() int
}

// startBatchTimer starts the background timer for batch processing
func (cw *ConsumerWorker) startBatchTimer() {
	cw.startOnce.Do(func() {
		cw.isStarted = true
		go func() {
			ticker := time.NewTicker(cw.getUploadFreq())
			defer ticker.Stop()

			for {
				select {
				case <-cw.ctx.Done():
					// Final check for any remaining jobs before exiting
					cw.checkAndTriggerBatch()
					return
				case <-ticker.C:
					cw.checkAndTriggerBatch()
				}
			}
		}()
	})
}

// checkAndTriggerBatch checks if we should trigger a batch based on size or time
func (cw *ConsumerWorker) checkAndTriggerBatch() {
	cw.batchMutex.Lock()
	defer cw.batchMutex.Unlock()

	// Check if we have enough jobs to trigger a batch
	if cw.currentBatchSize >= cw.getMaxBatchSize() || cw.currentBatchSize > 0 {
		cw.triggerBatch()
	}
}

// Work implements the workerpool.Worker interface.
// It only handles job ingestion and storage, while batch processing is handled by the timer.
func (cw *ConsumerWorker) Work() bool {
	select {
	case job, ok := <-cw.ch:
		if !ok {
			return false
		}

		cw.batchMutex.Lock()
		// Add job to the partition's queue
		key := getSourceDestKey(cw.sourceID, cw.destID)
		cw.addJobToPartition(key, &ConnectionJob{
			job:      job,
			sourceID: cw.sourceID,
			destID:   cw.destID,
		})
		cw.currentBatchSize++

		// Let the timer goroutine handle batch triggering
		cw.batchMutex.Unlock()

	case <-cw.ctx.Done():
		return false
	}

	return true
}

// triggerBatch triggers batch processing and resets batch state
func (cw *ConsumerWorker) triggerBatch() {
	if cw.currentBatchSize > 0 {
		key := getSourceDestKey(cw.sourceID, cw.destID)
		cw.logger.Infof("Triggering batch processing for %d jobs", cw.currentBatchSize)
		cw.pingBatchWorker(key)
		cw.currentBatchSize = 0
	}
}

// SleepDurations returns the min and max sleep durations for the worker when idle
func (cw *ConsumerWorker) SleepDurations() (min, max time.Duration) {
	return time.Millisecond * 100, time.Second * 5
}

// Stop implements the workerpool.Worker interface
func (cw *ConsumerWorker) Stop() {
	// The context cancellation will trigger final batch processing in the timer goroutine
}
