package batchrouter

import (
	"context"
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
	lastBatchTime    time.Time
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
	return &ConsumerWorker{
		sourceID:          sourceID,
		destID:            destID,
		ch:                ch,
		ctx:               ctx,
		logger:            logger.Child("consumer-worker").With("sourceID", sourceID, "destID", destID),
		addJobToPartition: callbacks.AddJobToPartition,
		pingBatchWorker:   callbacks.PingBatchWorker,
		getUploadFreq:     callbacks.GetUploadFreq,
		getMaxBatchSize:   callbacks.GetMaxBatchSize,
		lastBatchTime:     time.Now(),
	}
}

// ConsumerCallbacks contains callback functions needed by the consumer worker
type ConsumerCallbacks struct {
	AddJobToPartition func(partition string, job *ConnectionJob)
	PingBatchWorker   func(partition string)
	GetUploadFreq     func() time.Duration
	GetMaxBatchSize   func() int
}

// Work implements the workerpool.Worker interface.
// It processes jobs from the channel and manages batching based on size and time thresholds.
func (cw *ConsumerWorker) Work() bool {
	// Check if we need to trigger a batch based on time
	if cw.currentBatchSize > 0 && time.Since(cw.lastBatchTime) >= cw.getUploadFreq() {
		cw.triggerBatch()
		return true
	}

	// Try to read a job with a timeout to ensure we don't miss time-based batch triggers
	timeout := cw.getUploadFreq()
	if cw.currentBatchSize > 0 {
		// If we have jobs in the batch, use the remaining time until next batch
		remainingTime := cw.getUploadFreq() - time.Since(cw.lastBatchTime)
		if remainingTime > 0 {
			timeout = remainingTime
		} else {
			// If we're already past the upload frequency, trigger immediately
			cw.triggerBatch()
			return true
		}
	}

	// Set up a timer for the timeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case job, ok := <-cw.ch:
		if !ok {
			if cw.currentBatchSize > 0 {
				cw.triggerBatch()
			}
			return false
		}

		// Add job to the partition's queue
		key := getSourceDestKey(cw.sourceID, cw.destID)
		cw.addJobToPartition(key, &ConnectionJob{
			job:      job,
			sourceID: cw.sourceID,
			destID:   cw.destID,
		})
		cw.currentBatchSize++

		// If this is the first job in a new batch, update the batch start time
		if cw.currentBatchSize == 1 {
			cw.lastBatchTime = time.Now()
		}

		// Trigger batch processing if max batch size reached
		if cw.currentBatchSize >= cw.getMaxBatchSize() {
			cw.triggerBatch()
		}

	case <-timer.C:
		// Timer expired - trigger batch if we have any jobs
		if cw.currentBatchSize > 0 {
			cw.triggerBatch()
		}

	case <-cw.ctx.Done():
		// Context cancelled - trigger final batch if we have any jobs
		if cw.currentBatchSize > 0 {
			cw.triggerBatch()
		}
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
		cw.lastBatchTime = time.Now()
	}
}

// SleepDurations returns the min and max sleep durations for the worker when idle
func (cw *ConsumerWorker) SleepDurations() (min, max time.Duration) {
	return time.Millisecond * 100, time.Second * 5
}

// Stop implements the workerpool.Worker interface
func (cw *ConsumerWorker) Stop() {
	// Trigger final batch if we have any jobs
	if cw.currentBatchSize > 0 {
		cw.triggerBatch()
	}
}
