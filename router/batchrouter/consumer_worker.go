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
	var jobCount int

	// Create a timer channel for upload frequency checks
	uploadFreqTimer := time.NewTimer(cw.getUploadFreq())
	defer uploadFreqTimer.Stop()

	select {
	case job, ok := <-cw.ch:
		if !ok {
			return false
		}

		// Add job to the partition's queue
		key := getSourceDestKey(cw.sourceID, cw.destID)
		cw.addJobToPartition(key, &ConnectionJob{
			job:      job,
			sourceID: cw.sourceID,
			destID:   cw.destID,
		})
		jobCount++

		// Reset timer when we get our first job
		if jobCount == 1 {
			if !uploadFreqTimer.Stop() {
				<-uploadFreqTimer.C
			}
			uploadFreqTimer.Reset(cw.getUploadFreq())
		}

		// Trigger batch processing if max batch size reached
		if jobCount >= cw.getMaxBatchSize() {
			key := getSourceDestKey(cw.sourceID, cw.destID)
			cw.pingBatchWorker(key)
			if !uploadFreqTimer.Stop() {
				<-uploadFreqTimer.C
			}
			uploadFreqTimer.Reset(cw.getUploadFreq())
			return true
		}

	case <-uploadFreqTimer.C:
		cw.logger.Infof("Upload frequency threshold reached with %d jobs", jobCount)
		if jobCount > 0 {
			key := getSourceDestKey(cw.sourceID, cw.destID)
			cw.pingBatchWorker(key)
			uploadFreqTimer.Reset(cw.getUploadFreq())
			return true
		}
		uploadFreqTimer.Reset(cw.getUploadFreq())

	case <-cw.ctx.Done():
		return false
	}

	return true
}

// SleepDurations returns the min and max sleep durations for the worker when idle
func (cw *ConsumerWorker) SleepDurations() (min, max time.Duration) {
	return time.Millisecond * 100, time.Second * 5
}

// Stop implements the workerpool.Worker interface
func (cw *ConsumerWorker) Stop() {
	// No cleanup needed
}
