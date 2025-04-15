package batchrouter

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

// ConsumerWorker handles job consumption for a specific source-destination pair.
// It acts as a buffer between job ingestion and batch processing, storing jobs
// in partition queues for the batch worker to process.
type ConsumerWorker struct {
	sourceID  string
	destID    string
	jobChan   chan *jobsdb.JobT
	ctx       context.Context
	logger    logger.Logger
	callbacks *ConsumerCallbacks
}

// ConsumerCallbacks contains callback functions needed by the consumer worker
type ConsumerCallbacks struct {
	GetUploadFrequency func() time.Duration
	GetMaxBatchSize    func() int
	ProcessJobs        func(ctx context.Context, jobs []*jobsdb.JobT) error
	ReleaseWorker      func(sourceID, destID string)
	PartitionJobs      func(jobs []*jobsdb.JobT) [][]*jobsdb.JobT
}

// NewConsumerWorker creates a new consumer worker for a source-destination pair
func NewConsumerWorker(
	sourceID string,
	destID string,
	jobChan chan *jobsdb.JobT,
	ctx context.Context,
	logger logger.Logger,
	callbacks *ConsumerCallbacks,
) *ConsumerWorker {
	return &ConsumerWorker{
		sourceID:  sourceID,
		destID:    destID,
		jobChan:   jobChan,
		ctx:       ctx,
		logger:    logger.Child("consumer-worker").With("sourceID", sourceID, "destID", destID),
		callbacks: callbacks,
	}
}

// Work implements the workerpool.Worker interface.
// It consumes jobs from the channel and processes them in batches.
// When either the batch size threshold or the upload frequency timer is reached,
// it sends the collected jobs for processing using the callbacks.ProcessJobs method.
// This directly processes the jobs without sending them back to the buffer.
// Returns true if any jobs were processed.
func (c *ConsumerWorker) Work() bool {
	defer c.callbacks.ReleaseWorker(c.sourceID, c.destID)

	uploadFrequency := c.callbacks.GetUploadFrequency()
	maxBatchSize := c.callbacks.GetMaxBatchSize()

	var jobs []*jobsdb.JobT
	timer := time.NewTimer(uploadFrequency)
	defer timer.Stop()

	jobsProcessed := false

	for {
		select {
		case <-c.ctx.Done():
			if len(jobs) > 0 {
				partitionedJobs := c.callbacks.PartitionJobs(jobs)
				for _, jobBatch := range partitionedJobs {
					if err := c.callbacks.ProcessJobs(c.ctx, jobBatch); err != nil {
						c.logger.Errorf("Error processing jobs: %v", err)
					}
				}
				jobsProcessed = true
			}
			return jobsProcessed

		case job := <-c.jobChan:
			jobs = append(jobs, job)
			if len(jobs) >= maxBatchSize {
				partitionedJobs := c.callbacks.PartitionJobs(jobs)
				for _, jobBatch := range partitionedJobs {
					if err := c.callbacks.ProcessJobs(c.ctx, jobBatch); err != nil {
						c.logger.Errorf("Error processing jobs: %v", err)
					}
				}
				jobs = nil
				timer.Reset(uploadFrequency)
				jobsProcessed = true
			}

		case <-timer.C:
			if len(jobs) > 0 {
				partitionedJobs := c.callbacks.PartitionJobs(jobs)
				for _, jobBatch := range partitionedJobs {
					if err := c.callbacks.ProcessJobs(c.ctx, jobBatch); err != nil {
						c.logger.Errorf("Error processing jobs: %v", err)
					}
				}
				jobsProcessed = true
			}
			timer.Reset(uploadFrequency)
			return jobsProcessed
		}
	}
}

// SleepDurations returns the min and max sleep durations for the worker when idle
func (c *ConsumerWorker) SleepDurations() (min, max time.Duration) {
	return time.Millisecond * 100, time.Second * 5
}

// Stop implements the workerpool.Worker interface
func (c *ConsumerWorker) Stop() {
	// No cleanup needed
}

func (c *ConsumerWorker) IsIdle() bool {
	return len(c.jobChan) == 0
}
