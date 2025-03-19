package processor

import (
	"context"
	"errors"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type partitionWorker struct {
	partition string
	workers   []*pipelineWorker
	logger    logger.Logger
	stats     *processorStats
	handle    workerHandle
	g         *errgroup.Group
	ctx       context.Context
}

// newProcessorWorker creates a new worker for the specified partition
func newPartitionWorker(ctx context.Context, partition string, h workerHandle) *partitionWorker {
	w := &partitionWorker{
		partition: partition,
		logger:    h.logger().Child(partition),
		stats:     h.stats(),
		handle:    h,
	}
	w.g, w.ctx = errgroup.WithContext(ctx)

	// Create workers for each partition
	pipelinesPerPartition := h.config().pipelinesPerPartition
	w.workers = make([]*pipelineWorker, pipelinesPerPartition)
	for i := 0; i < pipelinesPerPartition; i++ {
		w.workers[i] = newPipelineWorker(partition, h)
	}

	return w
}

// Work processes jobs for the specified partition
// Returns true if work was done, false otherwise
func (w *partitionWorker) Work() bool {
	// If pipelining is disabled, use the legacy job handling path
	if !w.handle.config().enablePipelining {
		return w.handle.handlePendingGatewayJobs(w.partition)
	}

	start := time.Now()
	// Get jobs for this partition
	jobs := w.handle.getJobs(w.partition)

	// If no jobs were found, return false
	if len(jobs.Jobs) == 0 {
		return false
	}

	// Mark jobs as executing
	if err := w.handle.markExecuting(w.partition, jobs.Jobs); err != nil {
		w.logger.Error("Error marking jobs as executing", "error", err)
		panic(err)
	}

	// Record throughput metrics
	w.handle.stats().DBReadThroughput(w.partition).Count(throughputPerSecond(jobs.EventsCount, time.Since(start)))

	// Initialize rsources stats
	rsourcesStats := rsources.NewStatsCollector(w.handle.rsourcesService(), rsources.IgnoreDestinationID())
	rsourcesStats.BeginProcessing(jobs.Jobs)

	// Distribute jobs across partitions based on UserID
	jobsByPipeline := lo.GroupBy(jobs.Jobs, func(job *jobsdb.JobT) int {
		return int(misc.GetMurmurHash(job.UserID) % uint64(w.handle.config().pipelinesPerPartition))
	})

	// Create an errGroup to handle cancellation and manage goroutines
	g, gCtx := errgroup.WithContext(w.ctx)

	// Send jobs to their respective partitions for processing
	for pipelineIdx, pipelineJobs := range jobsByPipeline {
		partition := pipelineIdx
		jobs := pipelineJobs

		g.Go(func() error {
			subJobs := w.handle.jobSplitter(jobs, rsourcesStats)
			for _, subJob := range subJobs {
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case w.workers[partition].channel.preprocess <- subJob:
					// Job successfully sent to worker
				}
			}
			return nil
		})
	}

	// Wait for all goroutines to complete or for context to be cancelled
	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		w.logger.Error("Error while processing jobs", "error", err)
		panic(err)
	}

	// Handle rate limiting if needed
	if !jobs.LimitsReached {
		readLoopSleep := w.handle.config().readLoopSleep
		if elapsed := time.Since(start); elapsed < readLoopSleep.Load() {
			// Sleep for the remaining time, respecting context cancellation
			if err := misc.SleepCtx(w.ctx, readLoopSleep.Load()-elapsed); err != nil {
				return true
			}
		}
	}

	return true
}

// SleepDurations returns the min and max sleep durations for the worker
func (w *partitionWorker) SleepDurations() (min, max time.Duration) {
	return w.handle.config().readLoopSleep.Load(), w.handle.config().maxLoopSleep.Load()
}

// Stop stops the worker and waits until all its goroutines have stopped
func (w *partitionWorker) Stop() {
	for _, worker := range w.workers {
		worker.Stop()
	}
}
