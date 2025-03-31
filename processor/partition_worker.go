package processor

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type partitionWorker struct {
	partition string
	pipelines []*pipelineWorker
	logger    logger.Logger
	stats     *processorStats
	tracer    stats.Tracer
	handle    workerHandle
}

// newPartitionWorker creates a new worker for the specified partition
func newPartitionWorker(partition string, h workerHandle, t stats.Tracer) *partitionWorker {
	w := &partitionWorker{
		partition: partition,
		logger:    h.logger().Child(partition),
		stats:     h.stats(),
		tracer:    t,
		handle:    h,
	}
	// Create workers for each pipeline
	pipelinesPerPartition := h.config().pipelinesPerPartition
	w.pipelines = make([]*pipelineWorker, pipelinesPerPartition)
	for i := range pipelinesPerPartition {
		w.pipelines[i] = newPipelineWorker(partition, h, t)
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
	ctx, span := w.tracer.Start(context.Background(), "partitionWorker.Work", stats.SpanKindInternal,
		stats.SpanWithTags(stats.Tags{
			"partition": w.partition,
		}),
	)
	defer span.End()

	// Get jobs for this partition
	jobs := w.handle.getJobsStage(ctx, w.partition)

	// If no jobs were found, return false
	if len(jobs.Jobs) == 0 {
		return false
	}

	// Mark jobs as executing
	if err := w.handle.markExecuting(ctx, w.partition, jobs.Jobs); err != nil {
		w.logger.Error("Error marking jobs as executing", "error", err)
		panic(err)
	}

	// Distribute jobs across partitions based on UserID
	jobsByPipeline := lo.GroupBy(jobs.Jobs, func(job *jobsdb.JobT) int {
		return int(misc.GetMurmurHash(job.UserID) % uint64(w.handle.config().pipelinesPerPartition))
	})

	// Create an errGroup to handle cancellation and manage goroutines
	w.trace(ctx, "preProcess", func(ctx context.Context) {
		err := w.sendToPreProcess(ctx, jobsByPipeline)
		if err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Error("Error while processing jobs", "error", err)
			panic(err)
		}
	})

	// Handle rate limiting if needed
	if !jobs.LimitsReached {
		readLoopSleep := w.handle.config().readLoopSleep
		if elapsed := time.Since(start); elapsed < readLoopSleep.Load() {
			// Sleep for the remaining time, respecting context cancellation
			w.trace(ctx, "sleep", func(_ context.Context) {
				if err := misc.SleepCtx(context.Background(), readLoopSleep.Load()-elapsed); err != nil {
					panic(err)
				}
			})
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
	var wg sync.WaitGroup
	for _, pipeline := range w.pipelines {
		wg.Add(1)
		go func(p *pipelineWorker) {
			defer wg.Done()
			p.Stop()
		}(pipeline)
	}
	wg.Wait() // Wait for all stop operations to complete
}

func (w *partitionWorker) sendToPreProcess(ctx context.Context, jobsByPipeline map[int][]*jobsdb.JobT) error {
	g, gCtx := errgroup.WithContext(context.Background())

	// Send jobs to their respective partitions for processing
	for pipelineIdx, pipelineJobs := range jobsByPipeline {
		partition := pipelineIdx
		jobs := pipelineJobs
		// Initialize rsources stats
		rsourcesStats := rsources.NewStatsCollector(w.handle.rsourcesService(), rsources.IgnoreDestinationID())
		rsourcesStats.BeginProcessing(jobs)

		g.Go(func() error {
			subJobs := w.handle.jobSplitter(ctx, jobs, rsourcesStats)
			for _, subJob := range subJobs {
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case w.pipelines[partition].channel.preprocess <- subJob:
					// Job successfully sent to worker
				}
			}
			return nil
		})
	}

	// Wait for all goroutines to complete or for context to be cancelled
	return g.Wait()
}

func (w *partitionWorker) trace(ctx context.Context, name string, f func(ctx context.Context)) {
	_, span := w.tracer.Start(ctx, "partitionWorker.Work."+name, stats.SpanKindInternal,
		stats.SpanWithTags(stats.Tags{
			"partition": w.partition,
		}),
	)
	f(ctx)
	span.End()
}
