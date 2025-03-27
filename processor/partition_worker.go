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
	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/traces"
)

type partitionWorker struct {
	partition    string
	pipelines    []*pipelineWorker
	logger       logger.Logger
	tracer       stats.Tracer
	spanRecorder traces.SpanRecorder
	stats        *processorStats
	handle       workerHandle
}

// newPartitionWorker creates a new worker for the specified partition
func newPartitionWorker(partition string, h workerHandle, t stats.Tracer, spanRecorder traces.SpanRecorder) *partitionWorker {
	w := &partitionWorker{
		partition:    partition,
		logger:       h.logger().Child(partition),
		spanRecorder: spanRecorder,
		stats:        h.stats(),
		handle:       h,
		tracer:       t,
	}
	// Create workers for each pipeline
	pipelinesPerPartition := h.config().pipelinesPerPartition
	w.pipelines = make([]*pipelineWorker, pipelinesPerPartition)
	for i := range pipelinesPerPartition {
		w.pipelines[i] = newPipelineWorker(partition, h, spanRecorder)
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
		stats.SpanWithTimestamp(start),
		stats.SpanWithTags(stats.Tags{
			"partition": w.partition,
		}),
	)
	defer span.End()

	// Get jobs for this partition
	jobs := w.handle.getJobs(ctx, w.partition)

	// If no jobs were found, return false
	if len(jobs.Jobs) == 0 {
		return false
	}

	// Recording spans for getJobs.
	// WARNING: more than one message might be coming from the same HTTP request, let's make sure we don't start too
	// many spans. One span per traceID should suffice.
	seen := make(map[string]struct{})
	contexts := make([]context.Context, 0)
	for _, job := range jobs.Jobs {
		if err := jsonrs.Unmarshal(job.Parameters, &job.EventParameters); err != nil {
			w.logger.Errorn("Failed to unmarshal parameters object", logger.NewIntField("jobId", job.JobID))
			panic(err)
		}
		if job.EventParameters.TraceParent == "" {
			continue
		}
		if _, ok := seen[job.EventParameters.TraceParent]; ok {
			continue
		}
		seen[job.EventParameters.TraceParent] = struct{}{}
		ctx := stats.InjectTraceParentIntoContext(context.Background(), job.EventParameters.TraceParent)
		contexts = append(contexts, ctx)
	}
	go w.spanRecorder.RecordSpans(contexts, "partitionWorker.getJobs", stats.SpanKindInternal, start,
		traces.WithTags(stats.Tags{
			"partition": w.partition,
		}),
	)

	// Mark jobs as executing
	start = time.Now()
	err := w.handle.markExecuting(ctx, w.partition, jobs.Jobs)
	if err != nil {
		w.logger.Error("Error marking jobs as executing", "error", err)
		panic(err)
	}
	go w.spanRecorder.RecordSpans(contexts, "partitionWorker.markExecuting", stats.SpanKindInternal, start,
		traces.WithTags(stats.Tags{
			"partition": w.partition,
		}),
	)

	// Distribute jobs across partitions based on UserID
	jobsByPipeline := lo.GroupBy(jobs.Jobs, func(job *jobsdb.JobT) int {
		return int(misc.GetMurmurHash(job.UserID) % uint64(w.handle.config().pipelinesPerPartition))
	})

	// Create an errGroup to handle cancellation and manage goroutines
	g, gCtx := errgroup.WithContext(ctx)

	// Send jobs to their respective partitions for processing
	for pipelineIdx, pipelineJobs := range jobsByPipeline {
		partition := pipelineIdx
		jobs := pipelineJobs
		// Initialize rsources stats
		rsourcesStats := rsources.NewStatsCollector(w.handle.rsourcesService(), rsources.IgnoreDestinationID())
		rsourcesStats.BeginProcessing(jobs)

		g.Go(func() error {
			subJobs := w.handle.jobSplitter(gCtx, jobs, rsourcesStats)
			for _, subJob := range subJobs {
				start := time.Now()
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case w.pipelines[partition].channel.preprocess <- subJob:
					// Job successfully sent to worker
					w.spanRecorder.RecordJobsSpans(context.Background(), subJob.subJobs, "partitionWorker.preprocess",
						stats.SpanKindInternal, start,
						traces.WithTags(stats.Tags{
							"partition": w.partition,
						}))
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
			if err := misc.SleepCtx(context.Background(), readLoopSleep.Load()-elapsed); err != nil {
				panic(err)
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
