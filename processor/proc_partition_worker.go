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
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/tracing"
)

// procWorkerHandle abstracts processor's [Handle] from the proc pool workers, mirroring
// [workerHandle] for the gw pool so the proc workers can be unit-tested with a mock.
// It is satisfied by [workerHandleAdapter] (which embeds *Handle).
type procWorkerHandle interface {
	logger() logger.Logger
	config() workerHandleConfig
	rsourcesService() rsources.JobService
	stats() *processorStats

	getProcJobs(ctx context.Context, destinationID string) jobsdb.JobsResult
	procMarkExecuting(ctx context.Context, destinationID string, jobs []*jobsdb.JobT) error
	jobSplitter(ctx context.Context, jobs []*jobsdb.JobT, rsourcesStats rsources.StatsCollector) []subJob
	procRebuildStage(destinationID string, in subJob) (*transformationMessage, error)
	userTransformStage(partition string, in *transformationMessage) *userTransformData
	destinationTransformStage(partition string, in *userTransformData) *storeMessage
	// procStoreStage stores through the shared storeStage, directing the job-status
	// update to procDB instead of gatewayDB.
	procStoreStage(partition string, pipelineIndex int, in *storeMessage)
}

// procPartitionWorker drives the proc pool for a single destination (partition = destination ID).
// It is the post-fan-out counterpart of [gwPartitionWorker]: it polls the intermediate
// (proc) jobsdb filtered to its destination, distributes jobs across pipelines by userID
// (to preserve per-user ordering, §7) and feeds each [procPipelineWorker].
type procPartitionWorker struct {
	partition    string
	pipelines    []*procPipelineWorker
	logger       logger.Logger
	stats        *processorStats
	tracer       *tracing.Tracer
	handle       procWorkerHandle
	statsFactory stats.Stats
}

func newProcPartitionWorker(partition string, h procWorkerHandle, t stats.Tracer, statsFactory stats.Stats) *procPartitionWorker {
	w := &procPartitionWorker{
		partition:    partition,
		logger:       h.logger().Child("proc-consumer").Child(partition),
		stats:        h.stats(),
		tracer:       tracing.New(t, tracing.WithNamePrefix("procPartitionWorker")),
		handle:       h,
		statsFactory: statsFactory,
	}
	pipelinesPerPartition := h.config().pipelinesPerPartition
	w.pipelines = make([]*procPipelineWorker, pipelinesPerPartition)
	for i := range pipelinesPerPartition {
		w.pipelines[i] = newProcPipelineWorker(i, partition, h, tracing.New(t, tracing.WithNamePrefix("procPipelineWorker")))
	}
	return w
}

// Work polls the proc jobsdb for this partition and pipelines any jobs found.
// Returns true if work was done, false otherwise.
func (w *procPartitionWorker) Work() bool {
	start := time.Now()
	spanTags := stats.Tags{"partition": w.partition}
	ctx, span := w.tracer.Trace(context.Background(), "Work", tracing.WithTraceTags(spanTags))
	defer span.End()

	jobs := w.handle.getProcJobs(ctx, w.partition)
	if len(jobs.Jobs) == 0 {
		span.SetStatus(stats.SpanStatusOk, "No jobs found")
		return false
	}

	if err := w.handle.procMarkExecuting(ctx, w.partition, jobs.Jobs); err != nil {
		w.logger.Errorn("Error marking proc jobs as executing", obskit.Error(err))
		panic(err)
	}

	// Distribute jobs across pipelines based on UserID so a single user's events for a
	// destination are always drained by the same pipeline (per-user ordering, §7).
	jobsByPipeline := lo.GroupBy(jobs.Jobs, func(job *jobsdb.JobT) int {
		return int(misc.GetMurmurHash(job.UserID) % uint64(w.handle.config().pipelinesPerPartition))
	})

	if err := w.sendToRebuild(ctx, jobsByPipeline); err != nil && !errors.Is(err, context.Canceled) {
		w.logger.Errorn("Error while processing proc jobs", obskit.Error(err))
		panic(err)
	}

	if !jobs.LimitsReached {
		readLoopSleep := w.handle.config().readLoopSleep
		if elapsed := time.Since(start); elapsed < readLoopSleep.Load() {
			_ = w.tracer.TraceFunc(ctx, "Work.sleep", func(ctx context.Context) {
				if err := misc.SleepCtx(context.Background(), readLoopSleep.Load()-elapsed); err != nil {
					panic(err)
				}
			}, tracing.WithTraceTags(spanTags))
		}
	}

	return true
}

// SleepDurations returns the min and max sleep durations for the worker
func (w *procPartitionWorker) SleepDurations() (minSleep, maxSleep time.Duration) {
	return w.handle.config().readLoopSleep.Load(), w.handle.config().maxLoopSleep.Load()
}

// Stop stops the worker and waits until all its goroutines have stopped
func (w *procPartitionWorker) Stop() {
	var wg sync.WaitGroup
	for _, pipeline := range w.pipelines {
		wg.Add(1)
		go func(p *procPipelineWorker) {
			defer wg.Done()
			p.Stop()
		}(pipeline)
	}
	wg.Wait()
}

func (w *procPartitionWorker) sendToRebuild(ctx context.Context, jobsByPipeline map[int][]*jobsdb.JobT) error {
	spanTags := stats.Tags{"partition": w.partition}
	_, span := w.tracer.Trace(ctx, "Work.sendToRebuild", tracing.WithTraceTags(spanTags))
	defer span.End()

	g, gCtx := errgroup.WithContext(context.Background())

	for pipelineIdx, pipelineJobs := range jobsByPipeline {
		pipeline := pipelineIdx
		jobs := pipelineJobs
		rsourcesStats := rsources.NewStatsCollector(w.handle.rsourcesService(), "processor", w.statsFactory, rsources.IgnoreDestinationID())
		rsourcesStats.BeginProcessing(jobs)

		g.Go(func() error {
			subJobs := w.handle.jobSplitter(ctx, jobs, rsourcesStats)
			for _, subJob := range subJobs {
				waitStart := time.Now()
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case w.pipelines[pipeline].channel.rebuild <- subJob:
					w.tracer.RecordSpan(ctx, "Work.rebuildCh.wait", waitStart, tracing.WithRecordSpanTags(spanTags))
				}
			}
			return nil
		})
	}

	return g.Wait()
}
