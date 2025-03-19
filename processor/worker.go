package processor

import (
	"context"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type worker struct {
	partition string
	workers   []*partitionWorker
	logger    logger.Logger
	stats     *processorStats
	handle    workerHandle
	g         *errgroup.Group
}

// newProcessorWorker creates a new worker for the specified partition
func newProcessorWorker(partition string, h workerHandle) *worker {
	w := &worker{
		partition: partition,
		logger:    h.logger().Child(partition),
		stats:     h.stats(),
		handle:    h,
	}
	w.g, _ = errgroup.WithContext(context.Background())

	// Create workers for each partition
	pipelinesPerPartition := h.config().pipelinesPerPartition
	w.workers = make([]*partitionWorker, pipelinesPerPartition)
	for i := 0; i < pipelinesPerPartition; i++ {
		w.workers[i] = newWorker(partition, h)
	}

	return w
}

// Work processes jobs for the specified partition
// Returns true if work was done, false otherwise
func (w *worker) Work() bool {
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
	jobsByPartition := lo.GroupBy(jobs.Jobs, func(job *jobsdb.JobT) int {
		return int(misc.GetMurmurHash(job.UserID) % uint64(w.handle.config().pipelinesPerPartition))
	})

	// Send jobs to their respective partitions for processing
	var wg sync.WaitGroup
	for partition, partitionJobs := range jobsByPartition {
		wg.Add(1)
		go func(partition int, jobs []*jobsdb.JobT) {
			defer wg.Done()
			subJobs := w.handle.jobSplitter(jobs, rsourcesStats)
			for _, subJob := range subJobs {
				w.workers[partition].channel.preprocess <- subJob
			}
		}(partition, partitionJobs)
	}
	wg.Wait()

	// Handle rate limiting if needed
	if !jobs.LimitsReached {
		readLoopSleep := w.handle.config().readLoopSleep
		if elapsed := time.Since(start); elapsed < readLoopSleep.Load() {
			// Sleep for the remaining time
			if err := misc.SleepCtx(context.Background(), readLoopSleep.Load()-elapsed); err != nil {
				return true
			}
		}
	}

	return true
}

// SleepDurations returns the min and max sleep durations for the worker
func (w *worker) SleepDurations() (min, max time.Duration) {
	return w.handle.config().readLoopSleep.Load(), w.handle.config().maxLoopSleep.Load()
}

// Stop stops the worker and waits until all its goroutines have stopped
func (w *worker) Stop() {
	for _, worker := range w.workers {
		worker.Stop()
	}
}

// newWorker creates a new partition worker
func newWorker(partition string, h workerHandle) *partitionWorker {
	w := &partitionWorker{
		handle:    h,
		logger:    h.logger().Child(partition),
		partition: partition,
	}

	// Initialize lifecycle context
	w.lifecycle.ctx, w.lifecycle.cancel = context.WithCancel(context.Background())

	bufSize := h.config().pipelineBufferedItems
	w.channel.preprocess = make(chan subJob, bufSize)
	w.channel.preTransform = make(chan *preTransformationMessage, bufSize)
	w.channel.transform = make(chan *transformationMessage, bufSize)

	// Store channel needs a larger buffer to accommodate all processed events
	storeBufferSize := (bufSize + 1) * (h.config().maxEventsToProcess.Load()/h.config().subJobSize + 1)
	w.channel.store = make(chan *storeMessage, storeBufferSize)

	// Start processing goroutines
	w.start()

	return w
}

// partitionWorker picks jobs from jobsdb for the appropriate partition and performs all processing steps:
//  1. preprocess
//  2. transform
//  3. store
type partitionWorker struct {
	partition string
	handle    workerHandle
	logger    logger.Logger

	lifecycle struct { // worker lifecycle related fields
		ctx    context.Context    // worker context
		cancel context.CancelFunc // worker context cancel function
		wg     sync.WaitGroup     // worker wait group
	}
	channel struct { // worker channels
		preprocess   chan subJob                    // preprocess channel is used to send jobs to preprocess asynchronously when pipelining is enabled
		preTransform chan *preTransformationMessage // preTransform is used to send jobs to store to arc, esch and tracking plan validation
		transform    chan *transformationMessage    // transform channel is used to send jobs to transform asynchronously when pipelining is enabled
		store        chan *storeMessage             // store channel is used to send jobs to store asynchronously when pipelining is enabled
	}
}

// start launches the various worker goroutines for the pipelined processing
func (w *partitionWorker) start() {
	// Setup context cancellation handler
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.preprocess)
		<-w.lifecycle.ctx.Done()
	})

	// Preprocessing goroutine
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.preTransform)
		defer w.logger.Debugf("preprocessing routine stopped for worker: %s", w.partition)

		for jobs := range w.channel.preprocess {
			val, err := w.handle.processJobsForDest(w.partition, jobs)
			if err != nil {
				w.logger.Errorf("Error preprocessing jobs: %v", err)
				panic(err)
			}
			w.channel.preTransform <- val
		}
	})

	// Pre-transformation goroutine
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.transform)
		defer w.logger.Debugf("pretransform routine stopped for worker: %s", w.partition)

		for processedMessage := range w.channel.preTransform {
			val, err := w.handle.generateTransformationMessage(processedMessage)
			if err != nil {
				w.logger.Errorf("Error generating transformation message: %v", err)
				panic(err)
			}
			w.channel.transform <- val
		}
	})

	// Transformation goroutine
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.store)
		defer w.logger.Debugf("transform routine stopped for worker: %s", w.partition)

		for msg := range w.channel.transform {
			w.channel.store <- w.handle.transformations(w.partition, msg)
		}
	})

	// Storage goroutine
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer w.logger.Debugf("store routine stopped for worker: %s", w.partition)

		var mergedJob *storeMessage
		firstSubJob := true

		for subJob := range w.channel.store {
			// If this is the first subjob and it doesn't have more parts,
			// we can store it directly without merging
			if firstSubJob && !subJob.hasMore {
				w.handle.Store(w.partition, subJob)
				continue
			}

			// Initialize the merged job with the first subjob
			if firstSubJob {
				mergedJob = &storeMessage{
					rsourcesStats:         subJob.rsourcesStats,
					dedupKeys:             make(map[string]struct{}),
					procErrorJobsByDestID: make(map[string][]*jobsdb.JobT),
					sourceDupStats:        make(map[dupStatKey]int),
					start:                 subJob.start,
				}
				firstSubJob = false
			}

			// Merge this subjob with the accumulated one
			mergedJob.merge(subJob)

			// If this is the last subjob in the batch, store the merged result
			if !subJob.hasMore {
				w.handle.Store(w.partition, mergedJob)
				firstSubJob = true
			}
		}
	})
}

// Stop gracefully terminates the worker by canceling its context and waiting for goroutines to finish
func (w *partitionWorker) Stop() {
	w.lifecycle.cancel()
	w.lifecycle.wg.Wait()
}
