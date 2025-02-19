package processor

import (
	"context"
	"sync"
	"time"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

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
	handle    workerHandle
	g         *errgroup.Group
}

func newProcessorWorker(partition string, h workerHandle) *worker {
	w := &worker{
		partition: partition,
		logger:    h.logger().Child(partition),
		handle:    h,
	}
	w.g, _ = errgroup.WithContext(context.Background())
	w.workers = make([]*partitionWorker, h.config().numPartitions)
	for i := 0; i < h.config().numPartitions; i++ {
		worker := newWorker(partition, h)
		w.workers[i] = worker
	}

	return w
}

func (w *worker) Work() bool {
	if !w.handle.config().enablePipelining {
		return w.handle.handlePendingGatewayJobs(w.partition)
	}

	jobs := w.handle.getJobs(w.partition)

	start := time.Now()
	afterGetJobs := time.Now()
	if len(jobs.Jobs) == 0 {
		return false
	}
	if err := w.handle.markExecuting(w.partition, jobs.Jobs); err != nil {
		w.logger.Error(err)
		panic(err)
	}

	w.handle.stats().DBReadThroughput(w.partition).Count(throughputPerSecond(jobs.EventsCount, time.Since(start)))

	rsourcesStats := rsources.NewStatsCollector(w.handle.rsourcesService(), rsources.IgnoreDestinationID())
	rsourcesStats.BeginProcessing(jobs.Jobs)

	// First split jobs by murmur hash into partitions
	jobsByPartition := make(map[int][]*jobsdb.JobT)
	for _, job := range jobs.Jobs {
		hash := misc.GetMurmurHash(job.UserID)
		// Ensure positive partition by using absolute value
		partition := int(hash % uint64(w.handle.config().numPartitions))
		jobsByPartition[partition] = append(jobsByPartition[partition], job)
	}

	// For each partition, split into subjobs and send to workers
	for partition, partitionJobs := range jobsByPartition {
		// Split into subjobs
		chunks := lo.Chunk(partitionJobs, w.handle.config().subJobSize)
		subJobs := lo.Map(chunks, func(subJobs []*jobsdb.JobT, index int) subJob {
			return subJob{
				subJobs:       subJobs,
				hasMore:       index+1 < len(chunks),
				rsourcesStats: rsourcesStats,
			}
		})

		// Send subjobs to the worker for this partition
		for _, subJob := range subJobs {
			w.workers[partition].channel.preprocess <- subJob
		}
	}

	if !jobs.LimitsReached {
		readLoopSleep := w.handle.config().readLoopSleep
		if elapsed := time.Since(afterGetJobs); elapsed < readLoopSleep.Load() {
			if err := misc.SleepCtx(context.Background(), readLoopSleep.Load()-elapsed); err != nil {
				return true
			}
		}
	}

	return true
}

func (w *worker) SleepDurations() (min, max time.Duration) {
	return w.handle.config().readLoopSleep.Load(), w.handle.config().maxLoopSleep.Load()
}

// Stop stops the worker and waits until all its goroutines have stopped
func (w *worker) Stop() {
	for _, worker := range w.workers {
		worker.Stop()
	}
}

// newWorker creates a new worker
func newWorker(partition string, h workerHandle) *partitionWorker {
	w := &partitionWorker{
		handle:    h,
		logger:    h.logger().Child(partition),
		partition: partition,
	}
	w.lifecycle.ctx, w.lifecycle.cancel = context.WithCancel(context.Background())
	w.channel.preprocess = make(chan subJob, w.handle.config().pipelineBufferedItems)
	w.channel.preTransform = make(chan *preTransformationMessage, w.handle.config().pipelineBufferedItems)
	w.channel.transform = make(chan *transformationMessage, w.handle.config().pipelineBufferedItems)
	w.channel.store = make(chan *storeMessage, (w.handle.config().pipelineBufferedItems+1)*(w.handle.config().maxEventsToProcess.Load()/w.handle.config().subJobSize+1))
	w.start()

	return w
}

// worker picks jobs from jobsdb for the appropriate partition and performs all processing steps for them
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

// start starts the various worker goroutines
func (w *partitionWorker) start() {
	// wait for context to be cancelled
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.preprocess)
		<-w.lifecycle.ctx.Done()
	})

	// preprocess jobs
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.preTransform)
		defer w.logger.Debugf("preprocessing routine stopped for worker: %s", w.partition)
		for jobs := range w.channel.preprocess {
			val, err := w.handle.processJobsForDest(w.partition, jobs)
			if err != nil {
				panic(err)
			}
			w.channel.preTransform <- val
		}
	})

	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.transform)
		defer w.logger.Debugf("pretransform routine stopped for worker: %s", w.partition)
		for processedMessage := range w.channel.preTransform {
			val, err := w.handle.generateTransformationMessage(processedMessage)
			if err != nil {
				panic(err)
			}
			w.channel.transform <- val
		}
	})

	// transform jobs
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.store)
		defer w.logger.Debugf("transform routine stopped for worker: %s", w.partition)
		for msg := range w.channel.transform {
			w.channel.store <- w.handle.transformations(w.partition, msg)
		}
	})

	// store jobs
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		var mergedJob *storeMessage
		firstSubJob := true
		defer w.lifecycle.wg.Done()
		defer w.logger.Debugf("store routine stopped for worker: %s", w.partition)
		for subJob := range w.channel.store {

			if firstSubJob && !subJob.hasMore {
				w.handle.Store(w.partition, subJob)
				continue
			}

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
			mergedJob.merge(subJob)

			if !subJob.hasMore {
				w.handle.Store(w.partition, mergedJob)
				firstSubJob = true
			}
		}
	})
}

// Stop stops the worker and waits until all its goroutines have stopped
func (w *partitionWorker) Stop() {
	w.lifecycle.cancel()
	w.lifecycle.wg.Wait()
}
