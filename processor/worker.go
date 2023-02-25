package processor

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/rsources"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// newWorker creates a new worker
func newWorker(ctx context.Context, partition string, h workerHandle) *worker {
	w := &worker{
		handle:    h,
		logger:    h.logger().Child(partition),
		partition: partition,
	}
	w.lifecycle.ctx, w.lifecycle.cancel = context.WithCancel(ctx)
	w.channel.ping = make(chan struct{}, 1)
	w.channel.preprocess = make(chan subJob, w.handle.config().pipelineBufferedItems)
	w.channel.transform = make(chan *transformationMessage, w.handle.config().pipelineBufferedItems)
	w.channel.store = make(chan *storeMessage, (w.handle.config().pipelineBufferedItems+1)*(w.handle.config().maxEventsToProcess/w.handle.config().subJobSize+1))
	w.start()

	return w
}

// worker picks jobs from jobsdb for the appropriate partition and performs all processing steps for them
//  1. preprocess
//  2. transform
//  3. store
type worker struct {
	partition string
	handle    workerHandle
	logger    logger.Logger

	lifecycle struct { // worker lifecycle related fields
		ctx    context.Context    // worker context
		cancel context.CancelFunc // worker context cancel function
		wg     sync.WaitGroup     // worker wait group

		idleMu    sync.RWMutex // idle mutex
		idleSince time.Time    // idle since
	}

	channel struct { // worker channels
		ping       chan struct{}               // ping channel triggers the worker to start working
		preprocess chan subJob                 // preprocess channel is used to send jobs to preprocess asynchronously when pipelining is enabled
		transform  chan *transformationMessage // transform channel is used to send jobs to transform asynchronously when pipelining is enabled
		store      chan *storeMessage          // store channel is used to send jobs to store asynchronously when pipelining is enabled
	}
}

// start starts the various worker goroutines
func (w *worker) start() {
	// ping loop
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		var exponentialSleep misc.ExponentialNumber[time.Duration]
		defer w.lifecycle.wg.Done()
		defer close(w.channel.preprocess)
		defer close(w.channel.ping)
		defer w.logger.Debugf("ping loop stopped for worker: %s", w.partition)
		for {
			select {
			case <-w.lifecycle.ctx.Done():
				return
			case <-w.channel.ping:
			}

			w.setIdleSince(time.Time{})
			if w.pickJobs() {
				exponentialSleep.Reset()
			} else {
				if err := misc.SleepCtx(w.lifecycle.ctx, exponentialSleep.Next(w.handle.config().readLoopSleep, w.handle.config().maxLoopSleep)); err != nil {
					w.logger.Info("ping loop stopped for worker: ", w.partition, " due to: ", err)
					return
				}
				w.setIdleSince(time.Now())
			}
		}
	})

	if !w.handle.config().enablePipelining {
		return
	}

	// preprocess jobs
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.transform)
		defer w.logger.Debugf("preprocessing routine stopped for worker: %s", w.partition)
		for jobs := range w.channel.preprocess {
			w.channel.transform <- w.handle.processJobsForDest(w.partition, jobs, nil)
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
					uniqueMessageIds:      make(map[string]struct{}),
					procErrorJobsByDestID: make(map[string][]*jobsdb.JobT),
					sourceDupStats:        make(map[string]int),
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

// pickJobs picks the next set of jobs from the jobsdb and returns [true] if jobs were picked, [false] otherwise
func (w *worker) pickJobs() (picked bool) {
	if w.handle.config().enablePipelining {
		start := time.Now()
		jobs := w.handle.getJobs(w.partition)

		if len(jobs.Jobs) == 0 {
			return
		}
		picked = true

		if err := w.handle.markExecuting(jobs.Jobs); err != nil {
			w.logger.Error(err)
			panic(err)
		}

		w.handle.stats().DBReadThroughput.Count(throughputPerSecond(jobs.EventsCount, time.Since(start)))

		rsourcesStats := rsources.NewStatsCollector(w.handle.rsourcesService())
		rsourcesStats.BeginProcessing(jobs.Jobs)
		subJobs := w.handle.jobSplitter(jobs.Jobs, rsourcesStats)
		for _, subJob := range subJobs {
			w.channel.preprocess <- subJob
		}

		// sleepRatio is dependent on the number of events read in this loop
		sleepRatio := 1.0 - math.Min(1, float64(jobs.EventsCount)/float64(w.handle.config().maxEventsToProcess))
		if err := misc.SleepCtx(w.lifecycle.ctx, time.Duration(sleepRatio*float64(w.handle.config().readLoopSleep))); err != nil {
			return
		}
		return
	} else {
		return w.handle.handlePendingGatewayJobs(w.partition)
	}
}

func (w *worker) setIdleSince(t time.Time) {
	w.lifecycle.idleMu.Lock()
	defer w.lifecycle.idleMu.Unlock()
	w.lifecycle.idleSince = t
}

// Ping triggers the worker to pick more jobs
func (w *worker) Ping() {
	select {
	case w.channel.ping <- struct{}{}:
	default:
	}
}

// IdleSince returns the time when the worker was last idle. If the worker is not idle, it returns a zero time.
func (w *worker) IdleSince() time.Time {
	w.lifecycle.idleMu.RLock()
	defer w.lifecycle.idleMu.RUnlock()
	return w.lifecycle.idleSince
}

// Stop stops the worker and waits until all its goroutines have stopped
func (w *worker) Stop() {
	w.lifecycle.cancel()
	w.lifecycle.wg.Wait()
	w.logger.Debugf("Stopped worker: %s", w.partition)
}
