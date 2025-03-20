package processor

import (
	"context"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
)

// newPipelineWorker new worker which manages a single pipeline of a partition
func newPipelineWorker(partition string, h workerHandle) *pipelineWorker {
	w := &pipelineWorker{
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

// pipelineWorker performs all processing steps of a partition's pipeline:
//  1. preprocess
//  2. preTransform
//  3. transform
//  4. store
type pipelineWorker struct {
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
func (w *pipelineWorker) start() {
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
func (w *pipelineWorker) Stop() {
	w.lifecycle.cancel()
	w.lifecycle.wg.Wait()
}
