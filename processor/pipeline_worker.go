package processor

import (
	"context"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/tracing"
)

// newPipelineWorker new worker which manages a single pipeline of a partition
func newPipelineWorker(partition string, h workerHandle, t *tracing.Tracer) *pipelineWorker {
	w := &pipelineWorker{
		handle:    h,
		logger:    h.logger().Withn(logger.NewStringField("partition", partition)),
		tracer:    t,
		partition: partition,
	}

	// Initialize lifecycle context
	w.lifecycle.ctx, w.lifecycle.cancel = context.WithCancel(context.Background())

	bufSize := h.config().pipelineBufferedItems
	w.channel.preprocess = make(chan subJob, bufSize)
	w.channel.preTransform = make(chan *preTransformationMessage, bufSize)
	w.channel.usertransform = make(chan *transformationMessage, bufSize)
	w.channel.destinationtransform = make(chan *userTransformData, bufSize)

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
	tracer    *tracing.Tracer

	lifecycle struct { // worker lifecycle related fields
		ctx    context.Context    // worker context
		cancel context.CancelFunc // worker context cancel function
		wg     sync.WaitGroup     // worker wait group
	}
	channel struct { // worker channels
		preprocess           chan subJob                    // preprocess channel is used to send jobs to preprocess asynchronously when pipelining is enabled
		preTransform         chan *preTransformationMessage // preTransform is used to send jobs to store to arc, event schema and tracking plan validation
		usertransform        chan *transformationMessage    // userTransform channel is used to send jobs to transform asynchronously when pipelining is enabled
		destinationtransform chan *userTransformData        // destinationTransform channel is used to send jobs to transform asynchronously when pipelining is enabled
		store                chan *storeMessage             // store channel is used to send jobs to store asynchronously when pipelining is enabled
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

	// Common span tags
	spanTags := stats.Tags{"partition": w.partition}

	// Preprocessing goroutine
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.preTransform)
		defer w.logger.Debugn("preprocessing routine stopped")

		for jobs := range w.channel.preprocess {
			val, err := w.handle.preprocessStage(w.partition, jobs)
			if err != nil {
				w.logger.Errorn("Error preprocessing jobs", obskit.Error(err))
				panic(err)
			}
			waitStart := time.Now()
			w.channel.preTransform <- val
			w.tracer.RecordSpan(jobs.ctx, "start.preTransformCh.wait", waitStart, tracing.WithRecordSpanTags(spanTags))
		}
	})

	// Pre-transformation goroutine
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.usertransform)
		defer w.logger.Debugn("pretransform routine stopped")

		for processedMessage := range w.channel.preTransform {
			val, err := w.handle.pretransformStage(w.partition, processedMessage)
			if err != nil {
				w.logger.Errorn("Error generating transformation message", obskit.Error(err))
				panic(err)
			}
			waitStart := time.Now()
			w.channel.usertransform <- val
			w.tracer.RecordSpan(processedMessage.subJobs.ctx, "start.userTransformCh.wait", waitStart, tracing.WithRecordSpanTags(spanTags))
		}
	})

	// User transformation  goroutine
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.destinationtransform)
		defer w.logger.Debugn("usertransform routine stopped")

		for msg := range w.channel.usertransform {
			data := w.handle.userTransformStage(w.partition, msg)
			waitStart := time.Now()
			w.channel.destinationtransform <- data
			w.tracer.RecordSpan(msg.ctx, "start.destinationTransformCh.wait", waitStart, tracing.WithRecordSpanTags(spanTags))
		}
	})

	// Destination Transformation goroutine
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.store)
		defer w.logger.Debugn("destinationtransform routine stopped")

		for msg := range w.channel.destinationtransform {
			storeMsg := w.handle.destinationTransformStage(w.partition, msg)
			waitStart := time.Now()
			w.channel.store <- storeMsg
			w.tracer.RecordSpan(msg.ctx, "start.storeCh.wait", waitStart, tracing.WithRecordSpanTags(spanTags))
		}
	})

	// Storage goroutine
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer w.logger.Debugn("store routine stopped")

		var mergedJob *storeMessage
		firstSubJob := true

		for subJob := range w.channel.store {
			// If this is the first subjob, and it doesn't have more parts,
			// we can store it directly without merging
			if firstSubJob && !subJob.hasMore {
				w.handle.storeStage(w.partition, subJob)
				continue
			}

			// Initialize the merged job with the first subjob
			if firstSubJob {
				mergedJob = &storeMessage{
					ctx:                   subJob.ctx,
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
				w.handle.storeStage(w.partition, mergedJob)
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
