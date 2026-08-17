package processor

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/tracing"
)

// procPipelineWorker manages a single pipeline of the proc pool partition.
// It is the post-fan-out counterpart of [gwPipelineWorker]: it reads already fanned-out
// per-(source,destination) events from the intermediate (proc) jobsdb and runs the
// remaining stages, reusing the existing transform and store stages verbatim:
//
//	rebuild → userTransform → destinationTransform → store
//
// The only new stage is procRebuildStage (config re-hydration + dest-filter/consent
// re-check). userTransformStage, destinationTransformStage and storeStage are shared
// with gw pool; store commits job-status updates against procDB instead of gatewayDB
// via storeMessage.statusDB.
type procPipelineWorker struct {
	index     int
	partition string
	handle    procWorkerHandle
	logger    logger.Logger
	tracer    *tracing.Tracer

	lifecycle struct {
		ctx    context.Context
		cancel context.CancelFunc
		wg     sync.WaitGroup
	}
	channel struct {
		rebuild              chan subJob
		usertransform        chan *transformationMessage
		destinationtransform chan *userTransformData
		store                chan *storeMessage
	}
}

func newProcPipelineWorker(index int, partition string, h procWorkerHandle, t *tracing.Tracer) *procPipelineWorker {
	w := &procPipelineWorker{
		index:     index,
		handle:    h,
		logger:    h.logger().Withn(logger.NewStringField("partition", partition)),
		tracer:    t,
		partition: partition,
	}
	w.lifecycle.ctx, w.lifecycle.cancel = context.WithCancel(context.Background())

	bufSize := h.config().pipelineBufferedItems
	w.channel.rebuild = make(chan subJob, bufSize)
	w.channel.usertransform = make(chan *transformationMessage, bufSize)
	w.channel.destinationtransform = make(chan *userTransformData, bufSize)
	storeBufferSize := (bufSize + 1) * (h.config().maxEventsToProcess.Load()/h.config().subJobSize + 1)
	w.channel.store = make(chan *storeMessage, storeBufferSize)

	w.start()
	return w
}

func (w *procPipelineWorker) start() {
	// context cancellation handler closes the head of the pipeline
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.rebuild)
		<-w.lifecycle.ctx.Done()
	})

	spanTags := stats.Tags{"partition": w.partition}

	// Rebuild goroutine: re-hydrate config, re-run dest-filter/consent and produce the
	// post-fan-out transformationMessage from persisted proc jobs.
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.usertransform)
		defer w.logger.Debugn("proc rebuild routine stopped")

		for jobs := range w.channel.rebuild {
			val, err := w.handle.procRebuildStage(w.partition, jobs)
			if errors.Is(err, types.ErrProcessorStopping) {
				continue
			}
			if err != nil {
				w.logger.Errorn("Error rebuilding proc jobs", obskit.Error(err))
				panic(err)
			}
			waitStart := time.Now()
			w.channel.usertransform <- val
			w.tracer.RecordSpan(jobs.ctx, "start.userTransformCh.wait", waitStart, tracing.WithRecordSpanTags(spanTags))
		}
	})

	// User transformation goroutine (reused stage)
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.destinationtransform)
		defer w.logger.Debugn("proc usertransform routine stopped")

		for msg := range w.channel.usertransform {
			data := w.handle.userTransformStage(w.partition, msg)
			waitStart := time.Now()
			w.channel.destinationtransform <- data
			w.tracer.RecordSpan(msg.ctx, "start.destinationTransformCh.wait", waitStart, tracing.WithRecordSpanTags(spanTags))
		}
	})

	// Destination transformation goroutine (reused stage)
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer close(w.channel.store)
		defer w.logger.Debugn("proc destinationtransform routine stopped")

		for msg := range w.channel.destinationtransform {
			storeMsg := w.handle.destinationTransformStage(w.partition, msg)
			waitStart := time.Now()
			w.channel.store <- storeMsg
			w.tracer.RecordSpan(msg.ctx, "start.storeCh.wait", waitStart, tracing.WithRecordSpanTags(spanTags))
		}
	})

	// Storage goroutine (reused stage, targeting procDB for the status update)
	w.lifecycle.wg.Add(1)
	rruntime.Go(func() {
		defer w.lifecycle.wg.Done()
		defer w.logger.Debugn("proc store routine stopped")

		var mergedJob *storeMessage
		firstSubJob := true

		for subJob := range w.channel.store {
			if firstSubJob && !subJob.hasMore {
				w.handle.procStoreStage(w.partition, w.index, subJob)
				continue
			}

			if firstSubJob {
				mergedJob = &storeMessage{
					ctx:                   subJob.ctx,
					rsourcesStats:         subJob.rsourcesStats,
					dedupKeys:             make(map[string]struct{}),
					procErrorJobsByDestID: make(map[string][]procErrorJob),
					sourceDupStats:        make(map[dupStatKey]int),
					start:                 subJob.start,
				}
				firstSubJob = false
			}

			mergedJob.merge(subJob)

			if !subJob.hasMore {
				w.handle.procStoreStage(w.partition, w.index, mergedJob)
				firstSubJob = true
			}
		}
	})
}

// Stop gracefully terminates the worker by canceling its context and waiting for goroutines to finish
func (w *procPipelineWorker) Stop() {
	w.lifecycle.cancel()
	w.lifecycle.wg.Wait()
}
