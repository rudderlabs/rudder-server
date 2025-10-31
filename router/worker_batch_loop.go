package router

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/router/types"
)

type workerBatchLoop struct {
	ctx                      context.Context                                             // context for managing the lifecycle of the loop
	jobsBatchTimeout         config.ValueLoader[time.Duration]                           // timeout for processing jobs in a batch
	noOfJobsToBatchInAWorker config.ValueLoader[int]                                     // maximum number of jobs to batch in a worker before processing
	inputCh                  <-chan workerJob                                            // channel to receive jobs for processing
	enableBatching           bool                                                        // whether to enable batching of jobs
	batchTransform           func(routerJobs []types.RouterJobT) []types.DestinationJobT // function to transform router jobs into destination jobs in batch mode
	transform                func(routerJobs []types.RouterJobT) []types.DestinationJobT // function to transform router jobs into destination jobs in non-batch mode
	process                  func(destinationJobs []types.DestinationJobT)               // function to process the transformed destination jobs
	acceptWorkerJob          func(workerJob workerJob) *types.RouterJobT                 // function to accept a worker job and return a router job if applicable
	throughputStat           stats.Histogram                                             // stat to record throughput of the loop
}

// runLoop processes jobs from the input channel, batching them if it is enabled.
func (wl *workerBatchLoop) runLoop() {
	jobsBatchTimeout := time.After(wl.jobsBatchTimeout.Load())
	var routerJobs []types.RouterJobT
	captureThroughput := func(start time.Time, jobCount int) {
		elapsed := time.Since(start).Milliseconds()
		if elapsed == 0 {
			elapsed = 1 // prevent division by zero
		}
		wl.throughputStat.Observe(float64(jobCount) / (float64(elapsed) / 1000))
	}

	doProcessRouterJobs := func() {
		if len(routerJobs) > 0 {
			start := time.Now()
			var destinationJobs []types.DestinationJobT
			if wl.enableBatching {
				destinationJobs = wl.batchTransform(routerJobs)
			} else {
				destinationJobs = wl.transform(routerJobs)
			}
			wl.process(destinationJobs)
			captureThroughput(start, len(routerJobs))
			routerJobs = nil // reset routerJobs for the next batch
		}
		jobsBatchTimeout = time.After(wl.jobsBatchTimeout.Load()) // reset the timeout
	}
	for {
		select {
		case workerJob, ok := <-wl.inputCh:
			if !ok {
				doProcessRouterJobs() // process any remaining jobs in the batch
				return                // input channel is closed, exit the loop
			}
			if wl.ctx.Err() != nil {
				// Context is done, we are stopping the worker
				// it is fine to ignore any pending jobs in the input channel (executing state)
				// since when the router restarts, it will mark them as failed and reprocess them.
				// What is important is to stop as soon as possible.
				return
			}
			if !wl.enableBatching && workerJob.parameters.TransformAt != "router" && len(routerJobs) > 0 {
				// process the current batch if batching is not enabled, transform for the current job is not at router and there are pending jobs in the batch
				// (scenario where we are switching from router to processor transformation)
				doProcessRouterJobs()
			}
			start := time.Now()
			if routerJob := wl.acceptWorkerJob(workerJob); routerJob != nil {
				routerJobs = append(routerJobs, *routerJob)
				if wl.noOfJobsToBatchInAWorker.Load() <= len(routerJobs) {
					doProcessRouterJobs() // process the batch if it reaches the limit
				}
			} else { // job was not accepted to enter the batch, but was processed, so we need to capture its throughput
				captureThroughput(start, 1)
			}
		case <-jobsBatchTimeout:
			doProcessRouterJobs() // process any remaining jobs in the batch
		}
	}
}
