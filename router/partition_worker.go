package router

import (
	"context"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/metric"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/utils/cache"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// newPartitionWorker creates a worker that is responsible for picking up jobs for a single partition (none, workspace, destination).
// A partition worker uses multiple workers internally to process the jobs that are being picked up asynchronously.
func newPartitionWorker(ctx context.Context, rt *Handle, partition string) *partitionWorker {
	pw := &partitionWorker{
		logger:               rt.logger.Child("p-" + partition),
		rt:                   rt,
		partition:            partition,
		ctx:                  ctx,
		pickupBatchSizeGauge: newGaugeWithLastValue[int](stats.Default.NewTaggedStat("router_pickup_batch_size_gauge", stats.GaugeType, stats.Tags{"destType": rt.destType, "partition": partition})),
	}
	pw.g, _ = errgroup.WithContext(context.Background())
	pw.workers = make([]*worker, rt.noOfWorkers)
	deliveryTimeStat := stats.Default.NewTaggedStat("router_delivery_time", stats.TimerType, stats.Tags{"destType": rt.destType})
	routerDeliveryLatencyStat := stats.Default.NewTaggedStat("router_delivery_latency", stats.TimerType, stats.Tags{"destType": rt.destType})
	routerProxyStat := stats.Default.NewTaggedStat("router_proxy_latency", stats.TimerType, stats.Tags{"destType": rt.destType})

	bufferCapacityStat := stats.Default.NewTaggedStat("router_worker_buffer_capacity", stats.HistogramType, stats.Tags{"destType": rt.destType, "partition": partition})
	bufferSizeStat := stats.Default.NewTaggedStat("router_worker_buffer_size", stats.HistogramType, stats.Tags{"destType": rt.destType, "partition": partition})

	for i := 0; i < rt.noOfWorkers; i++ {
		ctx, cancelFunc := context.WithCancel(context.Background())
		workLoopThroughput := metric.NewSimpleMovingAverage(10)
		workLoopThroughputStat := stats.Default.NewTaggedStat("router_worker_work_loop_throughput", stats.HistogramType, stats.Tags{"destType": rt.destType, "partition": partition})
		worker := &worker{
			logger:     pw.logger.Child("w-" + strconv.Itoa(i)),
			partition:  partition,
			id:         i,
			ctx:        ctx,
			cancelFunc: cancelFunc,
			workerBuffer: newWorkerBuffer(
				rt.maxNoOfJobsPerChannel,
				newBufferSizeCalculatorSwitcher(
					rt.reloadableConfig.enableExperimentalBufferSizeCalculator,
					pw.pickupBatchSizeGauge,
					rt.noOfWorkers,
					rt.reloadableConfig.noOfJobsToBatchInAWorker,
					workLoopThroughput,
					rt.reloadableConfig.experimentalBufferSizeScalingFactor,
					rt.noOfJobsPerChannel,
				),
				&workerBufferStats{
					onceEvery:       kitsync.NewOnceEvery(5 * time.Second),
					currentCapacity: bufferCapacityStat,
					currentSize:     bufferSizeStat,
				}),
			barrier:                   rt.barrier,
			rt:                        rt,
			deliveryTimeStat:          deliveryTimeStat,
			routerDeliveryLatencyStat: routerDeliveryLatencyStat,
			routerProxyStat:           routerProxyStat,
			deliveryLatencyStatsCache: cache.NewStatsCache(func(labels deliveryMetricLabels) stats.Measurement {
				return stats.Default.NewTaggedStat("transformer_outgoing_request_latency", stats.TimerType, labels.ToStatTags())
			}),
			deliveryCountStatsCache: cache.NewStatsCache(func(labels deliveryMetricLabels) stats.Measurement {
				return stats.Default.NewTaggedStat("transformer_outgoing_request_count", stats.CountType, labels.ToStatTags())
			}),
			workLoopThroughput: newSmaHistogram(
				workLoopThroughput,
				workLoopThroughputStat,
				kitsync.NewOnceEvery(10*time.Second),
			),
		}
		pw.workers[i] = worker

		pw.g.Go(crash.Wrapper(func() error {
			worker.workLoop()
			return nil
		}))

	}
	return pw
}

type partitionWorker struct {
	// dependencies
	rt     *Handle
	logger logger.Logger

	// configuration
	partition string

	// state
	ctx                  context.Context
	g                    *errgroup.Group         // group against which all the workers are spawned
	pickupBatchSizeGauge GaugeWithLastValue[int] // gauge to track the pickup batch size used in the last pickup iteration
	workers              []*worker               // workers that are responsible for processing the jobs

	pickupCount   int  // number of jobs picked up by the workers in the last iteration
	limitsReached bool // whether the limits were reached in the last iteration
}

// Work picks up jobs for the partitioned worker and returns whether it worked or not
func (pw *partitionWorker) Work() bool {
	start := time.Now()
	pw.pickupCount, pw.limitsReached = pw.rt.pickup(pw.ctx, pw.partition, pw.workers, pw.pickupBatchSizeGauge)
	// the following stats are used to track the total time taken for the pickup process and the number of jobs picked up
	stats.Default.NewTaggedStat("router_generator_loop", stats.TimerType, stats.Tags{"destType": pw.rt.destType}).Since(start)
	stats.Default.NewTaggedStat("router_generator_events", stats.CountType, stats.Tags{"destType": pw.rt.destType, "partition": pw.partition}).Count(pw.pickupCount)

	worked := pw.pickupCount > 0
	if worked && !pw.limitsReached { // sleep only if we worked and we didn't reach the limits
		if sleepFor := pw.rt.reloadableConfig.readSleep.Load() - time.Since(start); sleepFor > 0 {
			_ = misc.SleepCtx(pw.ctx, sleepFor)
		}
	}
	return worked
}

// SleepDurations returns the min and max sleep durations for the partitioned worker while not working
func (pw *partitionWorker) SleepDurations() (min, max time.Duration) {
	return pw.rt.reloadableConfig.readSleep.Load(), pw.rt.reloadableConfig.readSleep.Load() * 10
}

// Stop stops the partitioned worker by closing the input channel of all its internal workers and waiting for them to finish
func (pw *partitionWorker) Stop() {
	for _, worker := range pw.workers {
		worker.cancelFunc()
		worker.workerBuffer.Close()
	}
	_ = pw.g.Wait()
}
