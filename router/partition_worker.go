package router

import (
	"context"
	"strconv"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// newPartitionWorker creates a worker that is responsible for picking up jobs for a single partition (none, workspace, destination).
// A partition worker uses multiple workers internally to process the jobs that are being picked up asynchronously.
func newPartitionWorker(ctx context.Context, rt *Handle, partition string) *partitionWorker {
	pw := &partitionWorker{
		logger:    rt.logger.Child("p-" + partition),
		rt:        rt,
		partition: partition,
		ctx:       ctx,
	}
	pw.g, _ = errgroup.WithContext(context.Background())
	pw.workers = make([]*worker, rt.noOfWorkers)
	for i := 0; i < rt.noOfWorkers; i++ {
		worker := &worker{
			logger:                    pw.logger.Child("w-" + strconv.Itoa(i)),
			partition:                 partition,
			id:                        i,
			input:                     make(chan workerJob, rt.workerInputBufferSize),
			barrier:                   rt.barrier,
			rt:                        rt,
			deliveryTimeStat:          stats.Default.NewTaggedStat("router_delivery_time", stats.TimerType, stats.Tags{"destType": rt.destType}),
			batchTimeStat:             stats.Default.NewTaggedStat("router_batch_time", stats.TimerType, stats.Tags{"destType": rt.destType}),
			routerDeliveryLatencyStat: stats.Default.NewTaggedStat("router_delivery_latency", stats.TimerType, stats.Tags{"destType": rt.destType}),
			routerProxyStat:           stats.Default.NewTaggedStat("router_proxy_latency", stats.TimerType, stats.Tags{"destType": rt.destType}),
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
	ctx     context.Context
	g       *errgroup.Group // group against which all the workers are spawned
	workers []*worker       // workers that are responsible for processing the jobs

	pickupCount   int  // number of jobs picked up by the workers in the last iteration
	limitsReached bool // whether the limits were reached in the last iteration
}

// Work picks up jobs for the partitioned worker and returns whether it worked or not
func (pw *partitionWorker) Work() bool {
	start := time.Now()
	pw.pickupCount, pw.limitsReached = pw.rt.pickup(pw.ctx, pw.partition, pw.workers)
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
		close(worker.input)
	}
	_ = pw.g.Wait()
}
