package router

import (
	"math"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/metric"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
)

// bufferSizeCalculator is a function that calculates the buffer size for a worker
type bufferSizeCalculator func() int

// newStandardBufferSizeCalculator uses the maximum of number of jobs to batch in a worker and number of jobs per channel
// to calculate the buffer size for a worker
func newStandardBufferSizeCalculator(
	noOfJobsToBatchInAWorker config.ValueLoader[int], // number of jobs that a worker can batch together
	noOfJobsPerChannel int, // number of jobs per channel
) bufferSizeCalculator {
	return func() int {
		m1 := noOfJobsToBatchInAWorker.Load()
		if m1 > noOfJobsPerChannel {
			return m1
		}
		return noOfJobsPerChannel
	}
}

// newExperimentalBufferSizeCalculator calculates the buffer size for a worker based on the following algorithm (minimum value returned is 1):
//
//  1. Calculate the average throughput of jobs processed by the work loop per second (workLoopThroughput)
//  2. Calculate the number of jobs that are queried during pickup divided by the number of workers (jobQueryBatchSize / noOfWorkers)
//  3. Use the number of jobs to batch in a worker (noOfJobsToBatchInAWorker)
//  4. Take the maximum of the three metrics above and multiply it by a scaling factor (1.2) to determine the buffer size
//
// Exceptional case:
//
//   - If throughput is less than 1 (workLoopThroughput < 1), set the buffer size to 1 so that we are forcing a slow buffer start & introducing backpressure in the buffer in case of slow processing.
func newExperimentalBufferSizeCalculator(
	jobQueryBatchSize config.ValueLoader[int64], // number of jobs that are queried during pickup
	noOfWorkers int, // number of workers processing jobs
	noOfJobsToBatchInAWorker config.ValueLoader[int], // number of jobs that a worker can batch together
	workLoopThroughput metric.SimpleMovingAverage, // sliding average of work loop throughput
) bufferSizeCalculator {
	return func() int {
		const minBufferSize = 1
		const multiplier = 1.2
		m1 := workLoopThroughput.Load() // at least the average throughput of the work loop
		if m1 < 1 {                     // if there is no throughput yet, the throughput is less than 1 per second, set buffer to minBufferSize
			return minBufferSize
		}
		m2 := float64(jobQueryBatchSize.Load() / int64(noOfWorkers)) // at least the average number of jobs per worker during pickup
		m3 := float64(noOfJobsToBatchInAWorker.Load())               // at least equal to the number of jobs to batch in a worker

		return int(
			math.Ceil( // round up
				// calculate the maximum of the three metrics to determine the buffer size
				math.Max(
					math.Max(math.Max(m1, m2), m3)*multiplier, // scale up by multiplier to provide some buffer
					minBufferSize, // ensure buffer size is at least minBufferSize
				)))
	}
}

// newBufferSizeCalculatorSwitcher returns a function that switches between the standard and experimental calculators based on the
// enableExperimentalBufferSizeCalculator flag
func newBufferSizeCalculatorSwitcher(
	enableExperimentalBufferSizeCalculator config.ValueLoader[bool],
	jobQueryBatchSize config.ValueLoader[int64], // number of jobs that are queried during pickup
	noOfWorkers int, // number of workers processing jobs
	noOfJobsToBatchInAWorker config.ValueLoader[int], // number of jobs that a worker can batch together
	workLoopThroughput metric.SimpleMovingAverage, // sliding average of work loop throughput
	noOfJobsPerChannel int, // number of jobs per channel
) bufferSizeCalculator {
	new := newExperimentalBufferSizeCalculator(
		jobQueryBatchSize,
		noOfWorkers,
		noOfJobsToBatchInAWorker,
		workLoopThroughput,
	)
	legacy := newStandardBufferSizeCalculator(
		noOfJobsToBatchInAWorker,
		noOfJobsPerChannel,
	)

	return func() int {
		if enableExperimentalBufferSizeCalculator.Load() {
			return new()
		}
		return legacy()
	}
}

// newSmaHistogram combines a SimpleMovingAverage with a stats.Histogram to periodically record the moving average into the histogram.
func newSmaHistogram(
	slidingAverage metric.SimpleMovingAverage,
	histogram stats.Histogram,
	onceEvery *kitsync.OnceEvery,
) stats.Histogram {
	return &smaHistogram{
		slidingAverage: slidingAverage,
		histogram:      histogram,
		onceEvery:      onceEvery,
	}
}

type smaHistogram struct {
	slidingAverage metric.SimpleMovingAverage
	histogram      stats.Histogram
	onceEvery      *kitsync.OnceEvery
}

func (s *smaHistogram) Observe(v float64) {
	s.slidingAverage.Observe(v)
	s.onceEvery.Do(func() {
		s.histogram.Observe(s.slidingAverage.Load())
	})
}
