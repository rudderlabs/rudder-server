package router

import (
	"math/rand"
	"testing"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/stats/metric"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	"github.com/rudderlabs/rudder-server/router/types"
)

// BenchmarkFindWorkerSlot exercises the hot path in Handle.findWorkerSlot for the
// event-ordering-disabled branch (see reserveAnyWorkerSlot in handle.go) against
// 512 workers.
//
// The benchmark sweeps three independent dimensions:
//
//   - mode: which workerBuffer configuration is in use:
//     simple                — newSimpleWorkerBuffer (fixed capacity, no calculator)
//     dynamic/standard      — newWorkerBuffer + newBufferSizeCalculatorSwitcher (experimental=false)
//     dynamic/experimental  — newWorkerBuffer + newBufferSizeCalculatorSwitcher (experimental=true)
//
//   - strategy: how a worker with capacity is chosen:
//     filter     — the old approach: lo.Filter on AvailableSlots, rand.Intn into the
//     survivor list, then ReserveSlot. Kept here as a baseline.
//     shuffleTry — the new approach (now in production): random start + sequential
//     walk + ReserveSlot until one succeeds. No filter slice, no
//     double check, no race window.
//
//   - scenario: per-worker fill ratio and/or fraction of workers fully exhausted,
//     so we can see how shuffleTry's "average tries before success" varies.
//
// Inputs to the calculator switcher mirror the production defaults from
// handle_lifecycle.go: maxNoOfJobsPerChannel=10000, noOfJobsPerChannel=1000,
// noOfJobsToBatchInAWorker=20, scalingFactor=2.0, minBufferSize=500.
func BenchmarkFindWorkerSlot(b *testing.B) {
	const (
		numWorkers             = 512
		maxNoOfJobsPerChannel  = 10000
		noOfJobsPerChannel     = 1000
		noOfJobsToBatchPerWork = 20
		jobQueryBatchSize      = 10000
		workLoopThroughputObs  = 100.0
		scalingFactor          = 2.0
		experimentalMinSize    = 500
		simpleBufferCapacity   = 1000 // matches the standard calculator's output for these defaults
	)

	// scenarios control the pre-occupation pattern across the 512 workers. They are
	// designed so shuffleTry's "average tries before success" varies meaningfully —
	// for filter the cost is roughly constant (always 512 AvailableSlots calls).
	//
	// perWorkerFillFrac: each worker is pre-filled to this fraction of its capacity
	//                    (so it still has a few slots free).
	// fullyFullFrac:     this fraction of workers are pre-filled to capacity (no
	//                    slots free at all).
	scenarios := []struct {
		name              string
		perWorkerFillFrac float64
		fullyFullFrac     float64
	}{
		{"allEmpty", 0.0, 0.0},                  // every worker has full capacity → shuffleTry wins on 1st try
		{"perWorkerHalf", 0.5, 0.0},             // every worker half-full → shuffleTry wins on 1st try
		{"halfWorkersFull", 0.0, 0.5},           // 50% of workers exhausted → shuffleTry ~2 tries avg
		{"mostWorkersFull_99pct", 0.0, 0.99},    // 99% of workers exhausted → shuffleTry ~100 tries avg
		{"mostWorkersFull_99_8pct", 0.0, 0.998}, // 99.8% of workers exhausted → shuffleTry ~500 tries avg (≈ filter's 512 RLocks)
	}

	// Each mode returns a freshly constructed worker buffer along with its effective
	// capacity (so we can pre-occupy the right number of slots for fill ratios).
	modes := []struct {
		name   string
		newBuf func() (buf *workerBuffer, capacity int)
	}{
		{
			name: "simple",
			newBuf: func() (*workerBuffer, int) {
				return newSimpleWorkerBuffer(simpleBufferCapacity), simpleBufferCapacity
			},
		},
		{
			name: "dynamic/standard",
			newBuf: func() (*workerBuffer, int) {
				calc := newBufferSizeCalculatorSwitcher(
					config.SingleValueLoader(false), // experimental disabled
					config.SingleValueLoader(jobQueryBatchSize),
					numWorkers,
					config.SingleValueLoader(noOfJobsToBatchPerWork),
					seededSMA(workLoopThroughputObs),
					config.SingleValueLoader(scalingFactor),
					noOfJobsPerChannel,
					config.SingleValueLoader(experimentalMinSize),
				)
				return newWorkerBuffer(maxNoOfJobsPerChannel, calc, newBufferStats()), calc()
			},
		},
		{
			name: "dynamic/experimental",
			newBuf: func() (*workerBuffer, int) {
				calc := newBufferSizeCalculatorSwitcher(
					config.SingleValueLoader(true), // experimental enabled
					config.SingleValueLoader(jobQueryBatchSize),
					numWorkers,
					config.SingleValueLoader(noOfJobsToBatchPerWork),
					seededSMA(workLoopThroughputObs),
					config.SingleValueLoader(scalingFactor),
					noOfJobsPerChannel,
					config.SingleValueLoader(experimentalMinSize),
				)
				return newWorkerBuffer(maxNoOfJobsPerChannel, calc, newBufferStats()), calc()
			},
		},
	}

	// Two strategies for finding a worker with an available slot:
	//
	//  - filter: the previous behavior — lo.Filter on AvailableSlots, then rand.Intn
	//    into the survivor list, then ReserveSlot on the chosen one. Kept here as a
	//    baseline to measure against.
	//
	//  - shuffleTry: the current production path (see reserveAnyWorkerSlot in
	//    handle.go) — pick a random starting offset and walk the workers slice
	//    sequentially (with wrap-around), calling ReserveSlot until one succeeds.
	//    Avoids the filter slice allocation, the double check, and the
	//    filter-then-reserve race window.
	strategies := []struct {
		name string
		find func(workers []*worker, rng *rand.Rand) *reservedSlot
	}{
		{
			name: "filter",
			find: func(workers []*worker, rng *rand.Rand) *reservedSlot {
				availableWorkers := lo.Filter(workers, func(w *worker, _ int) bool { return w.AvailableSlots() > 0 })
				if len(availableWorkers) == 0 {
					return nil
				}
				return availableWorkers[rng.Intn(len(availableWorkers))].ReserveSlot()
			},
		},
		{
			name: "shuffleTry",
			find: func(workers []*worker, rng *rand.Rand) *reservedSlot {
				n := len(workers)
				start := rng.Intn(n)
				for i := range n {
					if slot := workers[(start+i)%n].ReserveSlot(); slot != nil {
						return slot
					}
				}
				return nil
			},
		},
	}

	for _, mode := range modes {
		for _, strategy := range strategies {
			for _, sc := range scenarios {
				b.Run(mode.name+"/"+strategy.name+"/"+sc.name, func(b *testing.B) {
					workers := make([]*worker, numWorkers)
					var capacity int
					for i := range workers {
						buf, c := mode.newBuf()
						workers[i] = &worker{workerBuffer: buf}
						capacity = c
					}

					// Per-worker partial fill (every worker still has free slots).
					perWorkerOccupied := int(float64(capacity) * sc.perWorkerFillFrac)
					if perWorkerOccupied > 0 {
						for _, w := range workers {
							for range perWorkerOccupied {
								if s := w.ReserveSlot(); s != nil {
									s.Use(workerJob{})
								}
							}
						}
					}

					// Whole-worker exhaustion: pick a fraction of workers and fully fill them.
					fullyFullCount := int(float64(numWorkers) * sc.fullyFullFrac)
					for _, w := range workers[:fullyFullCount] {
						for {
							s := w.ReserveSlot()
							if s == nil {
								break
							}
							s.Use(workerJob{})
						}
					}

					rng := rand.New(rand.NewSource(1))

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						slot := strategy.find(workers, rng)
						if slot == nil {
							b.Fatal(types.ErrWorkerNoSlot)
						}
						// Release the slot to keep the buffer state stable across iterations.
						slot.Release()
					}
				})
			}
		}
	}
}

// seededSMA returns a SimpleMovingAverage primed with a single observation, so the
// experimental calculator sees a stable, non-zero throughput throughout the benchmark.
func seededSMA(observation float64) metric.SimpleMovingAverage {
	sma := metric.NewSimpleMovingAverage(1)
	sma.Observe(observation)
	return sma
}

// newBufferStats builds a workerBufferStats configured the same way as production
// (see partition_worker.go): 5s OnceEvery throttle plus capacity & size histograms.
// memstats is used so the benchmark stays isolated from the global stats registry.
func newBufferStats() *workerBufferStats {
	memStats, err := memstats.New()
	if err != nil {
		panic(err)
	}
	return &workerBufferStats{
		onceEvery:       kitsync.NewOnceEvery(5 * time.Second),
		currentCapacity: memStats.NewTaggedStat("router_worker_buffer_capacity", stats.HistogramType, nil),
		currentSize:     memStats.NewTaggedStat("router_worker_buffer_size", stats.HistogramType, nil),
	}
}
