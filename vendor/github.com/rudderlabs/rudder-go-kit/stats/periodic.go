package stats

import (
	"context"
	"runtime"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats/metric"
)

// GaugeFunc is an interface that implements the setting of a gauge value
// in a stats system. It should be expected that key will contain multiple
// parts separated by the '.' character in the form used by statsd (e.x.
// "mem.heap.alloc")
type gaugeFunc func(key string, val uint64)

// periodicStatsConfig is the configuration for the periodic stats collection
type periodicStatsConfig struct {
	enabled                 bool
	statsCollectionInterval int64
	enableCPUStats          bool
	enableMemStats          bool
	enableGCStats           bool
	metricManager           metric.Manager
}

// runtimeStatsCollector implements the periodic grabbing of informational data from the
// runtime package and outputting the values to a GaugeFunc.
type runtimeStatsCollector struct {
	// PauseDur represents the interval in between each set of stats output.
	// Defaults to 10 seconds.
	PauseDur time.Duration

	// EnableCPU determines whether CPU statistics will be output. Defaults to true.
	EnableCPU bool

	// EnableMem determines whether memory statistics will be output. Defaults to true.
	EnableMem bool

	// EnableGC determines whether garbage collection statistics will be output. EnableMem
	// must also be set to true for this to take affect. Defaults to true.
	EnableGC bool

	// done, when closed, is used to signal runtimeStatsCollector that is should stop collecting
	// statistics and the Run function should return. If done is set, upon shutdown
	// all gauges will be sent a final zero value to reset their values to 0.
	done chan struct{}

	gaugeFunc gaugeFunc
}

// New creates a new runtimeStatsCollector that will periodically output statistics to gaugeFunc. It
// will also set the values of the exported fields to the described defaults. The values
// of the exported defaults can be changed at any point before Run is called.
func newRuntimeStatsCollector(gaugeFunc gaugeFunc) runtimeStatsCollector {
	return runtimeStatsCollector{
		PauseDur:  10 * time.Second,
		EnableCPU: true,
		EnableMem: true,
		EnableGC:  true,
		gaugeFunc: gaugeFunc,
		done:      make(chan struct{}),
	}
}

// Run gathers statistics from package runtime and outputs them to the configured GaugeFunc every
// PauseDur. This function will not return until Done has been closed (or never if Done is nil),
// therefore it should be called in its own goroutine.
func (c runtimeStatsCollector) run(ctx context.Context) {
	defer close(c.done)
	defer c.zeroStats()
	c.outputStats()

	// Gauges are a 'snapshot' rather than a histogram. Pausing for some interval
	// aims to get a 'recent' snapshot out before statsd flushes metrics.
	tick := time.NewTicker(c.PauseDur)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			c.outputStats()
		}
	}
}

type cpuStats struct {
	NumGoroutine uint64
	NumCgoCall   uint64
}

// zeroStats sets all the stat guages to zero. On shutdown we want to zero them out so they don't persist
// at their last value until we start back up.
func (c runtimeStatsCollector) zeroStats() {
	if c.EnableCPU {
		cStats := cpuStats{}
		c.outputCPUStats(&cStats)
	}
	if c.EnableMem {
		mStats := runtime.MemStats{}
		c.outputMemStats(&mStats)
		if c.EnableGC {
			c.outputGCStats(&mStats)
		}
	}
}

func (c runtimeStatsCollector) outputStats() {
	if c.EnableCPU {
		cStats := cpuStats{
			NumGoroutine: uint64(runtime.NumGoroutine()),
			NumCgoCall:   uint64(runtime.NumCgoCall()),
		}
		c.outputCPUStats(&cStats)
	}
	if c.EnableMem {
		m := &runtime.MemStats{}
		runtime.ReadMemStats(m)
		c.outputMemStats(m)
		if c.EnableGC {
			c.outputGCStats(m)
		}
	}
}

func (c runtimeStatsCollector) outputCPUStats(s *cpuStats) {
	c.gaugeFunc("cpu.goroutines", s.NumGoroutine)
	c.gaugeFunc("cpu.cgo_calls", s.NumCgoCall)
}

func (c runtimeStatsCollector) outputMemStats(m *runtime.MemStats) {
	// General
	c.gaugeFunc("mem.alloc", m.Alloc)
	c.gaugeFunc("mem.total", m.TotalAlloc)
	c.gaugeFunc("mem.sys", m.Sys)
	c.gaugeFunc("mem.lookups", m.Lookups)
	c.gaugeFunc("mem.malloc", m.Mallocs)
	c.gaugeFunc("mem.frees", m.Frees)

	// Heap
	c.gaugeFunc("mem.heap.alloc", m.HeapAlloc)
	c.gaugeFunc("mem.heap.sys", m.HeapSys)
	c.gaugeFunc("mem.heap.idle", m.HeapIdle)
	c.gaugeFunc("mem.heap.inuse", m.HeapInuse)
	c.gaugeFunc("mem.heap.released", m.HeapReleased)
	c.gaugeFunc("mem.heap.objects", m.HeapObjects)

	// Stack
	c.gaugeFunc("mem.stack.inuse", m.StackInuse)
	c.gaugeFunc("mem.stack.sys", m.StackSys)
	c.gaugeFunc("mem.stack.mspan_inuse", m.MSpanInuse)
	c.gaugeFunc("mem.stack.mspan_sys", m.MSpanSys)
	c.gaugeFunc("mem.stack.mcache_inuse", m.MCacheInuse)
	c.gaugeFunc("mem.stack.mcache_sys", m.MCacheSys)

	c.gaugeFunc("mem.othersys", m.OtherSys)
}

func (c runtimeStatsCollector) outputGCStats(m *runtime.MemStats) {
	c.gaugeFunc("mem.gc.sys", m.GCSys)
	c.gaugeFunc("mem.gc.next", m.NextGC)
	c.gaugeFunc("mem.gc.last", m.LastGC)
	c.gaugeFunc("mem.gc.pause_total", m.PauseTotalNs)
	c.gaugeFunc("mem.gc.pause", m.PauseNs[(m.NumGC+255)%256])
	c.gaugeFunc("mem.gc.count", uint64(m.NumGC))
	c.gaugeFunc("mem.gc.cpu_percent", uint64(100*m.GCCPUFraction))
}

// metricStatsCollector implements the periodic grabbing of informational data from the
// metric package and outputting the values as stats
type metricStatsCollector struct {
	stats         Stats
	metricManager metric.Manager
	// PauseDur represents the interval in between each set of stats output.
	// Defaults to 60 seconds.
	pauseDur time.Duration

	// Done, when closed, is used to signal metricStatsCollector that is should stop collecting
	// statistics and the run function should return.
	done chan struct{}
}

// newMetricStatsCollector creates a new metricStatsCollector.
func newMetricStatsCollector(stats Stats, metricManager metric.Manager) metricStatsCollector {
	return metricStatsCollector{
		stats:         stats,
		metricManager: metricManager,
		pauseDur:      60 * time.Second,
		done:          make(chan struct{}),
	}
}

// run gathers statistics from package metric and outputs them as
func (c metricStatsCollector) run(ctx context.Context) {
	defer close(c.done)
	c.outputStats()

	// Gauges are a 'snapshot' rather than a histogram. Pausing for some interval
	// aims to get a 'recent' snapshot out before statsd flushes metrics.
	tick := time.NewTicker(c.pauseDur)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			c.outputStats()
		}
	}
}

func (c metricStatsCollector) outputStats() {
	if c.metricManager == nil {
		return
	}
	c.metricManager.GetRegistry(metric.PublishedMetrics).Range(func(key, value interface{}) bool {
		m := key.(metric.Measurement)
		switch value := value.(type) {
		case metric.Gauge:
			c.stats.NewTaggedStat(m.GetName(), GaugeType, m.GetTags()).
				Gauge(value.Value())
		case metric.Counter:
			c.stats.NewTaggedStat(m.GetName(), CountType, m.GetTags()).
				Count(int(value.Value()))
		case metric.MovingAverage:
			c.stats.NewTaggedStat(m.GetName(), GaugeType, m.GetTags()).
				Gauge(value.Value())
		}
		return true
	})
}
