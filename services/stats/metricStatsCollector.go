package stats

import (
	"time"

	"github.com/rudderlabs/rudder-server/services/metric"
)

// metricStatsCollector implements the periodic grabbing of informational data from the
// metric package and outputting the values as stats
type metricStatsCollector struct {
	// PauseDur represents the interval in between each set of stats output.
	// Defaults to 60 seconds.
	pauseDur time.Duration

	// Done, when closed, is used to signal metricStatsCollector that is should stop collecting
	// statistics and the run function should return.
	done chan struct{}
}

// newMetricStatsCollector creates a new metricStatsCollector.
func newMetricStatsCollector() metricStatsCollector {
	return metricStatsCollector{
		pauseDur: 60 * time.Second,
		done:     make(chan struct{}),
	}
}

// run gathers statistics from package metric and outputs them as
func (c metricStatsCollector) run() {
	c.outputStats()

	// Gauges are a 'snapshot' rather than a histogram. Pausing for some interval
	// aims to get a 'recent' snapshot out before statsd flushes metrics.
	tick := time.NewTicker(c.pauseDur)
	defer tick.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-tick.C:
			c.outputStats()
		}
	}
}

func (c metricStatsCollector) outputStats() {
	metric.GetManager().GetRegistry(metric.PUBLISHED_METRICS).Range(func(key, value interface{}) bool {
		m := key.(metric.Measurement)
		switch value.(type) {
		case metric.Gauge:
			NewTaggedStat(m.GetName(), GaugeType, Tags(m.GetTags())).
				Gauge(value.(metric.Gauge).Value())
		case metric.Counter:
			NewTaggedStat(m.GetName(), CountType, Tags(m.GetTags())).
				Count(int(value.(metric.Counter).Value()))
		case metric.MovingAverage:
			NewTaggedStat(m.GetName(), GaugeType, Tags(m.GetTags())).
				Gauge(value.(metric.MovingAverage).Value())
		}
		return true
	})
}
