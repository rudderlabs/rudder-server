package stats

import (
	"time"

	"gopkg.in/alexcesaro/statsd.v2"
)

// statsdMeasurement is the statsd-specific implementation of Measurement
type statsdMeasurement struct {
	genericMeasurement
	enabled bool
	name    string
	client  *statsdClient
}

// skip returns true if the stat should be skipped (stats disabled or client not ready)
func (m *statsdMeasurement) skip() bool {
	return !m.enabled || !m.client.ready()
}

// statsdCounter represents a counter stat
type statsdCounter struct {
	*statsdMeasurement
}

func (c *statsdCounter) Count(n int) {
	if c.skip() {
		return
	}
	c.client.statsd.Count(c.name, n)
}

// Increment increases the stat by 1. Is the Equivalent of Count(1). Only applies to CountType stats
func (c *statsdCounter) Increment() {
	if c.skip() {
		return
	}
	c.client.statsd.Increment(c.name)
}

// statsdGauge represents a gauge stat
type statsdGauge struct {
	*statsdMeasurement
}

// Gauge records an absolute value for this stat. Only applies to GaugeType stats
func (g *statsdGauge) Gauge(value interface{}) {
	if g.skip() {
		return
	}
	g.client.statsd.Gauge(g.name, value)
}

// statsdTimer represents a timer stat
type statsdTimer struct {
	*statsdMeasurement
	timing *statsd.Timing
}

// Start starts a new timing for this stat. Only applies to TimerType stats
// Deprecated: Use concurrent safe SendTiming() instead
func (t *statsdTimer) Start() {
	if t.skip() {
		return
	}
	timing := t.client.statsd.NewTiming()
	t.timing = &timing
}

// End send the time elapsed since the Start()  call of this stat. Only applies to TimerType stats
// Deprecated: Use concurrent safe SendTiming() instead
func (t *statsdTimer) End() {
	if t.skip() || t.timing == nil {
		return
	}
	t.timing.Send(t.name)
}

// Since sends the time elapsed since duration start. Only applies to TimerType stats
func (t *statsdTimer) Since(start time.Time) {
	t.SendTiming(time.Since(start))
}

// SendTiming sends a timing for this stat. Only applies to TimerType stats
func (t *statsdTimer) SendTiming(duration time.Duration) {
	if t.skip() {
		return
	}
	t.client.statsd.Timing(t.name, int(duration/time.Millisecond))
}

// RecordDuration records the duration of time between
// the call to this function and the execution of the function it returns.
// Only applies to TimerType stats
func (t *statsdTimer) RecordDuration() func() {
	start := time.Now()
	return func() {
		t.Since(start)
	}
}

// statsdHistogram represents a histogram stat
type statsdHistogram struct {
	*statsdMeasurement
}

// Observe sends an observation
func (h *statsdHistogram) Observe(value float64) {
	if h.skip() {
		return
	}
	h.client.statsd.Histogram(h.name, value)
}
