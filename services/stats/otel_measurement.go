package stats

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/instrument/syncfloat64"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
)

// otelMeasurement is the statsd-specific implementation of Measurement
type otelMeasurement struct {
	genericMeasurement
	disabled   bool
	attributes []attribute.KeyValue
}

// otelCounter represents a counter stat
type otelCounter struct {
	*otelMeasurement
	counter syncint64.Counter
}

func (c *otelCounter) Count(n int) {
	if !c.disabled {
		c.counter.Add(context.TODO(), int64(n), c.attributes...)
	}
}

// Increment increases the stat by 1. Is the Equivalent of Count(1). Only applies to CountType stats
func (c *otelCounter) Increment() {
	if !c.disabled {
		c.counter.Add(context.TODO(), 1, c.attributes...)
	}
}

// otelGauge represents a gauge stat
type otelGauge struct {
	*otelMeasurement
	value   interface{}
	valueMu sync.Mutex
}

// Gauge records an absolute value for this stat. Only applies to GaugeType stats
func (g *otelGauge) Gauge(value interface{}) {
	if g.disabled {
		return
	}
	g.valueMu.Lock()
	g.value = value
	g.valueMu.Unlock()
}

func (g *otelGauge) getValue() interface{} {
	if g.disabled {
		return nil
	}
	g.valueMu.Lock()
	v := g.value
	g.value = nil
	g.valueMu.Unlock()
	return v
}

// otelTimer represents a timer stat
type otelTimer struct {
	*otelMeasurement
	now   func() time.Time
	timer syncint64.Histogram
}

// Since sends the time elapsed since duration start. Only applies to TimerType stats
func (t *otelTimer) Since(start time.Time) {
	if !t.disabled {
		t.SendTiming(time.Since(start))
	}
}

// SendTiming sends a timing for this stat. Only applies to TimerType stats
func (t *otelTimer) SendTiming(duration time.Duration) {
	if !t.disabled {
		t.timer.Record(context.TODO(), duration.Milliseconds(), t.attributes...)
	}
}

// RecordDuration records the duration of time between
// the call to this function and the execution of the function it returns.
// Only applies to TimerType stats
func (t *otelTimer) RecordDuration() func() {
	if t.disabled {
		return func() {}
	}
	var start time.Time
	if t.now == nil {
		start = time.Now()
	} else {
		start = t.now()
	}
	return func() {
		t.Since(start)
	}
}

// otelHistogram represents a histogram stat
type otelHistogram struct {
	*otelMeasurement
	histogram syncfloat64.Histogram
}

// Observe sends an observation
func (h *otelHistogram) Observe(value float64) {
	if !h.disabled {
		h.histogram.Record(context.TODO(), value, h.attributes...)
	}
}
