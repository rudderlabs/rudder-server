package stats

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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
	counter metric.Int64Counter
}

func (c *otelCounter) Count(n int) {
	if !c.disabled {
		c.counter.Add(context.TODO(), int64(n), metric.WithAttributes(c.attributes...))
	}
}

// Increment increases the stat by 1. Is the Equivalent of Count(1). Only applies to CountType stats
func (c *otelCounter) Increment() {
	if !c.disabled {
		c.counter.Add(context.TODO(), 1, metric.WithAttributes(c.attributes...))
	}
}

// otelGauge represents a gauge stat
type otelGauge struct {
	*otelMeasurement
	value atomic.Value
}

// Gauge records an absolute value for this stat. Only applies to GaugeType stats
func (g *otelGauge) Gauge(value interface{}) {
	if g.disabled {
		return
	}
	g.value.Store(value)
}

func (g *otelGauge) getValue() interface{} {
	if g.disabled {
		return nil
	}
	return g.value.Load()
}

// otelTimer represents a timer stat
type otelTimer struct {
	*otelMeasurement
	now   func() time.Time
	timer metric.Float64Histogram
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
		t.timer.Record(context.TODO(), duration.Seconds(), metric.WithAttributes(t.attributes...))
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
	histogram metric.Float64Histogram
}

// Observe sends an observation
func (h *otelHistogram) Observe(value float64) {
	if !h.disabled {
		h.histogram.Record(context.TODO(), value, metric.WithAttributes(h.attributes...))
	}
}
