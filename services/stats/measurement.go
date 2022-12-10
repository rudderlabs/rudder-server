package stats

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/instrument/syncfloat64"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
)

// Counter represents a counter metric
type Counter interface {
	Count(n int)
	Increment()
}

// Gauge represents a gauge metric
type Gauge interface {
	Gauge(value interface{})
}

// Histogram represents a histogram metric
type Histogram interface {
	Observe(value float64)
}

// Timer represents a timer metric
type Timer interface {
	SendTiming(duration time.Duration)
	Since(start time.Time)
	RecordDuration() func()
}

// Measurement provides all stat measurement functions
// TODO: the API should not return a union of measurement methods, but rather a distinct type for each measurement type
type Measurement interface {
	Counter
	Gauge
	Histogram
	Timer
}

// otelMeasurement is the statsd-specific implementation of Measurement
type otelMeasurement struct {
	statType   string
	disabled   bool
	attributes []attribute.KeyValue
}

// Count default behavior is to panic as not supported operation
func (m *otelMeasurement) Count(_ int) {
	panic(fmt.Errorf("operation Count not supported for measurement type:%s", m.statType))
}

// Increment default behavior is to panic as not supported operation
func (m *otelMeasurement) Increment() {
	panic(fmt.Errorf("operation Increment not supported for measurement type:%s", m.statType))
}

// Gauge default behavior is to panic as not supported operation
func (m *otelMeasurement) Gauge(_ interface{}) {
	panic(fmt.Errorf("operation Gauge not supported for measurement type:%s", m.statType))
}

// Observe default behavior is to panic as not supported operation
func (m *otelMeasurement) Observe(_ float64) {
	panic(fmt.Errorf("operation Observe not supported for measurement type:%s", m.statType))
}

// SendTiming default behavior is to panic as not supported operation
func (m *otelMeasurement) SendTiming(_ time.Duration) {
	panic(fmt.Errorf("operation SendTiming not supported for measurement type:%s", m.statType))
}

// Since default behavior is to panic as not supported operation
func (m *otelMeasurement) Since(_ time.Time) {
	panic(fmt.Errorf("operation Since not supported for measurement type:%s", m.statType))
}

// RecordDuration default behavior is to panic as not supported operation
func (m *otelMeasurement) RecordDuration() func() {
	panic(fmt.Errorf("operation RecordDuration not supported for measurement type:%s", m.statType))
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
	values  []interface{}
	valueMu sync.Mutex
}

// Gauge records an absolute value for this stat. Only applies to GaugeType stats
func (g *otelGauge) Gauge(value interface{}) {
	if g.disabled {
		return
	}
	g.valueMu.Lock()
	g.values = append(g.values, value)
	g.valueMu.Unlock()
}

func (g *otelGauge) getValues() []interface{} {
	if g.disabled {
		return nil
	}
	g.valueMu.Lock()
	v := g.values
	g.values = nil
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
