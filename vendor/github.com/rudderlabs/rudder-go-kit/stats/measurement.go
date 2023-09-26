package stats

import (
	"fmt"
	"time"
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

type genericMeasurement struct {
	statType string
}

// Count default behavior is to panic as not supported operation
func (m *genericMeasurement) Count(_ int) {
	panic(fmt.Errorf("operation Count not supported for measurement type:%s", m.statType))
}

// Increment default behavior is to panic as not supported operation
func (m *genericMeasurement) Increment() {
	panic(fmt.Errorf("operation Increment not supported for measurement type:%s", m.statType))
}

// Gauge default behavior is to panic as not supported operation
func (m *genericMeasurement) Gauge(_ interface{}) {
	panic(fmt.Errorf("operation Gauge not supported for measurement type:%s", m.statType))
}

// Observe default behavior is to panic as not supported operation
func (m *genericMeasurement) Observe(_ float64) {
	panic(fmt.Errorf("operation Observe not supported for measurement type:%s", m.statType))
}

// Start default behavior is to panic as not supported operation
func (m *genericMeasurement) Start() {
	panic(fmt.Errorf("operation Start not supported for measurement type:%s", m.statType))
}

func (m *genericMeasurement) End() {
	panic(fmt.Errorf("operation End not supported for measurement type:%s", m.statType))
}

// SendTiming default behavior is to panic as not supported operation
func (m *genericMeasurement) SendTiming(_ time.Duration) {
	panic(fmt.Errorf("operation SendTiming not supported for measurement type:%s", m.statType))
}

// Since default behavior is to panic as not supported operation
func (m *genericMeasurement) Since(_ time.Time) {
	panic(fmt.Errorf("operation Since not supported for measurement type:%s", m.statType))
}

// RecordDuration default behavior is to panic as not supported operation
func (m *genericMeasurement) RecordDuration() func() {
	panic(fmt.Errorf("operation RecordDuration not supported for measurement type:%s", m.statType))
}
