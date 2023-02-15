package stats

import (
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"gopkg.in/alexcesaro/statsd.v2"
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

// statsdMeasurement is the statsd-specific implementation of Measurement
type statsdMeasurement struct {
	conf     *statsdConfig
	name     string
	statType string
	client   *statsdClient
}

// skip returns true if the stat should be skipped (stats disabled or client not ready)
func (m *statsdMeasurement) skip() bool {
	return !m.conf.enabled || !m.client.ready()
}

// Count default behavior is to panic as not supported operation
func (m *statsdMeasurement) Count(_ int) {
	panic(fmt.Errorf("operation Count not supported for measurement type:%s", m.statType))
}

// Increment default behavior is to panic as not supported operation
func (m *statsdMeasurement) Increment() {
	panic(fmt.Errorf("operation Increment not supported for measurement type:%s", m.statType))
}

// Gauge default behavior is to panic as not supported operation
func (m *statsdMeasurement) Gauge(_ interface{}) {
	panic(fmt.Errorf("operation Gauge not supported for measurement type:%s", m.statType))
}

// Observe default behavior is to panic as not supported operation
func (m *statsdMeasurement) Observe(_ float64) {
	panic(fmt.Errorf("operation Observe not supported for measurement type:%s", m.statType))
}

// Start default behavior is to panic as not supported operation
func (m *statsdMeasurement) Start() {
	panic(fmt.Errorf("operation Start not supported for measurement type:%s", m.statType))
}

func (m *statsdMeasurement) End() {
	panic(fmt.Errorf("operation End not supported for measurement type:%s", m.statType))
}

// SendTiming default behavior is to panic as not supported operation
func (m *statsdMeasurement) SendTiming(_ time.Duration) {
	panic(fmt.Errorf("operation SendTiming not supported for measurement type:%s", m.statType))
}

// Since default behavior is to panic as not supported operation
func (m *statsdMeasurement) Since(_ time.Time) {
	panic(fmt.Errorf("operation Since not supported for measurement type:%s", m.statType))
}

// RecordDuration default behavior is to panic as not supported operation
func (m *statsdMeasurement) RecordDuration() func() {
	panic(fmt.Errorf("operation RecordDuration not supported for measurement type:%s", m.statType))
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

// newStatsdMeasurement creates a new measurement of the specific type
func newStatsdMeasurement(conf *statsdConfig, log logger.Logger, name, statType string, client *statsdClient) Measurement {
	if strings.Trim(name, " ") == "" {
		byteArr := make([]byte, 2048)
		n := runtime.Stack(byteArr, false)
		stackTrace := string(byteArr[:n])
		log.Warnf("detected missing stat measurement name, using 'novalue':\n%v", stackTrace)
		name = "novalue"
	}
	baseMeasurement := &statsdMeasurement{
		conf:     conf,
		name:     name,
		statType: statType,
		client:   client,
	}
	switch statType {
	case CountType:
		return &statsdCounter{baseMeasurement}
	case GaugeType:
		return &statsdGauge{baseMeasurement}
	case TimerType:
		return &statsdTimer{statsdMeasurement: baseMeasurement}
	case HistogramType:
		return &statsdHistogram{baseMeasurement}
	default:
		panic(fmt.Errorf("unsupported measurement type %s", statType))
	}
}
