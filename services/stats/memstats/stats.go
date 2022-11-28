package memstats

import (
	"context"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats"
)

var _ stats.Stats = (*Store)(nil)

var _ stats.Measurement = (*Measurement)(nil)

type Store struct {
	once  sync.Once
	mu    sync.Mutex
	byKey map[string]*Measurement

	Now func() time.Time
}

type Measurement struct {
	mu        sync.Mutex
	startTime time.Time
	now       func() time.Time

	Tags stats.Tags
	Name string
	Type string

	sum       float64
	values    []float64
	durations []time.Duration
}

func (m *Measurement) LastValue() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.values) == 0 {
		return 0
	}

	return m.values[len(m.values)-1]
}

func (m *Measurement) Values() []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	s := make([]float64, len(m.values))
	copy(s, m.values)

	return s
}

func (m *Measurement) LastDuration() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.durations) == 0 {
		return 0
	}

	return m.durations[len(m.durations)-1]
}

func (m *Measurement) Durations() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()

	s := make([]time.Duration, len(m.durations))
	copy(s, m.durations)

	return s
}

// Count implements stats.Measurement
func (m *Measurement) Count(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Type != stats.CountType {
		panic("operation Count not supported for measurement type:" + m.Type)
	}

	m.sum += float64(n)
	m.values = append(m.values, m.sum)
}

// Increment implements stats.Measurement
func (m *Measurement) Increment() {
	if m.Type != stats.CountType {
		panic("operation Increment not supported for measurement type:" + m.Type)
	}

	m.Count(1)
}

// Gauge implements stats.Measurement
func (m *Measurement) Gauge(value interface{}) {
	if m.Type != stats.GaugeType {
		panic("operation Gauge not supported for measurement type:" + m.Type)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.values = append(m.values, value.(float64))
}

// Observe implements stats.Measurement
func (m *Measurement) Observe(value float64) {
	if m.Type != stats.HistogramType {
		panic("operation Observe not supported for measurement type:" + m.Type)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.values = append(m.values, value)
}

// Start implements stats.Measurement
func (m *Measurement) Start() {
	if m.Type != stats.TimerType {
		panic("operation Start not supported for measurement type:" + m.Type)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.startTime = m.now()
}

// End implements stats.Measurement
func (m *Measurement) End() {
	if m.Type != stats.TimerType {
		panic("operation End not supported for measurement type:" + m.Type)
	}

	m.SendTiming(m.now().Sub(m.startTime))
}

// Since implements stats.Measurement
func (m *Measurement) Since(start time.Time) {
	if m.Type != stats.TimerType {
		panic("operation Since not supported for measurement type:" + m.Type)
	}

	m.SendTiming(m.now().Sub(start))
}

// SendTiming implements stats.Measurement
func (m *Measurement) SendTiming(duration time.Duration) {
	if m.Type != stats.TimerType {
		panic("operation SendTiming not supported for measurement type:" + m.Type)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.durations = append(m.durations, duration)
}

func (ms *Store) init() {
	ms.once.Do(func() {
		ms.byKey = make(map[string]*Measurement)

		if ms.Now == nil {
			ms.Now = time.Now
		}
	})
}

// NewStat implements stats.Stats
func (ms *Store) NewStat(name, statType string) (m stats.Measurement) {
	return ms.NewTaggedStat(name, statType, nil)
}

// NewTaggedStat implements stats.Stats
func (ms *Store) NewTaggedStat(name, statType string, tags stats.Tags) stats.Measurement {
	return ms.NewSampledTaggedStat(name, statType, tags)
}

// NewSampledTaggedStat implements stats.Stats
func (ms *Store) NewSampledTaggedStat(name, statType string, tags stats.Tags) stats.Measurement {
	ms.init()
	ms.mu.Lock()
	defer ms.mu.Unlock()

	m := &Measurement{
		Name: name,
		Tags: tags,
		Type: statType,

		now: ms.Now,
	}

	ms.byKey[name+tags.String()] = m
	return m
}

// Get the stored measurement with the name and tags.
// If no measurement is found, nil is returned.
func (ms *Store) Get(name string, tags stats.Tags) *Measurement {
	ms.init()
	ms.mu.Lock()
	defer ms.mu.Unlock()

	return ms.byKey[name+tags.String()]
}

// Start implements stats.Stats
func (*Store) Start(_ context.Context) {}

// Stop implements stats.Stats
func (*Store) Stop() {}
