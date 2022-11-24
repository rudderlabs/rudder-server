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

	LastValue    float64
	Values       []float64
	LastDuration time.Duration
	Durations    []time.Duration
}

// Count implements stats.Measurement
func (m *Measurement) Count(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.Type != stats.CountType {
		panic("operation Count not supported for measurement type:" + m.Type)
	}

	m.LastValue += float64(n)
	m.Values = append(m.Values, m.LastValue)
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

	m.LastValue = value.(float64)
	m.Values = append(m.Values, m.LastValue)
}

// Observe implements stats.Measurement
func (m *Measurement) Observe(value float64) {
	if m.Type != stats.HistogramType {
		panic("operation Observe not supported for measurement type:" + m.Type)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.LastValue = value
	m.Values = append(m.Values, m.LastValue)
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

	m.LastDuration = duration
	m.Durations = append(m.Durations, m.LastDuration)
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
