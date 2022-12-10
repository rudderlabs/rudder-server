//go:generate mockgen -destination=../../mocks/services/stats/mock_stats.go -package mock_stats github.com/rudderlabs/rudder-server/services/stats Stats,Measurement

package stats

import (
	"context"
	"fmt"
	"sync"

	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/asyncfloat64"
	"go.opentelemetry.io/otel/metric/instrument/syncfloat64"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"
)

const (
	CountType     = "count"
	TimerType     = "timer"
	GaugeType     = "gauge"
	HistogramType = "histogram"
)

func init() {
	Default = &otelStats{}
}

// Default is the default (singleton) Stats instance
var Default Stats

// Stats manages stat Measurements
type Stats interface {
	// NewStat creates a new Measurement with provided Name and Type
	NewStat(name, statType string) (m Measurement)

	// NewTaggedStat creates a new Measurement with provided Name, Type and Tags
	NewTaggedStat(name, statType string, tags Tags) Measurement

	// NewSampledTaggedStat creates a new Measurement with provided Name, Type and Tags
	// Deprecated: use NewTaggedStat instead
	NewSampledTaggedStat(name, statType string, tags Tags) Measurement
}

// otelStats is an OTel-specific adapter that follows the Stats contract
type otelStats struct {
	meter     metric.Meter
	meterOnce sync.Once

	counters   map[string]syncint64.Counter
	countersMu sync.Mutex

	gauges   map[string]*gaugeWithTags
	gaugesMu sync.Mutex

	timers   map[string]syncint64.Histogram
	timersMu sync.Mutex

	histograms   map[string]syncfloat64.Histogram
	histogramsMu sync.Mutex
}

// NewStat creates a new Measurement with provided Name and Type
func (s *otelStats) NewStat(name, statType string) (m Measurement) {
	return s.getMeasurement(name, statType, nil)
}

// NewTaggedStat creates a new Measurement with provided Name, Type and Tags
func (s *otelStats) NewTaggedStat(name, statType string, tags Tags) (m Measurement) {
	return s.getMeasurement(name, statType, tags)
}

// NewSampledTaggedStat creates a new Measurement with provided Name, Type and Tags
// Deprecated: use NewTaggedStat instead
func (s *otelStats) NewSampledTaggedStat(name, statType string, tags Tags) (m Measurement) {
	return s.NewTaggedStat(name, statType, tags)
}

func (s *otelStats) getMeasurement(name, statType string, tags Tags) Measurement {
	s.meterOnce.Do(func() {
		if s.meter == nil {
			s.meter = global.MeterProvider().Meter("")
		}
	})
	switch statType {
	case CountType:
		instr := buildInstrument(s.meter, name, s.counters, &s.countersMu)
		return &otelCounter{counter: instr, tags: tags}
	case GaugeType:
		return s.getGauge(s.meter, name, tags)
	case TimerType:
		instr := buildInstrument(s.meter, name, s.timers, &s.timersMu, instrument.WithUnit(unit.Milliseconds))
		return &otelTimer{timer: instr, tags: tags}
	case HistogramType:
		instr := buildInstrument(s.meter, name, s.histograms, &s.histogramsMu)
		return &otelHistogram{histogram: instr, tags: tags}
	}
	return nil
}

func (s *otelStats) getGauge(meter metric.Meter, name string, tags Tags) *otelGauge {
	var (
		ok  bool
		gwt *gaugeWithTags
	)

	s.gaugesMu.Lock()
	if s.gauges == nil {
		s.gauges = make(map[string]*gaugeWithTags)
	} else {
		gwt, ok = s.gauges[name]
	}
	if !ok {
		g, err := meter.AsyncFloat64().Gauge(name)
		if err != nil {
			panic(fmt.Errorf("failed to create gauge %s: %w", name, err))
		}
		gwt = &gaugeWithTags{
			instrument:   g,
			tagsToValues: make(map[string]*otelGauge),
		}
		err = meter.RegisterCallback([]instrument.Asynchronous{g}, func(ctx context.Context) {
			var wg sync.WaitGroup
			defer wg.Wait()

			gwt.tagsToValuesMu.Lock() // hold the lock only for the time necessary to spawn the goroutines
			for _, measurement := range gwt.tagsToValues {
				if values := measurement.getValues(); len(values) > 0 {
					wg.Add(len(values))
					for _, v := range values {
						go func(v interface{}, tags []attribute.KeyValue) {
							defer wg.Done()
							gwt.instrument.Observe(ctx, cast.ToFloat64(v), tags...)
						}(v, measurement.tags)
					}
				}
			}
			gwt.tagsToValuesMu.Unlock()
		})
		if err != nil {
			panic(fmt.Errorf("failed to register callback for gauge %s: %w", name, err))
		}
		s.gauges[name] = gwt
	}
	s.gaugesMu.Unlock()

	gwt.tagsToValuesMu.Lock()
	defer gwt.tagsToValuesMu.Unlock()

	tagsToValueKey := name + "@" + tags.String()
	if _, ok = gwt.tagsToValues[tagsToValueKey]; !ok {
		gwt.tagsToValues[tagsToValueKey] = &otelGauge{tags: tags.otelAttributes()}
	}
	return gwt.tagsToValues[tagsToValueKey]
}

func buildInstrument[T any](
	meter metric.Meter, name string, m map[string]T, mu *sync.Mutex, opts ...instrument.Option,
) T {
	var (
		ok    bool
		instr T
	)

	mu.Lock()
	defer mu.Unlock()

	if m == nil {
		m = make(map[string]T)
	} else {
		instr, ok = m[name]
	}

	if !ok {
		var err error
		var value interface{}
		switch any(m).(type) {
		case map[string]syncint64.Counter:
			value, err = meter.SyncInt64().Counter(name, opts...)
		case map[string]syncint64.Histogram:
			value, err = meter.SyncInt64().Histogram(name, opts...)
		case map[string]syncfloat64.Histogram:
			value, err = meter.SyncFloat64().Histogram(name, opts...)
		default:
			panic(fmt.Errorf("unknown instrument type %T", instr))
		}
		if err != nil {
			panic(fmt.Errorf("failed to create instrument %T(%s): %w", instr, name, err))
		}
		instr = value.(T)
		m[name] = instr
	}

	return instr
}

type gaugeWithTags struct {
	instrument     asyncfloat64.Gauge
	tagsToValues   map[string]*otelGauge
	tagsToValuesMu sync.Mutex
}
