//go:generate mockgen -destination=../../mocks/services/stats/mock_stats.go -package mock_stats github.com/rudderlabs/rudder-server/services/stats Stats,Measurement

package stats

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/asyncfloat64"
	"go.opentelemetry.io/otel/metric/instrument/syncfloat64"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	svcMetric "github.com/rudderlabs/rudder-server/services/metric"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

const (
	CountType     = "count"
	TimerType     = "timer"
	GaugeType     = "gauge"
	HistogramType = "histogram"
)

func init() {
	Default = newStats(config.Default, logger.Default, svcMetric.Instance)
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

	// Start starts the stats service and the collection of periodic stats.
	Start(ctx context.Context)

	// Stop stops the service and the collection of periodic stats.
	Stop()
}

// newStats create a new Stats instance using the provided config, logger factory and metric manager as dependencies
func newStats(config *config.Config, loggerFactory *logger.Factory, metricManager svcMetric.Manager) Stats {
	excludedTags := make(map[string]struct{})
	excludedTagsSlice := config.GetStringSlice("statsExcludedTags", nil)
	for _, tag := range excludedTagsSlice {
		excludedTags[tag] = struct{}{}
	}

	return &otelStats{
		statsEnabled:             config.GetBool("enableStats", true),
		runtimeEnabled:           config.GetBool("RuntimeStats.enabled", true),
		statsCollectionInterval:  config.GetInt64("RuntimeStats.statsCollectionInterval", 10),
		enableCPUStats:           config.GetBool("RuntimeStats.enableCPUStats", true),
		enableMemStats:           config.GetBool("RuntimeStats.enabledMemStats", true),
		enableGCStats:            config.GetBool("RuntimeStats.enableGCStats", true),
		excludedTags:             excludedTags,
		logger:                   loggerFactory.NewLogger().Child("stats"),
		metricManager:            metricManager,
		stopBackgroundCollection: func() {},
		meter:                    global.MeterProvider().Meter(""),
	}
}

// otelStats is an OTel-specific adapter that follows the Stats contract
type otelStats struct {
	statsEnabled            bool
	runtimeEnabled          bool
	statsCollectionInterval int64
	enableCPUStats          bool
	enableMemStats          bool
	enableGCStats           bool
	excludedTags            map[string]struct{}

	meter        metric.Meter
	counters     map[string]syncint64.Counter
	countersMu   sync.Mutex
	gauges       map[string]*gaugeWithTags
	gaugesMu     sync.Mutex
	timers       map[string]syncint64.Histogram
	timersMu     sync.Mutex
	histograms   map[string]syncfloat64.Histogram
	histogramsMu sync.Mutex

	runtimeStatsCollector    runtimeStatsCollector
	metricsStatsCollector    metricStatsCollector
	stopBackgroundCollection func()
	metricManager            svcMetric.Manager
	logger                   logger.Logger
}

func (s *otelStats) Start(_ context.Context) {
	var backgroundCollectionCtx context.Context
	backgroundCollectionCtx, s.stopBackgroundCollection = context.WithCancel(context.Background())

	gaugeFunc := func(key string, val uint64) {
		s.getMeasurement("runtime_"+key, GaugeType, nil).Gauge(val)
	}
	s.metricsStatsCollector = newMetricStatsCollector(s, s.metricManager)
	rruntime.Go(func() {
		s.metricsStatsCollector.run(backgroundCollectionCtx)
	})

	if s.runtimeEnabled {
		s.runtimeStatsCollector = newRuntimeStatsCollector(gaugeFunc)
		s.runtimeStatsCollector.PauseDur = time.Duration(s.statsCollectionInterval) * time.Second
		s.runtimeStatsCollector.EnableCPU = s.enableCPUStats
		s.runtimeStatsCollector.EnableMem = s.enableMemStats
		s.runtimeStatsCollector.EnableGC = s.enableGCStats
		rruntime.Go(func() {
			s.runtimeStatsCollector.run(backgroundCollectionCtx)
		})
	}
}

func (s *otelStats) Stop() {
	s.stopBackgroundCollection()
	<-s.metricsStatsCollector.done
	if s.runtimeEnabled {
		<-s.runtimeStatsCollector.done
	}
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

func (*otelStats) getNoOpMeasurement(statType string) Measurement {
	switch statType {
	case CountType:
		return &otelCounter{otelMeasurement: &otelMeasurement{disabled: true}}
	case GaugeType:
		return &otelGauge{otelMeasurement: &otelMeasurement{disabled: true}}
	case TimerType:
		return &otelTimer{otelMeasurement: &otelMeasurement{disabled: true}}
	case HistogramType:
		return &otelHistogram{otelMeasurement: &otelMeasurement{disabled: true}}
	}
	panic(fmt.Errorf("unsupported measurement type %s", statType))
}

func (s *otelStats) getMeasurement(name, statType string, tags Tags) Measurement {
	if !s.statsEnabled {
		return s.getNoOpMeasurement(statType)
	}

	if strings.Trim(name, " ") == "" {
		byteArr := make([]byte, 2048)
		n := runtime.Stack(byteArr, false)
		stackTrace := string(byteArr[:n])
		s.logger.Warnf("detected missing stat measurement name, using 'novalue':\n%v", stackTrace)
		name = "novalue"
	}

	// Clean up tags based on deployment type. No need to send workspace id tag for free tier customers.
	for k, v := range tags {
		if strings.Trim(k, " ") == "" {
			s.logger.Warnf("removing empty tag key with value %s for measurement %s", v, name)
			delete(tags, k)
		}
		if _, ok := s.excludedTags[k]; ok {
			delete(tags, k)
		}
	}
	if tags == nil {
		tags = make(Tags)
	}
	attributes := tags.otelAttributes()

	switch statType {
	case CountType:
		instr := buildInstrument(s.meter, name, s.counters, &s.countersMu)
		return &otelCounter{
			counter:         instr,
			otelMeasurement: &otelMeasurement{attributes: attributes},
		}
	case GaugeType:
		return s.getGauge(s.meter, name, attributes, tags.String())
	case TimerType:
		instr := buildInstrument(s.meter, name, s.timers, &s.timersMu, instrument.WithUnit(unit.Milliseconds))
		return &otelTimer{
			timer:           instr,
			otelMeasurement: &otelMeasurement{attributes: attributes},
		}
	case HistogramType:
		instr := buildInstrument(s.meter, name, s.histograms, &s.histogramsMu)
		return &otelHistogram{
			histogram:       instr,
			otelMeasurement: &otelMeasurement{attributes: attributes},
		}
	default:
		panic(fmt.Errorf("unsupported measurement type %s", statType))
	}
}

func (s *otelStats) getGauge(meter metric.Meter, name string, attributes []attribute.KeyValue, tagsKey string) *otelGauge {
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
						go func(v interface{}, attributes []attribute.KeyValue) {
							defer wg.Done()
							gwt.instrument.Observe(ctx, cast.ToFloat64(v), attributes...)
						}(v, measurement.attributes)
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

	tagsToValueKey := name + "@" + tagsKey
	if _, ok = gwt.tagsToValues[tagsToValueKey]; !ok {
		gwt.tagsToValues[tagsToValueKey] = &otelGauge{
			otelMeasurement: &otelMeasurement{attributes: attributes},
		}
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
