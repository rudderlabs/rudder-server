//go:generate mockgen -destination=../../mocks/services/stats/mock_stats.go -package mock_stats github.com/rudderlabs/rudder-server/services/stats Stats,Measurement

package stats

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncfloat64"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/otel"
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
	Default = &otelStats{config: statsConfig{
		enabled: false,
	}}
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
	Start(ctx context.Context) error

	// Stop stops the service and the collection of periodic stats.
	Stop()
}

// NewStats create a new Stats instance using the provided config, logger factory and metric manager as dependencies
func NewStats(
	config *config.Config, loggerFactory *logger.Factory, metricManager svcMetric.Manager, opts ...Option,
) Stats {
	excludedTags := make(map[string]struct{})
	excludedTagsSlice := config.GetStringSlice("statsExcludedTags", nil)
	for _, tag := range excludedTagsSlice {
		excludedTags[tag] = struct{}{}
	}

	statsConfig := statsConfig{
		excludedTags:            excludedTags,
		enabled:                 config.GetBool("enableStats", true),
		runtimeEnabled:          config.GetBool("RuntimeStats.enabled", true),
		statsCollectionInterval: config.GetInt64("RuntimeStats.statsCollectionInterval", 10),
		enableCPUStats:          config.GetBool("RuntimeStats.enableCPUStats", true),
		enableMemStats:          config.GetBool("RuntimeStats.enabledMemStats", true),
		enableGCStats:           config.GetBool("RuntimeStats.enableGCStats", true),
		instanceName:            config.GetString("INSTANCE_ID", ""),
		namespaceIdentifier:     os.Getenv("KUBE_NAMESPACE"),
		otelConfig: otelStatsConfig{
			tracesEndpoint:        config.GetString("OpenTelemetry.Traces.Endpoint", ""),
			tracingSamplingRate:   config.GetFloat64("OpenTelemetry.Traces.SamplingRate", 0.1),
			metricsEndpoint:       config.GetString("OpenTelemetry.Metrics.Endpoint", ""),
			metricsExportInterval: config.GetDuration("OpenTelemetry.Metrics.ExportInterval", 5, time.Second),
		},
	}
	for _, opt := range opts {
		opt(&statsConfig)
	}
	return &otelStats{
		config:                   statsConfig,
		stopBackgroundCollection: func() {},
		metricManager:            metricManager,
		meter:                    global.MeterProvider().Meter(""),
		logger:                   loggerFactory.NewLogger().Child("stats"),
	}
}

// otelStats is an OTel-specific adapter that follows the Stats contract
type otelStats struct {
	config statsConfig

	meter        metric.Meter
	counters     map[string]syncint64.Counter
	countersMu   sync.Mutex
	gauges       map[string]*otelGauge
	gaugesMu     sync.Mutex
	timers       map[string]syncint64.Histogram
	timersMu     sync.Mutex
	histograms   map[string]syncfloat64.Histogram
	histogramsMu sync.Mutex

	otelManager              otel.Manager
	runtimeStatsCollector    runtimeStatsCollector
	metricsStatsCollector    metricStatsCollector
	stopBackgroundCollection func()
	metricManager            svcMetric.Manager
	logger                   logger.Logger
}

func (s *otelStats) Start(ctx context.Context) error {
	if !s.config.enabled {
		return nil
	}

	// Starting OpenTelemetry setup
	attrs := []attribute.KeyValue{attribute.String("instanceName", s.config.instanceName)}
	if s.config.namespaceIdentifier != "" {
		attrs = append(attrs, attribute.String("namespace", s.config.namespaceIdentifier))
	}
	res, err := otel.NewResource(s.config.serviceName, s.config.instanceName, s.config.serviceVersion, attrs...)
	if err != nil {
		return fmt.Errorf("failed to create open telemetry resource: %w", err)
	}

	options := []otel.Option{otel.WithInsecure()} // @TODO: could make this configurable
	if s.config.otelConfig.tracesEndpoint != "" {
		options = append(options, otel.WithTracerProvider(
			s.config.otelConfig.tracesEndpoint,
			s.config.otelConfig.tracingSamplingRate,
		))
	}
	if s.config.otelConfig.metricsEndpoint != "" {
		options = append(options, otel.WithMeterProvider(
			s.config.otelConfig.metricsEndpoint,
			otel.WithMeterProviderExportsInterval(s.config.otelConfig.metricsExportInterval),
		))
	}
	_, mp, err := s.otelManager.Setup(ctx, res, options...)
	if err != nil {
		return fmt.Errorf("failed to setup open telemetry: %w", err)
	}

	s.meter = mp.Meter("")

	// Starting background collection
	var backgroundCollectionCtx context.Context
	backgroundCollectionCtx, s.stopBackgroundCollection = context.WithCancel(context.Background())

	gaugeFunc := func(key string, val uint64) {
		s.getMeasurement("runtime_"+key, GaugeType, nil).Gauge(val)
	}
	s.metricsStatsCollector = newMetricStatsCollector(s, s.metricManager)
	rruntime.Go(func() {
		s.metricsStatsCollector.run(backgroundCollectionCtx)
	})

	if s.config.runtimeEnabled {
		s.runtimeStatsCollector = newRuntimeStatsCollector(gaugeFunc)
		s.runtimeStatsCollector.PauseDur = time.Duration(s.config.statsCollectionInterval) * time.Second
		s.runtimeStatsCollector.EnableCPU = s.config.enableCPUStats
		s.runtimeStatsCollector.EnableMem = s.config.enableMemStats
		s.runtimeStatsCollector.EnableGC = s.config.enableGCStats
		rruntime.Go(func() {
			s.runtimeStatsCollector.run(backgroundCollectionCtx)
		})
	}

	return nil
}

func (s *otelStats) Stop() {
	if !s.config.enabled {
		return
	}

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	if err := s.otelManager.Shutdown(ctx); err != nil {
		s.logger.Errorf("failed to shutdown open telemetry: %v", err)
	}

	s.stopBackgroundCollection()
	if s.metricsStatsCollector.done != nil {
		<-s.metricsStatsCollector.done
	}
	if s.config.runtimeEnabled && s.runtimeStatsCollector.done != nil {
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
		return &otelCounter{otelMeasurement: &otelMeasurement{statType: CountType, disabled: true}}
	case GaugeType:
		return &otelGauge{otelMeasurement: &otelMeasurement{statType: GaugeType, disabled: true}}
	case TimerType:
		return &otelTimer{otelMeasurement: &otelMeasurement{statType: TimerType, disabled: true}}
	case HistogramType:
		return &otelHistogram{otelMeasurement: &otelMeasurement{statType: HistogramType, disabled: true}}
	}
	panic(fmt.Errorf("unsupported measurement type %s", statType))
}

func (s *otelStats) getMeasurement(name, statType string, tags Tags) Measurement {
	if !s.config.enabled {
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
		if _, ok := s.config.excludedTags[k]; ok {
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
			otelMeasurement: &otelMeasurement{statType: CountType, attributes: attributes},
		}
	case GaugeType:
		return s.getGauge(s.meter, name, attributes, tags.String())
	case TimerType:
		instr := buildInstrument(s.meter, name, s.timers, &s.timersMu, instrument.WithUnit(unit.Milliseconds))
		return &otelTimer{
			timer:           instr,
			otelMeasurement: &otelMeasurement{statType: TimerType, attributes: attributes},
		}
	case HistogramType:
		instr := buildInstrument(s.meter, name, s.histograms, &s.histogramsMu)
		return &otelHistogram{
			histogram:       instr,
			otelMeasurement: &otelMeasurement{statType: HistogramType, attributes: attributes},
		}
	default:
		panic(fmt.Errorf("unsupported measurement type %s", statType))
	}
}

func (s *otelStats) getGauge(meter metric.Meter, name string, attributes []attribute.KeyValue, tagsKey string) *otelGauge {
	var (
		ok     bool
		og     *otelGauge
		mapKey = name + "|" + tagsKey
	)

	s.gaugesMu.Lock()
	defer s.gaugesMu.Unlock()

	if s.gauges == nil {
		s.gauges = make(map[string]*otelGauge)
	} else {
		og, ok = s.gauges[mapKey]
	}

	if !ok {
		g, err := meter.AsyncFloat64().Gauge(name)
		if err != nil {
			panic(fmt.Errorf("failed to create gauge %s: %w", name, err))
		}
		og = &otelGauge{otelMeasurement: &otelMeasurement{
			statType:   GaugeType,
			attributes: attributes,
		}}
		err = meter.RegisterCallback([]instrument.Asynchronous{g}, func(ctx context.Context) {
			if value := og.getValue(); value != nil {
				g.Observe(ctx, cast.ToFloat64(value), og.attributes...)
			}
		})
		if err != nil {
			panic(fmt.Errorf("failed to register callback for gauge %s: %w", name, err))
		}
		s.gauges[mapKey] = og
	}

	return og
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
