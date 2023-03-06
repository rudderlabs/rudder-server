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
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncfloat64"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"

	"github.com/rudderlabs/rudder-server/otel"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// otelStats is an OTel-specific adapter that follows the Stats contract
type otelStats struct {
	config     statsConfig
	otelConfig otelStatsConfig

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
	logger                   logger.Logger
}

func (s *otelStats) Start(ctx context.Context) error {
	if !s.config.enabled.Load() {
		return nil
	}

	// Starting OpenTelemetry setup
	var attrs []attribute.KeyValue
	if s.config.instanceName != "" {
		attrs = append(attrs, attribute.String("instanceName", s.config.instanceName))
	}
	if s.config.namespaceIdentifier != "" {
		attrs = append(attrs, attribute.String("namespace", s.config.namespaceIdentifier))
	}
	res, err := otel.NewResource(s.config.serviceName, s.config.instanceName, s.config.serviceVersion, attrs...)
	if err != nil {
		return fmt.Errorf("failed to create open telemetry resource: %w", err)
	}

	options := []otel.Option{otel.WithInsecure()} // @TODO: could make this configurable
	if s.otelConfig.tracesEndpoint != "" {
		options = append(options, otel.WithTracerProvider(
			s.otelConfig.tracesEndpoint,
			s.otelConfig.tracingSamplingRate,
		))
	}
	if s.otelConfig.metricsEndpoint != "" {
		options = append(options, otel.WithMeterProvider(
			s.otelConfig.metricsEndpoint,
			otel.WithMeterProviderExportsInterval(s.otelConfig.metricsExportInterval),
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
	s.metricsStatsCollector = newMetricStatsCollector(s, s.config.periodicStatsConfig.metricManager)
	rruntime.Go(func() {
		s.metricsStatsCollector.run(backgroundCollectionCtx)
	})

	if s.config.periodicStatsConfig.enabled {
		s.runtimeStatsCollector = newRuntimeStatsCollector(gaugeFunc)
		s.runtimeStatsCollector.PauseDur = time.Duration(s.config.periodicStatsConfig.statsCollectionInterval) * time.Second
		s.runtimeStatsCollector.EnableCPU = s.config.periodicStatsConfig.enableCPUStats
		s.runtimeStatsCollector.EnableMem = s.config.periodicStatsConfig.enableMemStats
		s.runtimeStatsCollector.EnableGC = s.config.periodicStatsConfig.enableGCStats
		rruntime.Go(func() {
			s.runtimeStatsCollector.run(backgroundCollectionCtx)
		})
	}

	s.logger.Infof("Stats started successfully in mode %q with metrics endpoint %q and traces endpoint %q",
		"OpenTelemetry", s.otelConfig.metricsEndpoint, s.otelConfig.tracesEndpoint,
	)

	return nil
}

func (s *otelStats) Stop() {
	if !s.config.enabled.Load() {
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
	if s.config.periodicStatsConfig.enabled && s.runtimeStatsCollector.done != nil {
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
	om := &otelMeasurement{
		genericMeasurement: genericMeasurement{statType: statType},
		disabled:           true,
	}
	switch statType {
	case CountType:
		return &otelCounter{otelMeasurement: om}
	case GaugeType:
		return &otelGauge{otelMeasurement: om}
	case TimerType:
		return &otelTimer{otelMeasurement: om}
	case HistogramType:
		return &otelHistogram{otelMeasurement: om}
	}
	panic(fmt.Errorf("unsupported measurement type %s", statType))
}

func (s *otelStats) getMeasurement(name, statType string, tags Tags) Measurement {
	if !s.config.enabled.Load() {
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

	om := &otelMeasurement{
		genericMeasurement: genericMeasurement{statType: statType},
		attributes:         tags.otelAttributes(),
	}

	switch statType {
	case CountType:
		instr := buildOTelInstrument(s.meter, name, s.counters, &s.countersMu)
		return &otelCounter{counter: instr, otelMeasurement: om}
	case GaugeType:
		return s.getGauge(s.meter, name, om.attributes, tags.String())
	case TimerType:
		instr := buildOTelInstrument(s.meter, name, s.timers, &s.timersMu, instrument.WithUnit(unit.Milliseconds))
		return &otelTimer{timer: instr, otelMeasurement: om}
	case HistogramType:
		instr := buildOTelInstrument(s.meter, name, s.histograms, &s.histogramsMu)
		return &otelHistogram{histogram: instr, otelMeasurement: om}
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
			genericMeasurement: genericMeasurement{statType: GaugeType},
			attributes:         attributes,
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

func buildOTelInstrument[T any](
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

type otelStatsConfig struct {
	tracesEndpoint        string
	tracingSamplingRate   float64
	metricsEndpoint       string
	metricsExportInterval time.Duration
}
