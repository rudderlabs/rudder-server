package stats

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/internal/otel"
)

const (
	defaultMeterName = ""
)

// otelStats is an OTel-specific adapter that follows the Stats contract
type otelStats struct {
	config        statsConfig
	otelConfig    otelStatsConfig
	resourceAttrs map[string]struct{}

	meter        metric.Meter
	counters     map[string]metric.Int64Counter
	countersMu   sync.Mutex
	gauges       map[string]*otelGauge
	gaugesMu     sync.Mutex
	timers       map[string]metric.Float64Histogram
	timersMu     sync.Mutex
	histograms   map[string]metric.Float64Histogram
	histogramsMu sync.Mutex

	otelManager              otel.Manager
	runtimeStatsCollector    runtimeStatsCollector
	metricsStatsCollector    metricStatsCollector
	stopBackgroundCollection func()
	logger                   logger.Logger

	httpServer                 *http.Server
	httpServerShutdownComplete chan struct{}
	prometheusRegisterer       prometheus.Registerer
	prometheusGatherer         prometheus.Gatherer
}

func (s *otelStats) Start(ctx context.Context, goFactory GoRoutineFactory) error {
	if !s.config.enabled.Load() {
		return nil
	}

	// Starting OpenTelemetry setup
	var attrs []attribute.KeyValue
	s.resourceAttrs = make(map[string]struct{})
	if s.config.instanceName != "" {
		sanitized := sanitizeTagKey("instanceName")
		attrs = append(attrs, attribute.String(sanitized, s.config.instanceName))
		s.resourceAttrs[sanitized] = struct{}{}
	}
	if s.config.namespaceIdentifier != "" {
		sanitized := sanitizeTagKey("namespace")
		attrs = append(attrs, attribute.String(sanitized, s.config.namespaceIdentifier))
		s.resourceAttrs[sanitized] = struct{}{}
	}
	res, err := otel.NewResource(s.config.serviceName, s.config.serviceVersion, attrs...)
	if err != nil {
		return fmt.Errorf("failed to create open telemetry resource: %w", err)
	}

	options := []otel.Option{otel.WithInsecure(), otel.WithLogger(s.logger)}
	if s.otelConfig.tracesEndpoint != "" {
		options = append(options, otel.WithTracerProvider(
			s.otelConfig.tracesEndpoint,
			s.otelConfig.tracingSamplingRate,
		))
	}

	meterProviderOptions := []otel.MeterProviderOption{
		otel.WithMeterProviderExportsInterval(s.otelConfig.metricsExportInterval),
	}
	if len(s.config.defaultHistogramBuckets) > 0 {
		meterProviderOptions = append(meterProviderOptions,
			otel.WithDefaultHistogramBucketBoundaries(s.config.defaultHistogramBuckets),
		)
	}
	if len(s.config.histogramBuckets) > 0 {
		for histogramName, buckets := range s.config.histogramBuckets {
			meterProviderOptions = append(meterProviderOptions,
				otel.WithHistogramBucketBoundaries(histogramName, defaultMeterName, buckets),
			)
		}
	}
	if s.otelConfig.metricsEndpoint != "" {
		options = append(options, otel.WithMeterProvider(append(meterProviderOptions,
			otel.WithGRPCMeterProvider(s.otelConfig.metricsEndpoint),
		)...))
	} else if s.otelConfig.enablePrometheusExporter {
		options = append(options, otel.WithMeterProvider(append(meterProviderOptions,
			otel.WithPrometheusExporter(s.prometheusRegisterer),
		)...))
	} else {
		return fmt.Errorf("no metrics endpoint or prometheus exporter enabled")
	}
	_, mp, err := s.otelManager.Setup(ctx, res, options...)
	if err != nil {
		return fmt.Errorf("failed to setup open telemetry: %w", err)
	}

	s.meter = mp.Meter(defaultMeterName)
	if s.otelConfig.enablePrometheusExporter && s.otelConfig.prometheusMetricsPort > 0 {
		s.httpServerShutdownComplete = make(chan struct{})
		s.httpServer = &http.Server{
			Addr: fmt.Sprintf(":%d", s.otelConfig.prometheusMetricsPort),
			Handler: promhttp.InstrumentMetricHandler(
				s.prometheusRegisterer, promhttp.HandlerFor(s.prometheusGatherer, promhttp.HandlerOpts{
					ErrorLog: &prometheusLogger{l: s.logger},
				}),
			),
		}
		goFactory.Go(func() {
			defer close(s.httpServerShutdownComplete)
			if err := s.httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				s.logger.Fatalf("Prometheus exporter failed: %v", err)
			}
		})
	}

	// Starting background collection
	var backgroundCollectionCtx context.Context
	backgroundCollectionCtx, s.stopBackgroundCollection = context.WithCancel(context.Background())

	gaugeFunc := func(key string, val uint64) {
		s.getMeasurement("runtime_"+key, GaugeType, nil).Gauge(val)
	}
	s.metricsStatsCollector = newMetricStatsCollector(s, s.config.periodicStatsConfig.metricManager)
	goFactory.Go(func() {
		s.metricsStatsCollector.run(backgroundCollectionCtx)
	})

	if s.config.periodicStatsConfig.enabled {
		s.runtimeStatsCollector = newRuntimeStatsCollector(gaugeFunc)
		s.runtimeStatsCollector.PauseDur = time.Duration(s.config.periodicStatsConfig.statsCollectionInterval) * time.Second
		s.runtimeStatsCollector.EnableCPU = s.config.periodicStatsConfig.enableCPUStats
		s.runtimeStatsCollector.EnableMem = s.config.periodicStatsConfig.enableMemStats
		s.runtimeStatsCollector.EnableGC = s.config.periodicStatsConfig.enableGCStats
		goFactory.Go(func() {
			s.runtimeStatsCollector.run(backgroundCollectionCtx)
		})
	}

	if s.otelConfig.enablePrometheusExporter {
		s.logger.Infof("Stats started in Prometheus mode on :%d", s.otelConfig.prometheusMetricsPort)
	} else {
		s.logger.Infof("Stats started in OpenTelemetry mode with metrics endpoint %q and traces endpoint %q",
			s.otelConfig.metricsEndpoint, s.otelConfig.tracesEndpoint,
		)
	}

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

	if s.httpServer != nil && s.httpServerShutdownComplete != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			s.logger.Errorf("failed to shutdown prometheus exporter: %v", err)
		}
		<-s.httpServerShutdownComplete
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
	newTags := make(Tags)
	for k, v := range tags {
		if strings.Trim(k, " ") == "" {
			s.logger.Warnf("removing empty tag key with value %q for measurement %q", v, name)
			continue
		}
		if _, ok := s.config.excludedTags[k]; ok {
			continue
		}
		sanitizedKey := sanitizeTagKey(k)
		if _, ok := s.config.excludedTags[sanitizedKey]; ok {
			continue
		}
		if _, ok := s.resourceAttrs[sanitizedKey]; ok {
			s.logger.Warnf("removing tag %q for measurement %q since it is a resource attribute", k, name)
			continue
		}
		newTags[sanitizedKey] = v
	}

	om := &otelMeasurement{
		genericMeasurement: genericMeasurement{statType: statType},
		attributes:         newTags.otelAttributes(),
	}

	switch statType {
	case CountType:
		instr := buildOTelInstrument(s.meter, name, s.counters, &s.countersMu)
		return &otelCounter{counter: instr, otelMeasurement: om}
	case GaugeType:
		return s.getGauge(s.meter, name, om.attributes, newTags.String())
	case TimerType:
		instr := buildOTelInstrument(s.meter, name, s.timers, &s.timersMu)
		return &otelTimer{timer: instr, otelMeasurement: om}
	case HistogramType:
		instr := buildOTelInstrument(s.meter, name, s.histograms, &s.histogramsMu)
		return &otelHistogram{histogram: instr, otelMeasurement: om}
	default:
		panic(fmt.Errorf("unsupported measurement type %s", statType))
	}
}

func (s *otelStats) getGauge(
	meter metric.Meter, name string, attributes []attribute.KeyValue, tagsKey string,
) *otelGauge {
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
		g, err := meter.Float64ObservableGauge(name)
		if err != nil {
			panic(fmt.Errorf("failed to create gauge %s: %w", name, err))
		}
		og = &otelGauge{otelMeasurement: &otelMeasurement{
			genericMeasurement: genericMeasurement{statType: GaugeType},
			attributes:         attributes,
		}}
		_, err = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
			if value := og.getValue(); value != nil {
				o.ObserveFloat64(g, cast.ToFloat64(value), metric.WithAttributes(attributes...))
			}
			return nil
		}, g)
		if err != nil {
			panic(fmt.Errorf("failed to register callback for gauge %s: %w", name, err))
		}
		s.gauges[mapKey] = og
	}

	return og
}

func buildOTelInstrument[T any](
	meter metric.Meter, name string, m map[string]T, mu *sync.Mutex,
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
		case map[string]metric.Int64Counter:
			value, err = meter.Int64Counter(name)
		case map[string]metric.Float64Histogram:
			value, err = meter.Float64Histogram(name)
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
	tracesEndpoint           string
	tracingSamplingRate      float64
	metricsEndpoint          string
	metricsExportInterval    time.Duration
	enablePrometheusExporter bool
	prometheusMetricsPort    int
}

type prometheusLogger struct{ l logger.Logger }

func (p *prometheusLogger) Println(v ...interface{}) { p.l.Error(v...) }
