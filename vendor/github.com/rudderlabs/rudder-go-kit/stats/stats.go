//go:generate mockgen -destination=mock_stats/mock_stats.go -package mock_stats github.com/rudderlabs/rudder-go-kit/stats Stats,Measurement
package stats

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	svcMetric "github.com/rudderlabs/rudder-go-kit/stats/metric"
)

const (
	CountType     = "count"
	TimerType     = "timer"
	GaugeType     = "gauge"
	HistogramType = "histogram"
)

func init() {
	// TODO once we drop statsd support we can do
	// Default = &otelStats{config: statsConfig{enabled: false}}
	Default = NewStats(config.Default, logger.Default, svcMetric.Instance)
}

// Default is the default (singleton) Stats instance
var Default Stats

type GoRoutineFactory interface {
	Go(function func())
}

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
	Start(ctx context.Context, goFactory GoRoutineFactory) error

	// Stop stops the service and the collection of periodic stats.
	Stop()
}

type loggerFactory interface {
	NewLogger() logger.Logger
}

// NewStats create a new Stats instance using the provided config, logger factory and metric manager as dependencies
func NewStats(
	config *config.Config, loggerFactory loggerFactory, metricManager svcMetric.Manager, opts ...Option,
) Stats {
	excludedTags := make(map[string]struct{})
	excludedTagsSlice := config.GetStringSlice("statsExcludedTags", nil)
	for _, tag := range excludedTagsSlice {
		excludedTags[tag] = struct{}{}
	}

	enabled := atomic.Bool{}
	enabled.Store(config.GetBool("enableStats", true))
	statsConfig := statsConfig{
		excludedTags:        excludedTags,
		enabled:             &enabled,
		instanceName:        config.GetString("INSTANCE_ID", ""),
		namespaceIdentifier: os.Getenv("KUBE_NAMESPACE"),
		periodicStatsConfig: periodicStatsConfig{
			enabled:                 config.GetBool("RuntimeStats.enabled", true),
			statsCollectionInterval: config.GetInt64("RuntimeStats.statsCollectionInterval", 10),
			enableCPUStats:          config.GetBool("RuntimeStats.enableCPUStats", true),
			enableMemStats:          config.GetBool("RuntimeStats.enabledMemStats", true),
			enableGCStats:           config.GetBool("RuntimeStats.enableGCStats", true),
			metricManager:           metricManager,
		},
	}
	for _, opt := range opts {
		opt(&statsConfig)
	}

	if config.GetBool("OpenTelemetry.enabled", false) {
		registerer := prometheus.DefaultRegisterer
		gatherer := prometheus.DefaultGatherer
		if statsConfig.prometheusRegisterer != nil {
			registerer = statsConfig.prometheusRegisterer
		}
		if statsConfig.prometheusGatherer != nil {
			gatherer = statsConfig.prometheusGatherer
		}
		return &otelStats{
			config:                   statsConfig,
			stopBackgroundCollection: func() {},
			meter:                    otel.GetMeterProvider().Meter(defaultMeterName),
			logger:                   loggerFactory.NewLogger().Child("stats"),
			prometheusRegisterer:     registerer,
			prometheusGatherer:       gatherer,
			otelConfig: otelStatsConfig{
				tracesEndpoint:           config.GetString("OpenTelemetry.traces.endpoint", ""),
				tracingSamplingRate:      config.GetFloat64("OpenTelemetry.traces.samplingRate", 0.1),
				metricsEndpoint:          config.GetString("OpenTelemetry.metrics.endpoint", ""),
				metricsExportInterval:    config.GetDuration("OpenTelemetry.metrics.exportInterval", 5, time.Second),
				enablePrometheusExporter: config.GetBool("OpenTelemetry.metrics.prometheus.enabled", false),
				prometheusMetricsPort:    config.GetInt("OpenTelemetry.metrics.prometheus.port", 0),
			},
		}
	}

	backgroundCollectionCtx, backgroundCollectionCancel := context.WithCancel(context.Background())

	return &statsdStats{
		config:                     statsConfig,
		logger:                     loggerFactory.NewLogger().Child("stats"),
		backgroundCollectionCtx:    backgroundCollectionCtx,
		backgroundCollectionCancel: backgroundCollectionCancel,
		statsdConfig: statsdConfig{
			tagsFormat:          config.GetString("statsTagsFormat", "influxdb"),
			statsdServerURL:     config.GetString("STATSD_SERVER_URL", "localhost:8125"),
			samplingRate:        float32(config.GetFloat64("statsSamplingRate", 1)),
			instanceName:        statsConfig.instanceName,
			namespaceIdentifier: statsConfig.namespaceIdentifier,
		},
		state: &statsdState{
			client:         &statsdClient{},
			clients:        make(map[string]*statsdClient),
			pendingClients: make(map[string]*statsdClient),
		},
	}
}

var DefaultGoRoutineFactory = defaultGoRoutineFactory{}

type defaultGoRoutineFactory struct{}

func (defaultGoRoutineFactory) Go(function func()) {
	go function()
}

func sanitizeTagKey(key string) string {
	return strings.Map(sanitizeRune, key)
}

// This function has been copied from the prometheus exporter.
// Thus changes done only here might not always produce the desired result when exporting to prometheus
// unless the prometheus exporter is also updated.
// The rationale behind the duplication is that this function is used across all our Stats modes (statsd, prom, otel...)
// and the one in the prometheus exporter is still used to sanitize some attributes set on a Resource level from
// the OpenTelemetry client itself or 3rd parties.
// Alternatively we could further customise the prometheus exporter and make it use the same function (this one).
func sanitizeRune(r rune) rune {
	if unicode.IsLetter(r) || unicode.IsDigit(r) || r == ':' || r == '_' {
		return r
	}
	return '_'
}
