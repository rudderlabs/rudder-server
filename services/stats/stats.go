//go:generate mockgen -destination=../../mocks/services/stats/mock_stats.go -package mock_stats github.com/rudderlabs/rudder-server/services/stats Stats,Measurement
package stats

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric/global"

	"github.com/rudderlabs/rudder-server/config"
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
	// TODO once we drop statsd support we can do
	// Default = &otelStats{config: statsConfig{enabled: false}}
	Default = NewStats(config.Default, logger.Default, svcMetric.Instance)
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
		return &otelStats{
			config:                   statsConfig,
			stopBackgroundCollection: func() {},
			meter:                    global.MeterProvider().Meter(""),
			logger:                   loggerFactory.NewLogger().Child("stats"),
			otelConfig: otelStatsConfig{
				tracesEndpoint:        config.GetString("OpenTelemetry.traces.endpoint", ""),
				tracingSamplingRate:   config.GetFloat64("OpenTelemetry.traces.samplingRate", 0.1),
				metricsEndpoint:       config.GetString("OpenTelemetry.metrics.endpoint", ""),
				metricsExportInterval: config.GetDuration("OpenTelemetry.metrics.exportInterval", 5, time.Second),
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
