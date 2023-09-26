package prometheus

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
)

type logger interface {
	Info(args ...interface{})
	Error(args ...interface{})
}

// config contains options for the exporter.
type config struct {
	registerer        prometheus.Registerer
	disableTargetInfo bool
	withoutUnits      bool
	aggregation       metric.AggregationSelector
	namespace         string
	logger            logger
}

// newConfig creates a validated config configured with options.
func newConfig(opts ...Option) config {
	cfg := config{}
	for _, opt := range opts {
		cfg = opt.apply(cfg)
	}

	if cfg.registerer == nil {
		cfg.registerer = prometheus.DefaultRegisterer
	}
	if cfg.logger == nil {
		cfg.logger = nopLogger{}
	}

	return cfg
}

func (cfg config) manualReaderOptions() []metric.ManualReaderOption {
	if cfg.aggregation != nil {
		return []metric.ManualReaderOption{
			metric.WithAggregationSelector(cfg.aggregation),
		}
	}
	return nil
}

// Option sets exporter option values.
type Option interface {
	apply(config) config
}

type optionFunc func(config) config

func (fn optionFunc) apply(cfg config) config {
	return fn(cfg)
}

// WithRegisterer configures which prometheus Registerer the Exporter will
// register with.  If no registerer is used the prometheus DefaultRegisterer is
// used.
func WithRegisterer(reg prometheus.Registerer) Option {
	return optionFunc(func(cfg config) config {
		cfg.registerer = reg
		return cfg
	})
}

// WithAggregationSelector configure the Aggregation Selector the exporter will
// use. If no AggregationSelector is provided the DefaultAggregationSelector is
// used.
func WithAggregationSelector(agg metric.AggregationSelector) Option {
	return optionFunc(func(cfg config) config {
		cfg.aggregation = agg
		return cfg
	})
}

// WithoutTargetInfo configures the Exporter to not export the resource target_info metric.
// If not specified, the Exporter will create a target_info metric containing
// the metrics' resource.Resource attributes.
func WithoutTargetInfo() Option {
	return optionFunc(func(cfg config) config {
		cfg.disableTargetInfo = true
		return cfg
	})
}

// WithoutUnits disables exporter's addition of unit suffixes to metric names,
// and will also prevent unit comments from being added in OpenMetrics once
// unit comments are supported.
//
// By default, metric names include a unit suffix to follow Prometheus naming
// conventions. For example, the counter metric request.duration, with unit
// milliseconds would become request_duration_milliseconds_total.
// With this option set, the name would instead be request_duration_total.
func WithoutUnits() Option {
	return optionFunc(func(cfg config) config {
		cfg.withoutUnits = true
		return cfg
	})
}

// WithNamespace configures the Exporter to prefix metric with the given namespace.
// Metadata metrics such as target_info and otel_scope_info are not prefixed since these
// have special behavior based on their name.
func WithNamespace(ns string) Option {
	return optionFunc(func(cfg config) config {
		ns = sanitizeName(ns)
		if !strings.HasSuffix(ns, "_") {
			// namespace and metric names should be separated with an underscore,
			// adds a trailing underscore if there is not one already.
			ns = ns + "_"
		}

		cfg.namespace = ns
		return cfg
	})
}

// WithLogger enables the logger
func WithLogger(l logger) Option {
	return optionFunc(func(cfg config) config {
		cfg.logger = l
		return cfg
	})
}

type nopLogger struct{}

func (nopLogger) Info(...interface{})  {}
func (nopLogger) Error(...interface{}) {}
