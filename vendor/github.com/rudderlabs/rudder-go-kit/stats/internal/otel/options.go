package otel

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/aggregation"
)

type (
	// Option allows to configure the OpenTelemetry initialization
	Option func(*config)
	// TracerProviderOption allows to configure the tracer provider
	TracerProviderOption func(providerConfig *tracerProviderConfig)
	// MeterProviderOption allows to configure the meter provider
	MeterProviderOption func(providerConfig *meterProviderConfig)
)

// WithRetryConfig allows to set the retry configuration
func WithRetryConfig(rc RetryConfig) Option {
	return func(c *config) {
		c.retryConfig = &rc
	}
}

// WithInsecure allows to set the GRPC connection to be insecure
func WithInsecure() Option {
	return func(c *config) {
		// Note the use of insecure transport here. TLS is recommended in production.
		c.withInsecure = true
	}
}

// WithTextMapPropagator allows to set the text map propagator
// e.g. propagation.TraceContext{}
func WithTextMapPropagator(tmp propagation.TextMapPropagator) Option {
	return func(c *config) {
		c.textMapPropagator = tmp
	}
}

// WithTracerProvider allows to set the tracer provider and specify if it should be the global one
// It is also possible to configure the sampling rate:
// samplingRate >= 1 will always sample.
// samplingRate < 0 is treated as zero.
func WithTracerProvider(endpoint string, samplingRate float64, opts ...TracerProviderOption) Option {
	return func(c *config) {
		c.tracesEndpoint = endpoint
		c.tracerProviderConfig.enabled = true
		c.tracerProviderConfig.samplingRate = samplingRate
		for _, opt := range opts {
			opt(&c.tracerProviderConfig)
		}
	}
}

// WithGlobalTracerProvider allows to set the tracer provider as the global one
func WithGlobalTracerProvider() TracerProviderOption {
	return func(c *tracerProviderConfig) {
		c.global = true
	}
}

// WithMeterProvider allows to set the meter provider and specify if it should be the global one plus other options.
func WithMeterProvider(opts ...MeterProviderOption) Option {
	return func(c *config) {
		c.meterProviderConfig.enabled = true
		for _, opt := range opts {
			opt(&c.meterProviderConfig)
		}
	}
}

// WithGlobalMeterProvider allows to set the meter provider as the global one
func WithGlobalMeterProvider() MeterProviderOption {
	return func(c *meterProviderConfig) {
		c.global = true
	}
}

// WithGRPCMeterProvider allows to set the meter provider to use GRPC
func WithGRPCMeterProvider(grpcEndpoint string) MeterProviderOption {
	return func(c *meterProviderConfig) {
		c.grpcEndpoint = &grpcEndpoint
	}
}

// WithMeterProviderExportsInterval configures the intervening time between exports (if less than or equal to zero,
// 60 seconds is used)
func WithMeterProviderExportsInterval(interval time.Duration) MeterProviderOption {
	return func(c *meterProviderConfig) {
		c.exportsInterval = interval
	}
}

// WithPrometheusExporter allows to enable the Prometheus exporter
func WithPrometheusExporter(registerer prometheus.Registerer) MeterProviderOption {
	return func(c *meterProviderConfig) {
		c.prometheusRegisterer = registerer
	}
}

// WithDefaultHistogramBucketBoundaries lets you overwrite the default buckets for all histograms.
func WithDefaultHistogramBucketBoundaries(boundaries []float64) MeterProviderOption {
	return func(c *meterProviderConfig) {
		c.defaultAggregationSelector = func(ik sdkmetric.InstrumentKind) aggregation.Aggregation {
			switch ik {
			case sdkmetric.InstrumentKindCounter, sdkmetric.InstrumentKindUpDownCounter,
				sdkmetric.InstrumentKindObservableCounter, sdkmetric.InstrumentKindObservableUpDownCounter:
				return aggregation.Sum{}
			case sdkmetric.InstrumentKindObservableGauge:
				return aggregation.LastValue{}
			case sdkmetric.InstrumentKindHistogram:
				return aggregation.ExplicitBucketHistogram{
					Boundaries: boundaries,
				}
			}
			panic("unknown instrument kind")
		}
	}
}

// WithHistogramBucketBoundaries allows the creation of a view to overwrite the default buckets of a given histogram.
// meterName is optional.
func WithHistogramBucketBoundaries(instrumentName, meterName string, boundaries []float64) MeterProviderOption {
	var scope instrumentation.Scope
	if meterName != "" {
		scope.Name = meterName
	}
	newView := sdkmetric.NewView(
		sdkmetric.Instrument{
			Name:  instrumentName,
			Scope: scope,
			Kind:  sdkmetric.InstrumentKindHistogram,
		},
		sdkmetric.Stream{
			Aggregation: aggregation.ExplicitBucketHistogram{
				Boundaries: boundaries,
			},
		},
	)
	return func(c *meterProviderConfig) {
		c.views = append(c.views, newView)
	}
}

// WithLogger allows to set the logger
func WithLogger(l logger) Option {
	return func(c *config) { c.logger = l }
}
