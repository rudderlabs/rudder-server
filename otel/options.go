package otel

import (
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/aggregation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type (
	// Option allows to configure the OpenTelemetry initialization
	Option func(*config)
	// TracerProviderOption allows to configure the tracer provider
	TracerProviderOption func(providerConfig *tracerProviderConfig)
	// MeterProviderOption allows to configure the meter provider
	MeterProviderOption func(providerConfig *meterProviderConfig)
)

// WithLogger allows to set a logger to print debug information
func WithLogger(l logger) Option {
	return func(c *config) {
		c.logger = l
	}
}

// WithGRPCConnectParams allows to set GRPC connection parameters
func WithGRPCConnectParams(cp grpc.ConnectParams) Option {
	return func(c *config) {
		c.grpcConnectParams = &cp
	}
}

// WithInsecureGRPC allows to set the GRPC connection to be insecure
func WithInsecureGRPC() Option {
	return func(c *config) {
		// Note the use of insecure transport here. TLS is recommended in production.
		c.dialOpts = append(c.dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
func WithTracerProvider(opts ...TracerProviderOption) Option {
	return func(c *config) {
		c.tracerProviderConfig.enabled = true
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

// WithMeterProviderExportsInterval configures the intervening time between exports (if less than or equal to zero,
// 60 seconds is used)
func WithMeterProviderExportsInterval(interval time.Duration) MeterProviderOption {
	return func(c *meterProviderConfig) {
		c.exportsInterval = interval
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

// WithRuntimeStats lets you enable the runtime stats
func WithRuntimeStats(collectionInterval time.Duration) MeterProviderOption {
	return func(c *meterProviderConfig) {
		c.runtimeStats = true
		c.runtimeStatsCollectionInterval = runtime.DefaultMinimumReadMemStatsInterval
		if collectionInterval > 0 {
			c.runtimeStatsCollectionInterval = collectionInterval
		}
	}
}
