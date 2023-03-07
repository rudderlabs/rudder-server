package otel

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"golang.org/x/sync/errgroup"
)

// DefaultRetryConfig represents the default retry configuration
var DefaultRetryConfig = RetryConfig{
	Enabled:         true,
	InitialInterval: 5 * time.Second,
	MaxInterval:     30 * time.Second,
	MaxElapsedTime:  time.Minute,
}

type Manager struct {
	tp *sdktrace.TracerProvider
	mp *sdkmetric.MeterProvider
}

// Setup simplifies the creation of tracer and meter providers with GRPC
func (m *Manager) Setup(
	ctx context.Context, res *resource.Resource, opts ...Option,
) (
	*sdktrace.TracerProvider,
	*sdkmetric.MeterProvider,
	error,
) {
	var c config
	for _, opt := range opts {
		opt(&c)
	}
	if c.retryConfig == nil {
		c.retryConfig = &DefaultRetryConfig
	}

	if !c.tracerProviderConfig.enabled && !c.meterProviderConfig.enabled {
		return nil, nil, fmt.Errorf("no trace provider or meter provider to initialize")
	}

	if c.tracerProviderConfig.enabled {
		tracerProviderOptions := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpoint(c.tracesEndpoint),
			otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
				Enabled:         c.retryConfig.Enabled,
				InitialInterval: c.retryConfig.InitialInterval,
				MaxInterval:     c.retryConfig.MaxInterval,
				MaxElapsedTime:  c.retryConfig.MaxElapsedTime,
			}),
		}
		if c.withInsecure {
			tracerProviderOptions = append(tracerProviderOptions, otlptracegrpc.WithInsecure())
		}
		traceExporter, err := otlptracegrpc.New(ctx, tracerProviderOptions...)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create trace exporter: %w", err)
		}

		m.tp = sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.TraceIDRatioBased(c.tracerProviderConfig.samplingRate)),
			sdktrace.WithResource(res),
			sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(traceExporter)),
		)

		if c.tracerProviderConfig.global {
			otel.SetTracerProvider(m.tp)
		}
	}

	if c.meterProviderConfig.enabled {
		meterProviderOptions := []otlpmetricgrpc.Option{
			otlpmetricgrpc.WithEndpoint(c.metricsEndpoint),
			otlpmetricgrpc.WithRetry(otlpmetricgrpc.RetryConfig{
				Enabled:         c.retryConfig.Enabled,
				InitialInterval: c.retryConfig.InitialInterval,
				MaxInterval:     c.retryConfig.MaxInterval,
				MaxElapsedTime:  c.retryConfig.MaxElapsedTime,
			}),
		}
		if c.withInsecure {
			meterProviderOptions = append(meterProviderOptions, otlpmetricgrpc.WithInsecure())
		}
		exp, err := otlpmetricgrpc.New(ctx, meterProviderOptions...)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create metric exporter: %w", err)
		}

		m.mp = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
				exp,
				sdkmetric.WithInterval(c.meterProviderConfig.exportsInterval),
			)),
			sdkmetric.WithView(c.meterProviderConfig.views...),
		)

		if c.meterProviderConfig.global {
			global.SetMeterProvider(m.mp)
		}
	}

	if c.textMapPropagator != nil {
		otel.SetTextMapPropagator(c.textMapPropagator)
	}

	return m.tp, m.mp, nil
}

// Shutdown allows you to gracefully clean up after the OTel manager (e.g. close underlying gRPC connection)
func (m *Manager) Shutdown(ctx context.Context) error {
	var g errgroup.Group
	if m.tp != nil {
		g.Go(func() error {
			return m.tp.Shutdown(ctx)
		})
	}
	if m.mp != nil {
		g.Go(func() error {
			return m.mp.Shutdown(ctx)
		})
	}

	done := make(chan error)
	go func() {
		done <- g.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// NewResource allows the creation of an OpenTelemetry resource
// https://opentelemetry.io/docs/concepts/glossary/#resource
func NewResource(svcName, instanceID, svcVersion string, attrs ...attribute.KeyValue) (*resource.Resource, error) {
	defaultAttrs := []attribute.KeyValue{
		semconv.ServiceNameKey.String(svcName),
		semconv.ServiceVersionKey.String(svcVersion),
		semconv.ServiceInstanceIDKey.String(instanceID),
	}
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, append(defaultAttrs, attrs...)...),
	)
}

// RetryConfig defines configuration for retrying batches in case of export failure
// using an exponential backoff.
type RetryConfig struct {
	// Enabled indicates whether to not retry sending batches in case of
	// export failure.
	Enabled bool
	// InitialInterval the time to wait after the first failure before
	// retrying.
	InitialInterval time.Duration
	// MaxInterval is the upper bound on backoff interval. Once this value is
	// reached the delay between consecutive retries will always be
	// `MaxInterval`.
	MaxInterval time.Duration
	// MaxElapsedTime is the maximum amount of time (including retries) spent
	// trying to send a request/batch.  Once this value is reached, the data
	// is discarded.
	MaxElapsedTime time.Duration
}

type config struct {
	retryConfig  *RetryConfig
	withInsecure bool

	*sdktrace.TracerProvider
	*sdkmetric.MeterProvider

	tracesEndpoint       string
	tracerProviderConfig tracerProviderConfig
	metricsEndpoint      string
	meterProviderConfig  meterProviderConfig

	textMapPropagator propagation.TextMapPropagator
}

type tracerProviderConfig struct {
	enabled      bool
	global       bool
	samplingRate float64
}

type meterProviderConfig struct {
	enabled         bool
	global          bool
	exportsInterval time.Duration
	views           []sdkmetric.View
}
