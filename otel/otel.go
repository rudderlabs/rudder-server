package otel

import (
	"context"
	"fmt"
	"net"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

// DefaultConnectParams returns the default gRPC connection parameters
var DefaultConnectParams = grpc.ConnectParams{
	Backoff: backoff.Config{
		BaseDelay:  5 * time.Second,
		Multiplier: 1.6,
		Jitter:     0.2,
		MaxDelay:   time.Minute,
	},
	MinConnectTimeout: 10 * time.Second,
}

type Manager struct {
	conn *grpc.ClientConn
}

// Setup simplifies the creation of tracer and meter providers with GRPC
func (m *Manager) Setup(
	ctx context.Context, res *resource.Resource, target string, opts ...Option,
) (
	tp *sdktrace.TracerProvider,
	mp *sdkmetric.MeterProvider,
	err error,
) {
	var c config
	for _, opt := range opts {
		opt(&c)
	}
	if c.grpcConnectParams == nil {
		c.grpcConnectParams = &DefaultConnectParams
	}

	if !c.tracerProviderConfig.enabled && !c.meterProviderConfig.enabled {
		return nil, nil, fmt.Errorf("no trace provider or meter provider to initialize")
	}

	if c.logger != nil {
		c.dialOpts = append(c.dialOpts, grpc.WithContextDialer(func(ctx context.Context, a string) (net.Conn, error) {
			network := "tcp"
			c.logger.Infof("OTel setup dialing %s/%s", network, a)
			return net.DialTimeout(network, a, c.grpcConnectParams.MinConnectTimeout)
		}))
	}

	m.conn, err = grpc.DialContext(ctx, target, append(c.dialOpts,
		grpc.WithBlock(),
		grpc.WithReturnConnectionError(),
	)...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	if c.tracerProviderConfig.enabled {
		traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(m.conn))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create trace exporter: %w", err)
		}

		bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithResource(res),
			sdktrace.WithSpanProcessor(bsp),
		)

		if c.tracerProviderConfig.global {
			otel.SetTracerProvider(tp)
		}
	}

	if c.meterProviderConfig.enabled {
		exp, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(m.conn))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create metric exporter: %w", err)
		}

		mp = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
				exp,
				sdkmetric.WithInterval(c.meterProviderConfig.exportsInterval),
			)),
			sdkmetric.WithView(c.meterProviderConfig.views...),
		)

		if c.meterProviderConfig.runtimeStats {
			err = runtime.Start(
				runtime.WithMeterProvider(mp),
				runtime.WithMinimumReadMemStatsInterval(c.meterProviderConfig.runtimeStatsCollectionInterval),
			)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to start runtime metrics: %w", err)
			}
		}

		if c.meterProviderConfig.global {
			global.SetMeterProvider(mp)
		}
	}

	if c.textMapPropagator != nil {
		otel.SetTextMapPropagator(c.textMapPropagator)
	}

	return tp, mp, nil
}

// Shutdown allows you to gracefully clean up after the OTel manager (e.g. close underlying gRPC connection)
func (m *Manager) Shutdown(ctx context.Context) (err error) {
	if m.conn != nil {
		done := make(chan struct{})
		go func() {
			err = m.conn.Close()
			close(done)
		}()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return err
		}
	}
	return ctx.Err()
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

type logger interface {
	Infof(format string, v ...interface{})
}

type config struct {
	dialOpts          []grpc.DialOption
	grpcConnectParams *grpc.ConnectParams

	tracerProviderConfig tracerProviderConfig
	meterProviderConfig  meterProviderConfig

	textMapPropagator propagation.TextMapPropagator

	logger logger
}

type tracerProviderConfig struct {
	enabled bool
	global  bool
}

type meterProviderConfig struct {
	enabled                        bool
	global                         bool
	exportsInterval                time.Duration
	views                          []sdkmetric.View
	runtimeStats                   bool
	runtimeStatsCollectionInterval time.Duration
}
