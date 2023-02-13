package otel

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	promClient "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/global"

	"github.com/rudderlabs/rudder-server/testhelper"
	dt "github.com/rudderlabs/rudder-server/testhelper/docker"
	otelTest "github.com/rudderlabs/rudder-server/testhelper/stats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

const (
	metricsPort = "8889"
)

// see https://opentelemetry.io/docs/collector/getting-started/
func TestCollector(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)

	container, grpcEndpoint := otelTest.StartOTelCollector(t, metricsPort,
		filepath.Join(cwd, "testdata", "otel-collector-config.yaml"),
	)

	ctx := context.Background()
	res, err := NewResource(t.Name(), "my-instance-id", "1.0.0")
	require.NoError(t, err)
	var om Manager
	tp, mp, err := om.Setup(ctx, res,
		WithInsecure(),
		WithTracerProvider(grpcEndpoint, 1.0),
		WithMeterProvider(grpcEndpoint,
			WithMeterProviderExportsInterval(100*time.Millisecond),
			WithHistogramBucketBoundaries("baz", "some-test", []float64{10, 20, 30}),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, om.Shutdown(context.Background())) })
	require.NotEqual(t, tp, otel.GetTracerProvider())
	require.NotEqual(t, mp, global.MeterProvider())

	m := mp.Meter("some-test")
	// foo counter
	counter, err := m.SyncInt64().Counter("foo")
	require.NoError(t, err)
	counter.Add(ctx, 1, attribute.String("hello", "world"))
	// bar counter
	counter, err = m.SyncInt64().Counter("bar")
	require.NoError(t, err)
	counter.Add(ctx, 5)
	// baz histogram
	h, err := m.SyncInt64().Histogram("baz")
	require.NoError(t, err)
	h.Record(ctx, 20, attribute.String("a", "b"))

	var (
		resp            *http.Response
		metrics         map[string]*promClient.MetricFamily
		metricsEndpoint = fmt.Sprintf("http://localhost:%d/metrics", dt.GetHostPort(t, metricsPort, container))
	)
	require.Eventuallyf(t, func() bool {
		resp, err = http.Get(metricsEndpoint)
		if err != nil {
			return false
		}
		defer func() { httputil.CloseResponse(resp) }()
		metrics, err = otelTest.ParsePrometheusMetrics(resp.Body)
		if err != nil {
			return false
		}
		if _, ok := metrics["foo"]; !ok {
			return false
		}
		if _, ok := metrics["bar"]; !ok {
			return false
		}
		if _, ok := metrics["baz"]; !ok {
			return false
		}
		return true
	}, 5*time.Second, 100*time.Millisecond, "err: %v, metrics: %+v", err, metrics)

	require.EqualValues(t, ptr("foo"), metrics["foo"].Name)
	require.EqualValues(t, ptr(promClient.MetricType_COUNTER), metrics["foo"].Type)
	require.Len(t, metrics["foo"].Metric, 1)
	require.EqualValues(t, &promClient.Counter{Value: ptr(1.0)}, metrics["foo"].Metric[0].Counter)
	require.ElementsMatch(t, []*promClient.LabelPair{
		// the label1=value1 is coming from the otel-collector-config.yaml (see const_labels)
		{Name: ptr("label1"), Value: ptr("value1")},
		{Name: ptr("hello"), Value: ptr("world")},
		{Name: ptr("job"), Value: ptr("TestCollector")},
		{Name: ptr("instance"), Value: ptr("my-instance-id")},
	}, metrics["foo"].Metric[0].Label)

	require.EqualValues(t, ptr("bar"), metrics["bar"].Name)
	require.EqualValues(t, ptr(promClient.MetricType_COUNTER), metrics["bar"].Type)
	require.Len(t, metrics["bar"].Metric, 1)
	require.EqualValues(t, &promClient.Counter{Value: ptr(5.0)}, metrics["bar"].Metric[0].Counter)
	require.ElementsMatch(t, []*promClient.LabelPair{
		// the label1=value1 is coming from the otel-collector-config.yaml (see const_labels)
		{Name: ptr("label1"), Value: ptr("value1")},
		{Name: ptr("job"), Value: ptr("TestCollector")},
		{Name: ptr("instance"), Value: ptr("my-instance-id")},
	}, metrics["bar"].Metric[0].Label)

	require.EqualValues(t, ptr("baz"), metrics["baz"].Name)
	require.EqualValues(t, ptr(promClient.MetricType_HISTOGRAM), metrics["baz"].Type)
	require.Len(t, metrics["baz"].Metric, 1)
	require.EqualValues(t, ptr(uint64(1)), metrics["baz"].Metric[0].Histogram.SampleCount)
	require.EqualValues(t, ptr(20.0), metrics["baz"].Metric[0].Histogram.SampleSum)
	require.ElementsMatch(t, []*promClient.Bucket{
		{CumulativeCount: ptr(uint64(0)), UpperBound: ptr(10.0)},
		{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(20.0)},
		{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(30.0)},
		{CumulativeCount: ptr(uint64(1)), UpperBound: ptr(math.Inf(0))},
	}, metrics["baz"].Metric[0].Histogram.Bucket)
	require.ElementsMatch(t, []*promClient.LabelPair{
		// the label1=value1 is coming from the otel-collector-config.yaml (see const_labels)
		{Name: ptr("label1"), Value: ptr("value1")},
		{Name: ptr("a"), Value: ptr("b")},
		{Name: ptr("job"), Value: ptr("TestCollector")},
		{Name: ptr("instance"), Value: ptr("my-instance-id")},
	}, metrics["baz"].Metric[0].Label)
}

func TestCollectorGlobals(t *testing.T) {
	grpcPort, err := testhelper.GetFreePort()
	require.NoError(t, err)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	collector, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "otel/opentelemetry-collector",
		Tag:        "0.67.0",
		PortBindings: map[docker.Port][]docker.PortBinding{
			"4317/tcp": {{HostPort: strconv.Itoa(grpcPort)}},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pool.Purge(collector); err != nil {
			t.Logf("Could not purge resource: %v", err)
		}
	})

	var (
		om       Manager
		ctx      = context.Background()
		endpoint = fmt.Sprintf("localhost:%d", grpcPort)
	)
	res, err := NewResource(t.Name(), "my-instance-id", "1.0.0")
	require.NoError(t, err)
	tp, mp, err := om.Setup(ctx, res,
		WithInsecure(),
		WithTracerProvider(endpoint, 1.0, WithGlobalTracerProvider()),
		WithMeterProvider(endpoint, WithGlobalMeterProvider()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, om.Shutdown(context.Background())) })
	require.Equal(t, tp, otel.GetTracerProvider())
	require.Equal(t, mp, global.MeterProvider())
}

func TestNonBlockingConnection(t *testing.T) {
	grpcPort, err := testhelper.GetFreePort()
	require.NoError(t, err)

	res, err := NewResource(t.Name(), "my-instance-id", "1.0.0")
	require.NoError(t, err)

	var (
		om       Manager
		ctx      = context.Background()
		endpoint = fmt.Sprintf("localhost:%d", grpcPort)
	)
	_, mp, err := om.Setup(ctx, res,
		WithInsecure(),
		WithMeterProvider(endpoint, WithMeterProviderExportsInterval(100*time.Millisecond)),
		WithRetryConfig(RetryConfig{
			Enabled:         true,
			InitialInterval: time.Second,
			MaxInterval:     time.Second,
			MaxElapsedTime:  time.Minute,
		}),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, om.Shutdown(context.Background()))
	}()

	meter := mp.Meter("test")
	fooCounter, err := meter.SyncInt64().Counter("foo")
	require.NoError(t, err)
	barCounter, err := meter.SyncFloat64().Counter("bar")
	require.NoError(t, err)

	// this counter will not be lost even though the container isn't even started. see MaxElapsedTime.
	fooCounter.Add(ctx, 123, attribute.String("hello", "world"))

	cwd, err := os.Getwd()
	require.NoError(t, err)

	container, _ := otelTest.StartOTelCollector(t, metricsPort,
		filepath.Join(cwd, "testdata", "otel-collector-config.yaml"),
		otelTest.WithStartCollectorPort(grpcPort),
	)
	barCounter.Add(ctx, 456) // this should be recorded

	var (
		resp            *http.Response
		metrics         map[string]*promClient.MetricFamily
		metricsEndpoint = fmt.Sprintf("http://localhost:%d/metrics", dt.GetHostPort(t, metricsPort, container))
	)

	require.Eventuallyf(t, func() bool {
		resp, err = http.Get(metricsEndpoint)
		if err != nil {
			return false
		}
		defer func() { httputil.CloseResponse(resp) }()
		metrics, err = otelTest.ParsePrometheusMetrics(resp.Body)
		if err != nil {
			return false
		}
		if _, ok := metrics["foo"]; !ok {
			return false
		}
		if _, ok := metrics["bar"]; !ok {
			return false
		}
		return true
	}, 10*time.Second, 100*time.Millisecond, "err: %v, metrics: %+v", err, metrics)

	require.EqualValues(t, ptr("foo"), metrics["foo"].Name)
	require.EqualValues(t, ptr(promClient.MetricType_COUNTER), metrics["foo"].Type)
	require.Len(t, metrics["foo"].Metric, 1)
	require.EqualValues(t, &promClient.Counter{Value: ptr(123.0)}, metrics["foo"].Metric[0].Counter)
	require.ElementsMatch(t, []*promClient.LabelPair{
		// the label1=value1 is coming from the otel-collector-config.yaml (see const_labels)
		{Name: ptr("label1"), Value: ptr("value1")},
		{Name: ptr("hello"), Value: ptr("world")},
		{Name: ptr("job"), Value: ptr("TestNonBlockingConnection")},
		{Name: ptr("instance"), Value: ptr("my-instance-id")},
	}, metrics["foo"].Metric[0].Label)

	require.EqualValues(t, ptr("bar"), metrics["bar"].Name)
	require.EqualValues(t, ptr(promClient.MetricType_COUNTER), metrics["bar"].Type)
	require.Len(t, metrics["bar"].Metric, 1)
	require.EqualValues(t, &promClient.Counter{Value: ptr(456.0)}, metrics["bar"].Metric[0].Counter)
	require.ElementsMatch(t, []*promClient.LabelPair{
		// the label1=value1 is coming from the otel-collector-config.yaml (see const_labels)
		{Name: ptr("label1"), Value: ptr("value1")},
		{Name: ptr("job"), Value: ptr("TestNonBlockingConnection")},
		{Name: ptr("instance"), Value: ptr("my-instance-id")},
	}, metrics["bar"].Metric[0].Label)
}

func ptr[T any](v T) *T {
	return &v
}
