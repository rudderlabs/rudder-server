package stats

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/global"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func Test_Measurement_Invalid_Operations(t *testing.T) {
	s := &otelStats{meter: global.MeterProvider().Meter(t.Name())}

	t.Run("counter invalid operations", func(t *testing.T) {
		require.Panics(t, func() {
			s.NewStat("test", CountType).Gauge(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", CountType).Observe(1.2)
		})
		require.Panics(t, func() {
			s.NewStat("test", CountType).RecordDuration()
		})
		require.Panics(t, func() {
			s.NewStat("test", CountType).SendTiming(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", CountType).Since(time.Now())
		})
	})

	t.Run("gauge invalid operations", func(t *testing.T) {
		require.Panics(t, func() {
			s.NewStat("test", GaugeType).Increment()
		})
		require.Panics(t, func() {
			s.NewStat("test", GaugeType).Count(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", GaugeType).Observe(1.2)
		})
		require.Panics(t, func() {
			s.NewStat("test", GaugeType).RecordDuration()
		})
		require.Panics(t, func() {
			s.NewStat("test", GaugeType).SendTiming(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", GaugeType).Since(time.Now())
		})
	})

	t.Run("histogram invalid operations", func(t *testing.T) {
		require.Panics(t, func() {
			s.NewStat("test", HistogramType).Increment()
		})
		require.Panics(t, func() {
			s.NewStat("test", HistogramType).Count(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", HistogramType).Gauge(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", HistogramType).RecordDuration()
		})
		require.Panics(t, func() {
			s.NewStat("test", HistogramType).SendTiming(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", HistogramType).Since(time.Now())
		})
	})

	t.Run("timer invalid operations", func(t *testing.T) {
		require.Panics(t, func() {
			s.NewStat("test", TimerType).Increment()
		})
		require.Panics(t, func() {
			s.NewStat("test", TimerType).Count(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", TimerType).Gauge(1)
		})
		require.Panics(t, func() {
			s.NewStat("test", TimerType).Observe(1.2)
		})
	})
}

func Test_Measurement_Operations(t *testing.T) {
	ctx := context.Background()

	t.Run("counter increment", func(t *testing.T) {
		r, s := newStats(t)
		s.NewStat("test-counter", CountType).Increment()
		md := getDataPoint[metricdata.Sum[int64]](ctx, t, r, "test-counter", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 1, md.DataPoints[0].Value)
	})

	t.Run("counter count", func(t *testing.T) {
		r, s := newStats(t)
		s.NewStat("test-counter", CountType).Count(10)
		md := getDataPoint[metricdata.Sum[int64]](ctx, t, r, "test-counter", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 10, md.DataPoints[0].Value)
	})

	t.Run("gauge", func(t *testing.T) {
		r, s := newStats(t)
		s.NewStat("test-gauge", GaugeType).Gauge(1234)
		md := getDataPoint[metricdata.Gauge[float64]](ctx, t, r, "test-gauge", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 1234, md.DataPoints[0].Value)
	})

	t.Run("tagged gauges", func(t *testing.T) {
		r, s := newStats(t)
		s.NewTaggedStat("test-tagged-gauge", GaugeType, Tags{"a": "b"}).Gauge(111)
		s.NewTaggedStat("test-tagged-gauge", GaugeType, Tags{"c": "d"}).Gauge(222)
		md := getDataPoint[metricdata.Gauge[float64]](ctx, t, r, "test-tagged-gauge", 0)
		require.Len(t, md.DataPoints, 2)
		// sorting data points by value since the collected time is the same
		sortDataPointsByValue(md.DataPoints)
		require.EqualValues(t, 111, md.DataPoints[0].Value)
		expectedAttrs1 := attribute.NewSet(attribute.String("a", "b"))
		require.True(t, expectedAttrs1.Equals(&md.DataPoints[0].Attributes))
		require.EqualValues(t, 222, md.DataPoints[1].Value)
		expectedAttrs2 := attribute.NewSet(attribute.String("c", "d"))
		require.True(t, expectedAttrs2.Equals(&md.DataPoints[1].Attributes))
	})

	t.Run("timer send timing", func(t *testing.T) {
		r, s := newStats(t)
		s.NewStat("test-timer-1", TimerType).SendTiming(10 * time.Second)
		md := getDataPoint[metricdata.Histogram](ctx, t, r, "test-timer-1", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 1, md.DataPoints[0].Count)
		require.EqualValues(t, (10*time.Second)/time.Millisecond, md.DataPoints[0].Sum)
	})

	t.Run("timer since", func(t *testing.T) {
		r, s := newStats(t)
		s.NewStat("test-timer-2", TimerType).Since(time.Now().Add(-time.Second))
		md := getDataPoint[metricdata.Histogram](ctx, t, r, "test-timer-2", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 1, md.DataPoints[0].Count)
		require.EqualValues(t, time.Second.Milliseconds(), md.DataPoints[0].Sum)
	})

	t.Run("timer RecordDuration", func(t *testing.T) {
		r, s := newStats(t)
		ot := s.NewStat("test-timer-3", TimerType)
		ot.(*otelTimer).now = func() time.Time {
			return time.Now().Add(-time.Second)
		}
		ot.RecordDuration()()
		md := getDataPoint[metricdata.Histogram](ctx, t, r, "test-timer-3", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 1, md.DataPoints[0].Count)
		require.InDelta(t, time.Second.Milliseconds(), md.DataPoints[0].Sum, 10)
	})

	t.Run("histogram", func(t *testing.T) {
		r, s := newStats(t)
		s.NewStat("test-hist-1", HistogramType).Observe(1.2)
		md := getDataPoint[metricdata.Histogram](ctx, t, r, "test-hist-1", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 1, md.DataPoints[0].Count)
		require.EqualValues(t, 1.2, md.DataPoints[0].Sum)
	})

	t.Run("tagged stats", func(t *testing.T) {
		r, s := newStats(t)
		s.NewTaggedStat("test-tagged", CountType, Tags{"key": "value"}).Increment()
		md1 := getDataPoint[metricdata.Sum[int64]](ctx, t, r, "test-tagged", 0)
		require.Len(t, md1.DataPoints, 1)
		require.EqualValues(t, 1, md1.DataPoints[0].Value)
		expectedAttrs := attribute.NewSet(attribute.String("key", "value"))
		require.True(t, expectedAttrs.Equals(&md1.DataPoints[0].Attributes))

		// same measurement name, different measurement type
		s.NewTaggedStat("test-tagged", GaugeType, Tags{"key": "value"}).Gauge(1234)
		md2 := getDataPoint[metricdata.Gauge[float64]](ctx, t, r, "test-tagged", 1)
		require.Len(t, md2.DataPoints, 1)
		require.EqualValues(t, 1234, md2.DataPoints[0].Value)
		require.True(t, expectedAttrs.Equals(&md2.DataPoints[0].Attributes))
	})
}

func TestTaggedGauges(t *testing.T) {
	ctx := context.Background()
	r, s := newStats(t)
	s.NewTaggedStat("test-gauge", GaugeType, Tags{"a": "b"}).Gauge(1)
	s.NewStat("test-gauge", GaugeType).Gauge(2)
	s.NewTaggedStat("test-gauge", GaugeType, Tags{"c": "d"}).Gauge(3)

	rm, err := r.Collect(ctx)
	require.NoError(t, err)

	var dp []metricdata.DataPoint[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			dp = append(dp, m.Data.(metricdata.Gauge[float64]).DataPoints...)
		}
	}
	sortDataPointsByValue(dp)

	require.Len(t, dp, 3)

	require.EqualValues(t, 1, dp[0].Value)
	expectedAttrs := attribute.NewSet(attribute.String("a", "b"))
	require.True(t, expectedAttrs.Equals(&dp[0].Attributes))

	require.EqualValues(t, 2, dp[1].Value)
	expectedAttrs = attribute.NewSet()
	require.True(t, expectedAttrs.Equals(&dp[1].Attributes))

	require.EqualValues(t, 3, dp[2].Value)
	expectedAttrs = attribute.NewSet(attribute.String("c", "d"))
	require.True(t, expectedAttrs.Equals(&dp[2].Attributes))
}

func getDataPoint[T any](ctx context.Context, t *testing.T, rdr sdkmetric.Reader, name string, idx int) (zero T) {
	t.Helper()
	rm, err := rdr.Collect(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(rm.ScopeMetrics), 1)
	require.GreaterOrEqual(t, len(rm.ScopeMetrics[0].Metrics), idx+1)
	require.Equal(t, name, rm.ScopeMetrics[0].Metrics[idx].Name)
	md, ok := rm.ScopeMetrics[0].Metrics[idx].Data.(T)
	require.Truef(t, ok, "Metric data is not of type %T but %T", zero, rm.ScopeMetrics[0].Metrics[idx].Data)
	return md
}

func sortDataPointsByValue[N int64 | float64](dp []metricdata.DataPoint[N]) {
	sort.Slice(dp, func(i, j int) bool {
		return dp[i].Value < dp[j].Value
	})
}

func newStats(t *testing.T) (sdkmetric.Reader, *otelStats) {
	t.Helper()
	manualRdr := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(resource.NewSchemaless(
			semconv.ServiceNameKey.String(t.Name()),
		)),
		sdkmetric.WithReader(manualRdr),
	)
	t.Cleanup(func() {
		_ = meterProvider.Shutdown(context.Background())
	})
	return manualRdr, &otelStats{meter: meterProvider.Meter(t.Name())}
}
