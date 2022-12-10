package stats

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	otelMetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/metric"
	"github.com/rudderlabs/rudder-server/utils/logger"
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
		r, m := newReaderWithMeter(t)
		s := &otelStats{meter: m, statsEnabled: true}
		s.NewStat("test-counter", CountType).Increment()
		md := getDataPoint[metricdata.Sum[int64]](ctx, t, r, "test-counter", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 1, md.DataPoints[0].Value)
	})

	t.Run("counter count", func(t *testing.T) {
		r, m := newReaderWithMeter(t)
		s := &otelStats{meter: m, statsEnabled: true}
		s.NewStat("test-counter", CountType).Count(10)
		md := getDataPoint[metricdata.Sum[int64]](ctx, t, r, "test-counter", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 10, md.DataPoints[0].Value)
	})

	t.Run("gauge", func(t *testing.T) {
		r, m := newReaderWithMeter(t)
		s := &otelStats{meter: m, statsEnabled: true}
		s.NewStat("test-gauge", GaugeType).Gauge(1234)
		md := getDataPoint[metricdata.Gauge[float64]](ctx, t, r, "test-gauge", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 1234, md.DataPoints[0].Value)
	})

	t.Run("tagged gauges", func(t *testing.T) {
		r, m := newReaderWithMeter(t)
		s := &otelStats{meter: m, statsEnabled: true}
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
		r, m := newReaderWithMeter(t)
		s := &otelStats{meter: m, statsEnabled: true}
		s.NewStat("test-timer-1", TimerType).SendTiming(10 * time.Second)
		md := getDataPoint[metricdata.Histogram](ctx, t, r, "test-timer-1", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 1, md.DataPoints[0].Count)
		require.EqualValues(t, (10*time.Second)/time.Millisecond, md.DataPoints[0].Sum)
	})

	t.Run("timer since", func(t *testing.T) {
		r, m := newReaderWithMeter(t)
		s := &otelStats{meter: m, statsEnabled: true}
		s.NewStat("test-timer-2", TimerType).Since(time.Now().Add(-time.Second))
		md := getDataPoint[metricdata.Histogram](ctx, t, r, "test-timer-2", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 1, md.DataPoints[0].Count)
		require.EqualValues(t, time.Second.Milliseconds(), md.DataPoints[0].Sum)
	})

	t.Run("timer RecordDuration", func(t *testing.T) {
		r, m := newReaderWithMeter(t)
		s := &otelStats{meter: m, statsEnabled: true}
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
		r, m := newReaderWithMeter(t)
		s := &otelStats{meter: m, statsEnabled: true}
		s.NewStat("test-hist-1", HistogramType).Observe(1.2)
		md := getDataPoint[metricdata.Histogram](ctx, t, r, "test-hist-1", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 1, md.DataPoints[0].Count)
		require.EqualValues(t, 1.2, md.DataPoints[0].Sum)
	})

	t.Run("tagged stats", func(t *testing.T) {
		r, m := newReaderWithMeter(t)
		s := &otelStats{meter: m, statsEnabled: true}
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

	t.Run("measurement with empty name", func(t *testing.T) {
		r, m := newReaderWithMeter(t)
		s := &otelStats{meter: m, statsEnabled: true, logger: logger.NOP}
		s.NewStat("", CountType).Increment()
		md := getDataPoint[metricdata.Sum[int64]](ctx, t, r, "novalue", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 1, md.DataPoints[0].Value)
		require.True(t, md.DataPoints[0].Attributes.Equals(newAttributesSet(t)))
	})

	t.Run("measurement with empty name and empty tag key", func(t *testing.T) {
		r, m := newReaderWithMeter(t)
		s := &otelStats{meter: m, statsEnabled: true, logger: logger.NOP}
		s.NewTaggedStat(" ", GaugeType, Tags{"key": "value", "": "value2", " ": "value3"}).Gauge(22)
		md := getDataPoint[metricdata.Gauge[float64]](ctx, t, r, "novalue", 0)
		require.Len(t, md.DataPoints, 1)
		require.EqualValues(t, 22, md.DataPoints[0].Value)
		require.True(t, md.DataPoints[0].Attributes.Equals(newAttributesSet(t,
			attribute.String("key", "value"),
		)))
	})
}

func TestTaggedGauges(t *testing.T) {
	ctx := context.Background()
	r, m := newReaderWithMeter(t)
	s := &otelStats{meter: m, statsEnabled: true}
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

func Test_Periodic_stats(t *testing.T) {
	type expectation struct {
		name string
		tags *attribute.Set
	}

	runTest := func(t *testing.T, prepareFunc func(c *config.Config, m metric.Manager), expected []expectation) {
		c := config.New()
		m := metric.NewManager()
		prepareFunc(c, m)

		l := logger.NewFactory(c)
		s := newStats(c, l, m)

		rdr, meter := newReaderWithMeter(t)
		s.(*otelStats).meter = meter

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// start stats
		s.Start(ctx)
		defer s.Stop()

		require.Eventually(t, func() bool {
			rm, err := rdr.Collect(ctx)
			require.NoError(t, err) // this should never fail, so it's OK to fail the test right away

			data := getMapFromScopeMetrics(rm.ScopeMetrics)
			for _, exp := range expected {
				if _, ok := data[exp.name]; !ok {
					t.Logf("No data for %q", exp.name)
					return false
				}
				if v, ok := data[exp.name].Data.(metricdata.Gauge[float64]); !ok {
					t.Logf("No gauge data for %q", exp.name)
					return false
				} else {
					if len(v.DataPoints) < 1 {
						t.Logf("No data points for %q", exp.name)
						return false
					}
					if exp.tags != nil && !exp.tags.Equals(&v.DataPoints[0].Attributes) {
						t.Logf("Unexpected tags for %q: %v", exp.name, v.DataPoints[0].Attributes)
						return false
					}
				}
			}
			return true
		}, time.Second, time.Millisecond)
	}

	t.Run("CPU stats", func(t *testing.T) {
		runTest(t, func(c *config.Config, m metric.Manager) {
			c.Set("RuntimeStats.enableCPUStats", true)
			c.Set("RuntimeStats.enabledMemStats", false)
			c.Set("RuntimeStats.enableGCStats", false)
		}, []expectation{
			{name: "runtime_cpu.goroutines"},
			{name: "runtime_cpu.cgo_calls"},
		})
	})

	t.Run("Mem stats", func(t *testing.T) {
		runTest(t, func(c *config.Config, m metric.Manager) {
			c.Set("RuntimeStats.enableCPUStats", false)
			c.Set("RuntimeStats.enabledMemStats", true)
			c.Set("RuntimeStats.enableGCStats", false)
		}, []expectation{
			{name: "runtime_mem.alloc"},
			{name: "runtime_mem.total"},
			{name: "runtime_mem.sys"},
			{name: "runtime_mem.lookups"},
			{name: "runtime_mem.malloc"},
			{name: "runtime_mem.frees"},
			{name: "runtime_mem.heap.alloc"},
			{name: "runtime_mem.heap.sys"},
			{name: "runtime_mem.heap.idle"},
			{name: "runtime_mem.heap.inuse"},
			{name: "runtime_mem.heap.released"},
			{name: "runtime_mem.heap.objects"},
			{name: "runtime_mem.stack.inuse"},
			{name: "runtime_mem.stack.sys"},
			{name: "runtime_mem.stack.mspan_inuse"},
			{name: "runtime_mem.stack.mspan_sys"},
			{name: "runtime_mem.stack.mcache_inuse"},
			{name: "runtime_mem.stack.mcache_sys"},
			{name: "runtime_mem.othersys"},
		})
	})

	t.Run("MemGC stats", func(t *testing.T) {
		runTest(t, func(c *config.Config, m metric.Manager) {
			c.Set("RuntimeStats.enableCPUStats", false)
			c.Set("RuntimeStats.enabledMemStats", true)
			c.Set("RuntimeStats.enableGCStats", true)
		}, []expectation{
			{name: "runtime_mem.alloc"},
			{name: "runtime_mem.total"},
			{name: "runtime_mem.sys"},
			{name: "runtime_mem.lookups"},
			{name: "runtime_mem.malloc"},
			{name: "runtime_mem.frees"},
			{name: "runtime_mem.heap.alloc"},
			{name: "runtime_mem.heap.sys"},
			{name: "runtime_mem.heap.idle"},
			{name: "runtime_mem.heap.inuse"},
			{name: "runtime_mem.heap.released"},
			{name: "runtime_mem.heap.objects"},
			{name: "runtime_mem.stack.inuse"},
			{name: "runtime_mem.stack.sys"},
			{name: "runtime_mem.stack.mspan_inuse"},
			{name: "runtime_mem.stack.mspan_sys"},
			{name: "runtime_mem.stack.mcache_inuse"},
			{name: "runtime_mem.stack.mcache_sys"},
			{name: "runtime_mem.othersys"},
			{name: "runtime_mem.gc.sys"},
			{name: "runtime_mem.gc.next"},
			{name: "runtime_mem.gc.last"},
			{name: "runtime_mem.gc.pause_total"},
			{name: "runtime_mem.gc.pause"},
			{name: "runtime_mem.gc.count"},
			{name: "runtime_mem.gc.cpu_percent"},
		})
	})

	t.Run("Pending events", func(t *testing.T) {
		runTest(t, func(c *config.Config, m metric.Manager) {
			c.Set("RuntimeStats.enableCPUStats", false)
			c.Set("RuntimeStats.enabledMemStats", false)
			c.Set("RuntimeStats.enableGCStats", false)
			m.GetRegistry(metric.PublishedMetrics).MustGetGauge(metric.PendingEventsMeasurement("table", "myWorkspace", "myDestType")).Set(1.0)
		}, []expectation{
			{name: "jobsdb_table_pending_events_count", tags: newAttributesSet(t,
				attribute.String("destType", "myDestType"),
				attribute.String("workspace", "myWorkspace"),
				attribute.String("workspaceId", "myWorkspace"),
			)},
		})
	})
}

func Test_Tags_Type(t *testing.T) {
	tags := Tags{
		"b": "value1",
		"a": "value2",
	}

	t.Run("strings method", func(t *testing.T) {
		for i := 0; i < 100; i++ { // just making sure we are not just lucky with the order
			require.Equal(t, []string{"a", "value2", "b", "value1"}, tags.Strings())
		}
	})

	t.Run("string method", func(t *testing.T) {
		require.Equal(t, "a,value2,b,value1", tags.String())
	})

	t.Run("special character replacement", func(t *testing.T) {
		specialTags := Tags{
			"b:1": "value1:1",
			"a:1": "value2:2",
		}
		require.Equal(t, []string{"a-1", "value2-2", "b-1", "value1-1"}, specialTags.Strings())
	})

	t.Run("empty tags", func(t *testing.T) {
		emptyTags := Tags{}
		require.Nil(t, emptyTags.Strings())
		require.Equal(t, "", emptyTags.String())
	})
}

func TestExcludedTags(t *testing.T) {
	c := config.New()
	c.Set("statsExcludedTags", []string{"workspaceId"})
	l := logger.NewFactory(c)
	m := metric.NewManager()
	s := newStats(c, l, m)

	rdr, meter := newReaderWithMeter(t)
	s.(*otelStats).meter = meter

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start stats
	s.Start(ctx)
	defer s.Stop()

	metricName := "test-workspaceId"
	s.NewTaggedStat(metricName, CountType, Tags{
		"workspaceId":            "nice-value",
		"should-not-be-filtered": "fancy-value",
	}).Increment()
	rm, err := rdr.Collect(ctx)
	require.NoError(t, err)

	data := getMapFromScopeMetrics(rm.ScopeMetrics)
	require.Contains(t, data, metricName)

	md, ok := data[metricName].Data.(metricdata.Sum[int64])
	require.True(t, ok)
	require.Len(t, md.DataPoints, 1)
	require.True(t, md.DataPoints[0].Attributes.Equals(
		newAttributesSet(t, attribute.String("should-not-be-filtered", "fancy-value")),
	))
}

func TestStartStop(t *testing.T) {
	c := config.New()
	l := logger.NewFactory(c)
	m := metric.NewManager()
	s := newStats(c, l, m)

	s.Start(context.Background())

	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for Stop()")
	}
}

func getMapFromScopeMetrics(sm []metricdata.ScopeMetrics) map[string]metricdata.Metrics {
	data := make(map[string]metricdata.Metrics)
	for _, scopeMetric := range sm {
		for _, m := range scopeMetric.Metrics {
			data[m.Name] = m
		}
	}
	return data
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

func newAttributesSet(t *testing.T, attrs ...attribute.KeyValue) *attribute.Set {
	t.Helper()
	set := attribute.NewSet(attrs...)
	return &set
}

func newReaderWithMeter(t *testing.T) (sdkmetric.Reader, otelMetric.Meter) {
	t.Helper()
	manualRdr := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(resource.NewSchemaless(semconv.ServiceNameKey.String(t.Name()))),
		sdkmetric.WithReader(manualRdr),
	)
	t.Cleanup(func() {
		_ = meterProvider.Shutdown(context.Background())
	})
	return manualRdr, meterProvider.Meter(t.Name())
}
