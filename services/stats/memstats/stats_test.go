package memstats_test

import (
	"context"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/services/stats/memstats"
	"github.com/stretchr/testify/require"
)

func TestStats(t *testing.T) {
	now := time.Now()

	store := memstats.New(
		memstats.WithNow(func() time.Time {
			return now
		}),
	)

	commonTags := stats.Tags{"tag1": "value1"}

	t.Run("test Count", func(t *testing.T) {
		name := "testCount"

		m := store.NewTaggedStat(name, stats.CountType, commonTags)

		m.Increment()

		require.Equal(t, 1.0, store.Get(name, commonTags).LastValue())
		require.Equal(t, []float64{1.0}, store.Get(name, commonTags).Values())

		m.Count(2)

		require.Equal(t, 3.0, store.Get(name, commonTags).LastValue())
		require.Equal(t, []float64{1.0, 3.0}, store.Get(name, commonTags).Values())
	})

	t.Run("test Gauge", func(t *testing.T) {
		name := "testGauge"
		m := store.NewTaggedStat(name, stats.GaugeType, commonTags)

		m.Gauge(1.0)

		require.Equal(t, 1.0, store.Get(name, commonTags).LastValue())
		require.Equal(t, []float64{1.0}, store.Get(name, commonTags).Values())

		m.Gauge(2.0)

		require.Equal(t, 2.0, store.Get(name, commonTags).LastValue())
		require.Equal(t, []float64{1.0, 2.0}, store.Get(name, commonTags).Values())
	})

	t.Run("test Histogram", func(t *testing.T) {
		name := "testHistogram"
		m := store.NewTaggedStat(name, stats.HistogramType, commonTags)

		m.Observe(1.0)

		require.Equal(t, 1.0, store.Get(name, commonTags).LastValue())
		require.Equal(t, []float64{1.0}, store.Get(name, commonTags).Values())

		m.Observe(2.0)

		require.Equal(t, 2.0, store.Get(name, commonTags).LastValue())
		require.Equal(t, []float64{1.0, 2.0}, store.Get(name, commonTags).Values())
	})

	t.Run("test Timer", func(t *testing.T) {
		name := "testTimer"

		m := store.NewTaggedStat(name, stats.TimerType, commonTags)

		m.SendTiming(time.Second)
		require.Equal(t, time.Second, store.Get(name, commonTags).LastDuration())
		require.Equal(t, []time.Duration{time.Second}, store.Get(name, commonTags).Durations())

		m.SendTiming(time.Minute)
		require.Equal(t, time.Minute, store.Get(name, commonTags).LastDuration())
		require.Equal(t,
			[]time.Duration{time.Second, time.Minute},
			store.Get(name, commonTags).Durations(),
		)

		func() {
			defer m.RecordDuration()()
			now = now.Add(time.Second)
		}()
		require.Equal(t, time.Second, store.Get(name, commonTags).LastDuration())
		require.Equal(t,
			[]time.Duration{time.Second, time.Minute, time.Second},
			store.Get(name, commonTags).Durations(),
		)

		m.Since(now.Add(-time.Minute))
		require.Equal(t, time.Minute, store.Get(name, commonTags).LastDuration())
		require.Equal(t,
			[]time.Duration{time.Second, time.Minute, time.Second, time.Minute},
			store.Get(name, commonTags).Durations(),
		)
	})

	t.Run("invalid operations", func(t *testing.T) {
		require.PanicsWithValue(t, "operation Count not supported for measurement type:gauge", func() {
			store.NewTaggedStat("invalid_count", stats.GaugeType, commonTags).Count(1)
		})
		require.PanicsWithValue(t, "operation Increment not supported for measurement type:gauge", func() {
			store.NewTaggedStat("invalid_inc", stats.GaugeType, commonTags).Increment()
		})
		require.PanicsWithValue(t, "operation Gauge not supported for measurement type:count", func() {
			store.NewTaggedStat("invalid_gauge", stats.CountType, commonTags).Gauge(1)
		})
		require.PanicsWithValue(t, "operation SendTiming not supported for measurement type:histogram", func() {
			store.NewTaggedStat("invalid_send_timing", stats.HistogramType, commonTags).SendTiming(time.Second)
		})
		require.PanicsWithValue(t, "operation RecordDuration not supported for measurement type:histogram", func() {
			store.NewTaggedStat("invalid_record_duration", stats.HistogramType, commonTags).RecordDuration()
		})
		require.PanicsWithValue(t, "operation Since not supported for measurement type:histogram", func() {
			store.NewTaggedStat("invalid_since", stats.HistogramType, commonTags).Since(time.Now())
		})
		require.PanicsWithValue(t, "operation Observe not supported for measurement type:timer", func() {
			store.NewTaggedStat("invalid_observe", stats.TimerType, commonTags).Observe(1)
		})
	})

	t.Run("no op", func(t *testing.T) {
		store.Start(context.Background())
		store.Stop()
	})

	t.Run("no tags", func(t *testing.T) {
		name := "no_tags"
		m := store.NewStat(name, stats.CountType)

		m.Increment()

		require.Equal(t, 1.0, store.Get(name, nil).LastValue())
	})
}
