package rmetrics

import (
	"testing"

	"github.com/influxdata/tdigest"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/stretchr/testify/require"
)

func TestGaugeQuantiles(t *testing.T) {
	memStats, err := memstats.New()
	require.NoError(t, err)
	hg := &histoGauge{
		name:  "test",
		tags:  map[string]string{"key": "value"},
		stats: memStats,
		td:    tdigest.New(),
	}
	t.Run("hundred values 0-99 - expect less than 1.5percent variation", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			hg.HistoGauge(float64(i))
		}
		hg.gaugeQuantiles()

		p25 := memStats.Get("test", map[string]string{"key": "value", "quantile": "25"}).LastValue()
		require.InEpsilon(t, 24.75, p25, 0.015)
		p50 := memStats.Get("test", map[string]string{"key": "value", "quantile": "50"}).LastValue()
		require.InEpsilon(t, 49.5, p50, 0.015)
		p75 := memStats.Get("test", map[string]string{"key": "value", "quantile": "75"}).LastValue()
		require.InEpsilon(t, 74.25, p75, 0.015)
		p95 := memStats.Get("test", map[string]string{"key": "value", "quantile": "95"}).LastValue()
		require.InEpsilon(t, 94.05, p95, 0.015)
		p99 := memStats.Get("test", map[string]string{"key": "value", "quantile": "99"}).LastValue()
		require.InEpsilon(t, 98.01, p99, 0.015)
	})

	t.Run(`ten values 0-9 - expect 12percent! variation`, func(t *testing.T) {
		for i := 0; i < 10; i++ {
			hg.HistoGauge(float64(i))
		}
		hg.gaugeQuantiles()

		p25 := memStats.Get("test", map[string]string{"key": "value", "quantile": "25"}).LastValue()
		require.InEpsilon(t, 2.25, p25, 0.12)
		p50 := memStats.Get("test", map[string]string{"key": "value", "quantile": "50"}).LastValue()
		require.InEpsilon(t, 4.5, p50, 0.12)
		p75 := memStats.Get("test", map[string]string{"key": "value", "quantile": "75"}).LastValue()
		require.InEpsilon(t, 6.75, p75, 0.12)
		p95 := memStats.Get("test", map[string]string{"key": "value", "quantile": "95"}).LastValue()
		require.InEpsilon(t, 8.55, p95, 0.12)
		p99 := memStats.Get("test", map[string]string{"key": "value", "quantile": "99"}).LastValue()
		require.InEpsilon(t, 8.91, p99, 0.12)
	})

	t.Run("thousan values 0-999 - expect less than 1percent variation", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			hg.HistoGauge(float64(i))
		}
		hg.gaugeQuantiles()

		p25 := memStats.Get("test", map[string]string{"key": "value", "quantile": "25"}).LastValue()
		require.InEpsilon(t, 249.75, p25, 0.01)
		p50 := memStats.Get("test", map[string]string{"key": "value", "quantile": "50"}).LastValue()
		require.InEpsilon(t, 499.5, p50, 0.01)
		p75 := memStats.Get("test", map[string]string{"key": "value", "quantile": "75"}).LastValue()
		require.InEpsilon(t, 749.25, p75, 0.01)
		p95 := memStats.Get("test", map[string]string{"key": "value", "quantile": "95"}).LastValue()
		require.InEpsilon(t, 949.05, p95, 0.01)
		p99 := memStats.Get("test", map[string]string{"key": "value", "quantile": "99"}).LastValue()
		require.InEpsilon(t, 989.01, p99, 0.01)
	})
}

/*
BenchmarkGaugeQuantileReset
BenchmarkGaugeQuantileReset-10            634662              1759 ns/op            1400 B/op         35 allocs/op
PASS
ok      github.com/rudderlabs/rudder-server/services/rmetrics   2.264s
*/
func BenchmarkGaugeQuantileReset(b *testing.B) {
	memStats, err := memstats.New()
	require.NoError(b, err)
	hg := &histoGauge{
		name:  "test",
		tags:  map[string]string{"key": "value"},
		stats: memStats,
		done:  make(chan struct{}),
		td:    tdigest.New(),
	}
	for j := 0; j < 15000; j++ {
		hg.HistoGauge(float64(j))
	}
	for i := 0; i < b.N; i++ {
		hg.gaugeQuantiles()
	}
}

/*
BenchmarkHistoGauge
BenchmarkHistoGauge-10          11010061                96.24 ns/op            0 B/op          0 allocs/op
PASS
ok      github.com/rudderlabs/rudder-server/services/rmetrics   1.523s
*/
func BenchmarkHistoGauge(b *testing.B) {
	memStats, err := memstats.New()
	require.NoError(b, err)
	hg := &histoGauge{
		name:  "test",
		tags:  map[string]string{"key": "value"},
		stats: memStats,
		done:  make(chan struct{}),
		td:    tdigest.New(),
	}
	for i := 0; i < b.N; i++ {
		hg.HistoGauge(float64(i))
	}
	hg.gaugeQuantiles()
}
