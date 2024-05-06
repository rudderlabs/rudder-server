package rmetrics

import (
	"context"
	"sync"
	"time"

	"github.com/influxdata/tdigest"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

var quantiles = map[string]float64{
	"99": 0.99,
	"95": 0.95,
	"75": 0.75,
	"50": 0.5,
	"25": 0.25,
}

type histoGauge struct {
	name string
	tags map[string]string

	stats stats.Stats

	lock sync.Mutex
	td   *tdigest.TDigest
	done chan struct{}
}

type HistoGauge interface {
	HistoGauge(val float64)
}

func (hg *histoGauge) GetName() string { return hg.name }

func (hg *histoGauge) GetTags() map[string]string { return hg.tags }

func NewHistoGauge(ctx context.Context, stats stats.Stats, name string, tags map[string]string) HistoGauge {
	hg := &histoGauge{
		name:  name,
		tags:  tags,
		stats: stats,
		done:  make(chan struct{}),
		td:    tdigest.New(),
	}
	hg.run(ctx)
	return hg
}

func (hg *histoGauge) HistoGauge(val float64) {
	hg.lock.Lock()
	hg.td.Add(val, 1)
	hg.lock.Unlock()
}

func (hg *histoGauge) run(ctx context.Context) {
	defer close(hg.done)
	tick := time.NewTicker(15 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			hg.gaugeQuantiles()
		}
	}
}

func (hg *histoGauge) gaugeQuantiles() {
	hg.lock.Lock()
	if hg.td.Count() == 0 {
		return
	}
	for quantile := range quantiles {
		tags := hg.tags
		tags["quantile"] = quantile
		hg.stats.NewTaggedStat(hg.name, stats.GaugeType, tags).
			Gauge(hg.td.Quantile(quantiles[quantile]))
	}
	hg.td.Reset()
	hg.lock.Unlock()
}
