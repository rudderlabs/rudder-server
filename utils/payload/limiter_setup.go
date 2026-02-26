package payload

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/mem"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type AdaptiveLimiterFunc func(int64) int64

// SetupAdaptiveLimiter creates a new AdaptiveLimiter, starts its RunLoop in a goroutine and periodically collects statistics.
func SetupAdaptiveLimiter(ctx context.Context, g *errgroup.Group) AdaptiveLimiterFunc {
	var freeMem FreeMemory
	if config.GetBoolVar(true, "AdaptivePayloadLimiter.enabled") {
		useRSS := config.GetReloadableBoolVar(true, "AdaptivePayloadLimiter.useRSS")
		freeMem = func() (float64, error) {
			s, err := mem.Get()
			if err != nil {
				return 0, err
			}
			if useRSS.Load() {
				return s.AvailableIgnoreCachePercent(), nil
			}
			return s.AvailablePercent, nil
		}
	}

	limiterConfig := AdaptiveLimiterConfig{
		FreeMemThresholdLimit: config.GetFloat64Var(30, "AdaptivePayloadLimiter.freeMemThresholdLimit"),
		FreeMemCriticalLimit:  config.GetFloat64Var(10, "AdaptivePayloadLimiter.freeMemCriticalLimit"),
		MaxThresholdFactor:    config.GetIntVar(9, 1, "AdaptivePayloadLimiter.maxThresholdFactor"),
		Log:                   logger.NewLogger().Child("payload_limiter"),
		FreeMemory:            freeMem,
	}

	// run tick periodically
	tickFrequency := config.GetDurationVar(1, time.Second, "AdaptivePayloadLimiter.tickFrequency")
	limiter := NewAdaptiveLimiter(limiterConfig)
	g.Go(func() error {
		limiter.RunLoop(ctx, func() <-chan time.Time {
			return time.After(tickFrequency)
		})
		return nil
	})

	// collect statistics
	statsFrequency := config.GetDurationVar(15, time.Second, "AdaptivePayloadLimiter.statsFrequency")
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(statsFrequency):
				limiterStats := limiter.Stats()

				stats.Default.NewStat(
					"adaptive_payload_limiter_state",
					stats.GaugeType,
				).Gauge(int(limiterStats.State))
				stats.Default.NewStat(
					"adaptive_payload_limiter_threshold_factor",
					stats.GaugeType,
				).Gauge(limiterStats.ThresholdFactor)
				if memStats, err := mem.Get(); err == nil {
					stats.Default.NewStat("mem_total_bytes", stats.GaugeType).
						Gauge(memStats.Total)
					stats.Default.NewStat("mem_available_bytes", stats.GaugeType).
						Gauge(memStats.Available)
					stats.Default.NewStat("mem_available_percent", stats.GaugeType).
						Gauge(memStats.AvailablePercent)
					stats.Default.NewStat("mem_used_bytes", stats.GaugeType).
						Gauge(memStats.Used)
					stats.Default.NewStat("mem_used_percent", stats.GaugeType).
						Gauge(memStats.UsedPercent)
				}
			}
		}
	})
	return limiter.Limit
}
