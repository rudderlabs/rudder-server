package throttler

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type adaptiveThrottleConfig struct {
	window   config.ValueLoader[time.Duration]
	minLimit config.ValueLoader[int64]
	maxLimit func() int64
}

const adaptiveDefaultMaxLimit = 1000 // 1000 requests per second

func (c *adaptiveThrottleConfig) readThrottlingConfig(config *config.Config, destName, destID string) {
	c.window = config.GetReloadableDurationVar(1, time.Second,
		fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destName, destID),
		fmt.Sprintf(`Router.throttler.%s.timeWindow`, destName),
		`Router.throttler.adaptive.timeWindow`)
	c.minLimit = config.GetReloadableInt64Var(1, 1,
		fmt.Sprintf(`Router.throttler.adaptive.%s.%s.minLimit`, destName, destID),
		fmt.Sprintf(`Router.throttler.adaptive.%s.minLimit`, destName),
		`Router.throttler.adaptive.minLimit`)
	maxLimit := config.GetReloadableInt64Var(0, 1,
		fmt.Sprintf(`Router.throttler.adaptive.%s.%s.maxLimit`, destName, destID),
		fmt.Sprintf(`Router.throttler.adaptive.%s.maxLimit`, destName),
		`Router.throttler.adaptive.maxLimit`)
	limitMultiplier := config.GetReloadableFloat64Var(1.5,
		fmt.Sprintf(`Router.throttler.adaptive.%s.%s.limitMultiplier`, destName, destID),
		fmt.Sprintf(`Router.throttler.adaptive.%s.limitMultiplier`, destName),
		`Router.throttler.adaptive.limitMultiplier`)
	limit := config.GetReloadableInt64Var(0, 1,
		fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID),
		fmt.Sprintf(`Router.throttler.%s.limit`, destName))
	defaultMaxLimit := config.GetReloadableInt64Var(adaptiveDefaultMaxLimit, 1, `Router.throttler.adaptive.defaultMaxLimit`)
	c.maxLimit = func() int64 {
		maxLimit := maxLimit.Load()
		staticLimit := limit.Load()
		staticLimitMultiplier := limitMultiplier.Load()
		if maxLimit > 0 {
			return maxLimit
		} else if staticLimit > 0 && staticLimitMultiplier > 0 {
			return int64(float64(staticLimit) * staticLimitMultiplier)
		}
		return defaultMaxLimit.Load()
	}
}

func (c *adaptiveThrottleConfig) enabled() bool {
	return c.minLimit.Load() > 0 && c.maxLimit() > 0 && c.window.Load() > 0 && c.minLimit.Load() <= c.maxLimit()
}

type adaptiveThrottler struct {
	limiter                limiter
	algorithm              adaptiveAlgorithm
	config                 adaptiveThrottleConfig
	limitFactorMeasurement stats.Measurement
}

// CheckLimitReached returns true if we're not allowed to process the number of events we asked for with cost.
func (t *adaptiveThrottler) CheckLimitReached(ctx context.Context, key string, cost int64) (limited bool, retErr error) {
	if !t.config.enabled() {
		return false, nil
	}
	allowed, _, err := t.limiter.Allow(ctx, cost, t.getLimit(), getWindowInSecs(t.config.window.Load()), key)
	if err != nil {
		return false, fmt.Errorf("could not limit: %w", err)
	}
	if !allowed {
		return true, nil
	}
	return false, nil
}

func (t *adaptiveThrottler) ResponseCodeReceived(code int) {
	t.algorithm.ResponseCodeReceived(code)
}

func (t *adaptiveThrottler) Shutdown() {
	t.algorithm.Shutdown()
}

func (t *adaptiveThrottler) getLimit() int64 {
	limitFactor := t.algorithm.LimitFactor()
	if t.limitFactorMeasurement != nil {
		t.limitFactorMeasurement.Gauge(limitFactor)
	}
	limit := int64(float64(t.config.maxLimit()) * limitFactor)
	return max(t.config.minLimit.Load(), limit)
}

func (t *adaptiveThrottler) getTimeWindow() time.Duration {
	return t.config.window.Load()
}
