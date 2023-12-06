package throttler

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type adaptiveThrottleConfig struct {
	window   misc.ValueLoader[time.Duration]
	minLimit misc.ValueLoader[int64]
	maxLimit misc.ValueLoader[int64]
}

func (c *adaptiveThrottleConfig) readThrottlingConfig(config *config.Config, destName, destID string) {
	c.window = config.GetReloadableDurationVar(0, time.Second,
		fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destName, destID),
		fmt.Sprintf(`Router.throttler.%s.timeWindow`, destName),
		`Router.throttler.adaptive.timeWindow`)
	c.minLimit = config.GetReloadableInt64Var(1, 1,
		fmt.Sprintf(`Router.throttler.adaptive.%s.%s.minLimit`, destName, destID),
		fmt.Sprintf(`Router.throttler.adaptive.%s.minLimit`, destName),
		`Router.throttler.adaptive.minLimit`)
	c.maxLimit = config.GetReloadableInt64Var(0, 1,
		fmt.Sprintf(`Router.throttler.adaptive.%s.%s.maxLimit`, destName, destID),
		fmt.Sprintf(`Router.throttler.adaptive.%s.maxLimit`, destName),
		fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID),
		fmt.Sprintf(`Router.throttler.%s.limit`, destName),
		`Router.throttler.adaptive.maxLimit`)
}

func (c *adaptiveThrottleConfig) enabled() bool {
	return c.minLimit.Load() > 0 && c.maxLimit.Load() > 0 && c.window.Load() > 0 && c.minLimit.Load() <= c.maxLimit.Load()
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
	defer t.limitFactorMeasurement.Gauge(limitFactor)
	limit := int64(float64(t.config.maxLimit.Load()) * limitFactor)
	return max(t.config.minLimit.Load(), limit)
}
