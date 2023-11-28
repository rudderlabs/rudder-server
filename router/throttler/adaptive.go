package throttler

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type adaptiveThrottleConfig struct {
	enabled  bool
	limit    int64
	window   *config.Reloadable[time.Duration]
	minLimit *config.Reloadable[int64]
	maxLimit *config.Reloadable[int64]
}

func (c *adaptiveThrottleConfig) readThrottlingConfig(config *config.Config, destName, destID string) {
	c.window = config.GetReloadableDurationVar(1, time.Second, fmt.Sprintf(`Router.throttler.adaptive.%s.timeWindow`, destID), fmt.Sprintf(`Router.throttler.adaptive.%s.timeWindow`, destName), `Router.throttler.adaptive.timeWindow`, fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destName, destID), fmt.Sprintf(`Router.throttler.%s.timeWindow`, destName))
	c.minLimit = config.GetReloadableInt64Var(1, 1, fmt.Sprintf(`Router.throttler.adaptive.%s.minLimit`, destID), fmt.Sprintf(`Router.throttler.adaptive.%s.minLimit`, destName), `Router.throttler.adaptive.minLimit`, fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID), fmt.Sprintf(`Router.throttler.%s.limit`, destName))
	c.maxLimit = config.GetReloadableInt64Var(250, 1, fmt.Sprintf(`Router.throttler.adaptive.%s.maxLimit`, destID), fmt.Sprintf(`Router.throttler.adaptive.%s.maxLimit`, destName), `Router.throttler.adaptive.maxLimit`, fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID), fmt.Sprintf(`Router.throttler.%s.limit`, destName))
	if c.window.Load() > 0 {
		c.enabled = true
	}
}

type adaptiveThrottler struct {
	limiter   limiter
	algorithm adaptiveAlgorithm
	config    adaptiveThrottleConfig
}

// CheckLimitReached returns true if we're not allowed to process the number of events we asked for with cost.
func (t *adaptiveThrottler) CheckLimitReached(ctx context.Context, key string, cost int64) (limited bool, retErr error) {
	if !t.config.enabled {
		return false, nil
	}
	if t.config.minLimit.Load() > t.config.maxLimit.Load() {
		return false, fmt.Errorf("minLimit %d is greater than maxLimit %d", t.config.minLimit.Load(), t.config.maxLimit.Load())
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

func (t *adaptiveThrottler) computeLimit() {
	if t.config.limit == 0 {
		t.config.limit = t.config.maxLimit.Load()
	}
	t.config.limit += int64(float64(t.config.limit) * t.algorithm.LimitFactor())
	t.config.limit = max(t.config.minLimit.Load(), min(t.config.limit, t.config.maxLimit.Load()))
}

func (t *adaptiveThrottler) ResponseCodeReceived(code int) {
	t.algorithm.ResponseCodeReceived(code)
}

func (t *adaptiveThrottler) Shutdown() {
	t.algorithm.Shutdown()
}

func (t *adaptiveThrottler) getLimit() int64 {
	t.computeLimit()
	return t.config.limit
}
