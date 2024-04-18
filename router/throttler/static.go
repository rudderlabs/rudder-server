package throttler

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type staticThrottleConfig struct {
	limit  config.ValueLoader[int64]
	window config.ValueLoader[time.Duration]
}

func (c *staticThrottleConfig) readThrottlingConfig(config *config.Config, destName, destID string) {
	c.limit = config.GetReloadableInt64Var(0, 1,
		fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID),
		fmt.Sprintf(`Router.throttler.%s.limit`, destName))
	c.window = config.GetReloadableDurationVar(0, time.Second,
		fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destName, destID),
		fmt.Sprintf(`Router.throttler.%s.timeWindow`, destName))
}

func (c *staticThrottleConfig) enabled() bool {
	return c.limit.Load() > 0 && c.window.Load() > 0
}

type staticThrottler struct {
	limiter limiter
	config  staticThrottleConfig
}

// CheckLimitReached returns true if we're not allowed to process the number of events we asked for with cost.
func (t *staticThrottler) CheckLimitReached(ctx context.Context, key string, cost int64) (limited bool, retErr error) {
	if !t.config.enabled() {
		return false, nil
	}
	allowed, _, err := t.limiter.Allow(ctx, cost, t.config.limit.Load(), getWindowInSecs(t.config.window.Load()), key)
	if err != nil {
		return false, fmt.Errorf("could not limit: %w", err)
	}
	if !allowed {
		return true, nil
	}
	return false, nil
}

func (t *staticThrottler) getLimit() int64 {
	return t.config.limit.Load()
}

func (t *staticThrottler) getTimeWindow() time.Duration {
	return t.config.window.Load()
}

func (t *staticThrottler) ResponseCodeReceived(code int) {
	// no-op
}

func (t *staticThrottler) Shutdown() {
	// no-op
}
