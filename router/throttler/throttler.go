package throttler

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
)

type limiter interface {
	// Allow returns true if the limit is not exceeded, false otherwise.
	Allow(ctx context.Context, cost, rate, window int64, key string) (bool, func(context.Context) error, error)
}

type Factory struct {
	Limiter limiter
}

func (f *Factory) New(destName, destID string) *Throttler {
	var conf throttlingConfig
	conf.readThrottlingConfig(destName, destID)
	return &Throttler{
		limiter: f.Limiter,
		config:  conf,
	}
}

type Throttler struct {
	limiter limiter
	config  throttlingConfig
}

// CheckLimitReached returns true if we're not allowed to process the number of events we asked for with cost.
func (t *Throttler) CheckLimitReached(key string, cost int64) (limited bool, retErr error) {
	if !t.config.enabled {
		return false, nil
	}

	ctx := context.TODO()
	allowed, _, err := t.limiter.Allow(ctx, cost, t.config.limit, getWindowInSecs(t.config.window), key)
	if err != nil {
		return false, fmt.Errorf("could not limit: %w", err)
	}
	if !allowed {
		return true, nil // no token to return when limited
	}
	return false, nil
}

type throttlingConfig struct {
	enabled bool
	limit   int64
	window  time.Duration
}

func (c *throttlingConfig) readThrottlingConfig(destName, destID string) {
	if config.IsSet(fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID)) {
		c.limit = config.GetInt64(fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID), 0)
	} else {
		c.limit = config.GetInt64(fmt.Sprintf(`Router.throttler.%s.limit`, destName), 0)
	}

	if config.IsSet(fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destName, destID)) {
		c.window = config.GetDuration(fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destName, destID), 0, time.Second)
	} else {
		c.window = config.GetDuration(fmt.Sprintf(`Router.throttler.%s.timeWindow`, destName), 0, time.Second)
	}

	// enable dest throttler
	if c.limit > 0 && c.window > 0 {
		c.enabled = true
	}
}

func getWindowInSecs(d time.Duration) int64 {
	return int64(d.Seconds())
}
