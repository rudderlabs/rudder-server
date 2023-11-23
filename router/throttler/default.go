package throttler

import (
	"context"
	"fmt"
)

type defaultThrottler struct {
	limiter limiter
	config  normalConfig
}

// CheckLimitReached returns true if we're not allowed to process the number of events we asked for with cost.
func (t *defaultThrottler) CheckLimitReached(key string, cost int64) (limited bool, retErr error) {
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

func (t *defaultThrottler) getLimit() int64 {
	return t.config.limit
}

func (t *defaultThrottler) ResponseCodeReceived(code int) {
	// no-op
}

func (t *defaultThrottler) ShutDown() {
	// no-op
}
