package throttler

import (
	"context"
	"fmt"
)

type adaptiveThrottler struct {
	limiter   limiter
	algorithm adaptiveAlgorithm
	config    adaptiveConfig
}

// CheckLimitReached returns true if we're not allowed to process the number of events we asked for with cost.
func (t *adaptiveThrottler) CheckLimitReached(key string, cost int64) (limited bool, retErr error) {
	if t.config.minLimit.Load() > t.config.maxLimit.Load() {
		return false, fmt.Errorf("minLimit %d is greater than maxLimit %d", t.config.minLimit.Load(), t.config.maxLimit.Load())
	}

	ctx := context.TODO()
	t.config.limit += int64(float64(t.config.limit) * t.algorithm.LimitFactor())
	t.config.limit = max(t.config.minLimit.Load(), min(t.config.limit, t.config.maxLimit.Load()))
	allowed, _, err := t.limiter.Allow(ctx, cost, t.config.limit, getWindowInSecs(t.config.window), key)
	if err != nil {
		return false, fmt.Errorf("could not limit: %w", err)
	}
	if !allowed {
		return true, nil // no token to return when limited
	}
	return false, nil
}

func (t *adaptiveThrottler) ResponseCodeReceived(code int) {
	t.algorithm.ResponseCodeReceived(code)
}

func (t *adaptiveThrottler) ShutDown() {
	t.algorithm.ShutDown()
}

func (t *adaptiveThrottler) getLimit() int64 {
	return t.config.limit
}
