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
	if !t.config.enabled {
		return false, nil
	}
	if t.config.minLimit.Load() > t.config.maxLimit.Load() {
		return false, fmt.Errorf("minLimit %d is greater than maxLimit %d", t.config.minLimit.Load(), t.config.maxLimit.Load())
	}
	t.computeLimit()
	allowed, _, err := t.limiter.Allow(context.TODO(), cost, t.config.limit, getWindowInSecs(t.config.window.Load()), key)
	if err != nil {
		return false, fmt.Errorf("could not limit: %w", err)
	}
	if !allowed {
		return true, nil // no token to return when limited
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

func (t *adaptiveThrottler) ShutDown() {
	t.algorithm.ShutDown()
}

func (t *adaptiveThrottler) getLimit() int64 {
	t.computeLimit()
	return t.config.limit
}
