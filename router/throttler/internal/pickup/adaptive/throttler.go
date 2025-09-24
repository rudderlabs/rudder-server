package adaptive

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/atomic"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
)

type throttler struct {
	destinationID string
	eventType     string
	key           string

	lastUsed  atomic.Time
	limiter   Limiter
	algorithm Algorithm
	log       Logger

	window     config.ValueLoader[time.Duration]
	minLimit   config.ValueLoader[int64]
	maxLimit   func() int64
	staticCost config.ValueLoader[bool]

	everyStats       *kitsync.OnceEvery
	limitFactorGauge stats.Gauge
	rateLimitGauge   stats.Gauge
}

func (t *throttler) enabled() bool {
	return t.minLimit.Load() > 0 && t.maxLimit() > 0 && t.window.Load() > 0 && t.minLimit.Load() <= t.maxLimit()
}

func (t *throttler) CheckLimitReached(ctx context.Context, cost int64) (limited bool, retErr error) {
	if !t.enabled() {
		return false, nil
	}
	t.updateStats()

	allowed, _, err := t.limiter.Allow(ctx, t.costFn(cost), t.getLimit(), t.getTimeWindowInSeconds(), t.key)
	if err != nil {
		return false, fmt.Errorf("throttling failed for %s: %w", t.key, err)
	}
	return !allowed, nil
}

func (t *throttler) ResponseCodeReceived(code int) {
	t.algorithm.ResponseCodeReceived(code)
}

func (t *throttler) Shutdown() {
	t.algorithm.Shutdown()
}

func (t *throttler) getLimitFactor() float64 {
	return t.algorithm.LimitFactor()
}

func (t *throttler) getMinLimit() int64 {
	return t.minLimit.Load()
}

func (t *throttler) getMaxLimit() int64 {
	return t.maxLimit()
}

func (t *throttler) getLimit() int64 {
	limit := int64(float64(t.getMaxLimit()) * t.getLimitFactor())
	return max(t.getMinLimit(), limit)
}

func (t *throttler) GetLimitPerSecond() int64 {
	if window := t.getTimeWindowInSeconds(); window > 0 {
		return (t.getLimit() + window - 1) / window // ceiling division
	}
	return 0
}

func (t *throttler) GetEventType() string {
	return t.eventType
}

func (t *throttler) GetLastUsed() time.Time {
	return t.lastUsed.Load()
}

func (t *throttler) getTimeWindowInSeconds() int64 {
	return int64(t.window.Load().Seconds())
}

func (t *throttler) updateStats() {
	t.everyStats.Do(func() {
		t.lastUsed.Store(time.Now())
		if window := t.getTimeWindowInSeconds(); window > 0 {
			t.rateLimitGauge.Gauge(t.getLimit() / window)
		}
		t.limitFactorGauge.Gauge(t.getLimitFactor())
	})
}

func (t *throttler) costFn(input int64) int64 {
	if t.staticCost.Load() {
		return 1
	}
	return input
}
