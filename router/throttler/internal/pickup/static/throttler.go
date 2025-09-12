package static

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
)

type throttler struct {
	destinationID string
	eventType     string
	key           string

	limiter    Limiter
	log        Logger
	limit      config.ValueLoader[int64]
	window     config.ValueLoader[time.Duration]
	staticCost config.ValueLoader[bool]

	onceEveryGauge *kitsync.OnceEvery
	rateLimitGauge stats.Gauge
}

func (t *throttler) CheckLimitReached(ctx context.Context, cost int64) (limited bool, retErr error) {
	if !t.enabled() {
		return false, nil
	}
	t.updateGauges()
	allowed, _, err := t.limiter.Allow(ctx, t.costFn(cost), t.getLimit(), t.getTimeWindowInSeconds(), t.key)
	if err != nil {
		return false, fmt.Errorf("throttling failed for %s: %w", t.key, err)
	}
	return !allowed, nil
}

func (t *throttler) enabled() bool {
	return t.limit.Load() > 0 && t.window.Load() > 0
}

func (t *throttler) getLimit() int64 {
	return t.limit.Load()
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

func (t *throttler) getTimeWindowInSeconds() int64 {
	return int64(t.window.Load().Seconds())
}

func (t *throttler) ResponseCodeReceived(code int) {
	// no-op
}

func (t *throttler) Shutdown() {
	// no-op
}

func (t *throttler) updateGauges() {
	t.onceEveryGauge.Do(func() {
		if window := t.getTimeWindowInSeconds(); window > 0 {
			t.rateLimitGauge.Gauge(t.getLimit() / window)
		}
	})
}

func (t *throttler) costFn(input int64) int64 {
	if t.staticCost.Load() {
		return 1
	}
	return input
}
