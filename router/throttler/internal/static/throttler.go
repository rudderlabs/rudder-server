package static

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/types"
)

type throttler struct {
	destinationID string
	eventType     string
	key           string

	limiter    types.Limiter
	log        Logger
	limit      config.ValueLoader[int64]
	window     config.ValueLoader[time.Duration]
	staticCost bool

	onceEveryInvalidConfig *kitsync.OnceEvery
	onceEveryGauge         *kitsync.OnceEvery
	rateLimitGauge         stats.Gauge
}

func (t *throttler) CheckLimitReached(ctx context.Context, cost int64) (limited bool, retErr error) {
	t.updateGauges()
	if !t.validConfiguration() {
		t.logInvalidConfigWarning()
		return false, nil
	}
	allowed, _, err := t.limiter.Allow(ctx, t.costFn(cost), t.GetLimit(), t.getTimeWindowInSeconds(), t.key)
	if err != nil {
		return false, fmt.Errorf("throttling failed for %s: %w", t.key, err)
	}
	return !allowed, nil
}

func (t *throttler) validConfiguration() bool {
	return t.limit.Load() > 0 && t.window.Load() > 0
}

func (t *throttler) GetLimit() int64 {
	return t.limit.Load()
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
			t.rateLimitGauge.Gauge(t.GetLimit() / window)
		}
	})
}

func (t *throttler) logInvalidConfigWarning() {
	t.onceEveryInvalidConfig.Do(func() {
		t.log.Warnn("Invalid configuration detected",
			logger.NewIntField("limit", t.GetLimit()),
			logger.NewDurationField("window", t.window.Load()),
		)
	})
}

func (t *throttler) costFn(input int64) int64 {
	if t.staticCost {
		return 1
	}
	return input
}
