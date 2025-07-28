package static

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/sync"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/types"
)

// NewPerEventTypeThrottler constructs a new static throttler for a specific event type of a destination
func NewPerEventTypeThrottler(destType, destinationID, eventType string, limiter types.Limiter, config *config.Config, stat stats.Stats) *perEventTypeThrottler {
	st := &perEventTypeThrottler{
		destinationID: destinationID,
		eventType:     eventType,
		key:           destinationID + ":" + eventType, // key is destinationID + ":" + eventType
		limiter:       limiter,
		every:         sync.NewEvery(time.Second),
		rateLimitGauge: stat.NewTaggedStat("throttling_rate_limit", stats.GaugeType, stats.Tags{
			"destinationId": destinationID,
			"destType":      destType,
			"eventType":     eventType,
			"adaptive":      "false",
		}),
	}
	st.limit = config.GetReloadableInt64Var(0, 1,
		fmt.Sprintf(`Router.throttler.%s.%s.%s.limit`, destType, destinationID, eventType),
		fmt.Sprintf(`Router.throttler.%s.%s.limit`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.%s.%s.limit`, destType, eventType),
		fmt.Sprintf(`Router.throttler.%s.limit`, destType),
	)
	st.window = config.GetReloadableDurationVar(0, time.Second,
		fmt.Sprintf(`Router.throttler.%s.%s.%s.timeWindow`, destType, destinationID, eventType),
		fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destType, eventType),
		fmt.Sprintf(`Router.throttler.%s.timeWindow`, destType),
	)
	return st
}

type perEventTypeThrottler struct {
	destinationID string
	eventType     string
	key           string // key is destinationID + ":" + eventType

	limiter types.Limiter
	limit   config.ValueLoader[int64]
	window  config.ValueLoader[time.Duration]

	every          *sync.Every
	rateLimitGauge stats.Gauge
}

func (t *perEventTypeThrottler) CheckLimitReached(ctx context.Context, _ int64) (limited bool, retErr error) {
	t.updateGauges()
	if !t.validConfiguration() {
		return false, nil
	}
	staticCost := int64(1) // cost should always be 1, since cost was supposed to address differences between different event types in a single throttler
	allowed, _, err := t.limiter.Allow(ctx, staticCost, t.GetLimit(), t.getTimeWindowInSeconds(), t.key)
	if err != nil {
		return false, fmt.Errorf("throttling failed for %s: %w", t.key, err)
	}
	return !allowed, nil
}

func (t *perEventTypeThrottler) validConfiguration() bool {
	return t.limit.Load() > 0 && t.window.Load() > 0
}

func (t *perEventTypeThrottler) GetLimit() int64 {
	return t.limit.Load()
}

func (t *perEventTypeThrottler) getTimeWindowInSeconds() int64 {
	return int64(t.window.Load().Seconds())
}

func (t *perEventTypeThrottler) ResponseCodeReceived(code int) {
	// no-op
}

func (t *perEventTypeThrottler) Shutdown() {
	// no-op
}

func (t *perEventTypeThrottler) updateGauges() {
	t.every.Do(func() {
		if window := t.getTimeWindowInSeconds(); window > 0 {
			t.rateLimitGauge.Gauge(t.GetLimit() / window)
		}
	})
}
