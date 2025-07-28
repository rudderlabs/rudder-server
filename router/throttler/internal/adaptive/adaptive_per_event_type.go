package adaptive

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/sync"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/types"
)

// NewPerEventTypeThrottler constructs a new adaptive throttler for a specific event type of a destination
func NewPerEventTypeThrottler(destType, destinationID, eventType string,
	perEventAlgorithm Algorithm,
	limiter types.Limiter, config *config.Config, stat stats.Stats,
) *perEventTypeThrottler {
	t := &perEventTypeThrottler{
		destinationID: destinationID,
		eventType:     eventType,
		key:           destinationID + ":" + eventType, // key is destinationID + ":" + eventType
		limiter:       limiter,
		algorithm:     perEventAlgorithm,
		every:         sync.NewEvery(time.Second),
		limitFactorGauge: stat.NewTaggedStat("adaptive_throttler_limit_factor", stats.GaugeType, stats.Tags{
			"destinationId": destinationID,
			"eventType":     eventType,
			"destType":      destType,
		}),
		rateLimitGauge: stat.NewTaggedStat("throttling_rate_limit", stats.GaugeType, stats.Tags{
			"destinationId": destinationID,
			"destType":      destType,
			"eventType":     eventType,
			"adaptive":      "true",
		}),
	}

	t.window = GetPerEventWindowConfig(config, destType, destinationID, eventType)
	t.minLimit = config.GetReloadableInt64Var(1, 1,
		fmt.Sprintf(`Router.throttler.adaptive.%s.%s.%s.minLimit`, destType, destinationID, eventType),
		fmt.Sprintf(`Router.throttler.adaptive.%s.%s.minLimit`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.adaptive.%s.%s.minLimit`, destType, eventType),
		fmt.Sprintf(`Router.throttler.adaptive.%s.minLimit`, destType),
		`Router.throttler.adaptive.minLimit`,
	)
	t.maxLimit = maxLimitFunc(config, destType, destinationID,
		[]string{
			fmt.Sprintf(`Router.throttler.adaptive.%s.%s.%s.maxLimit`, destType, destinationID, eventType),
			fmt.Sprintf(`Router.throttler.adaptive.%s.%s.maxLimit`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.adaptive.%s.%s.maxLimit`, destType, eventType),
			fmt.Sprintf(`Router.throttler.adaptive.%s.maxLimit`, destType),
			`Router.throttler.adaptive.maxLimit`,
		},
	)

	return t
}

type perEventTypeThrottler struct {
	destinationID string
	eventType     string
	key           string // key is destinationID + ":" + eventType

	limiter   types.Limiter
	algorithm Algorithm

	window   config.ValueLoader[time.Duration]
	minLimit config.ValueLoader[int64]
	maxLimit func() int64

	every            *sync.Every
	limitFactorGauge stats.Gauge
	rateLimitGauge   stats.Gauge
}

func (t *perEventTypeThrottler) validConfiguration() bool {
	return t.minLimit.Load() > 0 && t.maxLimit() > 0 && t.window.Load() > 0 && t.minLimit.Load() <= t.maxLimit()
}

// CheckLimitReached returns true if we're not allowed to process the number of events we asked for with cost.
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

func (t *perEventTypeThrottler) ResponseCodeReceived(code int) {
	t.algorithm.ResponseCodeReceived(code)
}

func (t *perEventTypeThrottler) Shutdown() {
	t.algorithm.Shutdown()
}

func (t *perEventTypeThrottler) getLimitFactor() float64 {
	return t.algorithm.LimitFactor()
}

func (t *perEventTypeThrottler) getMinLimit() int64 {
	return t.minLimit.Load()
}

func (t *perEventTypeThrottler) getMaxLimit() int64 {
	return t.maxLimit()
}

func (t *perEventTypeThrottler) GetLimit() int64 {
	limit := int64(float64(t.getMaxLimit()) * t.getLimitFactor())
	return max(t.getMinLimit(), limit)
}

func (t *perEventTypeThrottler) getTimeWindowInSeconds() int64 {
	return int64(t.window.Load().Seconds())
}

func (t *perEventTypeThrottler) updateGauges() {
	t.every.Do(func() {
		if window := t.getTimeWindowInSeconds(); window > 0 {
			t.rateLimitGauge.Gauge(t.GetLimit() / window)
		}
		t.limitFactorGauge.Gauge(t.getLimitFactor())
	})
}
