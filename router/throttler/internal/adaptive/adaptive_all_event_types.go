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

// NewAllEventTypesThrottler constructs a new adaptive throttler for all event types of a destination
func NewAllEventTypesThrottler(destType, destinationID string, algorithm Algorithm, limiter types.Limiter, config *config.Config, stat stats.Stats) *allEventTypesThrottler {
	t := &allEventTypesThrottler{
		destinationID: destinationID,
		limiter:       limiter,
		algorithm:     algorithm,
		every:         sync.NewEvery(time.Second),
		limitFactorGauge: stat.NewTaggedStat("adaptive_throttler_limit_factor", stats.GaugeType, stats.Tags{
			"destinationId": destinationID,
			"destType":      destType,
		}),
		rateLimitGauge: stat.NewTaggedStat("throttling_rate_limit", stats.GaugeType, stats.Tags{
			"destinationId": destinationID,
			"destType":      destType,
			"adaptive":      "true",
		}),
	}
	t.window = GetAllEventsWindowConfig(config, destType, destinationID)
	t.minLimit = config.GetReloadableInt64Var(1, 1,
		fmt.Sprintf(`Router.throttler.adaptive.%s.%s.minLimit`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.adaptive.%s.minLimit`, destType),
		`Router.throttler.adaptive.minLimit`,
	)
	t.maxLimit = maxLimitFunc(config, destType, destinationID,
		[]string{
			fmt.Sprintf(`Router.throttler.adaptive.%s.%s.maxLimit`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.adaptive.%s.maxLimit`, destType),
			`Router.throttler.adaptive.maxLimit`,
		},
	)

	return t
}

type allEventTypesThrottler struct {
	destinationID string

	limiter   types.Limiter
	algorithm Algorithm

	window   config.ValueLoader[time.Duration]
	minLimit config.ValueLoader[int64]
	maxLimit func() int64

	every            *sync.Every
	limitFactorGauge stats.Gauge
	rateLimitGauge   stats.Gauge
}

func (t *allEventTypesThrottler) validConfiguration() bool {
	return t.minLimit.Load() > 0 && t.maxLimit() > 0 && t.window.Load() > 0 && t.minLimit.Load() <= t.maxLimit()
}

func (t *allEventTypesThrottler) CheckLimitReached(ctx context.Context, cost int64) (limited bool, retErr error) {
	t.updateGauges()
	if !t.validConfiguration() {
		return false, nil
	}
	allowed, _, err := t.limiter.Allow(ctx, cost, t.GetLimit(), t.getTimeWindowInSeconds(), t.destinationID)
	if err != nil {
		return false, fmt.Errorf("throttling failed for %s: %w", t.destinationID, err)
	}
	return !allowed, nil
}

func (t *allEventTypesThrottler) ResponseCodeReceived(code int) {
	t.algorithm.ResponseCodeReceived(code)
}

func (t *allEventTypesThrottler) Shutdown() {
	t.algorithm.Shutdown()
}

func (t *allEventTypesThrottler) getLimitFactor() float64 {
	return t.algorithm.LimitFactor()
}

func (t *allEventTypesThrottler) getMinLimit() int64 {
	return t.minLimit.Load()
}

func (t *allEventTypesThrottler) getMaxLimit() int64 {
	return t.maxLimit()
}

func (t *allEventTypesThrottler) GetLimit() int64 {
	limit := int64(float64(t.getMaxLimit()) * t.getLimitFactor())
	return max(t.getMinLimit(), limit)
}

func (t *allEventTypesThrottler) getTimeWindowInSeconds() int64 {
	return int64(t.window.Load().Seconds())
}

func (t *allEventTypesThrottler) updateGauges() {
	t.every.Do(func() {
		if window := t.getTimeWindowInSeconds(); window > 0 {
			t.rateLimitGauge.Gauge(t.GetLimit() / window)
		}
		t.limitFactorGauge.Gauge(t.getLimitFactor())
	})
}
