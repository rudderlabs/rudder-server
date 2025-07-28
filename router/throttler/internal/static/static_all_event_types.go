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

// NewAllEventTypesThrottler constructs a new static throttler for all event types of a destination
func NewAllEventTypesThrottler(destType, destinationID string, limiter types.Limiter, config *config.Config, stat stats.Stats) *allEventTypesThrottler {
	st := &allEventTypesThrottler{
		limiter: limiter,
		every:   sync.NewEvery(time.Second),
		rateLimitGauge: stat.NewTaggedStat("throttling_rate_limit", stats.GaugeType, stats.Tags{
			"destinationId": destinationID,
			"destType":      destType,
			"adaptive":      "false",
		}),
	}
	st.destinationID = destinationID
	st.limit = config.GetReloadableInt64Var(0, 1,
		fmt.Sprintf(`Router.throttler.%s.%s.limit`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.%s.limit`, destType),
	)
	st.window = config.GetReloadableDurationVar(0, time.Second,
		fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.%s.timeWindow`, destType),
	)
	return st
}

type allEventTypesThrottler struct {
	limiter       types.Limiter
	destinationID string
	limit         config.ValueLoader[int64]
	window        config.ValueLoader[time.Duration]

	every          *sync.Every
	rateLimitGauge stats.Gauge
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

func (t *allEventTypesThrottler) validConfiguration() bool {
	return t.limit.Load() > 0 && t.window.Load() > 0
}

func (t *allEventTypesThrottler) GetLimit() int64 {
	return t.limit.Load()
}

func (t *allEventTypesThrottler) getTimeWindowInSeconds() int64 {
	return int64(t.window.Load().Seconds())
}

func (t *allEventTypesThrottler) ResponseCodeReceived(code int) {
	// no-op
}

func (t *allEventTypesThrottler) Shutdown() {
	// no-op
}

func (t *allEventTypesThrottler) updateGauges() {
	t.every.Do(func() {
		if window := t.getTimeWindowInSeconds(); window > 0 {
			t.rateLimitGauge.Gauge(t.GetLimit() / window)
		}
	})
}
