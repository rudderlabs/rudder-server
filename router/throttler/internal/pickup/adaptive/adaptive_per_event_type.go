package adaptive

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
)

// NewPerEventTypeThrottler constructs a new adaptive throttler for a specific event type of a destination
func NewPerEventTypeThrottler(destType, destinationID, eventType string,
	algorithm Algorithm,
	limiter Limiter, c *config.Config, stat stats.Stats, log Logger,
) *throttler {
	return &throttler{
		destinationID: destinationID,
		eventType:     eventType,
		key:           destinationID + ":" + eventType, // key is destinationID + ":" + eventType

		limiter:   limiter,
		algorithm: algorithm,
		log:       log,

		window: GetPerEventWindowConfig(c, destType, destinationID, eventType),
		minLimit: c.GetReloadableInt64Var(1, 1,
			fmt.Sprintf(`Router.throttler.%s.%s.%s.minLimit`, destType, destinationID, eventType),
			fmt.Sprintf(`Router.throttler.%s.%s.minLimit`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.%s.%s.minLimit`, destType, eventType),
			fmt.Sprintf(`Router.throttler.%s.minLimit`, destType),
			`Router.throttler.minLimit`,
		),
		maxLimit: maxLimitFunc(c, destType, destinationID,
			[]string{
				fmt.Sprintf(`Router.throttler.%s.%s.%s.maxLimit`, destType, destinationID, eventType),
				fmt.Sprintf(`Router.throttler.%s.%s.maxLimit`, destType, destinationID),
				fmt.Sprintf(`Router.throttler.%s.%s.maxLimit`, destType, eventType),
				fmt.Sprintf(`Router.throttler.%s.maxLimit`, destType),
				`Router.throttler.maxLimit`,
			},
		),
		// static cost for per-event-type throttler: cost was originally introduced to address rate limit differences between different event types, so not needed when using per-event-type throttler
		staticCost: config.SingleValueLoader(true),

		everyStats: kitsync.NewOnceEvery(200 * time.Millisecond),
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
}
