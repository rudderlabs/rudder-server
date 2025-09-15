package adaptive

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
)

// NewAllEventTypesThrottler constructs a new adaptive throttler for all event types of a destination
func NewAllEventTypesThrottler(destType, destinationID string, algorithm Algorithm, limiter Limiter, c *config.Config, stat stats.Stats, log Logger) *throttler {
	return &throttler{
		destinationID: destinationID,
		eventType:     "all",
		key:           destinationID,

		limiter:   limiter,
		algorithm: algorithm,
		log:       log,

		window: GetAllEventsWindowConfig(c, destType, destinationID),
		minLimit: c.GetReloadableInt64Var(1, 1,
			fmt.Sprintf(`Router.throttler.%s.%s.minLimit`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.%s.minLimit`, destType),
			`Router.throttler.minLimit`,
			// TODO: delete the following deprecated keys in the future
			fmt.Sprintf(`Router.throttler.adaptive.%s.%s.minLimit`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.adaptive.%s.minLimit`, destType),
			`Router.throttler.adaptive.minLimit`,
		),
		maxLimit: maxLimitFunc(c, destType, destinationID,
			[]string{
				fmt.Sprintf(`Router.throttler.%s.%s.maxLimit`, destType, destinationID),
				fmt.Sprintf(`Router.throttler.%s.maxLimit`, destType),
				`Router.throttler.maxLimit`,
				// TODO: delete the following deprecated keys in the future
				fmt.Sprintf(`Router.throttler.adaptive.%s.%s.maxLimit`, destType, destinationID),
				fmt.Sprintf(`Router.throttler.adaptive.%s.maxLimit`, destType),
				`Router.throttler.adaptive.maxLimit`,
			},
		),
		staticCost: c.GetReloadableBoolVar(true,
			`Router.throttler.adaptiveIgnoreThrottlingCosts`,
			`Router.throttler.adaptive.ignoreThrottlingCosts`, // TODO: delete this deprecated key in the future
			`Router.throttler.ignoreThrottlingCosts`,
		),

		everyGauge: kitsync.NewOnceEvery(time.Second),
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
}
