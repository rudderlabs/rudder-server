package adaptive

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
)

// NewAllEventTypesThrottler constructs a new adaptive throttler for all event types of a destination
func NewAllEventTypesThrottler(destType, destinationID string, algorithm Algorithm, limiter Limiter, config *config.Config, stat stats.Stats, log Logger) *throttler {
	return &throttler{
		destinationID: destinationID,
		eventType:     "all",
		key:           destinationID,

		limiter:   limiter,
		algorithm: algorithm,
		log:       log,

		window: GetAllEventsWindowConfig(config, destType, destinationID),
		minLimit: config.GetReloadableInt64Var(1, 1,
			fmt.Sprintf(`Router.throttler.adaptive.%s.%s.minLimit`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.adaptive.%s.minLimit`, destType),
			`Router.throttler.adaptive.minLimit`,
		),
		maxLimit: maxLimitFunc(config, destType, destinationID,
			[]string{
				fmt.Sprintf(`Router.throttler.adaptive.%s.%s.maxLimit`, destType, destinationID),
				fmt.Sprintf(`Router.throttler.adaptive.%s.maxLimit`, destType),
				`Router.throttler.adaptive.maxLimit`,
			},
		),
		staticCost: false,

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
