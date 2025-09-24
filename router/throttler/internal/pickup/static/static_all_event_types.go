package static

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
)

// NewAllEventTypesThrottler constructs a new static throttler for all event types of a destination
func NewAllEventTypesThrottler(destType, destinationID string, limiter Limiter, c *config.Config, stat stats.Stats, log Logger) *throttler {
	return &throttler{
		destinationID: destinationID,
		eventType:     "all",
		key:           destinationID, // key is destinationID

		limiter: limiter,
		log:     log,
		limit: c.GetReloadableInt64Var(0, 1,
			fmt.Sprintf(`Router.throttler.%s.%s.limit`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.%s.limit`, destType),
		),
		window: c.GetReloadableDurationVar(0, time.Second,
			fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.%s.timeWindow`, destType),
		),
		staticCost: c.GetReloadableBoolVar(true, `Router.throttler.ignoreThrottlingCosts`),

		everyStats: kitsync.NewOnceEvery(200 * time.Millisecond),
		rateLimitGauge: stat.NewTaggedStat("throttling_rate_limit", stats.GaugeType, stats.Tags{
			"destinationId": destinationID,
			"destType":      destType,
			"adaptive":      "false",
		}),
	}
}
