package static

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/types"
)

// NewAllEventTypesThrottler constructs a new static throttler for all event types of a destination
func NewAllEventTypesThrottler(destType, destinationID string, limiter types.Limiter, config *config.Config, stat stats.Stats, log Logger) *throttler {
	return &throttler{
		destinationID: destinationID,
		eventType:     "all",
		key:           destinationID, // key is destinationID

		limiter: limiter,
		log:     log,
		limit: config.GetReloadableInt64Var(0, 1,
			fmt.Sprintf(`Router.throttler.%s.%s.limit`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.%s.limit`, destType),
		),
		window: config.GetReloadableDurationVar(0, time.Second,
			fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.%s.timeWindow`, destType),
		),
		staticCost: false,

		onceEveryGauge: kitsync.NewOnceEvery(time.Second),
		rateLimitGauge: stat.NewTaggedStat("throttling_rate_limit", stats.GaugeType, stats.Tags{
			"destinationId": destinationID,
			"destType":      destType,
			"adaptive":      "false",
		}),
	}
}
