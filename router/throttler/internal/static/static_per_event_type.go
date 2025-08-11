package static

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/types"
)

// NewPerEventTypeThrottler constructs a new static throttler for a specific event type of a destination
func NewPerEventTypeThrottler(destType, destinationID, eventType string, limiter types.Limiter, config *config.Config, stat stats.Stats, log Logger) *throttler {
	return &throttler{
		destinationID: destinationID,
		eventType:     eventType,
		key:           destinationID + ":" + eventType, // key is destinationID + ":" + eventType

		limiter: limiter,
		log:     log,
		limit: config.GetReloadableInt64Var(0, 1,
			fmt.Sprintf(`Router.throttler.%s.%s.%s.limit`, destType, destinationID, eventType),
			fmt.Sprintf(`Router.throttler.%s.%s.limit`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.%s.%s.limit`, destType, eventType),
			fmt.Sprintf(`Router.throttler.%s.limit`, destType),
		),
		window: config.GetReloadableDurationVar(0, time.Second,
			fmt.Sprintf(`Router.throttler.%s.%s.%s.timeWindow`, destType, destinationID, eventType),
			fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destType, eventType),
			fmt.Sprintf(`Router.throttler.%s.timeWindow`, destType),
		),
		// static cost for per-event-type throttler: cost was originally introduced to address rate limit differences between different event types, so not needed when using per-event-type throttler
		staticCost: true,

		onceEveryGauge: kitsync.NewOnceEvery(time.Second),
		rateLimitGauge: stat.NewTaggedStat("throttling_rate_limit", stats.GaugeType, stats.Tags{
			"destinationId": destinationID,
			"destType":      destType,
			"eventType":     eventType,
			"adaptive":      "false",
		}),
	}
}
