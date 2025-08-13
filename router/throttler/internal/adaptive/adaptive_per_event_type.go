package adaptive

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/types"
)

// NewPerEventTypeThrottler constructs a new adaptive throttler for a specific event type of a destination
func NewPerEventTypeThrottler(destType, destinationID, eventType string,
	algorithm Algorithm,
	limiter types.Limiter, config *config.Config, stat stats.Stats, log Logger,
) *throttler {
	return &throttler{
		destinationID: destinationID,
		eventType:     eventType,
		key:           destinationID + ":" + eventType, // key is destinationID + ":" + eventType

		limiter:   limiter,
		algorithm: algorithm,
		log:       log,

		window: GetPerEventWindowConfig(config, destType, destinationID, eventType),
		minLimit: config.GetReloadableInt64Var(1, 1,
			fmt.Sprintf(`Router.throttler.adaptive.%s.%s.%s.minLimit`, destType, destinationID, eventType),
			fmt.Sprintf(`Router.throttler.adaptive.%s.%s.minLimit`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.adaptive.%s.%s.minLimit`, destType, eventType),
			fmt.Sprintf(`Router.throttler.adaptive.%s.minLimit`, destType),
			`Router.throttler.adaptive.minLimit`,
		),
		maxLimit: maxLimitFunc(config, destType, destinationID,
			[]string{
				fmt.Sprintf(`Router.throttler.adaptive.%s.%s.%s.maxLimit`, destType, destinationID, eventType),
				fmt.Sprintf(`Router.throttler.adaptive.%s.%s.maxLimit`, destType, destinationID),
				fmt.Sprintf(`Router.throttler.adaptive.%s.%s.maxLimit`, destType, eventType),
				fmt.Sprintf(`Router.throttler.adaptive.%s.maxLimit`, destType),
				`Router.throttler.adaptive.maxLimit`,
			},
		),
		// static cost for per-event-type throttler: cost was originally introduced to address rate limit differences between different event types, so not needed when using per-event-type throttler
		staticCost: true,

		everyGauge: kitsync.NewOnceEvery(time.Second),
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
