package static

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/switcher"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/types"
)

// NewThrottler constructs a new static throttler that can switch between all event types and per event type throttling.
func NewThrottler(destType, destinationID, eventType string, limiter types.Limiter, config *config.Config, stat stats.Stats, log logger.Logger) types.Throttler {
	return switcher.NewThrottlerSwitcher(
		config.GetReloadableBoolVar(false,
			fmt.Sprintf(`Router.throttler.%s.%s.throttlerPerEventType`, destType, destinationID),
			fmt.Sprintf(`Router.throttler.%s.throttlerPerEventType`, destType),
			"Router.throttler.throttlerPerEventType",
		),
		NewAllEventTypesThrottler(destType, destinationID, limiter, config, stat, log.Withn(logger.NewStringField("eventType", "all"))),
		NewPerEventTypeThrottler(destType, destinationID, eventType, limiter, config, stat, log.Withn(logger.NewStringField("eventType", eventType))),
	)
}
