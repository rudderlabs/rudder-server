package adaptive

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	throttlerconfig "github.com/rudderlabs/rudder-server/router/throttler/config"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/switcher"
	"github.com/rudderlabs/rudder-server/router/throttler/internal/types"
)

// NewThrottler constructs a new adaptive throttler that can switch between all event types and per event type throttling.
func NewThrottler(destType, destinationID, eventType string,
	perEventAlgorithm, allEventsAlgorithm Algorithm,
	limiter types.Limiter, config *config.Config, stat stats.Stats, log logger.Logger,
) types.Throttler {
	return switcher.NewThrottlerSwitcher(
		throttlerconfig.ThrottlerPerEventTypeEnabled(config, destType, destinationID),
		NewAllEventTypesThrottler(destType, destinationID, allEventsAlgorithm, limiter, config, stat, log.Withn(logger.NewStringField("eventType", "all"))),
		NewPerEventTypeThrottler(destType, destinationID, eventType, perEventAlgorithm, limiter, config, stat, log.Withn(logger.NewStringField("eventType", eventType))),
	)
}
