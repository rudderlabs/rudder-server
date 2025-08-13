package config

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"
)

// ThrottlerPerEventTypeEnabled returns a reloadable boolean indicating whether per event type throttling is enabled for the given destination type and ID.
func ThrottlerPerEventTypeEnabled(c *config.Config, destType, destinationID string) *config.Reloadable[bool] {
	return c.GetReloadableBoolVar(false,
		fmt.Sprintf(`Router.throttler.%s.%s.throttlerPerEventType`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.%s.throttlerPerEventType`, destType),
		"Router.throttler.throttlerPerEventType",
	)
}
