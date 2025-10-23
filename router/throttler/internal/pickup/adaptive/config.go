package adaptive

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

func GetAllEventsWindowConfig(config *config.Config, destType, destinationID string) config.ValueLoader[time.Duration] {
	return config.GetReloadableDurationVar(1, time.Second,
		fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.%s.timeWindow`, destType),
	)
}

func GetPerEventWindowConfig(config *config.Config, destType, destinationID, eventType string) config.ValueLoader[time.Duration] {
	return config.GetReloadableDurationVar(1, time.Second,
		fmt.Sprintf(`Router.throttler.%s.%s.%s.timeWindow`, destType, destinationID, eventType),
		fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destType, eventType),
		fmt.Sprintf(`Router.throttler.%s.timeWindow`, destType),
	)
}
