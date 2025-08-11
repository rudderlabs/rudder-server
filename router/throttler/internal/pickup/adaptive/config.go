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
		`Router.throttler.adaptive.timeWindow`,
	)
}

func GetPerEventWindowConfig(config *config.Config, destType, destinationID, eventType string) config.ValueLoader[time.Duration] {
	return config.GetReloadableDurationVar(1, time.Second,
		fmt.Sprintf(`Router.throttler.%s.%s.%s.timeWindow`, destType, destinationID, eventType),
		fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destType, eventType),
		fmt.Sprintf(`Router.throttler.%s.timeWindow`, destType),
		`Router.throttler.adaptive.timeWindow`,
	)
}

func maxLimitFunc(config *config.Config, destType, destinationID string, maxLimitKeys []string) func() int64 {
	maxLimit := config.GetReloadableInt64Var(0, 1, maxLimitKeys...)
	limitMultiplier := config.GetReloadableFloat64Var(1.5,
		fmt.Sprintf(`Router.throttler.adaptive.%s.%s.limitMultiplier`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.adaptive.%s.limitMultiplier`, destType),
		`Router.throttler.adaptive.limitMultiplier`,
	)
	staticLimit := config.GetReloadableInt64Var(0, 1,
		fmt.Sprintf(`Router.throttler.%s.%s.limit`, destType, destinationID),
		fmt.Sprintf(`Router.throttler.%s.limit`, destType),
	)
	defaultMaxLimit := config.GetReloadableInt64Var(DefaultMaxThrottlingLimit, 1, `Router.throttler.adaptive.defaultMaxLimit`)
	return func() int64 {
		maxLimit := maxLimit.Load()
		if maxLimit > 0 {
			return maxLimit
		} else {
			staticLimit := staticLimit.Load()
			staticLimitMultiplier := limitMultiplier.Load()
			if staticLimit > 0 && staticLimitMultiplier > 0 {
				return int64(float64(staticLimit) * staticLimitMultiplier)
			}
		}
		return defaultMaxLimit.Load()
	}
}
