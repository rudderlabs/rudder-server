package throttler

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type normalConfig struct {
	enabled bool
	limit   int64
	window  time.Duration
}

func (c *normalConfig) readThrottlingConfig(destName, destID string) {
	if config.IsSet(fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID)) {
		c.limit = config.GetInt64(fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID), 0)
	} else {
		c.limit = config.GetInt64(fmt.Sprintf(`Router.throttler.%s.limit`, destName), 0)
	}

	if config.IsSet(fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destName, destID)) {
		c.window = config.GetDuration(fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destName, destID), 0, time.Second)
	} else {
		c.window = config.GetDuration(fmt.Sprintf(`Router.throttler.%s.timeWindow`, destName), 0, time.Second)
	}

	// enable dest throttler
	if c.limit > 0 && c.window > 0 {
		c.enabled = true
	}
}

type adaptiveConfig struct {
	normalConfig
	minLimit *config.Reloadable[int64]
	maxLimit *config.Reloadable[int64]
}

func (c *adaptiveConfig) readThrottlingConfig(destName, destID string) {
	c.normalConfig.readThrottlingConfig(destName, destID)
	c.minLimit = config.GetReloadableInt64Var(1, 1, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.minLimit`, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.minLimit`, destName), `Router.throttler.adaptiveRateLimit.minLimit`, fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID), fmt.Sprintf(`Router.throttler.%s.limit`, destName))
	c.maxLimit = config.GetReloadableInt64Var(250, 1, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.maxLimit`, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.maxLimit`, destName), `Router.throttler.adaptiveRateLimit.maxLimit`, fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID), fmt.Sprintf(`Router.throttler.%s.limit`, destName))
}
