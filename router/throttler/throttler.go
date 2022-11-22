package throttler

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/internal/throttling"
)

type Factory struct {
	Limiter *throttling.Limiter
}

func (f *Factory) New(destName, destID string) *Throttler {
	var conf throttlingConfig
	conf.readThrottlingConfig(destName, destID)
	return &Throttler{
		limiter: f.Limiter,
		config:  conf,
	}
}

type Throttler struct {
	limiter *throttling.Limiter
	config  throttlingConfig
}

// CheckLimitReached returns true if we're not allowed to process the number of events we asked for with cost.
// Along with the boolean, it also returns a TokenReturner and an error. The TokenReturner should be called to return
// the tokens to the limiter (bucket) in the eventuality that we did not move forward with the request.
func (t *Throttler) CheckLimitReached(cost int64) (limited bool, tr throttling.TokenReturner, retErr error) {
	if !t.config.enabled {
		return false, nil, nil
	}

	ctx := context.TODO()
	rateLimitingKey := t.config.destID // TODO add workspace id here
	allowed, tr, err := t.limiter.Limit(
		ctx, cost, t.config.limit, getWindowInSecs(t.config.window), rateLimitingKey,
	)
	if err != nil {
		err = fmt.Errorf(`[[ %s-router-throttler: Error checking limitStatus: %w]]`, t.config.destID, err)
		return false, nil, err
	}
	if !allowed {
		return true, nil, nil // no token to return when limited
	}
	return false, tr, nil
}

type throttlingConfig struct {
	enabled          bool
	limit            int64
	window           time.Duration
	destName, destID string
}

func (c *throttlingConfig) readThrottlingConfig(destName, destID string) {
	c.destName = destName
	c.destID = destID

	if config.IsSet(fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID)) {
		config.RegisterInt64ConfigVariable(
			0, &c.limit, false, 1, fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID),
		)
	} else {
		config.RegisterInt64ConfigVariable(0, &c.limit, false, 1, fmt.Sprintf(`Router.throttler.%s.limit`, destName))
	}

	if config.IsSet(fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destName, destID)) ||
		config.IsSet(fmt.Sprintf(`Router.throttler.%s.%s.timeWindowInS`, destName, destID)) {
		config.RegisterDurationConfigVariable(
			0, &c.window, false, time.Second,
			fmt.Sprintf(`Router.throttler.%s.%s.timeWindow`, destName, destID),
			fmt.Sprintf(`Router.throttler.%s.%s.timeWindowInS`, destName, destID),
		)
	} else {
		config.RegisterDurationConfigVariable(
			0, &c.window, false, time.Second,
			fmt.Sprintf(`Router.throttler.%s.timeWindow`, destName),
			fmt.Sprintf(`Router.throttler.%s.timeWindowInS`, destName),
		)
	}

	// enable dest throttler
	if c.limit != 0 && c.window != 0 {
		c.enabled = true
	}
}

func getWindowInSecs(d time.Duration) int64 {
	return int64(d.Seconds())
}
