package throttler

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type switchingThrottler struct {
	adaptiveEnabled config.ValueLoader[bool]
	static          Throttler
	adaptive        Throttler
}

func (t *switchingThrottler) CheckLimitReached(ctx context.Context, key string, cost int64) (limited bool, retErr error) {
	return t.throttler().CheckLimitReached(ctx, key, cost)
}

func (t *switchingThrottler) ResponseCodeReceived(code int) {
	t.static.ResponseCodeReceived(code)
	t.adaptive.ResponseCodeReceived(code)
}

func (t *switchingThrottler) Shutdown() {
	t.static.Shutdown()
	t.adaptive.Shutdown()
}

func (t *switchingThrottler) getLimit() int64 {
	return t.throttler().getLimit()
}

func (t *switchingThrottler) getTimeWindow() time.Duration {
	return t.throttler().getTimeWindow()
}

func (t *switchingThrottler) throttler() Throttler {
	if t.adaptiveEnabled.Load() {
		return t.adaptive
	}
	return t.static
}
