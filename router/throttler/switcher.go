package throttler

import (
	"context"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

type switchingThrottler struct {
	adaptiveEnabled misc.ValueLoader[bool]
	normal          Throttler
	adaptive        Throttler
}

func (t *switchingThrottler) CheckLimitReached(ctx context.Context, key string, cost int64) (limited bool, retErr error) {
	return t.throttler().CheckLimitReached(ctx, key, cost)
}

func (t *switchingThrottler) ResponseCodeReceived(code int) {
	t.throttler().ResponseCodeReceived(code)
}

func (t *switchingThrottler) Shutdown() {
	t.normal.Shutdown()
	t.adaptive.Shutdown()
}

func (t *switchingThrottler) getLimit() int64 {
	return t.throttler().getLimit()
}

func (t *switchingThrottler) throttler() Throttler {
	if t.adaptiveEnabled.Load() {
		return t.adaptive
	}
	return t.normal
}
