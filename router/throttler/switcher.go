package throttler

import (
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type switchingThrottler struct {
	adaptiveEnabled misc.ValueLoader[bool]
	normal          Throttler
	adaptive        Throttler
}

func (t *switchingThrottler) CheckLimitReached(key string, cost int64) (limited bool, retErr error) {
	return t.throttler().CheckLimitReached(key, cost)
}

func (t *switchingThrottler) ResponseCodeReceived(code int) {
	t.throttler().ResponseCodeReceived(code)
}

func (t *switchingThrottler) ShutDown() {
	t.throttler().ShutDown()
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
