package throttling

import (
	"math"
	"time"
)

type getter interface {
	Get(key string) (interface{}, error)
}

type setter interface {
	Set(key string, value interface{}, expireAfter int64) error
}

type getterSetter interface {
	getter
	setter
}

// adjust the epoch to be relative to Jan 1, 2017 00:00:00 GMT to avoid floating
// point problems. this approach is good until "now" is 2,483,228,799 (Wed, 09
// Sep 2048 01:46:39 GMT), when the adjusted value is 16 digits.
const janFirst2017 = 1483228800

type gcra struct {
	getterSetter
}

func (g *gcra) limit(key string, cost, burst, rate, period int64) (
	allowed, remaining, retryAfter, resetAfter int64, err error,
) {
	var (
		emissionInterval = float64(period) / float64(rate)
		increment        = float64(cost) * emissionInterval
		burstOffset      = float64(burst) * emissionInterval
	)

	timeNow := time.Now()
	nowSecondsInMicro := timeNow.Unix() * 1000 * 1000
	microseconds := timeNow.UnixMicro() - nowSecondsInMicro
	now := (timeNow.Unix() - janFirst2017) + (microseconds / 1000000)

	value, err := g.Get(key)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	tat, ok := value.(int64)
	if !ok || now > tat {
		tat = now
	}

	newTat := float64(tat) + increment
	allowAt := newTat - burstOffset
	diff := float64(now) - allowAt
	remaining = int64(diff) / int64(emissionInterval)
	if remaining < 0 {
		resetAfter := tat - now
		retryAfter := int64(math.Ceil(diff * -1))
		return 0, 0, retryAfter, resetAfter, nil
	}

	resetAfter = int64(math.Ceil(newTat - float64(now)))
	if resetAfter > 0 {
		err = g.Set(key, int64(newTat), resetAfter)
		if err != nil {
			return 0, 0, 0, 0, err
		}
	}

	retryAfter = -1
	return cost, remaining, retryAfter, resetAfter, nil
}
