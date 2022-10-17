package throttling

import (
	"math"
	"sync"
	"time"
)

// adjust the epoch to be relative to Jan 1, 2017 00:00:00 GMT to avoid floating
// point problems. this approach is good until "now" is 2,483,228,799 (Wed, 09
// Sep 2048 01:46:39 GMT), when the adjusted value is 16 digits.
const janFirst2017 = 1483228800

// TODO add expiration mechanism? if we don't touch a key anymore it will stay in memory forever
type gcra struct {
	mu sync.Mutex
	m  map[string]interface{}
	ex map[string]time.Time
}

// TODO some of this logic can be simplified since we're not interested on remaining,retryAfter,resetAfter
func (g *gcra) limit(key string, cost, burst, rate, period int64) (
	allowed, remaining, retryAfter, resetAfter int64, err error,
) {
	g.mu.Lock()
	defer g.mu.Unlock()

	var (
		emissionInterval = int64(math.Ceil(float64(period) / float64(rate)))
		increment        = cost * emissionInterval
		burstOffset      = burst * emissionInterval
	)

	timeNow := time.Now()
	nowSecondsInMicro := timeNow.Unix() * 1000 * 1000
	microseconds := timeNow.UnixMicro() - nowSecondsInMicro
	now := (timeNow.Unix() - janFirst2017) + (microseconds / 1000000)

	value, err := g.get(key)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	tat, ok := value.(int64)
	if !ok || now > tat {
		tat = now
	}

	newTat := tat + increment
	allowAt := newTat - burstOffset
	diff := now - allowAt
	remaining = diff / emissionInterval
	if remaining < 0 {
		resetAfter := tat - now
		retryAfter := int64(math.Ceil(float64(diff * -1)))
		return 0, 0, retryAfter, resetAfter, nil
	}

	resetAfter = newTat - now
	if resetAfter > 0 {
		err = g.set(key, newTat, resetAfter)
		if err != nil {
			return 0, 0, 0, 0, err
		}
	}

	retryAfter = -1
	return cost, remaining, retryAfter, resetAfter, nil
}

func (g *gcra) get(key string) (interface{}, error) {
	v, ok := g.m[key]
	if !ok {
		return 0, nil
	}
	if time.Now().UnixNano() > g.ex[key].UnixNano() {
		delete(g.m, key)
		delete(g.ex, key)
		return 0, nil
	}
	return v, nil
}

func (g *gcra) set(key string, value interface{}, expiration int64) error {
	if g.m == nil {
		g.m = make(map[string]interface{})
		g.ex = make(map[string]time.Time)
	}
	g.m[key] = value
	g.ex[key] = time.Now().Add(time.Duration(expiration) * time.Second)
	return nil
}
