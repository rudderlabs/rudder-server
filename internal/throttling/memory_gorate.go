package throttling

import (
	"sync"
	"time"

	gorate "golang.org/x/time/rate"
)

// TODO add expiration mechanism? if we don't touch a key anymore it will stay in memory forever
type goRate struct {
	mu sync.Mutex
	m  map[string]*gorate.Limiter
}

func (i *goRate) limit(key string, cost, rate, period int64) *gorate.Reservation {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.m == nil {
		i.m = make(map[string]*gorate.Limiter)
	}
	window := time.Duration(period) * time.Second
	if _, ok := i.m[key]; !ok {
		i.m[key] = gorate.NewLimiter(gorate.Every(window), int(rate))
	}
	res := i.m[key].ReserveN(time.Now().Add(window), int(cost))
	return res
}
