package throttling

import (
	"fmt"
	"sync"
	"time"

	"github.com/throttled/throttled/v2"
	"github.com/throttled/throttled/v2/store/memstore"
)

const defaultMaxCASAttemptsLimit = 100

// TODO add expiration mechanism? if we don't touch a key anymore it will stay in memory forever
type gcra struct {
	mu sync.Mutex
	m  map[string]*throttled.GCRARateLimiter
}

func (g *gcra) limit(key string, cost, burst, rate, period int64) (
	bool, error,
) {
	rl, err := g.getLimiter(key, burst, rate, period)
	if err != nil {
		return false, err
	}

	limited, _, err := rl.RateLimit("key", int(cost))
	if err != nil {
		return false, fmt.Errorf("could not rate limit: %w", err)
	}

	return !limited, nil
}

func (g *gcra) getLimiter(key string, burst, rate, period int64) (*throttled.GCRARateLimiter, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.m == nil {
		g.m = make(map[string]*throttled.GCRARateLimiter)
	}

	rl, ok := g.m[key]
	if !ok {
		store, err := memstore.New(0)
		if err != nil {
			return nil, fmt.Errorf("could not create store: %w", err)
		}
		rl, err = throttled.NewGCRARateLimiter(store, throttled.RateQuota{
			MaxRate:  throttled.PerDuration(int(rate), time.Duration(period)*time.Second),
			MaxBurst: int(burst),
		})
		if err != nil {
			return nil, fmt.Errorf("could not create rate limiter: %w", err)
		}
		rl.SetMaxCASAttemptsLimit(defaultMaxCASAttemptsLimit)
		g.m[key] = rl
	}

	return rl, nil
}
