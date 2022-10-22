package throttling

import (
	"fmt"
	"sync"
	"time"

	"github.com/throttled/throttled/v2"
	"github.com/throttled/throttled/v2/store/memstore"

	"github.com/rudderlabs/rudder-server/internal/cachettl"
)

const defaultMaxCASAttemptsLimit = 100

type gcra struct {
	mu    sync.Mutex
	store *cachettl.Cache
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

	if g.store == nil {
		g.store = cachettl.New()
	}

	rl, ok := g.store.Get(key).(*throttled.GCRARateLimiter)
	if rl == nil || !ok {
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
		g.store.Put(key, rl, time.Duration(period)*time.Second)
	}

	return rl, nil
}
