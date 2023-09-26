package throttling

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/throttled/throttled/v2"

	"github.com/rudderlabs/rudder-go-kit/cachettl"
)

const defaultMaxCASAttemptsLimit = 100

type gcra struct {
	mu    sync.Mutex
	store *cachettl.Cache[string, *throttled.GCRARateLimiterCtx]
}

func (g *gcra) limit(ctx context.Context, key string, cost, burst, rate, period int64) (
	bool, error,
) {
	rl, err := g.getLimiter(key, burst, rate, period)
	if err != nil {
		return false, err
	}

	limited, _, err := rl.RateLimitCtx(ctx, "key", int(cost))
	if err != nil {
		return false, fmt.Errorf("could not rate limit: %w", err)
	}

	return !limited, nil
}

func (g *gcra) getLimiter(key string, burst, rate, period int64) (*throttled.GCRARateLimiterCtx, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.store == nil {
		g.store = cachettl.New[string, *throttled.GCRARateLimiterCtx]()
	}

	rl := g.store.Get(key)
	if rl == nil {
		var err error
		rl, err = throttled.NewGCRARateLimiterCtx(newGCRAMemStore(), throttled.RateQuota{
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

func newGCRAMemStore() throttled.GCRAStoreCtx {
	var v atomic.Int64
	v.Store(-1)
	return &gcraMemStore{
		v:       &v,
		timeNow: time.Now,
	}
}

type gcraMemStore struct {
	v       *atomic.Int64
	timeNow func() time.Time
}

// GetWithTime returns the value of the key or -1 if it does not exist.
// It also returns the current time at the Store.
// The time must be representable as a positive int64 of nanoseconds since the epoch.
func (ms *gcraMemStore) GetWithTime(_ context.Context, _ string) (int64, time.Time, error) {
	return ms.v.Load(), ms.timeNow(), nil
}

// SetIfNotExistsWithTTL sets the value of key only if it is not ready set in the store (-1 == not exists)
// it returns whether a new value was set.
func (ms *gcraMemStore) SetIfNotExistsWithTTL(_ context.Context, _ string, value int64, _ time.Duration) (bool, error) {
	swapped := ms.v.CompareAndSwap(-1, value)
	return swapped, nil
}

// CompareAndSwapWithTTL atomically compares the value at key to the old value.
// If it matches, it sets it to the new value and returns true, false otherwise.
func (ms *gcraMemStore) CompareAndSwapWithTTL(_ context.Context, _ string, old, new int64, _ time.Duration) (bool, error) {
	swapped := ms.v.CompareAndSwap(old, new)
	return swapped, nil
}
