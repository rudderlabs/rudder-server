package scenario

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/throttling"
)

func rateLimiter(limit int) (func(ctx context.Context, key string, cost int) error, error) {
	var mu sync.Mutex
	if limit == 0 {
		return func(context.Context, string, int) error { return nil }, nil
	}
	tl, err := throttling.New(throttling.WithInMemoryGCRA(int64(limit)))
	if err != nil {
		return nil, fmt.Errorf("could not create rate limiter: %w", err)
	}
	return func(ctx context.Context, key string, cost int) error {
		mu.Lock()
		defer mu.Unlock()
		var i int
		const quantity = 100
		for i < cost/quantity {
			allow, retryAfter, _, err := tl.AllowAfter(ctx, quantity, int64(limit), 1, key)
			if err != nil {
				return err
			}
			if !allow {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(retryAfter):
				}
			} else {
				i++
			}
		}
		return nil
	}, nil
}
