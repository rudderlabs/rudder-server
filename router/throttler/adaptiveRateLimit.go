package throttler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type timers struct {
	timer        time.Duration
	limitReached map[string]bool
	mu           sync.Mutex
}

func SetupRouterAdaptiveRateLimiter(ctx context.Context, errorCh <-chan string) func(destName, destID string, limit int64) int64 {
	shortTimeFrequency := config.GetDuration("Router.throttler.adaptiveRateLimit.shortTimeFrequency", 1, time.Second)
	longTimeFrequency := config.GetDuration("Router.throttler.adaptiveRateLimit.longTimeFrequency", 15, time.Second)
	shortTimer := &timers{
		timer:        shortTimeFrequency,
		limitReached: make(map[string]bool),
	}
	longTimer := &timers{
		timer:        longTimeFrequency,
		limitReached: make(map[string]bool),
	}

	go shortTimer.runLoop(ctx)
	go longTimer.runLoop(ctx)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case value := <-errorCh:
				shortTimer.updateLimitReached(value)
				longTimer.updateLimitReached(value)
			}
		}
	}()

	limiter := func(destName, destID string, limit int64) int64 {
		enabled := config.GetBoolVar(true, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.enabled`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.enabled`, destName))
		if !enabled {
			return limit
		}
		minLimit := config.GetInt64Var(1, 1, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.minLimit`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.minLimit`, destName), fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID), fmt.Sprintf(`Router.throttler.%s.limit`, destName))
		maxLimit := config.GetInt64Var(0, 1, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.maxLimit`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.maxLimit`, destName), fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID), fmt.Sprintf(`Router.throttler.%s.limit`, destName))
		if minLimit > maxLimit {
			return limit
		}
		changePercentage := config.GetInt64Var(0, 1, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.changePercentage`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.changePercentage`, destName))
		if shortTimer.getLimitReached(destID) {
			newLimit := limit - (limit * changePercentage / 100)
			limit = max(minLimit, newLimit)
		} else if !longTimer.getLimitReached(destID) {
			newLimit := limit + (limit * changePercentage / 100)
			limit = min(maxLimit, newLimit)
		}
		return limit
	}
	return limiter
}

func (t *timers) runLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(t.timer):
			t.resetLimitReached()
		}
	}
}

func (t *timers) updateLimitReached(destID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.limitReached[destID] = true
}

func (t *timers) getLimitReached(destID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.limitReached[destID]
}

func (t *timers) resetLimitReached() {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.limitReached)
}
