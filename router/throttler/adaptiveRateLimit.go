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
	limitSet     bool
}

func SetupRouterAdaptiveRateLimiter(ctx context.Context, errorCh <-chan string) func(destName, destID string, limit int64) int64 {
	shortTimeFrequency := config.GetDuration("Router.throttler.adaptiveRateLimit.shortTimeFrequency", 5, time.Second)
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
		enabled := config.GetBoolVar(true, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.enabled`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.enabled`, destName), `Router.throttler.adaptiveRateLimit.enabled`)

		if !enabled {
			return limit
		}
		minLimit := config.GetInt64Var(1, 1, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.minLimit`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.minLimit`, destName), `Router.throttler.adaptiveRateLimit.minLimit`, fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID), fmt.Sprintf(`Router.throttler.%s.limit`, destName))
		maxLimit := config.GetInt64Var(250, 1, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.maxLimit`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.maxLimit`, destName), `Router.throttler.adaptiveRateLimit.maxLimit`, fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID), fmt.Sprintf(`Router.throttler.%s.limit`, destName))

		if minLimit > maxLimit {
			return limit
		}
		minChangePercentage := config.GetInt64Var(30, 1, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.minChangePercentage`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.minChangePercentage`, destName), `Router.throttler.adaptiveRateLimit.minChangePercentage`)
		maxChangePercentage := config.GetInt64Var(10, 1, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.maxChangePercentage`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.maxChangePercentage`, destName), `Router.throttler.adaptiveRateLimit.maxChangePercentage`)
		if limit <= 0 {
			limit = maxLimit
		}
		newLimit := limit
		if shortTimer.getLimitReached(destID) && !shortTimer.limitSet {

			newLimit = limit - (limit * minChangePercentage / 100)
			shortTimer.limitSet = true
		} else if !longTimer.getLimitReached(destID) && !longTimer.limitSet {
			newLimit = limit + (limit * maxChangePercentage / 100)
			longTimer.limitSet = true
		}
		newLimit = max(minLimit, min(newLimit, maxLimit))
		return newLimit
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
	t.limitSet = false
}
