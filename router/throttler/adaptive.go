package throttler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type timer struct {
	frequency    *config.Reloadable[time.Duration]
	limitReached map[string]bool
	mu           sync.Mutex
	limitSet     bool
	cancel       context.CancelFunc
}

type adaptiveConfig struct {
	enabled             bool
	minLimit            *config.Reloadable[int64]
	maxLimit            *config.Reloadable[int64]
	minChangePercentage *config.Reloadable[int64]
	maxChangePercentage *config.Reloadable[int64]
}

type Adaptive struct {
	shortTimer *timer
	longTimer  *timer
	config     adaptiveConfig
}

func NewAdaptive(config *config.Config) *Adaptive {
	shortTimeFrequency := config.GetReloadableDurationVar(5, time.Second, "Router.throttler.adaptiveRateLimit.shortTimeFrequency")
	longTimeFrequency := config.GetReloadableDurationVar(15, time.Second, "Router.throttler.adaptiveRateLimit.longTimeFrequency")

	shortTimer := &timer{
		frequency:    shortTimeFrequency,
		limitReached: make(map[string]bool),
	}
	longTimer := &timer{
		frequency:    longTimeFrequency,
		limitReached: make(map[string]bool),
	}

	go shortTimer.run()
	go longTimer.run()

	return &Adaptive{
		shortTimer: shortTimer,
		longTimer:  longTimer,
	}
}

func (a *Adaptive) loadConfig(destName, destID string) {
	a.config.enabled = config.GetBoolVar(true, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.enabled`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.enabled`, destName), `Router.throttler.adaptiveRateLimit.enabled`)
	a.config.minLimit = config.GetReloadableInt64Var(1, 1, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.minLimit`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.minLimit`, destName), `Router.throttler.adaptiveRateLimit.minLimit`, fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID), fmt.Sprintf(`Router.throttler.%s.limit`, destName))
	a.config.maxLimit = config.GetReloadableInt64Var(250, 1, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.maxLimit`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.maxLimit`, destName), `Router.throttler.adaptiveRateLimit.maxLimit`, fmt.Sprintf(`Router.throttler.%s.%s.limit`, destName, destID), fmt.Sprintf(`Router.throttler.%s.limit`, destName))
	a.config.minChangePercentage = config.GetReloadableInt64Var(30, 1, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.minChangePercentage`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.minChangePercentage`, destName), `Router.throttler.adaptiveRateLimit.minChangePercentage`)
	a.config.maxChangePercentage = config.GetReloadableInt64Var(10, 1, fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.%s.maxChangePercentage`, destName, destID), fmt.Sprintf(`Router.throttler.adaptiveRateLimit.%s.maxChangePercentage`, destName), `Router.throttler.adaptiveRateLimit.maxChangePercentage`)
}

func (a *Adaptive) Limit(destName, destID string, limit int64) int64 {
	a.loadConfig(destName, destID)
	if !a.config.enabled || a.config.minLimit.Load() > a.config.maxLimit.Load() {
		return limit
	}
	if limit <= 0 {
		limit = a.config.maxLimit.Load()
	}
	newLimit := limit
	if a.shortTimer.getLimitReached(destID) && !a.shortTimer.limitSet {
		newLimit = limit - (limit * a.config.minChangePercentage.Load() / 100)
		a.shortTimer.limitSet = true
	} else if !a.longTimer.getLimitReached(destID) && !a.longTimer.limitSet {
		newLimit = limit + (limit * a.config.maxChangePercentage.Load() / 100)
		a.longTimer.limitSet = true
	}
	newLimit = max(a.config.minLimit.Load(), min(newLimit, a.config.maxLimit.Load()))
	return newLimit
}

func (a *Adaptive) SetLimitReached(destID string) {
	a.shortTimer.updateLimitReached(destID)
	a.longTimer.updateLimitReached(destID)
}

func (a *Adaptive) ShutDown() {
	a.shortTimer.cancel()
	a.longTimer.cancel()
}

func (t *timer) run() {
	ctx, cancel := context.WithCancel(context.Background())
	t.cancel = cancel
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(t.frequency.Load()):
			t.resetLimitReached()
		}
	}
}

func (t *timer) updateLimitReached(destID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.limitReached[destID] = true
}

func (t *timer) getLimitReached(destID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.limitReached[destID]
}

func (t *timer) resetLimitReached() {
	t.mu.Lock()
	defer t.mu.Unlock()
	clear(t.limitReached)
	t.limitSet = false
}
