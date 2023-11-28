package adaptivethrottlercounter

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type timer struct {
	frequency    time.Duration
	limitReached bool // set when a 429 is received
	mu           sync.Mutex
	limitSet     bool // set when the limit is set by the timer
	cancel       context.CancelFunc
}

type Adaptive struct {
	shortTimer               *timer
	longTimer                *timer
	decreaseLimitPercentage  *config.Reloadable[int64]
	increaseChangePercentage *config.Reloadable[int64]
}

func New(config *config.Config) *Adaptive {
	shortTimeFrequency := config.GetDuration("Router.throttler.adaptive.shortTimeFrequency", 5, time.Second)
	longTimeFrequency := config.GetDuration("Router.throttler.adaptive.longTimeFrequency", 15, time.Second)

	shortTimer := &timer{
		frequency: shortTimeFrequency,
	}
	longTimer := &timer{
		frequency: longTimeFrequency,
	}

	go shortTimer.run()
	go longTimer.run()

	return &Adaptive{
		shortTimer:               shortTimer,
		longTimer:                longTimer,
		decreaseLimitPercentage:  config.GetReloadableInt64Var(30, 1, "Router.throttler.adaptive.decreaseLimitPercentage"),
		increaseChangePercentage: config.GetReloadableInt64Var(10, 1, "Router.throttler.adaptive.increaseLimitPercentage"),
	}
}

func (a *Adaptive) LimitFactor() float64 {
	if a.shortTimer.getLimitReached() && !a.shortTimer.limitSet {
		a.shortTimer.limitSet = true
		return float64(-a.decreaseLimitPercentage.Load()) / 100
	} else if !a.longTimer.getLimitReached() && !a.longTimer.limitSet {
		a.longTimer.limitSet = true
		return float64(a.increaseChangePercentage.Load()) / 100
	}
	return 0.0
}

func (a *Adaptive) ResponseCodeReceived(code int) {
	if code == http.StatusTooManyRequests {
		a.shortTimer.updateLimitReached()
		a.longTimer.updateLimitReached()
	}
}

func (a *Adaptive) Shutdown() {
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
		case <-time.After(t.frequency):
			t.resetLimitReached()
		}
	}
}

func (t *timer) updateLimitReached() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.limitReached = true
}

func (t *timer) getLimitReached() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.limitReached
}

func (t *timer) resetLimitReached() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.limitReached = false
	t.limitSet = false
}
