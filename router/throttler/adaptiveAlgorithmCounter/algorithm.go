package adaptivethrottlercounter

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type timer struct {
	frequency    *config.Reloadable[time.Duration]
	limitReached bool
	mu           sync.Mutex
	limitSet     bool
	cancel       context.CancelFunc
}

type Adaptive struct {
	shortTimer *timer
	longTimer  *timer
}

func New(config *config.Config) *Adaptive {
	shortTimeFrequency := config.GetReloadableDurationVar(5, time.Second, "Router.throttler.adaptiveRateLimit.shortTimeFrequency")
	longTimeFrequency := config.GetReloadableDurationVar(15, time.Second, "Router.throttler.adaptiveRateLimit.longTimeFrequency")

	shortTimer := &timer{
		frequency: shortTimeFrequency,
	}
	longTimer := &timer{
		frequency: longTimeFrequency,
	}

	go shortTimer.run()
	go longTimer.run()

	return &Adaptive{
		shortTimer: shortTimer,
		longTimer:  longTimer,
	}
}

func (a *Adaptive) LimitFactor() float64 {
	decreaseLimitPercentage := config.GetInt64("Router.throttler.adaptiveRateLimit.decreaseLimitPercentage", 30)
	increaseChangePercentage := config.GetInt64("Router.throttler.adaptiveRateLimit.increaseChangePercentage", 10)
	if a.shortTimer.getLimitReached() && !a.shortTimer.limitSet {
		a.shortTimer.limitSet = true
		return float64(-decreaseLimitPercentage) / 100
	} else if !a.longTimer.getLimitReached() && !a.longTimer.limitSet {
		a.longTimer.limitSet = true
		return float64(increaseChangePercentage) / 100
	}
	return 0.0
}

func (a *Adaptive) ResponseCodeReceived(code int) {
	if code == http.StatusTooManyRequests {
		a.shortTimer.updateLimitReached()
		a.longTimer.updateLimitReached()
	}
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
