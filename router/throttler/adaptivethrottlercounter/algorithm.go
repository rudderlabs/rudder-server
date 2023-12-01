package adaptivethrottlercounter

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type timer struct {
	frequency            time.Duration // frequency at which the timer runs and resets the tooManyRequestsCount and totalRequestsCount
	delay                time.Duration
	mu                   sync.Mutex
	limitSet             bool // set when the limit is set by the timer
	cancel               context.CancelFunc
	tooManyRequestsCount int64
	totalRequestsCount   int64
}

type Adaptive struct {
	shortTimer             *timer
	longTimer              *timer
	decreaseRatePercentage *config.Reloadable[int64]
	increaseRatePercentage *config.Reloadable[int64]
}

func New(config *config.Config, destWindow time.Duration) *Adaptive {
	increaseRateEvaluationFrequency := config.GetInt64("Router.throttler.adaptive.increaseRateEvaluationFrequency", 2)
	decreaseRateDelay := config.GetInt64("Router.throttler.adaptive.decreaseRateDelay", 1)

	shortTimerDelay := time.Duration(decreaseRateDelay) * destWindow
	shortTimer := &timer{
		frequency: shortTimerDelay + destWindow,
		delay:     shortTimerDelay,
	}
	longTimer := &timer{
		frequency: time.Duration(increaseRateEvaluationFrequency) * destWindow,
	}

	go shortTimer.run()
	go longTimer.run()

	return &Adaptive{
		shortTimer:             shortTimer,
		longTimer:              longTimer,
		decreaseRatePercentage: config.GetReloadableInt64Var(30, 1, "Router.throttler.adaptive.decreaseRatePercentage"),
		increaseRatePercentage: config.GetReloadableInt64Var(10, 1, "Router.throttler.adaptive.increaseRatePercentage"),
	}
}

func (a *Adaptive) LimitFactor() float64 {
	resolution := min(a.decreaseRatePercentage.Load(), a.increaseRatePercentage.Load())
	if a.shortTimer.getLimitReached() && a.shortTimer.SetLimit(true) && a.shortTimer.tooManyRequestsCount*100 >= a.shortTimer.totalRequestsCount*resolution { // if the number of 429s in the last 1 second is greater than the resolution
		return 1 - float64(a.decreaseRatePercentage.Load())/100
	} else if !a.longTimer.getLimitReached() && a.longTimer.SetLimit(true) {
		return 1 + float64(a.increaseRatePercentage.Load())/100
	}
	return 1.0
}

func (a *Adaptive) ResponseCodeReceived(code int) {
	a.shortTimer.updateLimitReached(code)
	a.longTimer.updateLimitReached(code)
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
			if t.delay > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(t.delay):
					t.resetLimitReached()
				}
			}
		}
	}
}

func (t *timer) updateLimitReached(code int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if code == http.StatusTooManyRequests {
		t.tooManyRequestsCount++
	}
	t.totalRequestsCount++
}

func (t *timer) getLimitReached() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tooManyRequestsCount > 0
}

func (t *timer) resetLimitReached() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tooManyRequestsCount = 0
	t.totalRequestsCount = 0
	t.limitSet = false
}

func (t *timer) SetLimit(limitSet bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if limitSet == t.limitSet {
		return false
	}
	t.limitSet = limitSet
	return true
}
