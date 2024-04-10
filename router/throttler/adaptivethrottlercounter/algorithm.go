package adaptivethrottlercounter

import (
	"context"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type Adaptive struct {
	limitFactor          *limitFactor
	increaseLimitCounter *increaseLimitCounter
	decreaseLimitCounter *decreaseLimitCounter
	cancel               context.CancelFunc
	wg                   *sync.WaitGroup
}

func New(config *config.Config, window config.ValueLoader[time.Duration]) *Adaptive {
	lf := &limitFactor{value: 1}

	increaseWindowMultiplier := config.GetReloadableIntVar(2, 1, "Router.throttler.adaptive.increaseWindowMultiplier")
	increaseCounterWindow := func() time.Duration { return window.Load() * time.Duration(increaseWindowMultiplier.Load()) }
	increasePercentage := config.GetReloadableInt64Var(10, 1, "Router.throttler.adaptive.increasePercentage")
	ilc := &increaseLimitCounter{
		limitFactor:        lf,
		window:             increaseCounterWindow,
		increasePercentage: increasePercentage,
	}

	decreaseWaitWindowMultiplier := config.GetReloadableIntVar(1, 1, "Router.throttler.adaptive.decreaseWaitWindowMultiplier")
	decreaseWaitWindow := func() time.Duration { return window.Load() * time.Duration(decreaseWaitWindowMultiplier.Load()) }
	decreasePercentage := config.GetReloadableInt64Var(30, 1, "Router.throttler.adaptive.decreasePercentage")
	dlc := &decreaseLimitCounter{
		limitFactor:                 lf,
		window:                      func() time.Duration { return window.Load() },
		waitWindow:                  decreaseWaitWindow,
		decreasePercentage:          decreasePercentage,
		throttleTolerancePercentage: func() int64 { return min(increasePercentage.Load(), decreasePercentage.Load()) },
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go ilc.run(ctx, &wg)
	wg.Add(1)
	go dlc.run(ctx, &wg)

	return &Adaptive{
		increaseLimitCounter: ilc,
		decreaseLimitCounter: dlc,
		limitFactor:          lf,
		cancel:               cancel,
		wg:                   &wg,
	}
}

func (a *Adaptive) LimitFactor() float64 {
	return a.limitFactor.Get()
}

func (a *Adaptive) ResponseCodeReceived(code int) {
	a.increaseLimitCounter.ResponseCodeReceived(code)
	a.decreaseLimitCounter.ResponseCodeReceived(code)
}

func (a *Adaptive) Shutdown() {
	a.cancel()
	a.wg.Wait()
}

type limitFactor struct {
	mu    sync.RWMutex
	value float64
}

// Add adds value to the current value of the limit factor, and clamps it between 0 and 1
func (l *limitFactor) Add(value float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.value += value
	if l.value < 0 {
		l.value = 0
	}
	if l.value > 1 {
		l.value = 1
	}
}

func (l *limitFactor) Get() float64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.value
}
