package algorithm

import (
	"context"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type AdaptiveAlgorithm interface {
	// ResponseCodeReceived processes the response code and updates the limit factor accordingly
	ResponseCodeReceived(code int)
	// Shutdown stops the algorithm and waits for all goroutines to finish
	Shutdown()
	// LimitFactor returns a factor that is supposed to be used to multiply the limit, a number between 0 and 1
	LimitFactor() float64
}

type adaptiveAlgorithm struct {
	limitFactor          *limitFactor
	increaseLimitCounter *increaseLimitCounter
	decreaseLimitCounter *decreaseLimitCounter
	cancel               context.CancelFunc
	wg                   *sync.WaitGroup
}

// NewAdaptiveAlgorithm creates a new adaptive algorithm instance.
//
// An adaptive algorithm dynamically adjusts the limit factor based on the response codes received within a certain time window.
//
// Configurable parameters include:
// - increaseWindowMultiplier: Multiplier for the increase limit counter window.
// - increasePercentage: Percentage to increase the limit factor when no 429s are received.
// - decreaseWaitWindowMultiplier: Multiplier for the wait window after a decrease.
// - decreasePercentage: Percentage to decrease the limit factor when 429s are received.
// - throttleTolerancePercentage: Percentage of throttled requests that triggers a decrease in the limit factor
func NewAdaptiveAlgorithm(destType string, config *config.Config, window config.ValueLoader[time.Duration]) AdaptiveAlgorithm {
	lf := &limitFactor{value: 1}

	increaseWindowMultiplier := config.GetReloadableIntVar(2, 1,
		"Router.throttler."+destType+".adaptiveIncreaseWindowMultiplier",
		"Router.throttler.adaptiveIncreaseWindowMultiplier",
	)
	increaseCounterWindow := func() time.Duration { return window.Load() * time.Duration(increaseWindowMultiplier.Load()) }
	increasePercentage := config.GetReloadableInt64Var(10, 1,
		"Router.throttler."+destType+".adaptiveIncreasePercentage",
		"Router.throttler.adaptiveIncreasePercentage",
	)

	ilc := &increaseLimitCounter{
		limitFactor:        lf,
		window:             increaseCounterWindow,
		increasePercentage: increasePercentage,
	}

	decreaseWaitWindowMultiplier := config.GetReloadableIntVar(1, 1,
		"Router.throttler."+destType+".adaptiveDecreaseWaitWindowMultiplier",
		"Router.throttler.adaptiveDecreaseWaitWindowMultiplier",
	)
	decreaseWaitWindow := func() time.Duration { return window.Load() * time.Duration(decreaseWaitWindowMultiplier.Load()) }
	decreasePercentage := config.GetReloadableInt64Var(30, 1,
		"Router.throttler."+destType+".adaptiveDecreasePercentage",
		"Router.throttler.adaptiveDecreasePercentage",
	)
	throttleTolerancePercentage := config.GetReloadableInt64Var(10, 1,
		"Router.throttler."+destType+".adaptiveThrottleTolerancePercentage",
		"Router.throttler.adaptiveThrottleTolerancePercentage",
	)
	dlc := &decreaseLimitCounter{
		limitFactor:                 lf,
		window:                      func() time.Duration { return window.Load() },
		waitWindow:                  decreaseWaitWindow,
		decreasePercentage:          decreasePercentage,
		throttleTolerancePercentage: func() int64 { return throttleTolerancePercentage.Load() },
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go ilc.run(ctx, &wg)
	wg.Add(1)
	go dlc.run(ctx, &wg)

	return &adaptiveAlgorithm{
		increaseLimitCounter: ilc,
		decreaseLimitCounter: dlc,
		limitFactor:          lf,
		cancel:               cancel,
		wg:                   &wg,
	}
}

// LimitFactor returns the current limit factor
func (a *adaptiveAlgorithm) LimitFactor() float64 {
	return a.limitFactor.Get()
}

// ResponseCodeReceived processes the response code and updates the limit factor accordingly
func (a *adaptiveAlgorithm) ResponseCodeReceived(code int) {
	a.increaseLimitCounter.ResponseCodeReceived(code)
	a.decreaseLimitCounter.ResponseCodeReceived(code)
}

// Shutdown stops the algorithm and waits for all goroutines to finish
func (a *adaptiveAlgorithm) Shutdown() {
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
