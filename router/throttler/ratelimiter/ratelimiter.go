package ratelimiter

import (
	"time"
)

// LimitStore is the interface that represents limiter internal data store. Any database struct that implements LimitStore should have functions for incrementing counter of a given key and getting counter values of a given key for previous and current window
type LimitStore interface {
	// Inc increments current window limit counter for key
	Inc(key string, window time.Time) error
	// Inc increments current window limit counter for key
	Dec(key string, count int64, window time.Time) error
	// Get gets value of previous window counter and current window counter for key
	Get(key string, previousWindow, currentWindow time.Time) (prevValue, currValue int64, err error)
}

// RateLimiter is a simple rate-limiter for any resources inspired by Cloudflare's approach: https://blog.cloudflare.com/counting-things-a-lot-of-different-things/
type RateLimiter struct {
	dataStore     LimitStore
	requestsLimit int64
	windowSize    time.Duration
}

// New creates new rate limiter. A dataStore is internal limiter data store, requestsLimit and windowSize are parameters of limiter e.g. requestsLimit: 5 and windowSize: 1*time.Minute means that limiter allows up to 5 requests per minute
func New(dataStore LimitStore, requestsLimit int64, windowSize time.Duration) *RateLimiter {
	return &RateLimiter{
		dataStore:     dataStore,
		requestsLimit: requestsLimit,
		windowSize:    windowSize,
	}
}

// Inc increments limiter counter for a given key or returns error when it's not possible
func (r *RateLimiter) Inc(key string, currentTime time.Time) error {
	if currentTime.IsZero() {
		currentTime = time.Now()
	}
	currentWindow := currentTime.UTC().Truncate(r.windowSize)
	return r.dataStore.Inc(key, currentWindow)
}

// Inc increments limiter counter for a given key or returns error when it's not possible
func (r *RateLimiter) Dec(key string, count int64, currentTime time.Time) error {
	if currentTime.IsZero() {
		currentTime = time.Now()
	}
	currentWindow := currentTime.UTC().Truncate(r.windowSize)
	return r.dataStore.Dec(key, count, currentWindow)
}

// LimitStatus represents current status of limitation for a given key
type LimitStatus struct {
	// IsLimited is true when a given key should be rate-limited
	IsLimited bool
	// LimitDuration is not nil when IsLimited is true. It's the time for which a given key should be blocked before CurrentRate falls below declared in constructor requests limit
	LimitDuration *time.Duration
	// CurrentRate is approximated current requests rate per window size (declared in the constructor)
	CurrentRate float64
}

// Check checks status of rate-limiting for a key. It returns error when limiter data could not be read
func (r *RateLimiter) Check(key string, currentTime time.Time) (limitStatus *LimitStatus, err error) {
	if currentTime.IsZero() {
		currentTime = time.Now()
	}
	currentWindow := currentTime.UTC().Truncate(r.windowSize)
	previousWindow := currentWindow.Add(-r.windowSize)
	prevValue, currentValue, err := r.dataStore.Get(key, previousWindow, currentWindow)
	if err != nil {
		return nil, err
	}
	timeFromCurrWindow := currentTime.UTC().Sub(currentWindow)

	rate := float64((float64(r.windowSize)-float64(timeFromCurrWindow))/float64(r.windowSize))*float64(prevValue) + float64(currentValue)
	limitStatus = &LimitStatus{}
	if rate >= float64(r.requestsLimit) {
		limitStatus.IsLimited = true
		limitDuration := r.calcLimitDuration(prevValue, currentValue, timeFromCurrWindow)
		limitStatus.LimitDuration = &limitDuration
	}
	limitStatus.CurrentRate = rate

	return limitStatus, nil
}

func (r *RateLimiter) calcRate(timeFromCurrWindow time.Duration, prevValue, currentValue int64) float64 {
	return float64((float64(r.windowSize)-float64(timeFromCurrWindow))/float64(r.windowSize))*float64(prevValue) + float64(currentValue)
}

func (r *RateLimiter) calcLimitDuration(prevValue, currValue int64, timeFromCurrWindow time.Duration) time.Duration {
	// we should find x parameter in equation: x*prevValue+currentValue = r.requestsLimit
	// then (1.0-x)*windowSize is duration from current window start when limit can be removed
	// then ((1.0-x)*windowSize) - timeFromCurrWindow is duration since current time to the time when limit can be removed = limitDuration
	// --
	// if prevValue is zero then unblock is in the next window so we should use equation x*currentValue+nextWindowValue = r.requestsLimit
	// to calculate x parameter
	var limitDuration time.Duration
	if prevValue == 0 {
		// unblock in the next window where prevValue is currValue and currValue is zero (assuming that since limit start all requests are blocked)
		if currValue != 0 {
			nextWindowUnblockPoint := float64(r.windowSize) * (1.0 - (float64(r.requestsLimit) / float64(currValue)))
			timeToNextWindow := r.windowSize - timeFromCurrWindow
			limitDuration = timeToNextWindow + time.Duration(int64(nextWindowUnblockPoint)+1)
		} else {
			// when requestsLimit is 0 we want to block all requests - set limitDuration to -1
			limitDuration = -1
		}
	} else {
		currWindowUnblockPoint := float64(r.windowSize) * (1.0 - (float64(r.requestsLimit-currValue) / float64(prevValue)))
		limitDuration = time.Duration(int64(currWindowUnblockPoint+1)) - timeFromCurrWindow

	}
	return limitDuration
}
