package ratelimiter

import "time"

// LimitStore is the interface that represents limiter internal data store. Any database struct that implements LimitStore should have functions for incrementing counter of a given key and getting counter values of a given key for previous and current window
type LimitStore interface {
	// Inc increments current window limit counter for key
	Inc(key string, window time.Time) error
	// Get gets value of previous window counter and current window counter for key
	Get(key string, previousWindow, currentWindow time.Time) (prevValue int64, currValue int64, err error)
}
