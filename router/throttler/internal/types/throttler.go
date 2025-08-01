package types

import "context"

// Throttler interface defines methods for checking and managing request limits.
// Typically one would have a single throttler per destination id and (optionally) per event type.
type Throttler interface {
	// CheckLimitReached returns true if the limit is exceeded, false otherwise.
	CheckLimitReached(ctx context.Context, cost int64) (limited bool, err error)
	// ResponseCodeReceived is called when a response code is received.
	ResponseCodeReceived(code int)
	// Shutdown is called to clean up resources.
	Shutdown()
	// GetLimit returns the current limit.
	GetLimit() int64
}
