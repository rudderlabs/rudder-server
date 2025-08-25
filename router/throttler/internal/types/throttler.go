package types

import (
	"context"
	"time"
)

// PickupThrottler interface defines methods for checking and managing request limits.
// Typically one would have a single throttler per destination id and (optionally) per event type.
type PickupThrottler interface {
	// CheckLimitReached returns true if the limit is exceeded, false otherwise.
	CheckLimitReached(ctx context.Context, cost int64) (limited bool, err error)
	// ResponseCodeReceived is called when a response code is received.
	ResponseCodeReceived(code int)
	// Shutdown is called to clean up resources.
	Shutdown()
	// GetLimit returns the current limit.
	GetLimit() int64
}

type DeliveryThrottler interface {
	// Wait blocks until the next request can be sent and returns the duration blocked. An error is returned if the context is canceled.
	Wait(ctx context.Context) (time.Duration, error)
}
