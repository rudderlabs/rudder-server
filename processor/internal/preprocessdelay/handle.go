package preprocessdelay

import (
	"context"
	"time"
)

// NewHandle creates a new Handle that ensures that at least 'delay' time has
// passed since the most recent JobReceivedAt time before Sleep returns.
// If delay is less than or equal to zero, a no-op Handle is returned.
func NewHandle(delay time.Duration, sleeper Sleeper) Handle {
	if delay <= 0 {
		return &nopHandle{}
	}

	h := &handle{
		delay:   delay,
		sleeper: sleeper,
	}
	if h.sleeper == nil {
		h.sleeper = func(ctx context.Context, d time.Duration) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d):
				return nil
			}
		}
	}
	return h
}

type Handle interface {
	// JobReceivedAt records the time a job was received. It should be called
	// for each job that is part of the same processing batch.
	// If called multiple times, the most recent time is kept.
	JobReceivedAt(receivedAt time.Time)
	// Sleep sleeps until at least 'delay' time has passed since the most recent
	// JobReceivedAt time. If 'delay' time has already passed, it returns
	// immediately. If JobReceivedAt was never called, it returns immediately.
	// It returns any error returned by the Sleeper function.
	// The context can be used to cancel the sleep.
	Sleep(ctx context.Context) error
}

type Sleeper func(ctx context.Context, d time.Duration) error

type handle struct {
	delay                time.Duration
	sleeper              Sleeper
	mostRecentReceivedAt time.Time
}

func (h *handle) JobReceivedAt(receivedAt time.Time) {
	if receivedAt.After(h.mostRecentReceivedAt) {
		h.mostRecentReceivedAt = receivedAt
	}
}

func (h *handle) Sleep(ctx context.Context) error {
	if h.mostRecentReceivedAt.IsZero() {
		return nil
	}
	if sleepFor := h.delay - time.Since(h.mostRecentReceivedAt); sleepFor > 0 {
		return h.sleeper(ctx, sleepFor)
	}
	return nil
}

type nopHandle struct{}

func (h *nopHandle) JobReceivedAt(receivedAt time.Time) {}

func (h *nopHandle) Sleep(ctx context.Context) error { return nil }
