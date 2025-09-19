package router

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/router/throttler"
)

// MockPickupThrottler implements the PickupThrottler interface for testing
type MockPickupThrottler struct {
	limitPerSecond int64
	eventType      string
}

func (m *MockPickupThrottler) CheckLimitReached(ctx context.Context, cost int64) (bool, error) {
	return false, nil
}

func (m *MockPickupThrottler) ResponseCodeReceived(code int) {}

func (m *MockPickupThrottler) Shutdown() {}

func (m *MockPickupThrottler) GetLimitPerSecond() int64 {
	return m.limitPerSecond
}

func (m *MockPickupThrottler) GetEventType() string {
	return m.eventType
}

func TestHandle_getAdaptedJobQueryBatchSize(t *testing.T) {
	h := &Handle{}

	t.Run("no throttlers available should return original batch size", func(t *testing.T) {
		input := 100
		pickupThrottlers := func() []throttler.PickupThrottler {
			return []throttler.PickupThrottler{}
		}
		readSleep := 5 * time.Second
		maxLimit := 1000

		result := h.getAdaptedJobQueryBatchSize(input, pickupThrottlers, readSleep, maxLimit)

		require.Equal(t, input, result)
	})

	t.Run("multiple throttlers of eventType all should use first throttler limit", func(t *testing.T) {
		input := 100
		pickupThrottlers := func() []throttler.PickupThrottler {
			return []throttler.PickupThrottler{
				&MockPickupThrottler{limitPerSecond: 50, eventType: "all"},
				&MockPickupThrottler{limitPerSecond: 30, eventType: "all"},
			}
		}
		readSleep := 1 * time.Second
		maxLimit := 1000

		result := h.getAdaptedJobQueryBatchSize(input, pickupThrottlers, readSleep, maxLimit)

		// Should use the first throttler's limit (50) since subsequent global throttlers are ignored
		require.Equal(t, 50, result)
	})

	t.Run("multiple throttlers of eventType all returning zero as limit should return original batch size", func(t *testing.T) {
		input := 100
		pickupThrottlers := func() []throttler.PickupThrottler {
			return []throttler.PickupThrottler{
				&MockPickupThrottler{limitPerSecond: 0, eventType: "all"},
				&MockPickupThrottler{limitPerSecond: 0, eventType: "all"},
			}
		}
		readSleep := 1 * time.Second
		maxLimit := 1000

		result := h.getAdaptedJobQueryBatchSize(input, pickupThrottlers, readSleep, maxLimit)

		require.Equal(t, input, result)
	})

	t.Run("multiple throttlers of different eventTypes should return the sum", func(t *testing.T) {
		input := 100
		pickupThrottlers := func() []throttler.PickupThrottler {
			return []throttler.PickupThrottler{
				&MockPickupThrottler{limitPerSecond: 20, eventType: "track"},
				&MockPickupThrottler{limitPerSecond: 30, eventType: "identify"},
				&MockPickupThrottler{limitPerSecond: 25, eventType: "page"},
			}
		}
		readSleep := 1 * time.Second
		maxLimit := 1000

		result := h.getAdaptedJobQueryBatchSize(input, pickupThrottlers, readSleep, maxLimit)

		// Should sum all limits: 20 + 30 + 25 = 75
		require.Equal(t, 75, result)
	})

	t.Run("multiple throttlers of different eventTypes whose sum is greater than max limit should return max limit", func(t *testing.T) {
		input := 100
		pickupThrottlers := func() []throttler.PickupThrottler {
			return []throttler.PickupThrottler{
				&MockPickupThrottler{limitPerSecond: 400, eventType: "track"},
				&MockPickupThrottler{limitPerSecond: 300, eventType: "identify"},
				&MockPickupThrottler{limitPerSecond: 500, eventType: "page"},
			}
		}
		readSleep := 1 * time.Second
		maxLimit := 800

		result := h.getAdaptedJobQueryBatchSize(input, pickupThrottlers, readSleep, maxLimit)

		// Sum would be 1200, but should be capped at maxLimit
		require.Equal(t, maxLimit, result)
	})

	t.Run("small readSleep less than a second should use 1 sec min", func(t *testing.T) {
		input := 100
		pickupThrottlers := func() []throttler.PickupThrottler {
			return []throttler.PickupThrottler{
				&MockPickupThrottler{limitPerSecond: 50, eventType: "track"},
			}
		}
		readSleep := 300 * time.Millisecond // Less than 1 second
		maxLimit := 1000

		result := h.getAdaptedJobQueryBatchSize(input, pickupThrottlers, readSleep, maxLimit)

		// Should use 1 second minimum: 50 * 1 = 50
		require.Equal(t, 50, result)
	})

	t.Run("readSleep 2s should adapt batch size to 2 seconds", func(t *testing.T) {
		input := 100
		pickupThrottlers := func() []throttler.PickupThrottler {
			return []throttler.PickupThrottler{
				&MockPickupThrottler{limitPerSecond: 30, eventType: "track"},
			}
		}
		readSleep := 2 * time.Second
		maxLimit := 1000

		result := h.getAdaptedJobQueryBatchSize(input, pickupThrottlers, readSleep, maxLimit)

		// Should use 2 seconds: 30 * 2 = 60
		require.Equal(t, 60, result)
	})

	t.Run("readSleep with fractional seconds should round up", func(t *testing.T) {
		input := 100
		pickupThrottlers := func() []throttler.PickupThrottler {
			return []throttler.PickupThrottler{
				&MockPickupThrottler{limitPerSecond: 40, eventType: "track"},
			}
		}
		readSleep := 1500 * time.Millisecond // 1.5 seconds
		maxLimit := 1000

		result := h.getAdaptedJobQueryBatchSize(input, pickupThrottlers, readSleep, maxLimit)

		// Should round up to 2 seconds: 40 * 2 = 80
		require.Equal(t, 80, result)
	})
}
