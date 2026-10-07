package batchrouter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

// The warehouse-service abort guard decides whether jobs waiting on a failing warehouse
// service should be aborted (410) instead of retried. warehouseServiceFailedTime is shared
// by every destination of a destType and is zeroed by any warehouse batch that is not a ping
// failure, so the guard can be handed a zero time. It must not abort in that case: the zero
// time.Time is year 1, so treating it as the start of the outage discards jobs on their first
// attempt, which is how a two-minute warehouse restart turned into permanent data loss.
func TestWarehouseServiceRetryLimitReached(t *testing.T) {
	newHandle := func(maxRetry time.Duration) *Handle {
		return &Handle{
			warehouseServiceMaxRetryTime: config.SingleValueLoader(maxRetry),
		}
	}

	t.Run("zero failedSince never aborts", func(t *testing.T) {
		brt := newHandle(3 * time.Hour)
		require.False(t, brt.warehouseServiceRetryLimitReached(time.Time{}),
			"a zero failure start must not abort: it is year 1 and exceeds every retry window")
	})

	t.Run("within the retry window does not abort", func(t *testing.T) {
		brt := newHandle(3 * time.Hour)
		require.False(t, brt.warehouseServiceRetryLimitReached(time.Now()))
		require.False(t, brt.warehouseServiceRetryLimitReached(time.Now().Add(-23*time.Second)),
			"the incident aborted jobs 23s after their first attempt")
		require.False(t, brt.warehouseServiceRetryLimitReached(time.Now().Add(-2*time.Hour)))
	})

	t.Run("beyond the retry window aborts", func(t *testing.T) {
		brt := newHandle(3 * time.Hour)
		require.True(t, brt.warehouseServiceRetryLimitReached(time.Now().Add(-3*time.Hour-time.Minute)))
	})

	t.Run("honours a reconfigured window", func(t *testing.T) {
		brt := newHandle(time.Minute)
		require.False(t, brt.warehouseServiceRetryLimitReached(time.Now().Add(-30*time.Second)))
		require.True(t, brt.warehouseServiceRetryLimitReached(time.Now().Add(-2*time.Minute)))
	})
}
