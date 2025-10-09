package isolation_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/isolation"
	"github.com/rudderlabs/rudder-server/router/types"
)

func TestIsolationStrategy(t *testing.T) {
	c := config.New()
	const destinationID = "dest123"
	t.Run("none", func(r *testing.T) {
		strategy, err := isolation.GetStrategy(isolation.ModeNone, "", func(_ string) bool { return true }, c)
		require.NoError(t, err)

		t.Run("active partitions", func(t *testing.T) {
			partitions, err := strategy.ActivePartitions(context.Background(), nil)
			require.NoError(t, err)
			require.Equal(t, []string{""}, partitions)
		})
		t.Run("augment query params", func(t *testing.T) {
			var params jobsdb.GetQueryParams
			toAugment := params
			strategy.AugmentQueryParams("partition", &toAugment)
			require.Equal(t, params, toAugment)
		})
		t.Run("stop iteration", func(t *testing.T) {
			require.False(t, strategy.StopIteration(types.ErrBarrierExists, destinationID))
			require.False(t, strategy.StopIteration(types.ErrDestinationThrottled, destinationID))
		})
		t.Run("stop queries", func(t *testing.T) {
			require.False(t, strategy.StopQueries(types.ErrBarrierExists, destinationID))
			require.False(t, strategy.StopQueries(types.ErrDestinationThrottled, destinationID))
		})
		t.Run("supports pickup query throttling", func(t *testing.T) {
			require.False(t, strategy.SupportsPickupQueryThrottling())
		})
	})
	t.Run("workspace", func(r *testing.T) {
		strategy, err := isolation.GetStrategy(isolation.ModeWorkspace, "", func(_ string) bool { return true }, c)
		require.NoError(t, err)

		t.Run("augment query params", func(t *testing.T) {
			var params jobsdb.GetQueryParams
			strategy.AugmentQueryParams("partition", &params)
			var expected jobsdb.GetQueryParams
			expected.WorkspaceID = "partition"
			require.Equal(t, expected, params)
		})

		t.Run("stop iteration", func(t *testing.T) {
			require.False(t, strategy.StopIteration(types.ErrBarrierExists, destinationID))
			require.False(t, strategy.StopIteration(types.ErrDestinationThrottled, destinationID))
		})

		t.Run("stop queries", func(t *testing.T) {
			require.False(t, strategy.StopQueries(types.ErrBarrierExists, destinationID))
			require.False(t, strategy.StopQueries(types.ErrDestinationThrottled, destinationID))
		})

		t.Run("supports pickup query throttling", func(t *testing.T) {
			require.False(t, strategy.SupportsPickupQueryThrottling())
		})
	})
	t.Run("destination", func(r *testing.T) {
		strategy, err := isolation.GetStrategy(isolation.ModeDestination, "WEBHOOK", func(_ string) bool { return true }, c)
		require.NoError(t, err)
		t.Run("stop iteration", func(t *testing.T) {
			require.False(t, strategy.StopIteration(types.ErrBarrierExists, destinationID))
			require.True(t, strategy.StopIteration(types.ErrDestinationThrottled, destinationID))
		})

		t.Run("stop queries", func(t *testing.T) {
			destinationID := "dest123"

			t.Run("returns false for non-throttled errors", func(t *testing.T) {
				require.False(t, strategy.StopQueries(types.ErrBarrierExists, destinationID))
			})

			t.Run("returns false when throttlerPerEventType is disabled", func(t *testing.T) {
				// Default config has throttlerPerEventType disabled (false)
				require.False(t, strategy.StopQueries(types.ErrDestinationThrottled, destinationID))
			})

			t.Run("returns true when throttlerPerEventType is enabled", func(t *testing.T) {
				// Enable throttlerPerEventType for this destination
				c.Set("Router.throttler.WEBHOOK.dest123.throttlerPerEventType", true)
				require.True(t, strategy.StopQueries(types.ErrDestinationThrottled, destinationID))
			})

			t.Run("uses destination type fallback config", func(t *testing.T) {
				anotherDestID := "dest456"

				// No specific config for dest456, should use destination type config
				c.Set("Router.throttler.WEBHOOK.throttlerPerEventType", true)
				require.True(t, strategy.StopQueries(types.ErrDestinationThrottled, anotherDestID))
			})

			t.Run("uses global fallback config", func(t *testing.T) {
				anotherDestID := "dest789"

				// No specific config for dest789 or destination type, should use global config
				c.Set("Router.throttler.throttlerPerEventType", true)
				require.True(t, strategy.StopQueries(types.ErrDestinationThrottled, anotherDestID))
			})
		})

		t.Run("supports pickup query throttling when enabled", func(t *testing.T) {
			c.Set("Router.throttler.WEBHOOK.pickupQueryThrottlingEnabled", true)
			require.True(t, strategy.SupportsPickupQueryThrottling())
		})

		t.Run("doesn't support pickup query throttling when not enabled", func(t *testing.T) {
			c.Set("Router.throttler.WEBHOOK.pickupQueryThrottlingEnabled", false)
			require.False(t, strategy.SupportsPickupQueryThrottling())
		})
	})
}
