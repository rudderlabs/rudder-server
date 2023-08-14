package isolation_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/isolation"
	"github.com/rudderlabs/rudder-server/router/types"
)

func TestIsolationStrategy(t *testing.T) {
	t.Run("none", func(r *testing.T) {
		strategy, err := isolation.GetStrategy(isolation.ModeNone, "", func(_ string) bool { return true })
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
			require.False(t, strategy.StopIteration(types.ErrBarrierExists))
			require.False(t, strategy.StopIteration(types.ErrDestinationThrottled))
		})
	})
	t.Run("workspace", func(r *testing.T) {
		strategy, err := isolation.GetStrategy(isolation.ModeWorkspace, "", func(_ string) bool { return true })
		require.NoError(t, err)

		t.Run("augment query params", func(t *testing.T) {
			var params jobsdb.GetQueryParams
			strategy.AugmentQueryParams("partition", &params)
			var expected jobsdb.GetQueryParams
			expected.WorkspaceID = "partition"
			require.Equal(t, expected, params)
		})

		t.Run("stop iteration", func(t *testing.T) {
			require.False(t, strategy.StopIteration(types.ErrBarrierExists))
			require.False(t, strategy.StopIteration(types.ErrDestinationThrottled))
		})
	})
	t.Run("destination", func(r *testing.T) {
		strategy, err := isolation.GetStrategy(isolation.ModeDestination, "", func(_ string) bool { return true })
		require.NoError(t, err)
		t.Run("stop iteration", func(t *testing.T) {
			require.False(t, strategy.StopIteration(types.ErrBarrierExists))
			require.True(t, strategy.StopIteration(types.ErrDestinationThrottled))
		})
	})
}
