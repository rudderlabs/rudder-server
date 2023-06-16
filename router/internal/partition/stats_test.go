package partition_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/router/internal/partition"
)

func TestPartitionStats(t *testing.T) {
	t.Run("score when there are no stats", func(t *testing.T) {
		s := partition.NewStats()
		require.Equal(t, 100, s.Score("partition-1"))
	})

	t.Run("score when there are no successes or errors", func(t *testing.T) {
		s := partition.NewStats()
		s.Update("partition-1", 10*time.Second, 0, 0)
		require.Equal(t, 100, s.Score("partition-1"))
	})

	t.Run("lowest latency wins", func(t *testing.T) {
		s := partition.NewStats()
		s.Update("partition-1", 10*time.Second, 10, 0)
		s.Update("partition-2", 2*time.Second, 1, 0)
		s.Update("partition-3", 30*time.Second, 10, 0)

		require.Equal(t, 100, s.Score("partition-1"))
		require.Equal(t, 66, s.Score("partition-2"))
		require.Equal(t, 33, s.Score("partition-3"))
	})

	t.Run("lowest error ratio wins", func(t *testing.T) {
		s := partition.NewStats()
		s.Update("partition-1", 1*time.Second, 3, 0)
		s.Update("partition-2", 1*time.Second, 3, 1)
		s.Update("partition-3", 1*time.Second, 3, 2)

		require.Equal(t, 100, s.Score("partition-1"))
		require.Equal(t, 66, s.Score("partition-2"))
		require.Equal(t, 33, s.Score("partition-3"))
	})

	t.Run("outliers", func(t *testing.T) {
		s := partition.NewStats()
		s.Update("partition-1", 1*time.Second, 100, 10) // very high throughput, with 10% errors
		s.Update("partition-2", 10*time.Second, 100, 1) // low throughput, with 1% errors
		s.Update("partition-3", 10*time.Second, 100, 2)
		s.Update("partition-4", 10*time.Second, 100, 3)
		s.Update("partition-5", 10*time.Second, 100, 4)
		s.Update("partition-6", 10*time.Second, 100, 5)
		s.Update("partition-7", 10*time.Second, 100, 6)
		s.Update("partition-8", 10*time.Second, 100, 7)
		s.Update("partition-9", 10*time.Second, 100, 8)   // low throughput, with 8% errors
		s.Update("partition-10", 10*time.Second, 100, 99) // low throughput, with 99% errors

		require.Equal(t, 100, s.Score("partition-1"))
		require.Equal(t, 90, s.Score("partition-2"))
		require.Equal(t, 80, s.Score("partition-3"))
		require.Equal(t, 70, s.Score("partition-4"))
		require.Equal(t, 60, s.Score("partition-5"))
		require.Equal(t, 50, s.Score("partition-6"))
		require.Equal(t, 40, s.Score("partition-7"))
		require.Equal(t, 30, s.Score("partition-8"))
		require.Equal(t, 20, s.Score("partition-9"))
		require.Equal(t, 10, s.Score("partition-10"))
	})
}
