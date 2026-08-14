package router

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

func TestGetPartitionRouterConfigInt(t *testing.T) {
	t.Run("partition override wins over destination and global config", func(t *testing.T) {
		config.Reset()
		t.Cleanup(config.Reset)
		config.Set("Router.ITERABLE.partition-a.noOfWorkers", 8)
		config.Set("Router.ITERABLE.noOfWorkers", 4)
		config.Set("Router.noOfWorkers", 2)

		require.Equal(t, 8, getPartitionRouterConfigInt("noOfWorkers", "ITERABLE", "partition-a", 1))
	})

	t.Run("destination config wins over global config when partition override is absent", func(t *testing.T) {
		config.Reset()
		t.Cleanup(config.Reset)
		config.Set("Router.ITERABLE.noOfWorkers", 4)
		config.Set("Router.noOfWorkers", 2)

		require.Equal(t, 4, getPartitionRouterConfigInt("noOfWorkers", "ITERABLE", "partition-a", 1))
	})

	t.Run("global config is used when partition and destination overrides are absent", func(t *testing.T) {
		config.Reset()
		t.Cleanup(config.Reset)
		config.Set("Router.noOfWorkers", 2)

		require.Equal(t, 2, getPartitionRouterConfigInt("noOfWorkers", "ITERABLE", "partition-a", 1))
	})

	t.Run("default is used when no config exists", func(t *testing.T) {
		config.Reset()
		t.Cleanup(config.Reset)

		require.Equal(t, 7, getPartitionRouterConfigInt("noOfWorkers", "ITERABLE", "partition-a", 7))
	})

	t.Run("empty partition behaves like existing destination and global lookup", func(t *testing.T) {
		config.Reset()
		t.Cleanup(config.Reset)
		config.Set("Router.ITERABLE.noOfWorkers", 4)
		config.Set("Router.noOfWorkers", 2)

		require.Equal(t, getRouterConfigInt("noOfWorkers", "ITERABLE", 1), getPartitionRouterConfigInt("noOfWorkers", "ITERABLE", "", 1))
	})
}
