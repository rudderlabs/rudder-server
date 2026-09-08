package router

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetPartitionRouterConfigKeys(t *testing.T) {
	t.Run("with partition", func(t *testing.T) {
		require.Equal(t, []string{
			"Router.WEBHOOK.partition1.noOfWorkers",
			"Router.WEBHOOK.noOfWorkers",
			"Router.noOfWorkers",
		}, getPartitionRouterConfigKeys("noOfWorkers", "WEBHOOK", "partition1"))
	})

	t.Run("without partition", func(t *testing.T) {
		require.Equal(t, []string{
			"Router.WEBHOOK.noOfWorkers",
			"Router.noOfWorkers",
		}, getPartitionRouterConfigKeys("noOfWorkers", "WEBHOOK", ""))
	})
}
