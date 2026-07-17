package integrations

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
)

func TestCollectIntegrationFailureDetailedStats(t *testing.T) {
	statsStore, err := memstats.New()
	require.NoError(t, err)
	originalStats := stats.Default
	stats.Default = statsStore
	t.Cleanup(func() { stats.Default = originalStats })

	CollectIntegrationFailureDetailedStats(nil)
	CollectIntegrationFailureDetailedStats(map[string]string{})

	metricName := "integration.failure_detailed"
	require.Empty(t, statsStore.GetByName(metricName))

	statTags := map[string]string{
		"workspaceId":     "workspace-1",
		"destinationId":   "destination-1",
		"destinationType": "WEBHOOK",
		"errorCategory":   "network",
	}
	CollectIntegrationFailureDetailedStats(statTags)
	CollectIntegrationFailureDetailedStats(statTags)

	metrics := statsStore.GetByName(metricName)
	require.Len(t, metrics, 1)
	require.Equal(t, stats.Tags(statTags), metrics[0].Tags)
	require.Equal(t, float64(2), metrics[0].Value)
}
