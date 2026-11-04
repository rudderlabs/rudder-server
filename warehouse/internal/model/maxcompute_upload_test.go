package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionModeForTable(t *testing.T) {
	config := map[string]any{"partitionMode": "dynamic", "partitionModes": map[string]any{"fixed_table": "fixed", "dynamic_table": "dynamic"}}
	require.Equal(t, "fixed", PartitionModeForTable(config, "fixed_table"))
	require.Equal(t, "dynamic", PartitionModeForTable(config, "dynamic_table"))
	require.Equal(t, "dynamic", PartitionModeForTable(config, "other"))
	require.Empty(t, PartitionModeForTable(nil, "other"))
	require.Equal(t, "hour", PartitionGranularityForTable(map[string]any{
		"partitionType": "day", "partitionTypes": map[string]any{"fixed_table": "hour"},
	}, "fixed_table"))
	require.Equal(t, "day", PartitionGranularityForTable(map[string]any{"partitionType": "day"}, "other"))
}
