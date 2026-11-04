package warehouse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaxComputePartitionModeForTable(t *testing.T) {
	conf := map[string]any{"partitionModes": map[string]any{"tracks": "fixed", "other": "dynamic"}}
	require.Equal(t, "fixed", partitionModeForTable(conf, "tracks"))
	require.Equal(t, "dynamic", partitionModeForTable(conf, "other"))
	require.Empty(t, partitionModeForTable(conf, "missing"))
}
