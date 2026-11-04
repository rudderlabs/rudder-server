package schema

import (
	"testing"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestMaxComputePartitionSchemaIsTableSpecific(t *testing.T) {
	wh := model.Warehouse{Type: whutils.MAXCOMPUTE, Destination: backendconfig.DestinationT{Config: map[string]any{
		"partitionModes": map[string]any{"fixed_table": "fixed", "dynamic_table": "dynamic", "rudder_discards": "fixed"},
	}}}
	schema := enhanceDiscardsSchema(model.Schema{
		"fixed_table": {"id": "string"}, "dynamic_table": {"id": "string"},
	}, wh.Type)
	enhanceMaxComputePartitions(schema, wh)
	require.Equal(t, "datetime", schema["fixed_table"]["_partitiontime"])
	require.Equal(t, "datetime", schema["rudder_discards"]["_partitiontime"])
	require.Equal(t, "datetime", schema["rudder_discards"]["loaded_at"])
	require.NotContains(t, schema["dynamic_table"], "_partitiontime")
}
