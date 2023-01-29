package schemarepository_test

import (
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestUseGlue(t *testing.T) {
	testCases := []struct {
		name      string
		warehouse warehouseutils.Warehouse
		expected  bool
	}{
		{
			name: "use glue with region",
			warehouse: warehouseutils.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"useGlue": true,
						"region":  "test_region",
					},
				},
			},
			expected: true,
		},
		{
			name: "use glue without region",
			warehouse: warehouseutils.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"useGlue": true,
					},
				},
			},
		},
		{
			name: "without glue",
			warehouse: warehouseutils.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := schemarepository.UseGlue(&tc.warehouse)
			if actual != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, actual)
			}
		})
	}
}

func TestLoadFileBatching(t *testing.T) {
	batchSize := 10

	var loadFiles []warehouseutils.LoadFileT
	for i := 1; i <= 83; i += 1 {
		loadFiles = append(loadFiles, warehouseutils.LoadFileT{
			Location: "s3://test_bucket/test_path",
		})
	}

	batches := schemarepository.LoadFileBatching(loadFiles, batchSize)
	require.Equal(t, 1+(len(loadFiles)/batchSize), len(batches))

	var reconstruct []warehouseutils.LoadFileT
	for i := range batches {
		require.LessOrEqual(t, len(batches[i]), batchSize)
		reconstruct = append(reconstruct, batches[i]...)
	}

	require.Equal(t, loadFiles, reconstruct)
}
