package schemarepository_test

import (
	"testing"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestUseGlue(t *testing.T) {
	testCases := []struct {
		name      string
		warehouse model.Warehouse
		expected  bool
	}{
		{
			name: "use glue with region",
			warehouse: model.Warehouse{
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
			warehouse: model.Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"useGlue": true,
					},
				},
			},
		},
		{
			name: "without glue",
			warehouse: model.Warehouse{
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

	var loadFiles []warehouseutils.LoadFile
	for i := 1; i <= 83; i += 1 {
		loadFiles = append(loadFiles, warehouseutils.LoadFile{
			Location: "s3://test_bucket/test_path",
		})
	}

	batches := lo.Chunk(loadFiles, batchSize)
	require.Equal(t, 1+(len(loadFiles)/batchSize), len(batches))

	var reconstruct []warehouseutils.LoadFile
	for i := range batches {
		require.LessOrEqual(t, len(batches[i]), batchSize)
		reconstruct = append(reconstruct, batches[i]...)
	}

	require.Equal(t, loadFiles, reconstruct)
}
