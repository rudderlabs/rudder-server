package schemarepository_test

import (
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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
