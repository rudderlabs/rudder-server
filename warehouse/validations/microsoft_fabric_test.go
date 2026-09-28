package validations

import (
	"testing"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/microsoft-fabric/onelake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestCreateFileManagerUsesOneLakeForMicrosoftFabric(t *testing.T) {
	destination := &backendconfig.DestinationT{
		DestinationDefinition: backendconfig.DestinationDefinitionT{Name: warehouseutils.MicrosoftFabric},
		Config: map[string]any{
			"host": "onelake.dfs.fabric.microsoft.com", "fabricWorkspaceId": "11111111-1111-1111-1111-111111111111",
			"lakehouseId": "22222222-2222-2222-2222-222222222222", "tenantId": "tenant", "clientId": "client", "clientSecret": "secret",
		},
	}
	manager, err := createFileManager(destination)
	require.NoError(t, err)
	require.IsType(t, &onelake.Manager{}, manager)
}
