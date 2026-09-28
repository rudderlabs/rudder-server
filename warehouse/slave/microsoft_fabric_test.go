package slave

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/microsoft-fabric/onelake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestBasePayloadUsesOneLakeForMicrosoftFabric(t *testing.T) {
	payload := basePayload{DestinationType: warehouseutils.MicrosoftFabric}
	manager, err := payload.fileManager(map[string]any{
		"host": "onelake.dfs.fabric.microsoft.com", "fabricWorkspaceId": "11111111-1111-1111-1111-111111111111",
		"lakehouseId": "22222222-2222-2222-2222-222222222222", "tenantId": "tenant", "clientId": "client", "clientSecret": "secret",
	}, false)
	require.NoError(t, err)
	require.IsType(t, &onelake.Manager{}, manager)
}
