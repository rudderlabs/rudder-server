package filemanagerresolver

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/filemanager"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/microsoft-fabric/onelake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestNewDelegatesNonFabricProviders(t *testing.T) {
	settings := &filemanager.Settings{Provider: warehouseutils.S3}
	called := false
	expected, err := onelake.New(map[string]any{
		"host": "onelake.dfs.fabric.microsoft.com", "fabricWorkspaceId": "11111111-1111-1111-1111-111111111111",
		"lakehouseId": "22222222-2222-2222-2222-222222222222", "tenantId": "tenant", "clientId": "client", "clientSecret": "secret",
	})
	require.NoError(t, err)
	manager, err := New(warehouseutils.RS, map[string]any{}, settings, func(received *filemanager.Settings) (filemanager.FileManager, error) {
		called = true
		require.Same(t, settings, received)
		return expected, nil
	})
	require.NoError(t, err)
	require.Same(t, expected, manager)
	require.True(t, called)
}

func TestNewResolvesFabricToOneLake(t *testing.T) {
	manager, err := New(warehouseutils.MicrosoftFabric, map[string]any{
		"host": "onelake.dfs.fabric.microsoft.com", "fabricWorkspaceId": "11111111-1111-1111-1111-111111111111",
		"lakehouseId": "22222222-2222-2222-2222-222222222222", "tenantId": "tenant", "clientId": "client", "clientSecret": "secret",
	}, &filemanager.Settings{}, nil)
	require.NoError(t, err)
	require.IsType(t, &onelake.Manager{}, manager)
}
