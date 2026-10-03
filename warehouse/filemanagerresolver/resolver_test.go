package filemanagerresolver

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/microsoft-fabric/onelake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type fileManagerStub struct{ filemanager.FileManager }

func TestResolverDelegatesNonFabricSettingsUnchanged(t *testing.T) {
	settings := &filemanager.Settings{Provider: warehouseutils.S3, Config: map[string]any{"bucketName": "bucket"}}
	stub := &fileManagerStub{}
	var received *filemanager.Settings
	resolver := New(func(value *filemanager.Settings) (filemanager.FileManager, error) {
		received = value
		return stub, nil
	})

	manager, err := resolver(warehouseutils.RS, settings)
	require.NoError(t, err)
	require.Same(t, settings, received)
	require.Same(t, stub, manager)
}

func TestResolverSelectsOneLakeForFabric(t *testing.T) {
	called := false
	resolver := New(func(*filemanager.Settings) (filemanager.FileManager, error) {
		called = true
		return &fileManagerStub{}, nil
	})
	settings := &filemanager.Settings{
		Provider: warehouseutils.OneLake,
		Logger:   logger.NOP,
		Config: map[string]any{
			"fabricWorkspaceId": "11111111-1111-1111-1111-111111111111",
			"lakehouseId":       "22222222-2222-2222-2222-222222222222",
			"tenantId":          "tenant",
			"clientId":          "client",
			"clientSecret":      "secret",
		},
	}

	manager, err := resolver(warehouseutils.MicrosoftFabric, settings)
	require.NoError(t, err)
	require.IsType(t, &onelake.Manager{}, manager)
	require.False(t, called)
}

func TestResolverRequiresSettings(t *testing.T) {
	_, err := New(nil)(warehouseutils.MicrosoftFabric, nil)
	require.ErrorContains(t, err, "settings are required")
}
