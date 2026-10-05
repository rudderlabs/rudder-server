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

func TestFactoryDelegatesOtherProvidersUnchanged(t *testing.T) {
	settings := &filemanager.Settings{Provider: warehouseutils.S3, Config: map[string]any{"bucketName": "bucket"}}
	stub := &fileManagerStub{}
	var received *filemanager.Settings
	factory := New(func(value *filemanager.Settings) (filemanager.FileManager, error) {
		received = value
		return stub, nil
	})

	manager, err := factory(settings)
	require.NoError(t, err)
	require.Same(t, settings, received)
	require.Same(t, stub, manager)
}

func TestFactorySelectsOneLake(t *testing.T) {
	called := false
	factory := New(func(*filemanager.Settings) (filemanager.FileManager, error) {
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

	manager, err := factory(settings)
	require.NoError(t, err)
	require.IsType(t, &onelake.Manager{}, manager)
	require.False(t, called)
}
