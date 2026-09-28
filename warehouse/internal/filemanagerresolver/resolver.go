package filemanagerresolver

import (
	"github.com/rudderlabs/rudder-go-kit/filemanager"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/microsoft-fabric/onelake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// New resolves server-owned storage implementations by destination type and
// delegates all established providers to the injected base factory unchanged.
func New(destinationType string, destinationConfig map[string]any, settings *filemanager.Settings, baseFactory filemanager.Factory) (filemanager.FileManager, error) {
	if destinationType == warehouseutils.MicrosoftFabric {
		return onelake.New(destinationConfig)
	}
	if baseFactory == nil {
		baseFactory = filemanager.New
	}
	return baseFactory(settings)
}
