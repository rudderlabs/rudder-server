package filemanagerresolver

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/filemanager"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/microsoft-fabric/onelake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// Resolver selects destination-owned file managers before delegating standard
// object-storage providers to rudder-go-kit.
type Resolver func(destinationType string, settings *filemanager.Settings) (filemanager.FileManager, error)

func New(base filemanager.Factory) Resolver {
	if base == nil {
		base = filemanager.New
	}
	return func(destinationType string, settings *filemanager.Settings) (filemanager.FileManager, error) {
		if settings == nil {
			return nil, fmt.Errorf("file manager settings are required")
		}
		if destinationType == warehouseutils.MicrosoftFabric && settings.Provider == warehouseutils.OneLake {
			return onelake.New(settings.Config, settings.Logger)
		}
		return base(settings)
	}
}

var Default = New(filemanager.New)
