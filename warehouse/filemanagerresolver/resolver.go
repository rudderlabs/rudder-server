package filemanagerresolver

import (
	"github.com/rudderlabs/rudder-go-kit/filemanager"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/microsoft-fabric/onelake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// New wraps base so that providers rudder-go-kit does not know about, such as
// OneLake, are served by their own file managers.
func New(base filemanager.Factory) filemanager.Factory {
	return func(settings *filemanager.Settings) (filemanager.FileManager, error) {
		if settings.Provider == warehouseutils.OneLake {
			return onelake.New(settings.Config)
		}
		return base(settings)
	}
}

// Default is filemanager.New extended with the providers above.
var Default = New(filemanager.New)
