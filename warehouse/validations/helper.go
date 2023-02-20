package validations

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/logfield"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func warehouse(req *DestinationValidationRequest) warehouseutils.Warehouse {
	destination := req.Destination

	randomSourceId, randomSourceName := warehouseutils.RandHex(), warehouseutils.RandHex()
	return warehouseutils.Warehouse{
		Source: backendconfig.SourceT{
			ID:   randomSourceId,
			Name: randomSourceName,
		},
		Destination: destination,
		Namespace:   warehouseutils.ToSafeNamespace(destination.DestinationDefinition.Name, TestNamespace),
		Type:        destination.DestinationDefinition.Name,
		Identifier:  warehouseutils.GetWarehouseIdentifier(destination.DestinationDefinition.Name, randomSourceId, destination.ID),
	}
}

func fileManager(req *DestinationValidationRequest) (fileManager filemanager.FileManager, err error) {
	destination := req.Destination

	provider := warehouseutils.ObjectStorageType(destination.DestinationDefinition.Name, destination.Config, misc.IsConfiguredToUseRudderObjectStorage(destination.Config))

	fileManager, err = fileManagerFactory.New(&filemanager.SettingsT{
		Provider: provider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         provider,
			Config:           destination.Config,
			UseRudderStorage: misc.IsConfiguredToUseRudderObjectStorage(destination.Config),
			WorkspaceID:      req.Destination.WorkspaceID,
		}),
	})
	if err != nil {
		pkgLogger.Warnw("unable to create filemanager while running validations",
			logfield.DestinationID, destination.ID,
			logfield.DestinationType, destination.DestinationDefinition.Name,
			logfield.DestinationRevisionID, destination.RevisionID,
			logfield.WorkspaceID, destination.WorkspaceID,
			logfield.Provider, provider,
			logfield.Error, err.Error(),
		)
		return
	}
	fileManager.SetTimeout(fileManagerTimeout)
	return
}

func parseOptions(req json.RawMessage, v interface{}) error {
	if err := json.Unmarshal(req, v); err != nil {
		return err
	}
	return nil
}

func stagingTableName() string {
	return fmt.Sprintf(`%s_%s`, warehouseutils.CTStagingTablePrefix, warehouseutils.RandHex())
}
