package configuration_testing

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gofrs/uuid"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	TestTableSchemaMap = map[string]string{
		"id":  "int",
		"val": "string",
	}
	TestPayloadMap = map[string]interface{}{
		"id":  1,
		"val": "RudderStack",
	}
	TestNamespace  = "_rudderstack_setup_test"
	AlterColumnMap = map[string]string{
		"val_alter": "string",
	}
)

// warehouseAdapter returns warehouseT from info request
func warehouse(req *DestinationValidationRequest) warehouseutils.WarehouseT {
	destination := req.Destination

	randomSourceId := randomString()
	randomSourceName := randomString()
	return warehouseutils.WarehouseT{
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

// fileManager returns fileManager from validation request
func fileManager(req *DestinationValidationRequest) (fileManager filemanager.FileManager, err error) {
	destination := req.Destination

	provider := warehouseutils.ObjectStorageType(destination.DestinationDefinition.Name, destination.Config, misc.IsConfiguredToUseRudderObjectStorage(destination.Config))

	fileManager, err = fileManagerFactory.New(&filemanager.SettingsT{
		Provider: provider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         provider,
			Config:           destination.Config,
			UseRudderStorage: misc.IsConfiguredToUseRudderObjectStorage(destination.Config),
		}),
	})
	fileManager.SetTimeout(&fileManagerTimeout)
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to initiate file manager config for testing this destination id %s: err %v", destination.ID, err)
		return
	}
	return
}

func parseOptions(req json.RawMessage, v interface{}) error {
	if err := json.Unmarshal(req, v); err != nil {
		return err
	}
	return nil
}

func randomString() string {
	return strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
}

func stagingTableName() string {
	return fmt.Sprintf(`%s_%s`, warehouseutils.CTStagingTablePrefix, randomString())
}
