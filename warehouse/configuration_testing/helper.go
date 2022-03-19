package configuration_testing

import (
	"encoding/json"
	"github.com/gofrs/uuid"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"strings"
	"time"
)

const (
	InvalidStep        = "Invalid step"
	StagingTablePrefix = "setup_test_staging_"
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
	TestNamespace = "_rudderstack_setup_test"
)

// warehouseAdapter returns warehouseT from info request
func (ct *CTHandleT) warehouseAdapter() warehouseutils.WarehouseT {
	destination := ct.infoRequest.Destination

	randomSourceId := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	randomSourceName := strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	return warehouseutils.WarehouseT{
		Source: backendconfig.SourceT{
			ID:   randomSourceId,
			Name: randomSourceName,
		},
		Destination: destination,
		Namespace:   TestNamespace,
		Type:        destination.DestinationDefinition.Name,
		Identifier:  warehouseutils.GetWarehouseIdentifier(destination.DestinationDefinition.Name, randomSourceId, destination.ID),
	}
}

// fileManagerAdapter returns fileManager from info request
func (ct *CTHandleT) fileManagerAdapter() (fileManager filemanager.FileManager, err error) {
	destination := ct.infoRequest.Destination

	provider := warehouseutils.ObjectStorageType(destination.DestinationDefinition.Name, destination.Config, misc.IsConfiguredToUseRudderObjectStorage(destination.Config))

	fileManager, err = fileManagerFactory.New(&filemanager.SettingsT{
		Provider: provider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         provider,
			Config:           destination.Config,
			UseRudderStorage: misc.IsConfiguredToUseRudderObjectStorage(destination.Config)}),
	})
	fileManagerTimeout := time.Duration(15 * time.Second)
	fileManager.SetTimeout(&fileManagerTimeout)
	if err != nil {
		pkgLogger.Errorf("[DCT]: Failed to initiate file manager config for testing this destination id %s: err %v", destination.ID, err)
		return
	}
	return
}

func (ct *CTHandleT) parseOptions(req json.RawMessage, v interface{}) error {
	if err := json.Unmarshal(req, v); err != nil {
		return err
	}
	return nil
}
