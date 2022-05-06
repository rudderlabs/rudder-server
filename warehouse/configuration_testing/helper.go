package configuration_testing

import (
	"context"
	"encoding/json"
	"github.com/gofrs/uuid"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
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
	TestNamespace  = "_rudderstack_setup_test"
	AlterColumnMap = map[string]string{
		"val_alter": "string",
	}
)

// warehouseAdapter returns warehouseT from info request
func (ct *CTHandleT) warehouseAdapter() warehouseutils.WarehouseT {
	destination := ct.infoRequest.Destination

	randomSourceId := GetRandomString()
	randomSourceName := GetRandomString()
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

func GetRandomString() string {
	return strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
}

func (ct *CTHandleT) GetDestinationType() (destinationType string) {
	destination := ct.infoRequest.Destination
	destinationType = destination.DestinationDefinition.Name
	return
}

func (ct *CTHandleT) GetBigQueryHandle() *bigquery.HandleT {
	bqHandle := bigquery.HandleT{
		BQContext: context.Background(),
		Db:        ct.client.BQ,
		Namespace: ct.warehouse.Namespace,
		Warehouse: ct.warehouse,
		ProjectID: strings.TrimSpace(warehouseutils.GetConfigValue(bigquery.GCPProjectID, ct.warehouse)),
	}
	return &bqHandle
}

func (ct *CTHandleT) GetDatabricksHandle() *deltalake.HandleT {
	dbHandle := deltalake.HandleT{
		Namespace: ct.warehouse.Namespace,
		Warehouse: ct.warehouse,
	}
	return &dbHandle
}
