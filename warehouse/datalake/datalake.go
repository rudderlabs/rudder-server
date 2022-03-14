package datalake

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	schemarepository "github.com/rudderlabs/rudder-server/warehouse/datalake/schema-repository"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	pkgLogger logger.LoggerI
)

func init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("datalake")
}

type HandleT struct {
	SchemaRepository schemarepository.SchemaRepository
	Warehouse        warehouseutils.WarehouseT
	Uploader         warehouseutils.UploaderI
}

func (wh *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (err error) {
	wh.Warehouse = warehouse
	wh.Uploader = uploader

	wh.SchemaRepository, err = schemarepository.NewSchemaRepository(wh.Warehouse, wh.Uploader)

	return err
}

func (wh *HandleT) CrashRecover(warehouse warehouseutils.WarehouseT) (err error) {
	return nil
}

func (wh *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT) (warehouseutils.SchemaT, error) {
	return wh.SchemaRepository.FetchSchema(warehouse)
}

func (wh *HandleT) CreateSchema() (err error) {
	return wh.SchemaRepository.CreateSchema()
}

func (wh *HandleT) CreateTable(tableName string, columnMap map[string]string) (err error) {
	return wh.SchemaRepository.CreateTable(tableName, columnMap)
}

func (wh *HandleT) AddColumn(tableName string, columnName string, columnType string) (err error) {
	return wh.SchemaRepository.AddColumn(tableName, columnName, columnType)
}

func (wh *HandleT) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return wh.SchemaRepository.AlterColumn(tableName, columnName, columnType)
}

func (wh *HandleT) LoadTable(tableName string) error {
	pkgLogger.Infof("Skipping load for table %s : %s is a datalake destination", tableName, wh.Warehouse.Destination.ID)
	return nil
}

func (wh *HandleT) LoadUserTables() map[string]error {
	pkgLogger.Infof("Skipping load for user tables : %s is a datalake destination", wh.Warehouse.Destination.ID)
	// return map with nil error entries for identifies and users(if any) tables
	// this is so that they are marked as succeeded
	errorMap := map[string]error{warehouseutils.IdentifiesTable: nil}
	if len(wh.Uploader.GetTableSchemaInUpload(warehouseutils.UsersTable)) > 0 {
		errorMap[warehouseutils.UsersTable] = nil
	}
	return errorMap
}

func (wh *HandleT) LoadIdentityMergeRulesTable() error {
	pkgLogger.Infof("Skipping load for identity merge rules : %s is a datalake destination", wh.Warehouse.Destination.ID)
	return nil
}

func (wh *HandleT) LoadIdentityMappingsTable() error {
	pkgLogger.Infof("Skipping load for identity mappings : %s is a datalake destination", wh.Warehouse.Destination.ID)
	return nil
}

func (wh *HandleT) Cleanup() {
}

func (wh *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (bool, error) {
	return false, nil
}

func (wh *HandleT) TestConnection(warehouse warehouseutils.WarehouseT) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (wh *HandleT) DownloadIdentityRules(*misc.GZipWriter) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (wh *HandleT) GetTotalCountInTable(tableName string) (int64, error) {
	return 0, nil
}

func (wh *HandleT) Connect(warehouse warehouseutils.WarehouseT) (client.Client, error) {
	return client.Client{}, fmt.Errorf("datalake err :not implemented")
}

func (wh *HandleT) CreateTestSchema(warehouse warehouseutils.WarehouseT) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (wh *HandleT) CreateTestTable(warehouse warehouseutils.WarehouseT, stagingTableName string, columns map[string]string) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (wh *HandleT) LoadTestTable(location string, warehouse warehouseutils.WarehouseT, stagingTableName string, columns map[string]string, payloadMap map[string]interface{}, format string) error {
	return fmt.Errorf("datalake err :not implemented")
}
