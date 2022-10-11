package datalake

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	schemarepository "github.com/rudderlabs/rudder-server/warehouse/datalake/schema-repository"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// TODO: Handle error using error types.
var (
	pkgLogger logger.Logger
)

func Init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("datalake")
}

type HandleT struct {
	SchemaRepository schemarepository.SchemaRepository
	Warehouse        warehouseutils.Warehouse
	Uploader         warehouseutils.UploaderI
}

func (wh *HandleT) Setup(warehouse warehouseutils.Warehouse, uploader warehouseutils.UploaderI) (err error) {
	wh.Warehouse = warehouse
	wh.Uploader = uploader

	wh.SchemaRepository, err = schemarepository.NewSchemaRepository(wh.Warehouse, wh.Uploader)

	return err
}

func (*HandleT) CrashRecover(_ warehouseutils.Warehouse) (err error) {
	return nil
}

func (wh *HandleT) FetchSchema(warehouse warehouseutils.Warehouse) (warehouseutils.SchemaT, error) {
	return wh.SchemaRepository.FetchSchema(warehouse)
}

func (wh *HandleT) CreateSchema() (err error) {
	return wh.SchemaRepository.CreateSchema()
}

func (wh *HandleT) CreateTable(tableName string, columnMap map[string]string) (err error) {
	return wh.SchemaRepository.CreateTable(tableName, columnMap)
}

func (*HandleT) DropTable(_ string) (err error) {
	return fmt.Errorf("datalake err :not implemented")
}

func (wh *HandleT) AddColumn(tableName, columnName, columnType string) (err error) {
	return wh.SchemaRepository.AddColumn(tableName, columnName, columnType)
}

func (wh *HandleT) AlterColumn(tableName, columnName, columnType string) (err error) {
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

func (*HandleT) Cleanup() {
}

func (*HandleT) IsEmpty(_ warehouseutils.Warehouse) (bool, error) {
	return false, nil
}

func (*HandleT) TestConnection(_ warehouseutils.Warehouse) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (*HandleT) DownloadIdentityRules(*misc.GZipWriter) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (*HandleT) GetTotalCountInTable(_ string) (int64, error) {
	return 0, nil
}

func (*HandleT) Connect(_ warehouseutils.Warehouse) (client.Client, error) {
	return client.Client{}, fmt.Errorf("datalake err :not implemented")
}

func (*HandleT) LoadTestTable(_, _ string, _ map[string]interface{}, _ string) error {
	return fmt.Errorf("datalake err :not implemented")
}

func (*HandleT) SetConnectionTimeout(_ time.Duration) {
}
