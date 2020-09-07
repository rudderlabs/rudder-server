package manager

import (
	"errors"

	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type ManagerI interface {
	Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI)
	CrashRecover(warehouse warehouseutils.WarehouseT) (err error)
	FetchSchema(warehouse warehouseutils.WarehouseT) (warehouseutils.SchemaT, error)
	Connect() error
	MigrateSchema(diff warehouseutils.SchemaDiffT, currentSchemaInWarehouse warehouseutils.SchemaT) (err error)
	LoadTable(tableName string, tableSchemaInUpload warehouseutils.TableSchemaT, tableSchemaInWarehouse warehouseutils.TableSchemaT) error
	LoadUserTables(userTablesSchemasInUpload warehouseutils.SchemaT, userTablesSchemasInWarehouse warehouseutils.SchemaT) error
	Cleanup()
	IsEmpty(warehouse warehouseutils.WarehouseT) (bool, error)
	TestConnection(config warehouseutils.ConfigT) error
}

func New(destType string) (ManagerI, error) {
	switch destType {
	case "RS":
		var rs redshift.HandleT
		return &rs, nil
		// case "BQ":
		// 	var bq bigquery.HandleT
		// 	return &bq, nil
		// case "SNOWFLAKE":
		// 	var sf snowflake.HandleT
		// 	return &sf, nil
		// case "POSTGRES":
		// 	var pg postgres.HandleT
		// 	return &pg, nil
		// case "CLICKHOUSE":
		// 	var ch clickhouse.HandleT
		// 	return &ch, nil
	}

	return nil, errors.New("no provider configured for WarehouseManager")
}
