package manager

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type ManagerI interface {
	Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) error
	CrashRecover(warehouse warehouseutils.WarehouseT) (err error)
	FetchSchema(warehouse warehouseutils.WarehouseT) (warehouseutils.SchemaT, error)
	CreateSchema() (err error)
	CreateTable(tableName string, columnMap map[string]string) (err error)
	AddColumn(tableName string, columnName string, columnType string) (err error)
	AlterColumn(tableName string, columnName string, columnType string) (err error)
	LoadTable(tableName string) error
	LoadUserTables() map[string]error
	LoadIdentityMergeRulesTable() error
	LoadIdentityMappingsTable() error
	Cleanup()
	IsEmpty(warehouse warehouseutils.WarehouseT) (bool, error)
	TestConnection(warehouse warehouseutils.WarehouseT) error
	DownloadIdentityRules(*misc.GZipWriter) error
	GetTotalCountInTable(tableName string) (int64, error)
	Connect(warehouse warehouseutils.WarehouseT) (client.Client, error)
}

//New is a Factory function that returns a ManagerI of a given destination-type
func New(destType string) (ManagerI, error) {
	switch destType {
	case "RS":
		var rs redshift.HandleT
		return &rs, nil
	case "BQ":
		var bq bigquery.HandleT
		return &bq, nil
	case "SNOWFLAKE":
		var sf snowflake.HandleT
		return &sf, nil
	case "POSTGRES":
		var pg postgres.HandleT
		return &pg, nil
	case "CLICKHOUSE":
		var ch clickhouse.HandleT
		return &ch, nil
	}

	return nil, fmt.Errorf("Provider of type %s is not configured for WarehouseManager", destType)
}
