package manager

import (
	"fmt"
	azuresynapse "github.com/rudderlabs/rudder-server/warehouse/azure-synapse"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/datalake"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"

	"github.com/rudderlabs/rudder-server/utils/misc"
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
	LoadTestTable(client *client.Client, location string, warehouse warehouseutils.WarehouseT, stagingTableName string, columns map[string]string, payloadMap map[string]interface{}, format string) error
}

//New is a Factory function that returns a ManagerI of a given destination-type
func New(destType string) (ManagerI, error) {
	switch destType {
	case warehouseutils.RS:
		var rs redshift.HandleT
		return &rs, nil
	case warehouseutils.BQ:
		var bq bigquery.HandleT
		return &bq, nil
	case warehouseutils.SNOWFLAKE:
		var sf snowflake.HandleT
		return &sf, nil
	case warehouseutils.POSTGRES:
		var pg postgres.HandleT
		return &pg, nil
	case warehouseutils.CLICKHOUSE:
		var ch clickhouse.HandleT
		return &ch, nil
	case warehouseutils.MSSQL:
		var ms mssql.HandleT
		return &ms, nil
	case warehouseutils.AZURE_SYNAPSE:
		var as azuresynapse.HandleT
		return &as, nil
	case warehouseutils.S3_DATALAKE, warehouseutils.GCS_DATALAKE, warehouseutils.AZURE_DATALAKE:
		var dl datalake.HandleT
		return &dl, nil
	case warehouseutils.DELTALAKE:
		var dl deltalake.HandleT
		return &dl, nil
	}
	return nil, fmt.Errorf("Provider of type %s is not configured for WarehouseManager", destType)
}
