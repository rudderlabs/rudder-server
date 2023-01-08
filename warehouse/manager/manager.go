package manager

import (
	"context"
	"fmt"
	"time"

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

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type ManagerI interface {
	Setup(warehouse warehouseutils.Warehouse, uploader warehouseutils.UploaderI) error
	CrashRecover(warehouse warehouseutils.Warehouse) (err error)
	FetchSchema(warehouse warehouseutils.Warehouse) (warehouseutils.SchemaT, warehouseutils.SchemaT, error)
	CreateSchema() (err error)
	CreateTable(tableName string, columnMap map[string]string) (err error)
	AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error)
	AlterColumn(tableName, columnName, columnType string) (err error)
	LoadTable(tableName string) error
	LoadUserTables() map[string]error
	LoadIdentityMergeRulesTable() error
	LoadIdentityMappingsTable() error
	Cleanup()
	IsEmpty(warehouse warehouseutils.Warehouse) (bool, error)
	TestConnection(warehouse warehouseutils.Warehouse) error
	DownloadIdentityRules(*misc.GZipWriter) error
	GetTotalCountInTable(ctx context.Context, tableName string) (int64, error)
	Connect(warehouse warehouseutils.Warehouse) (client.Client, error)
	LoadTestTable(location, stagingTableName string, payloadMap map[string]interface{}, loadFileFormat string) error
	SetConnectionTimeout(timeout time.Duration)
}

type WarehouseDelete interface {
	DropTable(tableName string) (err error)
	DeleteBy(tableName []string, params warehouseutils.DeleteByParams) error
}

type WarehouseOperations interface {
	ManagerI
	WarehouseDelete
}

// New is a Factory function that returns a ManagerI of a given destination-type
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
		pg := postgres.NewHandle()
		postgres.WithConfig(pg, config.Default)
		return pg, nil
	case warehouseutils.CLICKHOUSE:
		ch := clickhouse.NewHandle()
		clickhouse.WithConfig(ch, config.Default)
		return ch, nil
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
	return nil, fmt.Errorf("provider of type %s is not configured for WarehouseManager", destType)
}

// NewWarehouseOperations is a Factory function that returns a WarehouseOperations of a given destination-type
func NewWarehouseOperations(destType string) (WarehouseOperations, error) {
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
		pg := postgres.NewHandle()
		postgres.WithConfig(pg, config.Default)
		return pg, nil
	case warehouseutils.CLICKHOUSE:
		ch := clickhouse.NewHandle()
		clickhouse.WithConfig(ch, config.Default)
		return ch, nil
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
	return nil, fmt.Errorf("provider of type %s is not configured for WarehouseManager", destType)
}
