package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/azure-synapse"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/datalake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Manager interface {
	Setup(warehouse warehouseutils.Warehouse, uploader warehouseutils.UploaderI) error
	CrashRecover(warehouse warehouseutils.Warehouse) (err error)
	FetchSchema(warehouse warehouseutils.Warehouse) (warehouseutils.SchemaT, warehouseutils.SchemaT, error)
	CreateSchema() (err error)
	CreateTable(tableName string, columnMap map[string]string) (err error)
	AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error)
	AlterColumn(tableName, columnName, columnType string) (model.AlterTableResponse, error)
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
	ErrorMappings() []model.JobError
}

type WarehouseDelete interface {
	DropTable(tableName string) (err error)
	DeleteBy(tableName []string, params warehouseutils.DeleteByParams) error
}

type WarehouseOperations interface {
	Manager
	WarehouseDelete
}

// New is a Factory function that returns a Manager of a given destination-type
func New(destType string) (Manager, error) {
	switch destType {
	case warehouseutils.RS:
		rs := redshift.NewRedshift()
		redshift.WithConfig(rs, config.Default)
		return rs, nil
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
		ch := clickhouse.NewClickhouse()
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
		dl := deltalake.NewDeltalake()
		deltalake.WithConfig(dl, config.Default)
		return dl, nil
	}
	return nil, fmt.Errorf("provider of type %s is not configured for WarehouseManager", destType)
}

// NewWarehouseOperations is a Factory function that returns a WarehouseOperations of a given destination-type
func NewWarehouseOperations(destType string) (WarehouseOperations, error) {
	switch destType {
	case warehouseutils.RS:
		rs := redshift.NewRedshift()
		redshift.WithConfig(rs, config.Default)
		return rs, nil
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
		ch := clickhouse.NewClickhouse()
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
		dl := deltalake.NewDeltalake()
		deltalake.WithConfig(dl, config.Default)
		return dl, nil
	}
	return nil, fmt.Errorf("provider of type %s is not configured for WarehouseManager", destType)
}
