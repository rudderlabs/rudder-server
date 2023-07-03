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

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Manager interface {
	Setup(ctx context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) error
	CrashRecover(ctx context.Context)
	FetchSchema(ctx context.Context) (model.Schema, model.Schema, error)
	CreateSchema(ctx context.Context) (err error)
	CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error)
	AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error)
	AlterColumn(ctx context.Context, tableName, columnName, columnType string) (model.AlterTableResponse, error)
	LoadTable(ctx context.Context, tableName string) error
	LoadUserTables(ctx context.Context) map[string]error
	LoadIdentityMergeRulesTable(ctx context.Context) error
	LoadIdentityMappingsTable(ctx context.Context) error
	Cleanup(ctx context.Context)
	IsEmpty(ctx context.Context, warehouse model.Warehouse) (bool, error)
	TestConnection(ctx context.Context, warehouse model.Warehouse) error
	DownloadIdentityRules(ctx context.Context, gzWriter *misc.GZipWriter) error
	GetTotalCountInTable(ctx context.Context, tableName string) (int64, error)
	Connect(ctx context.Context, warehouse model.Warehouse) (client.Client, error)
	LoadTestTable(ctx context.Context, location, stagingTableName string, payloadMap map[string]interface{}, loadFileFormat string) error
	SetConnectionTimeout(timeout time.Duration)
	ErrorMappings() []model.JobError
}

type WarehouseDelete interface {
	DropTable(ctx context.Context, tableName string) (err error)
	DeleteBy(ctx context.Context, tableName []string, params warehouseutils.DeleteByParams) error
}

type WarehouseOperations interface {
	Manager
	WarehouseDelete
}

// New is a Factory function that returns a Manager of a given destination-type
func New(destType string) (Manager, error) {
	switch destType {
	case warehouseutils.RS:
		rs := redshift.New()
		redshift.WithConfig(rs, config.Default)
		return rs, nil
	case warehouseutils.BQ:
		bq := bigquery.New()
		bigquery.WithConfig(bq, config.Default)
		return bq, nil
	case warehouseutils.SNOWFLAKE:
		sf := snowflake.New()
		snowflake.WithConfig(sf, config.Default)
		return sf, nil
	case warehouseutils.POSTGRES:
		pg := postgres.New()
		postgres.WithConfig(pg, config.Default)
		return pg, nil
	case warehouseutils.CLICKHOUSE:
		ch := clickhouse.New()
		clickhouse.WithConfig(ch, config.Default)
		return ch, nil
	case warehouseutils.MSSQL:
		ms := mssql.New()
		mssql.WithConfig(ms, config.Default)
		return ms, nil
	case warehouseutils.AzureSynapse:
		az := azuresynapse.New()
		azuresynapse.WithConfig(az, config.Default)
		return az, nil
	case warehouseutils.S3Datalake, warehouseutils.GCSDatalake, warehouseutils.AzureDatalake:
		dl := datalake.New()
		return dl, nil
	case warehouseutils.DELTALAKE:
		dl := deltalake.New()
		deltalake.WithConfig(dl, config.Default)
		return dl, nil
	}
	return nil, fmt.Errorf("provider of type %s is not configured for WarehouseManager", destType)
}

// NewWarehouseOperations is a Factory function that returns a WarehouseOperations of a given destination-type
func NewWarehouseOperations(destType string) (WarehouseOperations, error) {
	switch destType {
	case warehouseutils.RS:
		rs := redshift.New()
		redshift.WithConfig(rs, config.Default)
		return rs, nil
	case warehouseutils.BQ:
		bq := bigquery.New()
		bigquery.WithConfig(bq, config.Default)
		return bq, nil
	case warehouseutils.SNOWFLAKE:
		sf := snowflake.New()
		snowflake.WithConfig(sf, config.Default)
		return sf, nil
	case warehouseutils.POSTGRES:
		pg := postgres.New()
		postgres.WithConfig(pg, config.Default)
		return pg, nil
	case warehouseutils.CLICKHOUSE:
		ch := clickhouse.New()
		clickhouse.WithConfig(ch, config.Default)
		return ch, nil
	case warehouseutils.MSSQL:
		ms := mssql.New()
		mssql.WithConfig(ms, config.Default)
		return ms, nil
	case warehouseutils.AzureSynapse:
		az := azuresynapse.New()
		azuresynapse.WithConfig(az, config.Default)
		return az, nil
	case warehouseutils.S3Datalake, warehouseutils.GCSDatalake, warehouseutils.AzureDatalake:
		dl := datalake.New()
		return dl, nil
	case warehouseutils.DELTALAKE:
		dl := deltalake.New()
		deltalake.WithConfig(dl, config.Default)
		return dl, nil
	}
	return nil, fmt.Errorf("provider of type %s is not configured for WarehouseManager", destType)
}
