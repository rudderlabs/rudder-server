package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/types"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	azuresynapse "github.com/rudderlabs/rudder-server/warehouse/integrations/azure-synapse"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/datalake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Manager interface {
	Setup(ctx context.Context, warehouse model.Warehouse, uploader warehouseutils.Uploader) error
	FetchSchema(ctx context.Context) (model.Schema, error)
	CreateSchema(ctx context.Context) (err error)
	CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error)
	AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error)
	AlterColumn(ctx context.Context, tableName, columnName, columnType string) (model.AlterTableResponse, error)
	LoadTable(ctx context.Context, tableName string) (*types.LoadTableStats, error)
	LoadUserTables(ctx context.Context) map[string]error
	LoadIdentityMergeRulesTable(ctx context.Context) error
	LoadIdentityMappingsTable(ctx context.Context) error
	Cleanup(ctx context.Context)
	IsEmpty(ctx context.Context, warehouse model.Warehouse) (bool, error)
	TestConnection(ctx context.Context, warehouse model.Warehouse) error
	DownloadIdentityRules(ctx context.Context, gzWriter *misc.GZipWriter) error
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
func New(destType string, conf *config.Config, logger logger.Logger, stats stats.Stats) (Manager, error) {
	switch destType {
	case warehouseutils.RS:
		return redshift.New(conf, logger, stats), nil
	case warehouseutils.BQ:
		return bigquery.New(conf, logger), nil
	case warehouseutils.SNOWFLAKE, warehouseutils.SnowpipeStreaming:
		return snowflake.New(conf, logger, stats), nil
	case warehouseutils.POSTGRES:
		return postgres.New(conf, logger, stats), nil
	case warehouseutils.CLICKHOUSE:
		return clickhouse.New(conf, logger, stats), nil
	case warehouseutils.MSSQL:
		return mssql.New(conf, logger, stats), nil
	case warehouseutils.AzureSynapse:
		return azuresynapse.New(conf, logger, stats), nil
	case warehouseutils.S3Datalake, warehouseutils.GCSDatalake, warehouseutils.AzureDatalake:
		return datalake.New(conf, logger), nil
	case warehouseutils.DELTALAKE:
		return deltalake.New(conf, logger, stats), nil
	}
	return nil, fmt.Errorf("provider of type %s is not configured for WarehouseManager", destType)
}

// NewWarehouseOperations is a Factory function that returns a WarehouseOperations of a given destination-type
func NewWarehouseOperations(destType string, conf *config.Config, logger logger.Logger, stats stats.Stats) (WarehouseOperations, error) {
	switch destType {
	case warehouseutils.RS:
		return redshift.New(conf, logger, stats), nil
	case warehouseutils.BQ:
		return bigquery.New(conf, logger), nil
	case warehouseutils.SNOWFLAKE, warehouseutils.SnowpipeStreaming:
		return snowflake.New(conf, logger, stats), nil
	case warehouseutils.POSTGRES:
		return postgres.New(conf, logger, stats), nil
	case warehouseutils.CLICKHOUSE:
		return clickhouse.New(conf, logger, stats), nil
	case warehouseutils.MSSQL:
		return mssql.New(conf, logger, stats), nil
	case warehouseutils.AzureSynapse:
		return azuresynapse.New(conf, logger, stats), nil
	case warehouseutils.S3Datalake, warehouseutils.GCSDatalake, warehouseutils.AzureDatalake:
		return datalake.New(conf, logger), nil
	case warehouseutils.DELTALAKE:
		return deltalake.New(conf, logger, stats), nil
	}
	return nil, fmt.Errorf("provider of type %s is not configured for WarehouseManager", destType)
}
