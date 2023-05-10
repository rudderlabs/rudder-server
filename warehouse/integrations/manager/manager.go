package manager

import (
	"context"
	"fmt"
	"time"

	deltalake_native "github.com/rudderlabs/rudder-server/warehouse/integrations/deltalake-native"

	postgreslegacy "github.com/rudderlabs/rudder-server/warehouse/integrations/postgres-legacy"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	azuresynapse "github.com/rudderlabs/rudder-server/warehouse/integrations/azure-synapse"
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
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Manager interface {
	Setup(warehouse model.Warehouse, uploader warehouseutils.Uploader) error
	CrashRecover()
	FetchSchema() (model.Schema, model.Schema, error)
	CreateSchema() (err error)
	CreateTable(tableName string, columnMap model.TableSchema) (err error)
	AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error)
	AlterColumn(tableName, columnName, columnType string) (model.AlterTableResponse, error)
	LoadTable(ctx context.Context, tableName string) error
	LoadUserTables(ctx context.Context) map[string]error
	LoadIdentityMergeRulesTable() error
	LoadIdentityMappingsTable() error
	Cleanup()
	IsEmpty(warehouse model.Warehouse) (bool, error)
	TestConnection(ctx context.Context, warehouse model.Warehouse) error
	DownloadIdentityRules(*misc.GZipWriter) error
	GetTotalCountInTable(ctx context.Context, tableName string) (int64, error)
	Connect(warehouse model.Warehouse) (client.Client, error)
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
// TODO: Remove flag for useLegacy once the postgres new implementation is stable
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
		if config.Default.GetBool("Warehouse.postgres.useLegacy", true) {
			pg := postgreslegacy.New()
			postgreslegacy.WithConfig(pg, config.Default)
			return pg, nil
		}

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
	case warehouseutils.AZURE_SYNAPSE:
		az := azuresynapse.New()
		azuresynapse.WithConfig(az, config.Default)
		return az, nil
	case warehouseutils.S3_DATALAKE, warehouseutils.GCS_DATALAKE, warehouseutils.AZURE_DATALAKE:
		dl := datalake.New()
		return dl, nil
	case warehouseutils.DELTALAKE:
		if config.Default.GetBool("Warehouse.deltalake.useNative", false) {
			dl := deltalake_native.New()
			deltalake_native.WithConfig(dl, config.Default)
			return dl, nil
		}

		dl := deltalake.New()
		deltalake.WithConfig(dl, config.Default)
		return dl, nil
	}
	return nil, fmt.Errorf("provider of type %s is not configured for WarehouseManager", destType)
}

// NewWarehouseOperations is a Factory function that returns a WarehouseOperations of a given destination-type
// TODO: Remove flag for useLegacy once the postgres new implementation is stable
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
		if config.Default.GetBool("Warehouse.postgres.useLegacy", true) {
			pg := postgreslegacy.New()
			postgreslegacy.WithConfig(pg, config.Default)
			return pg, nil
		}

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
	case warehouseutils.AZURE_SYNAPSE:
		az := azuresynapse.New()
		azuresynapse.WithConfig(az, config.Default)
		return az, nil
	case warehouseutils.S3_DATALAKE, warehouseutils.GCS_DATALAKE, warehouseutils.AZURE_DATALAKE:
		dl := datalake.New()
		return dl, nil
	case warehouseutils.DELTALAKE:
		if config.Default.GetBool("Warehouse.deltalake.useNative", false) {
			dl := deltalake_native.New()
			deltalake_native.WithConfig(dl, config.Default)
			return dl, nil
		}

		dl := deltalake.New()
		deltalake.WithConfig(dl, config.Default)
		return dl, nil
	}
	return nil, fmt.Errorf("provider of type %s is not configured for WarehouseManager", destType)
}
