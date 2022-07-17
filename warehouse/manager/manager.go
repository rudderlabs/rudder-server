package manager

import (
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

	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type QueryManagerI interface {
	Connect(warehouse warehouseutils.WarehouseT) (client.Client, error)
	FetchSchema(warehouse warehouseutils.WarehouseT) (warehouseutils.SchemaT, error)
	CreateSchema() (err error)
	CreateTable(tableName string, columnMap map[string]string) (err error)
	AddColumn(tableName, columnName, columnType string) (err error)
	AlterColumn(tableName, columnName, columnType string) (err error)
	IsEmpty(warehouse warehouseutils.WarehouseT) (bool, error)
	TestConnection(warehouse warehouseutils.WarehouseT) error
	SetConnectionTimeout(timeout time.Duration)
	GetTotalCountInTable(tableName string) (int64, error)
}

type UploadManagerI interface {
	Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) error
	CrashRecover(warehouse warehouseutils.WarehouseT) (err error)
	LoadTable(tableName string) error
	LoadUserTables() map[string]error
	LoadIdentityMergeRulesTable() error
	LoadIdentityMappingsTable() error
	Cleanup()
	DownloadIdentityRules(*misc.GZipWriter) error
	LoadTestTable(location, stagingTableName string, payloadMap map[string]interface{}, loadFileFormat string) error
}

type WarehouseDelete interface {
	DropTable(tableName string) (err error)
}

type WarehouseOperations interface {
	QueryManagerI
	UploadManagerI
	WarehouseDelete
}

// New is a Factory function that returns a QueryManagerI of a given destination-type
func NewQueryManager(destType string) (QueryManagerI, error) {
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

func NewUploadManager(destType string) (UploadManagerI, error) {
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
