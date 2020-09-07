package manager

import (
	"errors"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type ManagerI interface {
	Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) error
	CrashRecover(warehouse warehouseutils.WarehouseT) (err error)
	FetchSchema(warehouse warehouseutils.WarehouseT) (warehouseutils.SchemaT, error)
	MigrateSchema(diff warehouseutils.SchemaDiffT) (err error)
	LoadTable(tableName string) error
	LoadUserTables() map[string]error
	LoadIdentityTables() map[string]error
	Cleanup()
	IsEmpty(warehouse warehouseutils.WarehouseT) (bool, error)
	TestConnection(warehouse warehouseutils.WarehouseT) error
	PreLoadIdentityTables() error
	DownloadIdentityRules(*misc.GZipWriter) error
}

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

	return nil, errors.New("no provider configured for WarehouseManager")
}
