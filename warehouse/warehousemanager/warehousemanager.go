package warehousemanager

import (
	"errors"

	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type WarehouseManager interface {
	MigrateSchema(warehouseutils.SchemaT, warehouseutils.SchemaDiffT) (err error, state string)
	Process(config warehouseutils.ConfigT) (error, string)
	CrashRecover(config warehouseutils.ConfigT) (err error)
	FetchSchema(warehouse warehouseutils.WarehouseT, namespace string) (map[string]map[string]string, error)
	TestConnection(config warehouseutils.ConfigT) error
	IsEmpty(warehouse warehouseutils.WarehouseT) (bool, error)
}

func NewWhManager(destType string, destID string, namespace string) (WarehouseManager, error) {
	switch destType {
	case "RS":
		var rs redshift.HandleT{Namespace: namespace, DestinationID: destdestID }
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
