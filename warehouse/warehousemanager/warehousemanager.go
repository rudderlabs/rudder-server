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
	Process(config warehouseutils.ConfigT) error
	CrashRecover(config warehouseutils.ConfigT) (err error)
	FetchSchema(warehouse warehouseutils.WarehouseT, namespace string) (map[string]map[string]string, error)
	TestConnection(config warehouseutils.ConfigT) error
}

func NewWhManager(destType string) (WarehouseManager, error) {
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
