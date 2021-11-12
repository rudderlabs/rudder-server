package manager

import (
	"fmt"
	"github.com/rudderlabs/rudder-server/app"
	azuresynapse "github.com/rudderlabs/rudder-server/warehouse/azure-synapse"
	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/datalake"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

//New is a Factory function that returns a ManagerI of a given destination-type
func New(destType string, app app.Interface) (warehouseutils.ManagerI, error) {
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
	case "MSSQL":
		var ms mssql.HandleT
		return &ms, nil
	case "AZURE_SYNAPSE":
		var as azuresynapse.HandleT
		return &as, nil
	case "S3_DATALAKE", "GCS_DATALAKE", "AZURE_DATALAKE":
		var dl datalake.HandleT
		return &dl, nil
	case "DELTALAKE":
		if app.Features().DeltaLake != nil {
			return app.Features().DeltaLake.GetManager(), nil
		}
		return nil, fmt.Errorf("Enterprise features of type %s is not configured", destType)
	}
	return nil, fmt.Errorf("Provider of type %s is not configured for WarehouseManager", destType)
}
