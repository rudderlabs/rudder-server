package config

import (
	"github.com/rudderlabs/rudder-go-kit/config"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func MaxParallelLoadsMap(conf *config.Config) map[string]int {
	return map[string]int{
		whutils.BQ:            conf.GetIntVar(20, 1, "Warehouse.bigquery.maxParallelLoads"),
		whutils.RS:            conf.GetIntVar(8, 1, "Warehouse.redshift.maxParallelLoads"),
		whutils.POSTGRES:      conf.GetIntVar(8, 1, "Warehouse.postgres.maxParallelLoads"),
		whutils.MSSQL:         conf.GetIntVar(8, 1, "Warehouse.mssql.maxParallelLoads"),
		whutils.SNOWFLAKE:     conf.GetIntVar(8, 1, "Warehouse.snowflake.maxParallelLoads"),
		whutils.CLICKHOUSE:    conf.GetIntVar(8, 1, "Warehouse.clickhouse.maxParallelLoads"),
		whutils.DELTALAKE:     conf.GetIntVar(8, 1, "Warehouse.deltalake.maxParallelLoads"),
		whutils.S3Datalake:    conf.GetIntVar(8, 1, "Warehouse.s3_datalake.maxParallelLoads"),
		whutils.GCSDatalake:   conf.GetIntVar(8, 1, "Warehouse.gcs_datalake.maxParallelLoads"),
		whutils.AzureDatalake: conf.GetIntVar(8, 1, "Warehouse.azure_datalake.maxParallelLoads"),
	}
}

func ColumnCountLimitMap(conf *config.Config) map[string]int {
	return map[string]int{
		whutils.AzureSynapse: conf.GetIntVar(1024, 1, "Warehouse.azure_synapse.columnCountLimit"),
		whutils.BQ:           conf.GetIntVar(10000, 1, "Warehouse.bigquery.columnCountLimit"),
		whutils.CLICKHOUSE:   conf.GetIntVar(1000, 1, "Warehouse.clickhouse.columnCountLimit"),
		whutils.MSSQL:        conf.GetIntVar(1024, 1, "Warehouse.mssql.columnCountLimit"),
		whutils.POSTGRES:     conf.GetIntVar(1600, 1, "Warehouse.postgres.columnCountLimit"),
		whutils.RS:           conf.GetIntVar(1600, 1, "Warehouse.redshift.columnCountLimit"),
		whutils.S3Datalake:   conf.GetIntVar(10000, 1, "Warehouse.s3_datalake.columnCountLimit"),
	}
}
