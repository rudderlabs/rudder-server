package config

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func MaxParallelLoadsMap(conf *config.Config) map[string]int {
	return map[string]int{
		whutils.BQ:            conf.GetInt("Warehouse.bigquery.maxParallelLoads", 20),
		whutils.RS:            conf.GetInt("Warehouse.redshift.maxParallelLoads", 8),
		whutils.POSTGRES:      conf.GetInt("Warehouse.postgres.maxParallelLoads", 8),
		whutils.MSSQL:         conf.GetInt("Warehouse.mssql.maxParallelLoads", 8),
		whutils.SNOWFLAKE:     conf.GetInt("Warehouse.snowflake.maxParallelLoads", 8),
		whutils.CLICKHOUSE:    conf.GetInt("Warehouse.clickhouse.maxParallelLoads", 8),
		whutils.DELTALAKE:     conf.GetInt("Warehouse.deltalake.maxParallelLoads", 8),
		whutils.S3Datalake:    conf.GetInt("Warehouse.s3_datalake.maxParallelLoads", 8),
		whutils.GCSDatalake:   conf.GetInt("Warehouse.gcs_datalake.maxParallelLoads", 8),
		whutils.AzureDatalake: conf.GetInt("Warehouse.azure_datalake.maxParallelLoads", 8),
	}
}

func ColumnCountLimitMap(conf *config.Config) map[string]int {
	return map[string]int{
		whutils.AzureSynapse: conf.GetInt("Warehouse.azure_synapse.columnCountLimit", 1024),
		whutils.BQ:           conf.GetInt("Warehouse.bigquery.columnCountLimit", 10000),
		whutils.CLICKHOUSE:   conf.GetInt("Warehouse.clickhouse.columnCountLimit", 1000),
		whutils.MSSQL:        conf.GetInt("Warehouse.mssql.columnCountLimit", 1024),
		whutils.POSTGRES:     conf.GetInt("Warehouse.postgres.columnCountLimit", 1600),
		whutils.RS:           conf.GetInt("Warehouse.redshift.columnCountLimit", 1600),
		whutils.S3Datalake:   conf.GetInt("Warehouse.s3_datalake.columnCountLimit", 10000),
	}
}
