package config

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// MaxParallelLoadsMap returns, per destination type, Warehouse.<name>.maxParallelLoads or the default given here.
func MaxParallelLoadsMap(conf *config.Config) map[string]int {
	return perDestIntVar(conf, "maxParallelLoads", map[string]int{
		whutils.BQ:              20,
		whutils.RS:              8,
		whutils.POSTGRES:        8,
		whutils.MSSQL:           8,
		whutils.SNOWFLAKE:       8,
		whutils.CLICKHOUSE:      8,
		whutils.DELTALAKE:       8,
		whutils.S3Datalake:      8,
		whutils.GCSDatalake:     8,
		whutils.AzureDatalake:   8,
		whutils.MicrosoftFabric: 8,
	})
}

// ColumnCountLimitMap returns, per destination type, Warehouse.<name>.columnCountLimit or the default given here.
func ColumnCountLimitMap(conf *config.Config) map[string]int {
	return perDestIntVar(conf, "columnCountLimit", map[string]int{
		whutils.AzureSynapse:    1024,
		whutils.MicrosoftFabric: 1024,
		whutils.BQ:              10000,
		whutils.CLICKHOUSE:      1000,
		whutils.MSSQL:           1024,
		whutils.POSTGRES:        1600,
		whutils.RS:              1600,
		whutils.S3Datalake:      10000,
	})
}

func perDestIntVar(conf *config.Config, key string, defaults map[string]int) map[string]int {
	return lo.MapValues(defaults, func(defaultValue int, destType string) int {
		return conf.GetIntVar(defaultValue, 1, fmt.Sprintf("Warehouse.%s.%s", whutils.WHDestNameMap[destType], key))
	})
}
