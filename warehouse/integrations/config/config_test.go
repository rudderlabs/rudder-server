package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	kitconfig "github.com/rudderlabs/rudder-go-kit/config"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestMicrosoftFabricDefaults(t *testing.T) {
	conf := kitconfig.New()
	require.Equal(t, 8, MaxParallelLoadsMap(conf)[warehouseutils.MicrosoftFabric])
	require.Equal(t, 1024, ColumnCountLimitMap(conf)[warehouseutils.MicrosoftFabric])
}

func TestPerDestinationOverrides(t *testing.T) {
	for key, mapFn := range map[string]func(*kitconfig.Config) map[string]int{
		"maxParallelLoads": MaxParallelLoadsMap,
		"columnCountLimit": ColumnCountLimitMap,
	} {
		for destType := range mapFn(kitconfig.New()) {
			name, ok := warehouseutils.WHDestNameMap[destType]
			require.Truef(t, ok, "%s has no WHDestNameMap entry", destType)

			conf := kitconfig.New()
			conf.Set("Warehouse."+name+"."+key, 42)
			require.Equalf(t, 42, mapFn(conf)[destType], "Warehouse.%s.%s", name, key)
		}
	}
}
