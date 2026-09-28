package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	kitconfig "github.com/rudderlabs/rudder-go-kit/config"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestMicrosoftFabricLimits(t *testing.T) {
	conf := kitconfig.New()
	require.Equal(t, 8, MaxParallelLoadsMap(conf)[warehouseutils.MicrosoftFabric])
	require.Equal(t, 1024, ColumnCountLimitMap(conf)[warehouseutils.MicrosoftFabric])
}
