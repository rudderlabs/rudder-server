package manager

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	microsoftfabric "github.com/rudderlabs/rudder-server/warehouse/integrations/microsoft-fabric"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestMicrosoftFabricManagerRegistration(t *testing.T) {
	manager, err := newManager(warehouseutils.MicrosoftFabric, config.New(), logger.NOP, stats.Default)
	require.NoError(t, err)
	require.IsType(t, &microsoftfabric.Fabric{}, manager)

	operations, err := NewWarehouseOperations(warehouseutils.MicrosoftFabric, config.New(), logger.NOP, stats.Default)
	require.NoError(t, err)
	require.IsType(t, &microsoftfabric.Fabric{}, operations)
}
