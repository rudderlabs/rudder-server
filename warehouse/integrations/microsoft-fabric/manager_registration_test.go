package microsoftfabric_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	microsoftfabric "github.com/rudderlabs/rudder-server/warehouse/integrations/microsoft-fabric"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestManagerRegistration(t *testing.T) {
	operations, err := manager.NewWarehouseOperations(warehouseutils.MicrosoftFabric, config.New(), logger.NOP, stats.NOP)
	require.NoError(t, err)
	require.IsType(t, &microsoftfabric.MicrosoftFabric{}, operations)

	wrapped, err := manager.New(warehouseutils.MicrosoftFabric, config.New(), logger.NOP, stats.NOP)
	require.NoError(t, err)
	require.NotNil(t, wrapped)
}
