package asyncdestinationmanager

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func TestBQStreamAllEventsIsRegistered(t *testing.T) {
	t.Parallel()
	require.True(t, common.IsAsyncRegularDestination("BQSTREAM_ALL_EVENTS"))

	destination := &backendconfig.DestinationT{
		DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "BQSTREAM_ALL_EVENTS"},
	}
	m, err := NewManager(config.Default, logger.NOP, stats.NOP, destination, nil)
	require.NoError(t, err)
	require.NotNil(t, m)
}
