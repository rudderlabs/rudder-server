package reporting

import (
	"testing"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

func TestFeatureSetup(t *testing.T) {
	config.Reset()
	logger.Reset()

	f := &Factory{
		EnterpriseToken: "dummy-token",
	}
	instanceA := f.Setup(&backendconfig.NOOP{})
	instanceB := f.GetReportingInstance()

	instanceC := f.Setup(&backendconfig.NOOP{})
	instanceD := f.GetReportingInstance()

	require.Equal(t, instanceA, instanceB)
	require.Equal(t, instanceB, instanceC)
	require.Equal(t, instanceC, instanceD)

	f = &Factory{}
	instanceE := f.Setup(&backendconfig.NOOP{})
	instanceF := f.GetReportingInstance()
	require.Equal(t, instanceE, instanceF)
	require.NotEqual(t, instanceE, backendconfig.NOOP{})
}
