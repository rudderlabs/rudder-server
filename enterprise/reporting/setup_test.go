package reporting

import (
	"testing"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/types"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/stretchr/testify/require"
)

func TestFeatureSetup(t *testing.T) {
	config.Reset()
	logger.Reset()

	f := &Factory{
		EnterpriseToken: "dummy-token",
	}
	instanceA := f.Setup(&backendconfig.NOOP{})
	instanceB := f.GetReportingInstance(types.Report)

	instanceC := f.Setup(&backendconfig.NOOP{})
	instanceD := f.GetReportingInstance(types.Report)

	require.Equal(t, instanceA.ReportingInstance, instanceB)
	require.Equal(t, instanceB, instanceC.ReportingInstance)
	require.Equal(t, instanceC.ReportingInstance, instanceD)

	f = &Factory{}
	instanceE := f.Setup(&backendconfig.NOOP{})
	instanceF := f.GetReportingInstance(types.Report)
	require.Equal(t, instanceE.ReportingInstance, instanceF)
	require.NotEqual(t, instanceE.ReportingInstance, backendconfig.NOOP{})
}
