package reporting

import (
	"testing"

	"github.com/rudderlabs/rudder-server/config"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

func TestFeatureSetup(t *testing.T) {
	config.Load()
	logger.Init()

	f := &Factory{
		EnterpriseToken: "dummy-token",
	}
	instanceA := f.Setup(&NOOPConfig{})
	instanceB := f.GetReportingInstance()

	instanceC := f.Setup(&NOOPConfig{})
	instanceD := f.GetReportingInstance()

	require.Equal(t, instanceA, instanceB)
	require.Equal(t, instanceB, instanceC)
	require.Equal(t, instanceC, instanceD)
}
