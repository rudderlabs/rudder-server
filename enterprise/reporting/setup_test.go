package reporting

import (
	"fmt"
	"strconv"
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
	mstInstA := f.GetReportingInstance()
	instanceB := mstInstA.GetReportingInstance(types.Report)

	mstInstanceC := f.Setup(&backendconfig.NOOP{})
	instanceC := mstInstanceC.GetReportingInstance(types.Report)
	instanceD := f.Setup(&backendconfig.NOOP{}).GetReportingInstance(types.Report)

	require.Equal(t, instanceA.GetReportingInstance(types.Report), instanceB)
	require.Equal(t, instanceB, instanceC)
	require.Equal(t, instanceC, instanceD)

	f = &Factory{}
	mstE := f.Setup(&backendconfig.NOOP{})
	instanceE := f.Setup(&backendconfig.NOOP{}).GetReportingInstance(types.Report)
	instanceF := mstE.GetReportingInstance(types.Report)
	require.Equal(t, instanceE, instanceF)
	require.NotEqual(t, instanceE, backendconfig.NOOP{})
}

func TestSetupForNoop(t *testing.T) {
	config.Reset()
	logger.Reset()

	type noopTc struct {
		reportingEnabled      bool
		errorReportingEnabled bool
		enterpriseTokenExists bool
	}

	tests := []noopTc{
		{
			reportingEnabled:      false,
			errorReportingEnabled: true,
			enterpriseTokenExists: true,
		},
		{
			reportingEnabled:      true,
			errorReportingEnabled: false,
			enterpriseTokenExists: true,
		},
		{
			reportingEnabled:      true,
			errorReportingEnabled: true,
			enterpriseTokenExists: false,
		},
	}

	for _, tc := range tests {
		testCaseName := fmt.Sprintf("should be NOOP for error-reporting, when reportingEnabled=%v, errorReportingEnabled=%v, enterpriseToken exists(%v)", tc.reportingEnabled, tc.errorReportingEnabled, tc.enterpriseTokenExists)
		t.Run(testCaseName, func(t *testing.T) {
			t.Setenv("RSERVER_REPORTING_ENABLED", strconv.FormatBool(tc.reportingEnabled))
			t.Setenv("RSERVER_REPORTING_ERROR_REPORTING_ENABLED", strconv.FormatBool(tc.errorReportingEnabled))

			f := &Factory{}
			if tc.enterpriseTokenExists {
				f = &Factory{
					EnterpriseToken: "dummy-token",
				}
			}
			f.Setup(&backendconfig.NOOP{})
			instance := f.GetReportingInstance()
			require.Equal(t, instance.GetReportingInstance(types.ErrorDetailReport), &NOOP{})
		})

	}
}
