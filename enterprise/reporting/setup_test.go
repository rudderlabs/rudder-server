package reporting

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/config"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
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
			med := NewReportingMediator(logger.NOP, f.EnterpriseToken)
			require.Equal(t, med.createErrorReportInstance(&backendconfig.NOOP{}), &NOOP{})
		})

	}
}
