package reporting

import (
	"context"
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
	instanceA := f.Setup(context.Background(), &backendconfig.NOOP{})
	instanceB := f.instance

	instanceC := f.Setup(context.Background(), &backendconfig.NOOP{})
	instanceD := f.instance

	require.Equal(t, instanceA, instanceB)
	require.Equal(t, instanceB, instanceC)
	require.Equal(t, instanceC, instanceD)

	f = &Factory{}
	instanceE := f.Setup(context.Background(), &backendconfig.NOOP{})
	instanceF := f.instance
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
		expectedDelegates     int
	}

	tests := []noopTc{
		{
			reportingEnabled:      false,
			errorReportingEnabled: true,
			enterpriseTokenExists: true,
			expectedDelegates:     0,
		},
		{
			reportingEnabled:      true,
			errorReportingEnabled: false,
			enterpriseTokenExists: true,
			expectedDelegates:     1,
		},
		{
			reportingEnabled:      true,
			errorReportingEnabled: true,
			enterpriseTokenExists: false,
			expectedDelegates:     0,
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
			med := NewReportingMediator(context.Background(), logger.NOP, f.EnterpriseToken, &backendconfig.NOOP{})
			require.Len(t, med.delegates, tc.expectedDelegates)
		})

	}
}
