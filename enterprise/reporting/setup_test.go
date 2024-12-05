package reporting

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

func TestFeatureSetup(t *testing.T) {
	logger.Reset()
	config := config.New()

	f := &Factory{
		EnterpriseToken: "dummy-token",
	}
	instanceA := f.Setup(context.Background(), config, &backendconfig.NOOP{})
	instanceB := f.instance

	instanceC := f.Setup(context.Background(), config, &backendconfig.NOOP{})
	instanceD := f.instance

	require.Equal(t, instanceA, instanceB)
	require.Equal(t, instanceB, instanceC)
	require.Equal(t, instanceC, instanceD)

	f = &Factory{}
	instanceE := f.Setup(context.Background(), config, &backendconfig.NOOP{})
	instanceF := f.instance
	require.Equal(t, instanceE, instanceF)
	require.NotEqual(t, instanceE, backendconfig.NOOP{})
}

func TestSetupForDelegates(t *testing.T) {
	logger.Reset()
	config := config.New()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	config.Set("DB.host", postgresContainer.Host)
	config.Set("DB.port", postgresContainer.Port)
	config.Set("DB.user", postgresContainer.User)
	config.Set("DB.name", postgresContainer.Database)
	config.Set("DB.password", postgresContainer.Password)

	testCases := []struct {
		reportingEnabled           bool
		errorReportingEnabled      bool
		enterpriseTokenExists      bool
		errorIndexReportingEnabled bool
		expectedDelegates          int
	}{
		{
			reportingEnabled:           false,
			errorReportingEnabled:      true,
			enterpriseTokenExists:      true,
			errorIndexReportingEnabled: false,
			expectedDelegates:          0,
		},
		{
			reportingEnabled:           true,
			errorReportingEnabled:      false,
			enterpriseTokenExists:      true,
			errorIndexReportingEnabled: false,
			expectedDelegates:          2,
		},
		{
			reportingEnabled:           true,
			errorReportingEnabled:      true,
			enterpriseTokenExists:      true,
			errorIndexReportingEnabled: false,
			expectedDelegates:          3,
		},
		{
			reportingEnabled:           true,
			errorReportingEnabled:      true,
			enterpriseTokenExists:      true,
			errorIndexReportingEnabled: true,
			expectedDelegates:          4,
		},
		{
			reportingEnabled:           true,
			errorReportingEnabled:      true,
			enterpriseTokenExists:      false,
			errorIndexReportingEnabled: false,
			expectedDelegates:          0,
		},
	}

	for _, tc := range testCases {
		testCaseName := fmt.Sprintf("should be NOOP for error-reporting, when reportingEnabled=%v, errorReportingEnabled=%v, errorIndexReportingEnabled=%v, enterpriseToken exists(%v)",
			tc.reportingEnabled,
			tc.errorReportingEnabled,
			tc.errorIndexReportingEnabled,
			tc.enterpriseTokenExists,
		)
		t.Run(testCaseName, func(t *testing.T) {
			t.Setenv("RSERVER_REPORTING_ENABLED", strconv.FormatBool(tc.reportingEnabled))
			t.Setenv("RSERVER_REPORTING_ERROR_REPORTING_ENABLED", strconv.FormatBool(tc.errorReportingEnabled))
			t.Setenv("RSERVER_REPORTING_ERROR_INDEX_REPORTING_ENABLED", strconv.FormatBool(tc.errorIndexReportingEnabled))

			f := &Factory{}
			if tc.enterpriseTokenExists {
				f = &Factory{
					EnterpriseToken: "dummy-token",
				}
			}
			med := NewReportingMediator(context.Background(), config, logger.NOP, f.EnterpriseToken, &backendconfig.NOOP{})
			require.Len(t, med.reporters, tc.expectedDelegates)
		})

	}
}
