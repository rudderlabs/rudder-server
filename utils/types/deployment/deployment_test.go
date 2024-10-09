package deployment_test

import (
	"fmt"
	"testing"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/admin"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

func Test_GetFromEnv(t *testing.T) {
	envScope := []string{"HOSTED_SERVICE", "DEPLOYMENT_TYPE"}
	t.Logf("Only %s will be considered for environment variable", envScope)

	testcases := []struct {
		err          error
		envs         map[string]string
		name         string
		expectedType deployment.Type
	}{
		// DedicatedType:
		{
			name:         "no env variables - default type",
			envs:         map[string]string{},
			expectedType: deployment.DedicatedType,
		},
		{
			name: "DEPLOYMENT_TYPE is DEDICATED",
			envs: map[string]string{
				"DEPLOYMENT_TYPE": "DEDICATED",
			},
			expectedType: deployment.DedicatedType,
		},
		{
			name: "DEPLOYMENT_TYPE is DEDICATED override HOSTED_SERVICE",
			envs: map[string]string{
				"DEPLOYMENT_TYPE": "DEDICATED",
				"HOSTED_SERVICE":  "true",
			},
			expectedType: deployment.DedicatedType,
		},
		{
			name: "legacy HOSTED_SERVICE is false",
			envs: map[string]string{
				"HOSTED_SERVICE": "false",
			},
			expectedType: deployment.DedicatedType,
		},

		// MultiTenantType:
		{
			envs: map[string]string{
				"DEPLOYMENT_TYPE": "MULTITENANT",
			},
			expectedType: deployment.MultiTenantType,
		},
		{
			envs: map[string]string{
				"DEPLOYMENT_TYPE": "MULTITENANT",
				"HOSTED_SERVICE":  "true",
			},
			expectedType: deployment.MultiTenantType,
		},

		// Invalid:
		{
			envs: map[string]string{
				"DEPLOYMENT_TYPE": "A_NOT_VALID_TYPE",
			},
			err: fmt.Errorf("invalid deployment type: \"A_NOT_VALID_TYPE\""),
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			for _, v := range envScope {
				t.Setenv(v, tt.envs[v])
			}

			dType, err := deployment.GetFromEnv()
			require.Equal(t, tt.expectedType, dType)
			require.Equal(t, tt.err, err)
		})
	}
}

func Test_GetConnectionToken(t *testing.T) {
	config.Reset()
	logger.Reset()
	admin.Init()
	testcases := []struct {
		name               string
		dType              string
		expectedBool       bool
		secret             string
		namespace          string
		workspaceToken     string
		expectedIdentifier string
		tokenType          string
		err                error
	}{
		{
			name:               "DedicatedType",
			dType:              "DEDICATED",
			secret:             "",
			namespace:          "",
			expectedIdentifier: "dedicated",
			expectedBool:       false,
			workspaceToken:     "dedicated",
			tokenType:          "WORKSPACE_TOKEN",
		},
		{
			name:               "MultiTenantType",
			dType:              "MULTITENANT",
			secret:             "secret",
			namespace:          "namespace",
			expectedIdentifier: "namespace",
			expectedBool:       true,
			tokenType:          "NAMESPACE",
		},
		{
			name:               "MultiTenantType",
			dType:              "MULTITENANT",
			secret:             "secret",
			namespace:          "free-us-1",
			expectedIdentifier: "free-us-1",
			expectedBool:       true,
			tokenType:          "NAMESPACE",
		},
		{
			name:               "MultiTenantType",
			dType:              "badType",
			secret:             "secret",
			namespace:          "free-us-1",
			expectedIdentifier: "",
			expectedBool:       false,
			err:                fmt.Errorf(`invalid deployment type: "badType"`),
		},
	}

	for _, tt := range testcases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DEPLOYMENT_TYPE", tt.dType)
			t.Setenv("HOSTED_SERVICE_SECRET", tt.secret)
			t.Setenv("WORKSPACE_NAMESPACE", tt.namespace)
			t.Setenv("WORKSPACE_TOKEN", tt.workspaceToken)
			identifier, tokenType, isMultiWorkspace, err := deployment.GetConnectionToken()
			require.Equal(t, tt.err, err)
			require.Equal(t, tt.expectedIdentifier, identifier)
			require.Equal(t, tt.expectedBool, isMultiWorkspace)
			require.Equal(t, tt.tokenType, tokenType)
		})
	}
}
