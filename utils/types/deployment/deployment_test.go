package deployment_test

import (
	"fmt"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	"github.com/stretchr/testify/require"
)

func restoreEnvVariable(name string) {

}

func Test_GetFromEnv(t *testing.T) {
	t.Setenv("XYZ_URL", "http://example.com")

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

		// HostedType:
		{
			name: "legacy HOSTED_SERVICE is set",
			envs: map[string]string{
				"HOSTED_SERVICE": "true",
			},
			expectedType: deployment.HostedType,
		},
		{
			envs: map[string]string{
				"DEPLOYMENT_TYPE": "HOSTED",
			},
			expectedType: deployment.HostedType,
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
			err: fmt.Errorf("Invalid deployment type: \"A_NOT_VALID_TYPE\""),
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
