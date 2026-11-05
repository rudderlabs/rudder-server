package warehouseutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMaxComputeErrorReasons(t *testing.T) {
	for _, tc := range []struct {
		name, message, want string
		role                bool
	}{
		{"membership", "ODPS-0420095: User does not exist in the project", "project_membership", false},
		{"numeric RAM ID", "ODPS-0030001: user name is not a valid account", "project_membership", false},
		{"schema edition", "ODPS-0110061: project doesn't support schema", "schema_not_enabled", false},
		{"identifier length", "ODPS-0130071: too long, max length is 128", "identifier_length", false},
		{"full scan", "ODPS-0130071: full scan of all partitions is prohibited", "fullscan_guard", false},
		{"role trust", "NoPermission while assuming role", "role_assumption", true},
		{"OSS key", "InvalidAccessKeyId", "access_key_authentication", false},
		{"OSS signature", "SignatureDoesNotMatch", "access_key_authentication", false},
		{"ODPS key", "ODPS authentication or permission denied", "access_key_authentication", false},
		{"caller keys", "MaxCompute role caller credentials unavailable", "caller_credentials_missing", true},
		{"unknown", "ODPS-1111111: unknown", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reason, guidance := MaxComputeErrorReason(tc.message, tc.role)
			require.Equal(t, tc.want, reason)
			if reason != "" {
				require.NotEmpty(t, guidance)
				require.NotContains(t, guidance, tc.message)
			}
		})
	}
}

func TestMaxComputeAuthModeValidation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		config    map[string]any
		workspace string
		role      bool
		valid     bool
	}{
		{"role", map[string]any{"iamRoleARN": "odps-role", "ossRoleArn": "oss-role", "bucketName": "bucket", "region": "cn-hangzhou"}, "workspace", true, true},
		{"access key", map[string]any{"accessKeyId": "id", "accessKeySecret": "secret", "ossRoleArn": "oss-role", "bucketName": "bucket", "region": "cn-hangzhou"}, "", false, true},
		{"both", map[string]any{"iamRoleARN": "odps-role", "accessKeyId": "id", "accessKeySecret": "secret", "ossRoleArn": "oss-role", "bucketName": "bucket", "region": "cn-hangzhou"}, "workspace", false, false},
		{"neither", map[string]any{"ossRoleArn": "oss-role", "bucketName": "bucket", "region": "cn-hangzhou"}, "workspace", false, false},
		{"partial", map[string]any{"accessKeyId": "id", "ossRoleArn": "oss-role", "bucketName": "bucket", "region": "cn-hangzhou"}, "workspace", false, false},
		{"missing workspace", map[string]any{"iamRoleARN": "odps-role", "ossRoleArn": "oss-role", "bucketName": "bucket", "region": "cn-hangzhou"}, "", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			role, modeErr := MaxComputeRoleMode(tc.config)
			if tc.valid {
				require.NoError(t, modeErr)
				require.Equal(t, tc.role, role)
				require.NoError(t, ValidateMaxComputeSettings(tc.config, tc.workspace))
			} else {
				require.Error(t, ValidateMaxComputeSettings(tc.config, tc.workspace))
			}
		})
	}
}
