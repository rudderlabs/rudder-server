package maxcompute

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
			role, modeErr := RoleMode(tc.config)
			if tc.valid {
				require.NoError(t, modeErr)
				require.Equal(t, tc.role, role)
				require.NoError(t, ValidateSettings(tc.config, tc.workspace))
			} else {
				require.Error(t, ValidateSettings(tc.config, tc.workspace))
			}
		})
	}
}
