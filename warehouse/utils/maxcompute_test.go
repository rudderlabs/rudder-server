package warehouseutils_test

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestValidateMaxComputeOSSConfig(t *testing.T) {
	require.ErrorContains(t, warehouseutils.ValidateMaxComputeOSSConfig(nil), "configuration is required")
	base := map[string]any{"bucketName": "bucket", "region": "cn-hangzhou", "ossRoleArn": "read-role"}
	for _, tc := range []struct {
		name string
		set  map[string]any
		want string
		role bool
	}{
		{"static", map[string]any{"accessKeyId": "id", "accessKeySecret": "secret"}, "", false},
		{"role", map[string]any{"iamRoleARN": "acs:ram::123:role/test"}, "", true},
		{"no bucket", map[string]any{"accessKeyId": "id", "accessKeySecret": "secret", "bucketName": " "}, "bucketName", false},
		{"no region", map[string]any{"accessKeyId": "id", "accessKeySecret": "secret", "region": ""}, "region", false},
		{"no read role", map[string]any{"accessKeyId": "id", "accessKeySecret": "secret", "ossRoleArn": ""}, "ossRoleArn", false},
		{"partial ID", map[string]any{"accessKeyId": "id"}, "complete pair", false},
		{"partial secret", map[string]any{"accessKeySecret": "secret"}, "complete pair", false},
		{"neither", nil, "required", false},
		{"mixed role and keys", map[string]any{"iamRoleARN": "role", "accessKeyId": "id", "accessKeySecret": "secret"}, "cannot be combined", false},
		{"role and partial key", map[string]any{"iamRoleARN": "role", "accessKeyId": "id"}, "cannot be combined", false},
		{"non-string key", map[string]any{"accessKeyId": 123}, "accessKeyId must be a string", false},
		{"non-string secret", map[string]any{"accessKeySecret": 123}, "accessKeySecret must be a string", false},
		{"non-string arn", map[string]any{"iamRoleARN": 123}, "iamRoleARN must be a string", false},
		{"role with blank keys", map[string]any{"iamRoleARN": "role", "accessKeyId": " ", "accessKeySecret": " "}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := make(map[string]any)
			maps.Copy(conf, base)
			maps.Copy(conf, tc.set)
			err := warehouseutils.ValidateMaxComputeOSSConfig(conf)
			if tc.want == "" {
				require.NoError(t, err)
				role, err := warehouseutils.MaxComputeRoleMode(conf)
				require.NoError(t, err)
				require.Equal(t, tc.role, role)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestMaxComputeGetObjectNameConvertsDestinationConfig(t *testing.T) {
	conf := map[string]any{
		"bucketName": "customer-bucket", "region": "cn-hangzhou", "ossRoleArn": "read-role",
		"accessKeyId": "id", "accessKeySecret": "secret",
		"endpoint": "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
	}
	location := "https://customer-bucket.oss-cn-hangzhou.aliyuncs.com/prefix/file.json.gz"
	_, err := warehouseutils.GetObjectName(location, conf, warehouseutils.AlibabaOSS)
	require.ErrorContains(t, err, "workspace ID")
	key, err := warehouseutils.GetObjectName(location, conf, warehouseutils.AlibabaOSS, "workspace")
	require.NoError(t, err)
	require.Equal(t, "prefix/file.json.gz", key)
	require.NotContains(t, conf, "endPoint")
}
