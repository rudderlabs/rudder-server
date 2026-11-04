package warehouseutils_test

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestValidateMaxComputeOSSConfig(t *testing.T) {
	require.ErrorContains(t, warehouseutils.ValidateMaxComputeOSSConfig(nil), "configuration is required")
	base := map[string]any{"bucketName": "bucket", "region": "cn-hangzhou", "accessKeyID": "id", "accessKey": "secret"}
	for _, tc := range []struct {
		name string
		set  map[string]any
		want string
	}{
		{"static", nil, ""},
		{"role", map[string]any{"roleBasedAuth": true, "iamRoleARN": "acs:ram::123:role/test", "accessKeyID": "", "accessKey": ""}, ""},
		{"no bucket", map[string]any{"bucketName": " "}, "bucketName"},
		{"no region", map[string]any{"region": ""}, "region"},
		{"partial static", map[string]any{"accessKey": ""}, "accessKeyID and accessKey"},
		{"mixed role and keys", map[string]any{"roleBasedAuth": true, "iamRoleARN": "role"}, "cannot be combined"},
		{"role without arn", map[string]any{"roleBasedAuth": true, "accessKeyID": "", "accessKey": ""}, "iamRoleARN"},
		{"arn without role", map[string]any{"iamRoleARN": "role"}, "roleBasedAuth"},
		{"non-boolean role", map[string]any{"roleBasedAuth": "true"}, "roleBasedAuth must be a boolean"},
		{"non-string key", map[string]any{"accessKeyID": 123}, "accessKeyID must be a string"},
		{"non-string secret", map[string]any{"accessKey": 123}, "accessKey must be a string"},
		{"non-string arn", map[string]any{"iamRoleARN": 123}, "iamRoleARN must be a string"},
		{"role without keys", map[string]any{"roleBasedAuth": true, "iamRoleARN": "role", "accessKeyID": " ", "accessKey": " "}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conf := make(map[string]any)
			maps.Copy(conf, base)
			maps.Copy(conf, tc.set)
			err := warehouseutils.ValidateMaxComputeOSSConfig(conf)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}
