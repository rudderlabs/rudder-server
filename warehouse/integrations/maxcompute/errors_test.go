package maxcompute

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestErrorMappingsUseSanitizedDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, diagnostic string
		want             model.JobErrorType
	}{
		{"project membership", "ODPS-0420095: User does not exist in the project", model.PermissionError},
		{"RAM numeric ID", "ODPS-0030001: user name is not a valid account", model.PermissionError},
		{"schema edition", "ODPS-0110061: project doesn't support schema", model.PermissionError},
		{"full scan", "ODPS-0130071: full scan of all partitions is not allowed", model.PermissionError},
		{"identifier length", "ODPS-0130071: identifier too long, max length is 128", model.UncategorizedError},
		{"assume role", "NoPermission", model.PermissionError},
		{"invalid access key", "InvalidAccessKeyId", model.PermissionError},
		{"signature mismatch", "SignatureDoesNotMatch", model.PermissionError},
		{"caller keys", "MaxCompute role caller credentials unavailable", model.PermissionError},
		{"missing table", "TableNotFound", model.ResourceNotFoundError},
		{"cell limit", "ODPS-0121095: cell exceeds 8 MB", model.ColumnSizeError},
		{"column count", "MaxCompute table exceeds 1200 columns", model.ColumnCountError},
		{"generic", "ODPS-1234567: unexpected failure", model.UncategorizedError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			safe := errors.New(tc.diagnostic)
			if tc.name != "caller keys" && tc.name != "column count" {
				safe = client.ODPSSafeError(safe)
			}
			got := model.UncategorizedError
			for _, mapping := range (&MaxCompute{}).ErrorMappings() {
				if mapping.Format.MatchString(safe.Error()) {
					got = mapping.Type
					break
				}
			}
			require.Equal(t, tc.want, got, safe.Error())
		})
	}
}
