package transformer

import (
	"testing"

	"github.com/stretchr/testify/require"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestGetDataType(t *testing.T) {
	testCases := []struct {
		name, destType, key string
		val                 any
		isJSONKey           bool
		expected            string
	}{
		// Primitive types
		{"Primitive Type Int", whutils.POSTGRES, "someKey", 42, false, "int"},
		{"Primitive Type Float", whutils.POSTGRES, "someKey", 42.0, false, "int"},
		{"Primitive Type Float (non-int)", whutils.POSTGRES, "someKey", 42.5, false, "float"},
		{"Primitive Type Bool", whutils.POSTGRES, "someKey", true, false, "boolean"},

		// Valid timestamp
		{"Valid Timestamp String", whutils.POSTGRES, "someKey", "2022-10-05T14:48:00.000Z", false, "datetime"},

		// JSON Key cases for different destinations
		{"Postgres JSON Key", whutils.POSTGRES, "someKey", "someValue", true, "json"},
		{"Snowflake JSON Key", whutils.SNOWFLAKE, "someKey", "someValue", true, "json"},
		{"Redshift JSON Key", whutils.RS, "someKey", "someValue", true, "json"},

		// Redshift with text and string types
		{"Redshift Text Type", whutils.RS, "someKey", string(make([]byte, 513)), false, "text"},
		{"Redshift String Type", whutils.RS, "someKey", "shortValue", false, "string"},
		{"Redshift String Type", whutils.RS, "someKey", nil, false, "string"},

		// Empty string values
		{"Empty String Value", whutils.POSTGRES, "someKey", "", false, "string"},
		{"Empty String with JSON Key", whutils.POSTGRES, "someKey", "", true, "json"},

		// Unsupported types (should default to string)
		{"Unsupported Type Struct", whutils.POSTGRES, "someKey", struct{}{}, false, "string"},
		{"Unsupported Type Map", whutils.POSTGRES, "someKey", map[string]any{"key": "value"}, false, "string"},

		// Special string values
		{"Special Timestamp-like String", whutils.POSTGRES, "someKey", "not-a-timestamp", false, "string"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := dataTypeFor(tc.destType, tc.key, tc.val, tc.isJSONKey)
			require.Equal(t, tc.expected, actual)
		})
	}
}
