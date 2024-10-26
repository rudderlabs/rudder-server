package transformer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestGetDataType(t *testing.T) {
	testCases := []struct {
		name, destType, key           string
		val                           any
		isJSONKey, enableArraySupport bool
		expected                      string
	}{
		// Primitive types
		{"Primitive Type Int", whutils.POSTGRES, "someKey", 42, false, false, "int"},
		{"Primitive Type Float", whutils.POSTGRES, "someKey", 42.0, false, false, "int"},
		{"Primitive Type Float (non-int)", whutils.POSTGRES, "someKey", 42.5, false, false, "float"},
		{"Primitive Type Bool", whutils.POSTGRES, "someKey", true, false, false, "boolean"},

		// Valid timestamp
		{"Valid Timestamp String", whutils.POSTGRES, "someKey", "2022-10-05T14:48:00.000Z", false, false, "datetime"},

		// JSON Key cases for different destinations
		{"Postgres JSON Key", whutils.POSTGRES, "someKey", "someValue", true, false, "json"},
		{"Snowflake JSON Key", whutils.SNOWFLAKE, "someKey", "someValue", true, false, "json"},
		{"Redshift JSON Key", whutils.RS, "someKey", "someValue", true, false, "json"},

		// Redshift with text and string types
		{"Redshift Text Type", whutils.RS, "someKey", string(make([]byte, 513)), false, false, "text"},
		{"Redshift String Type", whutils.RS, "someKey", "shortValue", false, false, "string"},
		{"Redshift String Type", whutils.RS, "someKey", nil, false, false, "string"},

		// ClickHouse - Array support enabled
		{"ClickHouse Array Type Int", whutils.CLICKHOUSE, "someKey", []any{1, 2, 3}, false, true, "array(int)"},
		{"ClickHouse Array Type Mixed Int and Float", whutils.CLICKHOUSE, "someKey", []any{1, 2.5}, false, true, "array(float)"},
		{"ClickHouse Array Type Mixed Int, Float, and String", whutils.CLICKHOUSE, "someKey", []any{1, 2.5, "text"}, false, true, "array(string)"},
		{"ClickHouse Array Type Mixed String, Int and Float", whutils.CLICKHOUSE, "someKey", []any{"text", 1, 2.5}, false, true, "array(string)"},
		{"ClickHouse Array Type All Strings", whutils.CLICKHOUSE, "someKey", []any{"one", "two"}, false, true, "array(string)"},
		{"ClickHouse Empty Array", whutils.CLICKHOUSE, "someKey", []any{}, false, true, "string"}, // Empty array should return "string"
		{"ClickHouse Array Type All Floats", whutils.CLICKHOUSE, "someKey", []any{1.1, 2.2, 3.3}, false, true, "array(float)"},
		{"ClickHouse Array Int then Float", whutils.CLICKHOUSE, "someKey", []any{1, 2.5, 3}, false, true, "array(float)"},
		{"ClickHouse Array Break on String", whutils.CLICKHOUSE, "someKey", []any{1, 2.5, "text"}, false, true, "array(string)"}, // Breaks on string

		// ClickHouse - Array support disabled
		{"ClickHouse No Array Support", whutils.CLICKHOUSE, "someKey", []any{1, 2, 3}, false, false, "string"},
		{"ClickHouse No Array Support Mixed", whutils.CLICKHOUSE, "someKey", []any{1, 2.5}, false, false, "string"},
		{"ClickHouse No Array Support with Strings", whutils.CLICKHOUSE, "someKey", []any{"one", "two"}, false, false, "string"},
		{"ClickHouse No Array Support Empty Array", whutils.CLICKHOUSE, "someKey", []any{}, false, false, "string"},

		// Complex Nested Arrays (Array support enabled)
		{"ClickHouse Nested Arrays Mixed Int and Float", whutils.CLICKHOUSE, "someKey", []any{[]any{1, 2.5}, []any{3, 4}}, false, true, "array(string)"},
		{"ClickHouse Nested Arrays Int", whutils.CLICKHOUSE, "someKey", []any{[]any{1, 2}, []any{3, 4}}, false, true, "array(array(int))"},

		// Empty string values
		{"Empty String Value", whutils.POSTGRES, "someKey", "", false, false, "string"},
		{"Empty String with JSON Key", whutils.POSTGRES, "someKey", "", true, false, "json"},

		// Unsupported types (should default to string)
		{"Unsupported Type Struct", whutils.POSTGRES, "someKey", struct{}{}, false, false, "string"},
		{"Unsupported Type Map", whutils.POSTGRES, "someKey", map[string]any{"key": "value"}, false, false, "string"},

		// Special string values
		{"Special Timestamp-like String", whutils.POSTGRES, "someKey", "not-a-timestamp", false, false, "string"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			destinationTransformer := &transformer{}
			destinationTransformer.config.enableArraySupport = config.SingleValueLoader(tc.enableArraySupport)

			actual := destinationTransformer.getDataType(tc.destType, tc.key, tc.val, tc.isJSONKey)
			require.Equal(t, tc.expected, actual)
		})
	}
}
