package transformer

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/types"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestDataType(t *testing.T) {
	anySlice, validationErrorSlice := make([]any, 600), make([]types.ValidationError, 600)
	anyMap := make(map[string]any)
	for i := 0; i < 600; i++ {
		anySlice[i] = i
		validationErrorSlice[i] = types.ValidationError{
			Type: "type",
		}
		anyMap[strconv.Itoa(i)] = i
	}

	testCases := []struct {
		name, destType, key string
		val                 any
		isJSONKey           bool
		expected            string
	}{
		// Primitive types
		{"Primitive Type Int", whutils.POSTGRES, "someKey", 42, false, "int"},
		{"Primitive Type Float(64)", whutils.POSTGRES, "someKey", 42.0, false, "int"},
		{"Primitive Type Float(32)", whutils.POSTGRES, "someKey", float32(42.0), false, "int"},
		{"Primitive Type Float(non-int)", whutils.POSTGRES, "someKey", 42.5, false, "float"},
		{"Primitive Type Bool", whutils.POSTGRES, "someKey", true, false, "boolean"},

		// Timestamp
		{"Valid Timestamp String", whutils.POSTGRES, "someKey", "2022-10-05T14:48:00.000Z", false, "datetime"},
		{"Invalid Timestamp String", whutils.POSTGRES, "someKey", "2022/10-05T", false, "string"},

		// JSON Keys
		{"Postgres JSON Key", whutils.POSTGRES, "someKey", `{"key": "value"}`, true, "json"},
		{"Snowflake JSON Key", whutils.SNOWFLAKE, "someKey", `{"key": "value"}`, true, "json"},
		{"Snowpipe Streaming JSON Key", whutils.SnowpipeStreaming, "someKey", `{"key": "value"}`, true, "json"},
		{"Redshift JSON Key", whutils.RS, "someKey", `{"key": "value"}`, true, "json"},

		// Violation Errors
		{"Postgres violationErrors", whutils.POSTGRES, violationErrors, `{"key": "value"}`, false, "json"},
		{"Snowflake violationErrors", whutils.SNOWFLAKE, violationErrors, `{"key": "value"}`, false, "json"},
		{"Snowpipe Streaming violationErrors", whutils.SnowpipeStreaming, violationErrors, `{"key": "value"}`, false, "json"},
		{"Redshift violationErrors", whutils.RS, violationErrors, `{"key": "value"}`, false, "string"},

		// Redshift with text and string types
		{"Redshift Text Type (Any Slice)", whutils.RS, "someKey", anySlice, false, "text"},
		{"Redshift String Type (Any Slice)", whutils.RS, "someKey", []any{1, 2, 3}, false, "string"},
		{"Redshift Text Type (Validation Error Slice)", whutils.RS, "someKey", validationErrorSlice, false, "text"},
		{"Redshift String Type (Validation Error Slice)", whutils.RS, "someKey", []types.ValidationError{{Type: "type"}, {Type: "type"}, {Type: "type"}}, false, "string"},
		{"Redshift Text Type (Any Map)", whutils.RS, "someKey", anyMap, false, "text"},
		{"Redshift String Type (Any Map)", whutils.RS, "someKey", map[string]any{"1": 1, "2": 2, "3": 3}, false, "string"},
		{"Redshift Text Type", whutils.RS, "someKey", strings.Repeat("a", 600), false, "text"},
		{"Redshift String Type (nil)", whutils.RS, "someKey", nil, false, "string"},
		{"Redshift String Type (shortValue)", whutils.RS, "someKey", "shortValue", false, "string"},

		{"Empty String Value", whutils.CLICKHOUSE, "someKey", "", false, "string"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, dataTypeFor(tc.destType, tc.key, tc.val, tc.isJSONKey))
		})
	}
}

// BenchmarkDataType
// BenchmarkDataType/int
// BenchmarkDataType/int-12         	494599164	         2.360 ns/op
// BenchmarkDataType/float
// BenchmarkDataType/float-12       	507710589	         2.363 ns/op
// BenchmarkDataType/bool
// BenchmarkDataType/bool-12        	583417570	         2.071 ns/op
// BenchmarkDataType/string
// BenchmarkDataType/string-12      	24060914	        51.80 ns/op
// BenchmarkDataType/text
// BenchmarkDataType/text-12        	 4835743	       246.4 ns/op
// BenchmarkDataType/json
// BenchmarkDataType/json-12        	22701242	        53.24 ns/op
// BenchmarkDataType/datetime
// BenchmarkDataType/datetime-12    	 1463211	       814.5 ns/op
func BenchmarkDataType(b *testing.B) {
	key := "someKey"

	b.Run("int", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.POSTGRES, key, 42, false)
		}
	})
	b.Run("float", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.POSTGRES, key, 42.5, false)
		}
	})
	b.Run("bool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.POSTGRES, key, true, false)
		}
	})
	b.Run("string", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.POSTGRES, key, "shortValue", false)
		}
	})
	b.Run("text", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.RS, key, strings.Repeat("a", 600), false)
		}
	})
	b.Run("json", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.RS, key, `{"key": "value"}`, true)
		}
	})
	b.Run("datetime", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.POSTGRES, key, "2022-10-05T14:48:00.000Z", false)
		}
	})
}
