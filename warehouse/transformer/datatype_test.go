package transformer

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestDataType(t *testing.T) {
	sliceOfIntegers := make([]int, 600)
	for i := 0; i < 512; i++ {
		sliceOfIntegers[i] = i
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
		{"Redshift Text Type (Array)", whutils.RS, "someKey", [600]int{1, 2, 3}, false, "text"},
		{"Redshift Text Type (Slice)", whutils.RS, "someKey", sliceOfIntegers, false, "text"},
		{"Redshift Text Type (Array)", whutils.RS, "someKey", [3]int{1, 2, 3}, false, "string"},
		{"Redshift Text Type (Slice)", whutils.RS, "someKey", []int{1, 2, 3}, false, "string"},
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
// BenchmarkDataType/text-12        	 4626680	       281.2 ns/op
// BenchmarkDataType/json
// BenchmarkDataType/json-12        	23007733	        58.03 ns/op
// BenchmarkDataType/datetime
// BenchmarkDataType/datetime-12    	 1463211	       814.5 ns/op
func BenchmarkDataType(b *testing.B) {
	b.Run("int", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.POSTGRES, "someKey", 42, false)
		}
	})
	b.Run("float", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.POSTGRES, "someKey", 42.5, false)
		}
	})
	b.Run("bool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.POSTGRES, "someKey", true, false)
		}
	})
	b.Run("string", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.POSTGRES, "someKey", "shortValue", false)
		}
	})
	b.Run("text", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.RS, "someKey", strings.Repeat("a", 600), false)
		}
	})
	b.Run("json", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.RS, "someKey", `{"key": "value"}`, true)
		}
	})
	b.Run("datetime", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			dataTypeFor(whutils.POSTGRES, "someKey", "2022-10-05T14:48:00.000Z", false)
		}
	})
}
