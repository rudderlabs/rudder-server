package utils

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/types"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestIsRudderSources(t *testing.T) {
	testCases := []struct {
		name  string
		event map[string]any
		want  bool
	}{
		{name: "channel is sources", event: map[string]any{"channel": "sources"}, want: true},
		{name: "CHANNEL is sources", event: map[string]any{"CHANNEL": "sources"}, want: true},
		{name: "channel is not sources", event: map[string]any{"channel": "not-sources"}, want: false},
		{name: "CHANNEL is not sources", event: map[string]any{"CHANNEL": "not-sources"}, want: false},
		{name: "empty event", event: map[string]any{}, want: false},
		{name: "nil event", event: nil, want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsRudderSources(tc.event))
		})
	}
}

func TestIsObject(t *testing.T) {
	testCases := []struct {
		name string
		val  any
		want bool
	}{
		{name: "map", val: map[string]any{}, want: true},
		{name: "not map", val: "not map", want: false},
		{name: "nil", val: nil, want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsObject(tc.val))
		})
	}
}

func TestFullEventColumnTypeByDestTypeMapping(t *testing.T) {
	for _, destType := range whutils.WarehouseDestinations {
		require.NotNilf(t, fullEventColumnTypeByDestType[destType], "Full event column type not found for destination type %s", destType)
	}
}

func TestValidTimestamp(t *testing.T) {
	testCases := []struct {
		name, timestamp string
		expected        bool
	}{
		{name: "Timestamp without timezone", timestamp: "2021-06-01T00:00:00.000Z", expected: true},
		{name: "Timestamp with timezone", timestamp: "2021-06-01T00:00:00.000+00:00", expected: true},
		{name: "Invalid timestamp", timestamp: "invalid-timestamp", expected: false},
		{name: "Invalid RFC3339 timestamp (day-month-year)", timestamp: "23-05-2024T10:00:00Z", expected: false},
		{name: "Invalid RFC3339 timestamp (Invalid hour)", timestamp: "2024-05-23T25:00:00Z", expected: false},
		{name: "Empty timestamp", timestamp: "", expected: false},
		{name: "Timestamps at bounds (minTimeInMs)", timestamp: "0001-01-01T00:00:00.000Z", expected: true},
		{name: "Timestamps at bounds (maxTimeInMs)", timestamp: "9999-12-31T23:59:59.999Z", expected: true},
		{name: "Time-only", timestamp: "05:23:59.244Z", expected: false},
		{name: "Date Time only", timestamp: "2021-06-01 00:00:00", expected: true},
		{name: "Date-only", timestamp: "2023-06-14", expected: true},
		{name: "Positive year and time input", timestamp: "+2023-06-14T05:23:59.244Z", expected: false},
		{name: "Negative year and time input", timestamp: "-2023-06-14T05:23:59.244Z", expected: false},
		{name: "Malicious string input should return false", timestamp: "%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216%u002e%u002e%u2216Windows%u2216win%u002ein", expected: false},
		{name: "Date time ISO 8601", timestamp: "2025-04-02T01:09:03", expected: true},
		{name: "Date time Millis timezone", timestamp: "2025-04-02 01:09:03.000+0530", expected: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, ValidTimestamp(tc.timestamp))
		})
	}
}

// BenchmarkValidTimestamp/ValidTimestamp_Valid
// BenchmarkValidTimestamp/ValidTimestamp_Valid-12         				36466681		32.00 ns/op
// BenchmarkValidTimestamp/ValidTimestamp_Invalid
// BenchmarkValidTimestamp/ValidTimestamp_Invalid-12       	 			2823615	       	423.4 ns/op
// BenchmarkValidTimestamp/ValidTimestamp_Invalid_Big_String
// BenchmarkValidTimestamp/ValidTimestamp_Invalid_Big_String-12     	7731496	       	154.8 ns/op
func BenchmarkValidTimestamp(b *testing.B) {
	b.Run("ValidTimestamp_Valid", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ValidTimestamp("2023-11-10T12:34:56Z")
		}
	})
	b.Run("ValidTimestamp_Invalid", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ValidTimestamp("invalid-timestamp")
		}
	})
	b.Run("ValidTimestamp_Invalid Big String", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ValidTimestamp(strings.Repeat("a", 1000))
		}
	})
}

type Person struct {
	Name string
	Age  int
}

func (p Person) String() string {
	return fmt.Sprintf("Person(Name: %s, Age: %d)", p.Name, p.Age)
}

func TestToString(t *testing.T) {
	testCases := []struct {
		input    interface{}
		expected string
	}{
		{nil, ""},                                // nil
		{"", ""},                                 // empty string
		{"Hello", "Hello"},                       // non-empty string
		{123, "123"},                             // int
		{123.45, "123.45"},                       // float
		{true, "true"},                           // bool true
		{false, "false"},                         // bool false
		{[]any{1, 2, 3}, "[1 2 3]"},              // slice
		{map[string]any{"key": 1}, "map[key:1]"}, // map
		{struct{}{}, "{}"},                       // empty struct
		{struct{ Field string }{"value"}, "{value}"},                     // struct with field
		{Person{Name: "Alice", Age: 30}, "Person(Name: Alice, Age: 30)"}, // struct with String method
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("ToString(%v)", tc.input), func(t *testing.T) {
			require.Equal(t, tc.expected, ToString(tc.input))
		})
	}
}

func TestIsBlank(t *testing.T) {
	testCases := []struct {
		name     string
		input    interface{}
		expected bool
	}{
		{"NilValue", nil, true},
		{"EmptyString", "", true},
		{"NonEmptyString", "Hello", false},
		{"IntZero", 0, false},
		{"IntNonZero", 123, false},
		{"FloatZero", 0.0, false},
		{"FloatNonZero", 123.45, false},
		{"BoolFalse", false, false},
		{"BoolTrue", true, false},
		{"EmptySlice", []any{}, true},
		{"NonEmptySlice", []any{1, 2, 3}, false},
		{"OneBlankStringSlice", []any{""}, true},
		{"ManyBlankStringSlice", []any{"", "", "", ""}, false},
		{"NestedOneBlankStringSlice", []any{[]any{[]any{}}}, true},
		{"NestedOneManyBlankStringSlice1", []any{[]any{[]any{}, []any{}}}, false},
		{"NestedOneManyBlankStringSlice2", []any{[]any{[]any{}}, []any{}}, false},
		{"EmptyMap", map[string]any{}, true},
		{"NonEmptyMap", map[string]any{"key": 1}, false},
		{"EmptyStruct", struct{}{}, false},
		{"StructWithField", struct{ Field string }{"value"}, false},
		{"StructWithMethod", Person{Name: "Alice", Age: 30}, false},
		{"EmptyValidationError", []types.ValidationError{}, true},
		{"NonEmptyValidationError", []types.ValidationError{{Type: "something"}}, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, IsBlank(tc.input))
		})
	}
}
