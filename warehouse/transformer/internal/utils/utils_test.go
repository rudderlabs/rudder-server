package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, ValidTimestamp(tc.timestamp))
		})
	}
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
		{[]int{1, 2, 3}, "[1 2 3]"},              // slice
		{map[string]int{"key": 1}, "map[key:1]"}, // map
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
		{"NilValue", nil, true},                                     // nil
		{"EmptyString", "", true},                                   // empty string
		{"NonEmptyString", "Hello", false},                          // non-empty string
		{"IntZero", 0, false},                                       // integer zero
		{"IntNonZero", 123, false},                                  // non-zero integer
		{"FloatZero", 0.0, false},                                   // float zero
		{"FloatNonZero", 123.45, false},                             // non-zero float
		{"BoolFalse", false, false},                                 // boolean false
		{"BoolTrue", true, false},                                   // boolean true
		{"EmptySlice", []int{}, false},                              // empty slice
		{"NonEmptySlice", []int{1, 2, 3}, false},                    // non-empty slice
		{"EmptyMap", map[string]int{}, false},                       // empty map
		{"NonEmptyMap", map[string]int{"key": 1}, false},            // non-empty map
		{"EmptyStruct", struct{}{}, false},                          // empty struct
		{"StructWithField", struct{ Field string }{"value"}, false}, // non-empty struct
		{"StructWithMethod", Person{Name: "Alice", Age: 30}, false}, // struct with String method
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, IsBlank(tc.input))
		})
	}
}
