package utils

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/araddon/dateparse"
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
		{name: "Date time Micros Colon timezone", timestamp: "2025-02-22 03:46:41.714247+00:00", expected: true},
		{name: "Date time ISO millis timezone", timestamp: "2025-04-13T11:24:48.000+1000", expected: true},
		{name: "Date time Colon timezone", timestamp: "2025-04-18 02:00:00+00:00", expected: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, ValidTimestamp(tc.timestamp))
		})
	}
}

// BenchmarkValidTimestamp
// BenchmarkValidTimestamp/old_parser
// BenchmarkValidTimestamp/old_parser/ValidTimestamp_Valid
// BenchmarkValidTimestamp/old_parser/ValidTimestamp_Valid-12         	   77277	     16020 ns/op
// BenchmarkValidTimestamp/old_parser/ValidTimestamp_Invalid
// BenchmarkValidTimestamp/old_parser/ValidTimestamp_Invalid-12       	 1314688	       903.8 ns/op
// BenchmarkValidTimestamp/old_parser/ValidTimestamp_Invalid_Big_String
// BenchmarkValidTimestamp/old_parser/ValidTimestamp_Invalid_Big_String-12         	  294691	      4207 ns/op
// BenchmarkValidTimestamp/new_parser
// BenchmarkValidTimestamp/new_parser/ValidTimestamp_Valid
// BenchmarkValidTimestamp/new_parser/ValidTimestamp_Valid-12                      	  310105	      3711 ns/op
// BenchmarkValidTimestamp/new_parser/ValidTimestamp_Invalid
// BenchmarkValidTimestamp/new_parser/ValidTimestamp_Invalid-12                    	 4276358	       278.7 ns/op
// BenchmarkValidTimestamp/new_parser/ValidTimestamp_Invalid_Big_String
// BenchmarkValidTimestamp/new_parser/ValidTimestamp_Invalid_Big_String-12         	  162741	      7419 ns/op
func BenchmarkValidTimestamp(b *testing.B) {
	testDates := []string{
		"2012/03/19 10:11:59",
		"2012/03/19 10:11:59.3186369",
		"2009-08-12T22:15:09-07:00",
		"2014-04-26 17:24:37.3186369",
		"2012-08-03 18:31:59.257000000",
		"2013-04-01 22:43:22",
		"2014-04-26 17:24:37.123",
		"2014-12-16 06:20:00 UTC",
		"1384216367189",
		"1332151919",
		"2014-05-11 08:20:13,787",
		"2014-04-26 05:24:37 PM",
		"2014-04-26",
	}
	timeFormats := []string{
		// ISO 8601ish formats
		time.RFC3339Nano,
		time.RFC3339,

		// Unusual formats, prefer formats with timezones
		time.RFC1123Z,
		time.RFC1123,
		time.RFC822Z,
		time.RFC822,
		time.UnixDate,
		time.RubyDate,
		time.ANSIC,

		// Hilariously, Go doesn't have a const for it's own time layout.
		// See: https://code.google.com/p/go/issues/detail?id=6587
		"2006-01-02 15:04:05.999999999 -0700 MST",

		// No timezone information
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}

	oldParser := func(input string) (time.Time, error) {
		for _, format := range timeFormats {
			t, err := time.Parse(format, input)
			if err == nil {
				return t, nil
			}
		}
		return time.Time{}, errors.New("invalid timestamp")
	}
	newParser := func(input string) (time.Time, error) {
		return dateparse.ParseAny(input)
	}

	var t time.Time
	var err error

	b.Run("old parser", func(b *testing.B) {
		b.Run("ValidTimestamp_Valid", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, dateStr := range testDates {
					t, err = oldParser(dateStr)
				}
			}
		})
		b.Run("ValidTimestamp_Invalid", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				t, err = oldParser("invalid-timestamp")
			}
		})
		b.Run("ValidTimestamp_Invalid Big String", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				t, err = oldParser(strings.Repeat("a", 1000))
			}
		})
	})
	b.Run("new parser", func(b *testing.B) {
		b.Run("ValidTimestamp_Valid", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, dateStr := range testDates {
					t, err = newParser(dateStr)
				}
			}
		})
		b.Run("ValidTimestamp_Invalid", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				t, err = newParser("invalid-timestamp")
			}
		})
		b.Run("ValidTimestamp_Invalid Big String", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				t, err = newParser(strings.Repeat("a", 1000))
			}
		})
	})

	_ = t
	_ = err
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
		{"EmptyMap", map[string]any{}, false},
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

func TestExtractMessageID(t *testing.T) {
	tests := []struct {
		name       string
		event      map[string]any
		expectedID string
	}{
		{
			name: "messageId present",
			event: map[string]any{
				"messageId": "custom-message-id",
			},
			expectedID: "custom-message-id",
		},
		{
			name: "messageId missing",
			event: map[string]any{
				"otherKey": "value",
			},
			expectedID: "auto-custom-message-id",
		},
		{
			name: "messageId blank",
			event: map[string]any{
				"messageId": "",
			},
			expectedID: "auto-custom-message-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := &types.TransformerEvent{
				Message: tt.event,
			}
			require.Equal(t, tt.expectedID, ExtractMessageID(event, func() string {
				return "custom-message-id"
			}))
		})
	}
}

func TestExtractReceivedAt(t *testing.T) {
	tests := []struct {
		name         string
		event        *types.TransformerEvent
		expectedTime string
	}{
		{
			name: "receivedAt present and valid",
			event: &types.TransformerEvent{
				Message: map[string]any{
					"receivedAt": "2023-10-19T14:00:00.000Z",
				},
			},
			expectedTime: "2023-10-19T14:00:00.000Z",
		},
		{
			name: "receivedAt missing in both event and metadata",
			event: &types.TransformerEvent{
				Message: map[string]any{
					"otherKey": "value",
				},
			},
			expectedTime: "2023-10-20T12:34:56.789Z",
		},
		{
			name: "receivedAt missing in event but present in metadata",
			event: &types.TransformerEvent{
				Message: map[string]any{
					"otherKey": "value",
				},
				Metadata: types.Metadata{
					ReceivedAt: "2023-10-19T14:00:00.000Z",
				},
			},
			expectedTime: "2023-10-19T14:00:00.000Z",
		},
		{
			name: "receivedAt invalid format",
			event: &types.TransformerEvent{
				Message: map[string]any{
					"receivedAt": "invalid-format",
				},
			},
			expectedTime: "2023-10-20T12:34:56.789Z",
		},
		{
			name: "receivedAt is not a string",
			event: &types.TransformerEvent{
				Message: map[string]any{
					"receivedAt": 12345,
				},
			},
			expectedTime: "2023-10-20T12:34:56.789Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expectedTime, ExtractReceivedAt(tt.event, func() time.Time {
				return time.Date(2023, time.October, 20, 12, 34, 56, 789000000, time.UTC)
			}))
		})
	}
}
