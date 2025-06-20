package utils

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
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
		{name: "Date time Micros Colon timezone", timestamp: "2025-04-02 01:09:03.714247+00:00", expected: true},
		{name: "Date time ISO millis timezone", timestamp: "2025-04-02T01:09:03.000+1000", expected: true},
		{name: "Date time Colon timezone", timestamp: "2025-04-02 01:09:03+00:00", expected: true},
		{name: "Day out of range", timestamp: "1988-04-31", expected: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, ValidTimestamp(tc.timestamp))
		})
	}
}

func TestToTimestamp(t *testing.T) {
	testCases := []struct {
		name, timestamp string
		expected        string
	}{
		{name: "Timestamp without timezone", timestamp: "2021-06-01T00:00:00.000Z", expected: "2021-06-01T00:00:00.000Z"},
		{name: "Timestamp with timezone", timestamp: "2021-06-01T00:00:00.000+00:00", expected: "2021-06-01T00:00:00.000Z"},
		{name: "Timestamps at bounds (minTimeInMs)", timestamp: "0001-01-01T00:00:00.000Z", expected: "0001-01-01T00:00:00.000Z"},
		{name: "Timestamps at bounds (maxTimeInMs)", timestamp: "9999-12-31T23:59:59.999Z", expected: "9999-12-31T23:59:59.999Z"},
		{name: "Date Time only", timestamp: "2021-06-01 00:00:00", expected: "2021-06-01T00:00:00.000Z"},
		{name: "Date-only", timestamp: "2023-06-14", expected: "2023-06-14T00:00:00.000Z"},
		{name: "Date time ISO 8601", timestamp: "2025-04-02T01:09:03", expected: "2025-04-02T01:09:03.000Z"},
		{name: "Date time Millis timezone", timestamp: "2025-04-02 01:09:03.000+0530", expected: "2025-04-01T19:39:03.000Z"},
		{name: "Date time Micros Colon timezone", timestamp: "2025-04-02 01:09:03.714247+00:00", expected: "2025-04-02T01:09:03.714Z"},
		{name: "Date time ISO millis timezone", timestamp: "2025-04-02T01:09:03.000+1000", expected: "2025-04-01T15:09:03.000Z"},
		{name: "Date time Colon timezone", timestamp: "2025-04-02 01:09:03+00:00", expected: "2025-04-02T01:09:03.000Z"},
		{name: "Date-only: Day out of range", timestamp: "1988-04-31", expected: "1988-05-01T00:00:00.000Z"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, ToTimestamp(tc.timestamp))
		})
	}
}

// BenchmarkValidTimestamp
// BenchmarkValidTimestamp/old_parser
// BenchmarkValidTimestamp/old_parser-12         	    6999	    163017 ns/op
// BenchmarkValidTimestamp/new_parser
// BenchmarkValidTimestamp/new_parser-12         	   89427	     13392 ns/op
func BenchmarkValidTimestamp(b *testing.B) {
	timeFormats := []string{
		time.RFC3339Nano,
		time.DateOnly,
		misc.RFC3339Milli,
		time.RFC3339,
		time.RFC1123Z,
		time.RFC1123,
		time.RFC822Z,
		time.RFC822,
		time.UnixDate,
		time.DateTime,
		time.RubyDate,
		time.ANSIC,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.000-0700",
	}

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
	for i := 0; i < 100; i++ {
		testDates = append(testDates, strings.Repeat("a", 1000))
	}
	for i := 0; i < 100; i++ {
		testDates = append(testDates, uuid.NewString())
	}

	oldValidTimestamp := func(input string) bool {
		if len(input) > validTimestampFormatsMaxLength {
			return false
		}
		var t time.Time
		var err error

		for _, format := range timeFormats {
			t, err = time.Parse(format, input)
			if err == nil {
				break
			}
		}
		return !t.IsZero() && !t.Before(minTimeInMs) && !t.After(maxTimeInMs)
	}

	b.Run("old parser", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, dateStr := range testDates {
				oldValidTimestamp(dateStr)
			}
		}
	})
	b.Run("new parser", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, dateStr := range testDates {
				ValidTimestamp(dateStr)
			}
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
		{"OneBlankNilSlice", []any{nil}, false},
		{"NonEmptySlice", []any{1, 2, 3}, false},
		{"OneBlankMapSlice", []any{map[string]any{}}, false},
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
			require.Equal(t, tc.expected, IsEmptyString(tc.input))
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
		{
			name: "receivedAt is not a string but present in metadata",
			event: &types.TransformerEvent{
				Message: map[string]any{
					"receivedAt": 12345,
				},
				Metadata: types.Metadata{
					ReceivedAt: "2023-10-19T14:00:00.000Z",
				},
			},
			expectedTime: "2023-10-19T14:00:00.000Z",
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

func TestMarshalJSON(t *testing.T) {
	type testCase struct {
		name     string
		input    any
		wantJSON string
	}

	type S struct {
		A string `json:"a"`
		B string `json:"b"`
	}

	cases := []testCase{
		{
			name:     "simple map",
			input:    map[string]any{"foo": "<bar>", "baz": 123},
			wantJSON: `{"baz":123,"foo":"<bar>"}`,
		},
		{
			name:     "struct with special chars",
			input:    S{A: "<hello>", B: "&world;"},
			wantJSON: `{"a":"<hello>","b":"&world;"}`,
		},
		{
			name:     "slice",
			input:    []any{"<", ">", "&"},
			wantJSON: `["<",">","&"]`,
		},
		{
			name:     "nil",
			input:    nil,
			wantJSON: "null",
		},
		{
			name:     "number",
			input:    42,
			wantJSON: "42",
		},
		{
			name:     "boolean",
			input:    true,
			wantJSON: "true",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := MarshalJSON(tc.input)
			require.NoError(t, err)
			require.NotNil(t, out)
			require.Equal(t, tc.wantJSON, string(out))
		})
	}
}

func TestUTF16RuneCountInString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{
			name:     "Empty string",
			input:    "",
			expected: 0,
		},
		{
			name:     "ASCII characters",
			input:    "hello",
			expected: 5, // All are single code units
		},
		{
			name:     "BMP characters",
			input:    "हिन्दी", // All in BMP
			expected: len([]rune("हिन्दी")),
		},
		{
			name:     "Supplementary characters (Emoji)",
			input:    "😀", // U+1F600 is supplementary (needs surrogate pair)
			expected: 2,
		},
		{
			name:     "Mixed BMP and supplementary",
			input:    "a😀b", // 'a' = 1, 😀 = 2, 'b' = 1
			expected: 4,
		},
		{
			name:     "Multiple supplementary characters",
			input:    "👨‍👩‍👧‍👦",
			expected: 11,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, UTF16RuneCountInString(tt.input))
		})
	}
}
