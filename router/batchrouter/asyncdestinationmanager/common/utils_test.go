package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFormatCSVValue(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		input    any
		expected string
	}{
		{name: "nil top-level becomes empty cell", input: nil, expected: ""},
		{name: "string passthrough", input: "hello", expected: "hello"},
		{name: "bool passthrough", input: true, expected: "true"},
		{name: "small int float renders without exponent", input: float64(1), expected: "1"},
		{name: "negative int float", input: float64(-42), expected: "-42"},
		{name: "large int float never goes scientific", input: float64(1234567890), expected: "1234567890"},
		{name: "very large int float never goes scientific", input: float64(9876543210), expected: "9876543210"},
		// float64 only has ~15-16 digits of precision; an 18-digit literal loses
		// the low digits when stored as float64. The point of this case is to
		// verify the formatter never emits scientific notation at that scale.
		{name: "18-digit numeric does not render in scientific notation", input: float64(123456789012345678), expected: "123456789012345680"},
		{name: "small fractional float preserved", input: 0.1, expected: "0.1"},
		{name: "fractional float preserved", input: 1234.5, expected: "1234.5"},
		{name: "empty array", input: []any{}, expected: "[]"},
		{name: "string array", input: []any{"a", "b"}, expected: "[\"a\",\"b\"]"},
		{
			name:     "array of large numbers without exponent",
			input:    []any{float64(1234567890), float64(9876543210)},
			expected: "[1234567890,9876543210]",
		},
		{
			name:     "mixed array with nil renders as JSON null",
			input:    []any{float64(1234567890), "abc", nil, true},
			expected: "[1234567890,\"abc\",null,true]",
		},
		{
			name:     "nested array",
			input:    []any{[]any{float64(1), float64(2)}, []any{float64(3), float64(4)}},
			expected: "[[1,2],[3,4]]",
		},
		{name: "empty map", input: map[string]any{}, expected: "{}"},
		{
			name:     "map with large numeric value",
			input:    map[string]any{"id": float64(1234567890)},
			expected: "{\"id\":1234567890}",
		},
		{
			name:     "map keys sorted deterministically",
			input:    map[string]any{"b": float64(2), "a": float64(1)},
			expected: "{\"a\":1,\"b\":2}",
		},
		{
			name:     "map keys with null values",
			input:    map[string]any{"b": float64(2), "a": float64(1), "c": nil},
			expected: "{\"a\":1,\"b\":2,\"c\":null}",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := FormatCSVValue(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}
