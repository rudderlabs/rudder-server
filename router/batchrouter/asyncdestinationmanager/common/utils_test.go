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
		{name: "small fractional float preserved", input: 0.1, expected: "0.1"},
		{name: "fractional float preserved", input: 1234.5, expected: "1234.5"},
		{name: "empty array", input: []any{}, expected: "[]"},
		{name: "string array", input: []any{"a", "b"}, expected: "[a b]"},
		{
			name:     "array of large numbers without exponent",
			input:    []any{float64(1234567890), float64(9876543210)},
			expected: "[1234567890 9876543210]",
		},
		{
			name:     "mixed array with nil retains <nil> for nested null",
			input:    []any{float64(1234567890), "abc", nil, true},
			expected: "[1234567890 abc <nil> true]",
		},
		{
			name:     "nested array",
			input:    []any{[]any{float64(1), float64(2)}, []any{float64(3), float64(4)}},
			expected: "[[1 2] [3 4]]",
		},
		{name: "empty map", input: map[string]any{}, expected: "map[]"},
		{
			name:     "map with large numeric value",
			input:    map[string]any{"id": float64(1234567890)},
			expected: "map[id:1234567890]",
		},
		{
			name:     "map keys sorted deterministically",
			input:    map[string]any{"b": float64(2), "a": float64(1)},
			expected: "map[a:1 b:2]",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, FormatCSVValue(tc.input))
		})
	}
}
