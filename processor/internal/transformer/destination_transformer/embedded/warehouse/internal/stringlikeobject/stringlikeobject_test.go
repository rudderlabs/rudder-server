package stringlikeobject

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsStringLikeObject(t *testing.T) {
	testCases := []struct {
		name     string
		input    map[string]any
		expected bool
	}{
		{
			name:     "empty map",
			input:    map[string]any{},
			expected: false,
		},
		{
			name: "valid string-like object with 0 and 1",
			input: map[string]any{
				"0": "a",
				"1": "b",
			},
			expected: true,
		},
		{
			name: "valid string-like object with 1 and 2",
			input: map[string]any{
				"1": "x",
				"2": "y",
			},
			expected: true,
		},
		{
			name: "empty key",
			input: map[string]any{
				"":  "",
				"1": "x",
				"2": "y",
			},
			expected: false,
		},
		{
			name: "invalid key type",
			input: map[string]any{
				"0":   "a",
				"one": "b",
			},
			expected: false,
		},
		{
			name: "value is not a string",
			input: map[string]any{
				"0": 123,
			},
			expected: false,
		},
		{
			name: "value string length not 1",
			input: map[string]any{
				"0": "ab",
			},
			expected: false,
		},
		{
			name: "missing key (1) in sequence",
			input: map[string]any{
				"0": "a",
				"2": "b",
			},
			expected: false,
		},
		{
			name: "non-consecutive keys (1 ia missing)",
			input: map[string]any{
				"0": "a",
				"2": "b",
				"3": "c",
			},
			expected: false,
		},
		{
			name: "valid string-like object with non-negative integer keys",
			input: map[string]any{
				"0": "a",
				"1": "b",
				"2": "c",
			},
			expected: true,
		},
		{
			name: "valid string-like object with gaps (at 3)",
			input: map[string]any{
				"1": "x",
				"2": "y",
				"4": "z",
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, IsStringLikeObject(tc.input))
		})
	}
}

func TestStringLikeObjectToString(t *testing.T) {
	testCases := []struct {
		name     string
		input    map[string]any
		expected any
	}{
		{
			name: "valid string-like object with non-negative integer keys",
			input: map[string]any{
				"0": "a",
				"1": "b",
				"2": "c",
			},
			expected: "abc",
		},
		{
			name: "valid string-like object with 1 and 2",
			input: map[string]any{
				"1": "x",
				"2": "y",
			},
			expected: "xy",
		},
		{
			name: "valid string-like object with big keys",
			input: func() map[string]any {
				m := make(map[string]any, 1040)
				for i := 0; i < 1040; i++ {
					m[strconv.Itoa(i)] = string('a' + rune(i%26))
				}
				return m
			}(),
			expected: strings.Repeat("abcdefghijklmnopqrstuvwxyz", 40),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, ToString(tc.input))
		})
	}
}
