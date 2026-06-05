package bqstreamv2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDestConfig_Decode(t *testing.T) {
	tests := []struct {
		name      string
		input     map[string]any
		expected  destConfig
		wantError bool
	}{
		{
			name: "Valid Input",
			input: map[string]any{
				"project":     "test-project",
				"namespace":   "test-namespace",
				"credentials": "test-credentials",
			},
			expected: destConfig{
				ProjectID:   "test-project",
				Namespace:   "test_namespace",
				Credentials: "test-credentials",
			},
			wantError: false,
		},
		{
			name: "Invalid Input",
			input: map[string]any{
				"project": 123, // Invalid type
			},
			wantError: true,
		},
		{
			name:  "Empty Map",
			input: map[string]any{},
			expected: destConfig{
				Namespace: "stringempty",
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c destConfig
			err := c.Decode(tt.input)

			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, c)
			}
		})
	}
}
