package backendconfig

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDestinationT_MarshalJSON_HasDynamicConfigAlwaysPresent(t *testing.T) {
	tests := []struct {
		name             string
		destination      DestinationT
		expectedKeyValue bool
	}{
		{
			name: "HasDynamicConfig is false",
			destination: DestinationT{
				ID:               "test-dest-1",
				Name:             "Test Destination 1",
				HasDynamicConfig: false,
			},
			expectedKeyValue: false,
		},
		{
			name: "HasDynamicConfig is true",
			destination: DestinationT{
				ID:               "test-dest-2",
				Name:             "Test Destination 2",
				HasDynamicConfig: true,
			},
			expectedKeyValue: true,
		},
		{
			name:             "Zero value DestinationT",
			destination:      DestinationT{},
			expectedKeyValue: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal the destination to JSON
			jsonData, err := json.Marshal(tt.destination)
			require.NoError(t, err)

			// Unmarshal to a map to check for key presence
			var result map[string]interface{}
			err = json.Unmarshal(jsonData, &result)
			require.NoError(t, err)

			// Verify that hasDynamicConfig key is present
			value, exists := result["hasDynamicConfig"]
			require.True(t, exists, "hasDynamicConfig key should always be present in marshaled JSON")

			// Verify the value is correct
			boolValue, ok := value.(bool)
			require.True(t, ok, "hasDynamicConfig value should be a boolean")
			require.Equal(t, tt.expectedKeyValue, boolValue, "hasDynamicConfig value should match expected")
		})
	}
}
