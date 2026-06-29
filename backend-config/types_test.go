package backendconfig

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
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
			jsonData, err := jsonrs.Marshal(tt.destination)
			require.NoError(t, err)

			// Unmarshal to a map to check for key presence
			var result map[string]any
			err = jsonrs.Unmarshal(jsonData, &result)
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

func TestDestinationT_Version(t *testing.T) {
	t.Run("marshals as a number, omitted when zero", func(t *testing.T) {
		for _, v := range []int{1, 2} {
			data, err := jsonrs.Marshal(DestinationT{ID: "d1", Version: v})
			require.NoError(t, err)

			var raw map[string]json.RawMessage
			require.NoError(t, jsonrs.Unmarshal(data, &raw))
			val, ok := raw["version"]
			require.True(t, ok, "non-zero version must be present")
			require.JSONEq(t, strconv.Itoa(v), string(val), "version must serialize as a JSON number")
		}

		// version 0 (== v1 default, == absent) is omitted to keep it off the wire
		data, err := jsonrs.Marshal(DestinationT{ID: "d1", Version: 0})
		require.NoError(t, err)
		var raw map[string]json.RawMessage
		require.NoError(t, jsonrs.Unmarshal(data, &raw))
		_, ok := raw["version"]
		require.False(t, ok, "zero version must be omitted (omitempty)")
	})

	// The workspace blob carries `version` on each destination. Config-backend writes it lowercase;
	// these blobs mirror the exact unmarshal targets used in production.
	t.Run("round-trips through single-workspace and namespace unmarshal paths", func(t *testing.T) {
		const blob = `{
			"workspaceId": "ws-1",
			"sources": [{
				"id": "s1",
				"destinations": [
					{"id": "with-version", "version": 1},
					{"id": "without-version"}
				]
			}]
		}`

		assertVersions := func(t *testing.T, cfg ConfigT) {
			t.Helper()
			require.Len(t, cfg.Sources, 1)
			dests := cfg.Sources[0].Destinations
			require.Len(t, dests, 2)
			require.Equal(t, 1, dests[0].Version, "blob version must populate DestinationT.Version")
			require.Zero(t, dests[1].Version, "missing version yields zero (treated as v1 downstream)")
		}

		// single-workspace path: single_workspace.go unmarshals the blob into ConfigT
		var single ConfigT
		require.NoError(t, jsonrs.Unmarshal([]byte(blob), &single))
		assertVersions(t, single)

		// namespace path: namespace_config.go unmarshals into map[string]*ConfigT
		var namespace map[string]*ConfigT
		require.NoError(t, jsonrs.Unmarshal([]byte(`{"ws-1": `+blob+`}`), &namespace))
		require.NotNil(t, namespace["ws-1"])
		assertVersions(t, *namespace["ws-1"])
	})

	t.Run("version key is case-insensitive on unmarshal", func(t *testing.T) {
		var cfg ConfigT
		require.NoError(t, jsonrs.Unmarshal([]byte(`{"sources":[{"destinations":[{"Version":2}]}]}`), &cfg))
		require.Equal(t, 2, cfg.Sources[0].Destinations[0].Version)
	})
}
