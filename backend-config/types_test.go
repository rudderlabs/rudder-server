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

func TestDestinationDefinitionVersioningMetadata(t *testing.T) {
	const blob = `{
		"workspaceId": "ws-customerio",
		"sources": [{
			"id": "source-1",
			"destinations": [{
				"id": "customerio-prod",
				"name": "Customer IO Prod",
				"version": 1,
				"destinationDefinition": {
					"id": "customerio-definition",
					"name": "CUSTOMERIO",
					"displayName": "Customer.io",
					"config": {
						"apiVersion": "v2",
						"secretKeys": ["appApiKey"]
					},
					"version": "2.0",
					"versions": {
						"1": {
							"version": "1.0",
							"status": "supported",
							"retirementDate": "2026-12-31",
							"migrationDocsUrl": "https://example.com/customerio-migration",
							"config": {
								"apiVersion": "v1",
								"secretKeys": ["siteId", "apiKey"]
							},
							"configSchema": {
								"type": "object"
							},
							"uiConfig": {
								"legacy": true
							}
						}
					}
				}
			}]
		}]
	}`

	assertVersionedCustomerIODestination := func(t *testing.T, cfg ConfigT) {
		t.Helper()
		require.Len(t, cfg.Sources, 1)
		require.Len(t, cfg.Sources[0].Destinations, 1)

		destination := cfg.Sources[0].Destinations[0]
		require.Equal(t, "customerio-prod", destination.ID)
		require.Equal(t, 1, destination.Version, "destination instance pin must remain distinct from definition current version")

		definition := destination.DestinationDefinition
		require.Equal(t, "CUSTOMERIO", definition.Name)
		require.Equal(t, "2.0", definition.Version, "current definition version should survive unmarshal")
		require.Equal(t, map[string]any{
			"apiVersion": "v2",
			"secretKeys": []any{"appApiKey"},
		}, definition.Config)

		require.Contains(t, definition.Versions, "1")
		legacyDefinition := definition.Versions["1"]
		require.Equal(t, "1.0", legacyDefinition.Version)
		require.Equal(t, "supported", legacyDefinition.Status)
		require.Equal(t, "2026-12-31", legacyDefinition.RetirementDate)
		require.Equal(t, "https://example.com/customerio-migration", legacyDefinition.MigrationDocsURL)
		require.Equal(t, map[string]any{
			"apiVersion": "v1",
			"secretKeys": []any{"siteId", "apiKey"},
		}, legacyDefinition.Config)
		require.Equal(t, map[string]any{"type": "object"}, legacyDefinition.ConfigSchema)
		require.Equal(t, map[string]any{"legacy": true}, legacyDefinition.UIConfig)
	}

	t.Run("unmarshals current-v2 definition with a v1-pinned destination", func(t *testing.T) {
		var single ConfigT
		require.NoError(t, jsonrs.Unmarshal([]byte(blob), &single))
		assertVersionedCustomerIODestination(t, single)

		var namespace map[string]*ConfigT
		require.NoError(t, jsonrs.Unmarshal([]byte(`{"ws-customerio": `+blob+`}`), &namespace))
		require.NotNil(t, namespace["ws-customerio"])
		assertVersionedCustomerIODestination(t, *namespace["ws-customerio"])
	})

	t.Run("round-trips definition version archive without conflating destination version", func(t *testing.T) {
		var cfg ConfigT
		require.NoError(t, jsonrs.Unmarshal([]byte(blob), &cfg))

		data, err := jsonrs.Marshal(cfg)
		require.NoError(t, err)

		var raw struct {
			Sources []struct {
				Destinations []struct {
					Version               int `json:"version"`
					DestinationDefinition struct {
						Version  string                                   `json:"version"`
						Versions map[string]DestinationDefinitionVersionT `json:"versions"`
					} `json:"destinationDefinition"`
				} `json:"destinations"`
			} `json:"sources"`
		}
		require.NoError(t, jsonrs.Unmarshal(data, &raw))
		require.Equal(t, 1, raw.Sources[0].Destinations[0].Version)
		require.Equal(t, "2.0", raw.Sources[0].Destinations[0].DestinationDefinition.Version)
		require.Equal(t, "1.0", raw.Sources[0].Destinations[0].DestinationDefinition.Versions["1"].Version)
	})
}
