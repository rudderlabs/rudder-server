package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

var testKey DestinationConfigSetting = destConfSetting("testKey")

func TestWarehouse_GetBoolDestinationConfig(t *testing.T) {
	testCases := []struct {
		name      string
		warehouse Warehouse
		key       DestinationConfigSetting
		expected  bool
	}{
		{
			name: "key exists and is true",
			warehouse: Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"testKey": true,
					},
				},
			},
			key:      testKey,
			expected: true,
		},
		{
			name: "key exists and is false",
			warehouse: Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"testKey": false,
					},
				},
			},
			key:      testKey,
			expected: false,
		},
		{
			name: "key exists and is true as string",
			warehouse: Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"testKey": "true",
					},
				},
			},
			key:      testKey,
			expected: false,
		},
		{
			name: "key does not exist",
			warehouse: Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"otherTestKey": true,
					},
				},
			},
			key:      testKey,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.warehouse.GetBoolDestinationConfig(tc.key))
		})
	}
}

func TestWarehouse_GetMapDestinationConfig(t *testing.T) {
	testCases := []struct {
		name      string
		warehouse Warehouse
		key       DestinationConfigSetting
		expected  map[string]any
	}{
		{
			name: "key exists and is map",
			warehouse: Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"testKey": map[string]any{
							"key": "value",
						},
					},
				},
			},
			key: testKey,
			expected: map[string]any{
				"key": "value",
			},
		},
		{
			name: "key exists and is not map",
			warehouse: Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"testKey": "value",
					},
				},
			},
			key:      testKey,
			expected: map[string]any{},
		},
		{
			name: "key does not exist",
			warehouse: Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"otherTestKey": map[string]any{
							"key": "value",
						},
					},
				},
			},
			key:      testKey,
			expected: map[string]any{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.warehouse.GetMapDestinationConfig(tc.key))
		})
	}
}

func TestWarehouse_GetStringDestinationConfig(t *testing.T) {
	overrideConfig := config.New()
	overrideConfig.Set("Warehouse.pipeline.source.destination.testKey", "overrideValue")

	testCases := []struct {
		name      string
		warehouse Warehouse
		conf      *config.Config
		key       DestinationConfigSetting
		expected  string
	}{
		{
			name: "key exists and is string",
			warehouse: Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"testKey": "value",
					},
				},
			},
			conf:     config.New(),
			key:      testKey,
			expected: "value",
		},
		{
			name: "key exists and is string but config override",
			warehouse: Warehouse{
				Source: backendconfig.SourceT{
					ID: "source",
				},
				Destination: backendconfig.DestinationT{
					ID: "destination",
					Config: map[string]any{
						"testKey": "value",
					},
				},
			},
			conf:     overrideConfig,
			key:      testKey,
			expected: "overrideValue",
		},
		{
			name: "key exists and is not string",
			warehouse: Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"testKey": true,
					},
				},
			},
			conf:     config.New(),
			key:      testKey,
			expected: "",
		},
		{
			name: "key does not exist",
			warehouse: Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]any{
						"otherTestKey": "value",
					},
				},
			},
			conf:     config.New(),
			key:      testKey,
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.warehouse.GetStringDestinationConfig(tc.conf, tc.key))
		})
	}
}
