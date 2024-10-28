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
					Config: map[string]interface{}{
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
					Config: map[string]interface{}{
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
					Config: map[string]interface{}{
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
					Config: map[string]interface{}{
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
		expected  map[string]interface{}
	}{
		{
			name: "key exists and is map",
			warehouse: Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"testKey": map[string]interface{}{
							"key": "value",
						},
					},
				},
			},
			key: testKey,
			expected: map[string]interface{}{
				"key": "value",
			},
		},
		{
			name: "key exists and is not map",
			warehouse: Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"testKey": "value",
					},
				},
			},
			key:      testKey,
			expected: map[string]interface{}{},
		},
		{
			name: "key does not exist",
			warehouse: Warehouse{
				Destination: backendconfig.DestinationT{
					Config: map[string]interface{}{
						"otherTestKey": map[string]interface{}{
							"key": "value",
						},
					},
				},
			},
			key:      testKey,
			expected: map[string]interface{}{},
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
					Config: map[string]interface{}{
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
					Config: map[string]interface{}{
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
					Config: map[string]interface{}{
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
					Config: map[string]interface{}{
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
