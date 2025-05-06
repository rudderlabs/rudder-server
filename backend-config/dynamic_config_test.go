package backendconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetHasDynamicConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		expectValue bool
	}{
		{
			name: "with dynamic config pattern",
			config: map[string]interface{}{
				"apiKey": "{{ message.context.apiKey || \"default-api-key\" }}",
			},
			expectValue: true,
		},
		{
			name: "with dynamic config pattern in nested map",
			config: map[string]interface{}{
				"credentials": map[string]interface{}{
					"apiKey": "{{ message.context.apiKey || \"default-api-key\" }}",
				},
			},
			expectValue: true,
		},
		{
			name: "without dynamic config pattern",
			config: map[string]interface{}{
				"apiKey": "static-api-key",
			},
			expectValue: false,
		},
		{
			name:        "with nil config",
			config:      nil,
			expectValue: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &ConfigT{}
			dest := &DestinationT{
				Config: tt.config,
			}

			config.setHasDynamicConfig(dest)
			assert.Equal(t, tt.expectValue, dest.HasDynamicConfig)
		})
	}
}

func TestProcessDynamicConfig(t *testing.T) {
	// Create a config with a source and destination that has dynamic config
	config := &ConfigT{
		Sources: []SourceT{
			{
				ID: "source-1",
				Destinations: []DestinationT{
					{
						ID: "dest-1",
						Config: map[string]interface{}{
							"apiKey": "{{ message.context.apiKey || \"default-api-key\" }}",
						},
					},
					{
						ID: "dest-2",
						Config: map[string]interface{}{
							"apiKey": "static-api-key",
						},
					},
				},
			},
		},
	}

	// Process dynamic config
	config.processDynamicConfig()

	// Verify that HasDynamicConfig is set correctly for each destination
	assert.True(t, config.Sources[0].Destinations[0].HasDynamicConfig, "Destination with dynamic config should have HasDynamicConfig=true")
	assert.False(t, config.Sources[0].Destinations[1].HasDynamicConfig, "Destination without dynamic config should have HasDynamicConfig=false")
}
