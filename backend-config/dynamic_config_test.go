// This file contains tests for the dynamic config functionality in the backendconfig package.
// These tests are in a separate package (backendconfig_test) to test the package from an external
// perspective, only using exported functions and types. This is more like how users of the package
// would use it.
package backendconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/backend-config/dynamicconfig"
	mock_dynamicconfig "github.com/rudderlabs/rudder-server/mocks/backend-config/dynamicconfig"
)

// TestUpdateDynamicConfig tests the basic functionality of UpdateHasDynamicConfig
func TestUpdateDynamicConfig(t *testing.T) {
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
			dest := &backendconfig.DestinationT{
				ID:         "test-dest",
				RevisionID: "test-rev",
				Config:     tt.config,
			}

			cache := make(backendconfig.DynamicConfigMapCache)
			dest.UpdateHasDynamicConfig(cache)
			assert.Equal(t, tt.expectValue, dest.HasDynamicConfig)

			// Check that the cache was updated correctly
			if dest.ID != "" {
				cachedInfo, exists := cache.Get(dest.ID)
				assert.True(t, exists, "Cache should contain an entry for the destination")
				assert.NotNil(t, cachedInfo, "Cache entry should not be nil")
				assert.Equal(t, dest.RevisionID, cachedInfo.RevisionID, "Cache should have the correct RevisionID")
				assert.Equal(t, tt.expectValue, cachedInfo.HasDynamicConfig, "Cache should have the correct HasDynamicConfig value")
			}
		})
	}
}

// TestProcessDynamicConfigWithCache tests the integration of UpdateHasDynamicConfig with a config structure
func TestProcessDynamicConfigWithCache(t *testing.T) {
	// Create a config with a source and destination that has dynamic config
	config := &backendconfig.ConfigT{
		Sources: []backendconfig.SourceT{
			{
				ID: "source-1",
				Destinations: []backendconfig.DestinationT{
					{
						ID:         "dest-1",
						RevisionID: "rev-1",
						Config: map[string]interface{}{
							"apiKey": "{{ message.context.apiKey || \"default-api-key\" }}",
						},
					},
					{
						ID:         "dest-2",
						RevisionID: "rev-2",
						Config: map[string]interface{}{
							"apiKey": "static-api-key",
						},
					},
				},
			},
		},
	}

	// Process dynamic config with a local cache
	cache := make(backendconfig.DynamicConfigMapCache)
	for i := range config.Sources {
		for j := range config.Sources[i].Destinations {
			dest := &config.Sources[i].Destinations[j]
			dest.UpdateHasDynamicConfig(cache)
		}
	}

	// Verify that HasDynamicConfig is set correctly for each destination
	assert.True(t, config.Sources[0].Destinations[0].HasDynamicConfig, "Destination with dynamic config should have HasDynamicConfig=true")
	assert.False(t, config.Sources[0].Destinations[1].HasDynamicConfig, "Destination without dynamic config should have HasDynamicConfig=false")

	// Verify that the cache contains the correct entries
	assert.Equal(t, 2, cache.Len(), "Cache should have 2 entries")

	dest1Info, exists := cache.Get("dest-1")
	assert.True(t, exists, "Cache should contain an entry for dest-1")
	assert.NotNil(t, dest1Info, "Cache entry for dest-1 should not be nil")
	assert.Equal(t, "rev-1", dest1Info.RevisionID, "Cache should have the correct RevisionID for dest-1")
	assert.Equal(t, true, dest1Info.HasDynamicConfig, "Cache should have the correct HasDynamicConfig value for dest-1")

	dest2Info, exists := cache.Get("dest-2")
	assert.True(t, exists, "Cache should contain an entry for dest-2")
	assert.NotNil(t, dest2Info, "Cache entry for dest-2 should not be nil")
	assert.Equal(t, "rev-2", dest2Info.RevisionID, "Cache should have the correct RevisionID for dest-2")
	assert.Equal(t, false, dest2Info.HasDynamicConfig, "Cache should have the correct HasDynamicConfig value for dest-2")

	// Test cache reuse - change the config but keep the same RevisionID
	config.Sources[0].Destinations[0].HasDynamicConfig = false // Reset the flag
	config.Sources[0].Destinations[0].UpdateHasDynamicConfig(cache)
	assert.True(t, config.Sources[0].Destinations[0].HasDynamicConfig, "Should use cached value when RevisionID is the same")

	// Test cache invalidation - change the RevisionID
	config.Sources[0].Destinations[0].RevisionID = "rev-1-updated"
	config.Sources[0].Destinations[0].HasDynamicConfig = false // Reset the flag
	config.Sources[0].Destinations[0].UpdateHasDynamicConfig(cache)
	assert.True(t, config.Sources[0].Destinations[0].HasDynamicConfig, "Should recompute when RevisionID changes")

	updatedInfo, exists := cache.Get("dest-1")
	assert.True(t, exists, "Cache should contain an entry for dest-1")
	assert.NotNil(t, updatedInfo, "Cache entry for dest-1 should not be nil")
	assert.Equal(t, "rev-1-updated", updatedInfo.RevisionID, "Cache should be updated with the new RevisionID")
}

// TestUpdateDynamicConfigWithMockCache tests the UpdateHasDynamicConfig method using a mock cache
func TestUpdateDynamicConfigWithMockCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a mock cache
	mockCache := mock_dynamicconfig.NewMockCache(ctrl)

	// Test case 1: Cache hit - destination with dynamic config
	dest1 := &backendconfig.DestinationT{
		ID:         "dest-1",
		RevisionID: "rev-1",
		Config: map[string]interface{}{
			"apiKey": "{{ message.context.apiKey || \"default-api-key\" }}",
		},
	}

	// Set up expectations for the mock cache
	// Expect a call to Get with "dest-1" and return a cached entry
	cachedInfo := &dynamicconfig.DestinationRevisionInfo{
		RevisionID:       "rev-1",
		HasDynamicConfig: true,
	}
	mockCache.EXPECT().Get("dest-1").Return(cachedInfo, true)

	// Call the method under test
	dest1.UpdateHasDynamicConfig(mockCache)

	// Verify that HasDynamicConfig was set from the cache
	assert.True(t, dest1.HasDynamicConfig, "Should use cached value when RevisionID matches")

	// Test case 2: Cache miss - destination with dynamic config
	dest2 := &backendconfig.DestinationT{
		ID:         "dest-2",
		RevisionID: "rev-2",
		Config: map[string]interface{}{
			"apiKey": "{{ message.context.apiKey || \"default-api-key\" }}",
		},
	}

	// Set up expectations for the mock cache
	// Expect a call to Get with "dest-2" and return no cached entry
	mockCache.EXPECT().Get("dest-2").Return(nil, false)
	// Expect a call to Set with the computed value
	mockCache.EXPECT().Set("dest-2", dynamicconfig.DestinationRevisionInfo{
		RevisionID:       "rev-2",
		HasDynamicConfig: true,
	})

	// Call the method under test
	dest2.UpdateHasDynamicConfig(mockCache)

	// Verify that HasDynamicConfig was computed correctly
	assert.True(t, dest2.HasDynamicConfig, "Should compute HasDynamicConfig when no cached value exists")

	// Test case 3: Cache hit but RevisionID mismatch
	dest3 := &backendconfig.DestinationT{
		ID:         "dest-3",
		RevisionID: "rev-3-new",
		Config: map[string]interface{}{
			"apiKey": "static-api-key", // No dynamic config
		},
	}

	// Set up expectations for the mock cache
	// Expect a call to Get with "dest-3" and return a cached entry with different RevisionID
	outdatedInfo := &dynamicconfig.DestinationRevisionInfo{
		RevisionID:       "rev-3-old",
		HasDynamicConfig: true, // Different from what would be computed
	}
	mockCache.EXPECT().Get("dest-3").Return(outdatedInfo, true)
	// Expect a call to Set with the newly computed value
	mockCache.EXPECT().Set("dest-3", dynamicconfig.DestinationRevisionInfo{
		RevisionID:       "rev-3-new",
		HasDynamicConfig: false, // Computed value is different from cached
	})

	// Call the method under test
	dest3.UpdateHasDynamicConfig(mockCache)

	// Verify that HasDynamicConfig was recomputed
	assert.False(t, dest3.HasDynamicConfig, "Should recompute HasDynamicConfig when RevisionID changes")
}

// Helper function to create a test config with a specified number of sources and destinations
func createTestConfig(numSources, numDestPerSource int, withDynamicConfig bool) *backendconfig.ConfigT {
	config := &backendconfig.ConfigT{}

	for i := 0; i < numSources; i++ {
		source := backendconfig.SourceT{
			ID:   string(rune('A' + i)),
			Name: "Source " + string(rune('A'+i)),
		}

		for j := 0; j < numDestPerSource; j++ {
			dest := backendconfig.DestinationT{
				ID:   string(rune('A'+i)) + string(rune('0'+j)),
				Name: "Destination " + string(rune('A'+i)) + string(rune('0'+j)),
			}

			// Add dynamic config pattern to some destinations if requested
			if withDynamicConfig && (i+j)%3 == 0 {
				dest.Config = map[string]interface{}{
					"apiKey": "{{ message.context.apiKey || \"default-api-key\" }}",
				}
			} else {
				dest.Config = map[string]interface{}{
					"apiKey": "static-api-key",
				}
			}

			source.Destinations = append(source.Destinations, dest)
		}

		config.Sources = append(config.Sources, source)
	}

	return config
}

// Benchmark for processing dynamic config with the optimized implementation
// This simulates the first run where all configs need to be processed
func BenchmarkProcessDynamicConfig_FirstRun(b *testing.B) {
	config := createTestConfig(5, 10, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset the RevisionID to simulate first run
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				config.Sources[s].Destinations[d].RevisionID = ""
				config.Sources[s].Destinations[d].HasDynamicConfig = false
			}
		}

		// Process with a local cache
		cache := make(backendconfig.DynamicConfigMapCache)
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				dest := &config.Sources[s].Destinations[d]
				dest.UpdateHasDynamicConfig(cache)
			}
		}
	}
}

// Benchmark for processing dynamic config with the optimized implementation
// This simulates subsequent runs where no configs have changed
func BenchmarkProcessDynamicConfig_NoChanges(b *testing.B) {
	config := createTestConfig(5, 10, true)

	// Process once to set up the cache
	cache := make(backendconfig.DynamicConfigMapCache)
	for s := range config.Sources {
		for d := range config.Sources[s].Destinations {
			dest := &config.Sources[s].Destinations[d]
			dest.UpdateHasDynamicConfig(cache)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reuse the same cache for subsequent runs
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				dest := &config.Sources[s].Destinations[d]
				dest.UpdateHasDynamicConfig(cache)
			}
		}
	}
}

// Benchmark for processing dynamic config with a large number of sources and destinations
func BenchmarkProcessDynamicConfig_LargeConfig(b *testing.B) {
	config := createTestConfig(20, 50, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset the RevisionID to simulate first run
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				config.Sources[s].Destinations[d].RevisionID = ""
				config.Sources[s].Destinations[d].HasDynamicConfig = false
			}
		}

		// Process with a local cache
		cache := make(backendconfig.DynamicConfigMapCache)
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				dest := &config.Sources[s].Destinations[d]
				dest.UpdateHasDynamicConfig(cache)
			}
		}
	}
}

// Benchmark for processing dynamic config with a large number of sources and destinations
// with no changes between runs
func BenchmarkProcessDynamicConfig_LargeConfig_NoChanges(b *testing.B) {
	config := createTestConfig(20, 50, true)

	// Process once to set up the cache
	cache := make(backendconfig.DynamicConfigMapCache)
	for s := range config.Sources {
		for d := range config.Sources[s].Destinations {
			dest := &config.Sources[s].Destinations[d]
			dest.UpdateHasDynamicConfig(cache)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reuse the same cache for subsequent runs
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				dest := &config.Sources[s].Destinations[d]
				dest.UpdateHasDynamicConfig(cache)
			}
		}
	}
}

// Benchmark for processing dynamic config with a config that has no dynamic patterns
func BenchmarkProcessDynamicConfig_NoDynamicConfig(b *testing.B) {
	config := createTestConfig(5, 10, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset the RevisionID to simulate first run
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				config.Sources[s].Destinations[d].RevisionID = ""
				config.Sources[s].Destinations[d].HasDynamicConfig = false
			}
		}

		// Process with a local cache
		cache := make(backendconfig.DynamicConfigMapCache)
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				dest := &config.Sources[s].Destinations[d]
				dest.UpdateHasDynamicConfig(cache)
			}
		}
	}
}

// Benchmark for processing dynamic config with a config that has no dynamic patterns
// with no changes between runs
func BenchmarkProcessDynamicConfig_NoDynamicConfig_NoChanges(b *testing.B) {
	config := createTestConfig(5, 10, false)

	// Process once to set up the cache
	cache := make(backendconfig.DynamicConfigMapCache)
	for s := range config.Sources {
		for d := range config.Sources[s].Destinations {
			dest := &config.Sources[s].Destinations[d]
			dest.UpdateHasDynamicConfig(cache)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reuse the same cache for subsequent runs
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				dest := &config.Sources[s].Destinations[d]
				dest.UpdateHasDynamicConfig(cache)
			}
		}
	}
}
