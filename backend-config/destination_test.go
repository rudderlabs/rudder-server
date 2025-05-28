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
	"github.com/rudderlabs/rudder-server/backend-config/destination"
	mock_destination "github.com/rudderlabs/rudder-server/mocks/backend-config/destination"
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

			cache := make(backendconfig.DestinationCache)
			dest.UpdateDerivedFields(cache)
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
	cache := make(backendconfig.DestinationCache)
	for i := range config.Sources {
		for j := range config.Sources[i].Destinations {
			dest := &config.Sources[i].Destinations[j]
			dest.UpdateDerivedFields(cache)
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
	config.Sources[0].Destinations[0].UpdateDerivedFields(cache)
	assert.True(t, config.Sources[0].Destinations[0].HasDynamicConfig, "Should use cached value when RevisionID is the same")

	// Test cache invalidation - change the RevisionID
	config.Sources[0].Destinations[0].RevisionID = "rev-1-updated"
	config.Sources[0].Destinations[0].HasDynamicConfig = false // Reset the flag
	config.Sources[0].Destinations[0].UpdateDerivedFields(cache)
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
	mockCache := mock_destination.NewMockCache(ctrl)

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
	cachedInfo := &destination.RevisionInfo{
		RevisionID:       "rev-1",
		HasDynamicConfig: true,
	}
	mockCache.EXPECT().Get("dest-1").Return(cachedInfo, true)

	// Call the method under test
	dest1.UpdateDerivedFields(mockCache)

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
	mockCache.EXPECT().Set("dest-2", &destination.RevisionInfo{
		RevisionID:       "rev-2",
		HasDynamicConfig: true,
	})

	// Call the method under test
	dest2.UpdateDerivedFields(mockCache)

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
	outdatedInfo := &destination.RevisionInfo{
		RevisionID:       "rev-3-old",
		HasDynamicConfig: true, // Different from what would be computed
	}
	mockCache.EXPECT().Get("dest-3").Return(outdatedInfo, true)
	// Expect a call to Set with the newly computed value
	mockCache.EXPECT().Set("dest-3", &destination.RevisionInfo{
		RevisionID:       "rev-3-new",
		HasDynamicConfig: false, // Computed value is different from cached
	})

	// Call the method under test
	dest3.UpdateDerivedFields(mockCache)

	// Verify that HasDynamicConfig was recomputed
	assert.False(t, dest3.HasDynamicConfig, "Should recompute HasDynamicConfig when RevisionID changes")
}

// TestProcessDestinationsInSources tests the ProcessDestinationsInSources utility function
func TestProcessDestinationsInSources(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a mock cache
	mockCache := mock_destination.NewMockCache(ctrl)

	// Create a test config with sources and destinations
	sources := []backendconfig.SourceT{
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
		{
			ID: "source-2",
			Destinations: []backendconfig.DestinationT{
				{
					ID:         "dest-3",
					RevisionID: "rev-3",
					Config: map[string]interface{}{
						"apiKey": "{{ message.context.apiKey || \"default-api-key\" }}",
					},
				},
			},
		},
	}

	// Set up expectations for the mock cache
	// For dest-1
	mockCache.EXPECT().Get("dest-1").Return(nil, false)
	mockCache.EXPECT().Set("dest-1", &destination.RevisionInfo{
		RevisionID:       "rev-1",
		HasDynamicConfig: true,
	})

	// For dest-2
	mockCache.EXPECT().Get("dest-2").Return(nil, false)
	mockCache.EXPECT().Set("dest-2", &destination.RevisionInfo{
		RevisionID:       "rev-2",
		HasDynamicConfig: false,
	})

	// For dest-3
	mockCache.EXPECT().Get("dest-3").Return(nil, false)
	mockCache.EXPECT().Set("dest-3", &destination.RevisionInfo{
		RevisionID:       "rev-3",
		HasDynamicConfig: true,
	})

	// Call the utility function
	backendconfig.ProcessDestinationsInSources(sources, mockCache)

	// Verify that HasDynamicConfig is set correctly for each destination
	assert.True(t, sources[0].Destinations[0].HasDynamicConfig, "Destination with dynamic config should have HasDynamicConfig=true")
	assert.False(t, sources[0].Destinations[1].HasDynamicConfig, "Destination without dynamic config should have HasDynamicConfig=false")
	assert.True(t, sources[1].Destinations[0].HasDynamicConfig, "Destination with dynamic config should have HasDynamicConfig=true")

	// Test with cached values
	// Reset the HasDynamicConfig flags
	sources[0].Destinations[0].HasDynamicConfig = false
	sources[0].Destinations[1].HasDynamicConfig = true
	sources[1].Destinations[0].HasDynamicConfig = false

	// Set up expectations for the mock cache with cached values
	// For dest-1 (cached)
	cachedInfo1 := &destination.RevisionInfo{
		RevisionID:       "rev-1",
		HasDynamicConfig: true,
	}
	mockCache.EXPECT().Get("dest-1").Return(cachedInfo1, true)

	// For dest-2 (cached)
	cachedInfo2 := &destination.RevisionInfo{
		RevisionID:       "rev-2",
		HasDynamicConfig: false,
	}
	mockCache.EXPECT().Get("dest-2").Return(cachedInfo2, true)

	// For dest-3 (cached)
	cachedInfo3 := &destination.RevisionInfo{
		RevisionID:       "rev-3",
		HasDynamicConfig: true,
	}
	mockCache.EXPECT().Get("dest-3").Return(cachedInfo3, true)

	// Call the utility function again
	backendconfig.ProcessDestinationsInSources(sources, mockCache)

	// Verify that HasDynamicConfig is set correctly from the cache
	assert.True(t, sources[0].Destinations[0].HasDynamicConfig, "Should use cached value when RevisionID matches")
	assert.False(t, sources[0].Destinations[1].HasDynamicConfig, "Should use cached value when RevisionID matches")
	assert.True(t, sources[1].Destinations[0].HasDynamicConfig, "Should use cached value when RevisionID matches")
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
		cache := make(backendconfig.DestinationCache)
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				dest := &config.Sources[s].Destinations[d]
				dest.UpdateDerivedFields(cache)
			}
		}
	}
}

// Benchmark for processing dynamic config with the optimized implementation
// This simulates subsequent runs where no configs have changed
func BenchmarkProcessDynamicConfig_NoChanges(b *testing.B) {
	config := createTestConfig(5, 10, true)

	// Process once to set up the cache
	cache := make(backendconfig.DestinationCache)
	for s := range config.Sources {
		for d := range config.Sources[s].Destinations {
			dest := &config.Sources[s].Destinations[d]
			dest.UpdateDerivedFields(cache)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reuse the same cache for subsequent runs
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				dest := &config.Sources[s].Destinations[d]
				dest.UpdateDerivedFields(cache)
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
		cache := make(backendconfig.DestinationCache)
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				dest := &config.Sources[s].Destinations[d]
				dest.UpdateDerivedFields(cache)
			}
		}
	}
}

// Benchmark for processing dynamic config with a large number of sources and destinations
// with no changes between runs
func BenchmarkProcessDynamicConfig_LargeConfig_NoChanges(b *testing.B) {
	config := createTestConfig(20, 50, true)

	// Process once to set up the cache
	cache := make(backendconfig.DestinationCache)
	for s := range config.Sources {
		for d := range config.Sources[s].Destinations {
			dest := &config.Sources[s].Destinations[d]
			dest.UpdateDerivedFields(cache)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reuse the same cache for subsequent runs
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				dest := &config.Sources[s].Destinations[d]
				dest.UpdateDerivedFields(cache)
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
		cache := make(backendconfig.DestinationCache)
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				dest := &config.Sources[s].Destinations[d]
				dest.UpdateDerivedFields(cache)
			}
		}
	}
}

// Benchmark for processing dynamic config with a config that has no dynamic patterns
// with no changes between runs
func BenchmarkProcessDynamicConfig_NoDynamicConfig_NoChanges(b *testing.B) {
	config := createTestConfig(5, 10, false)

	// Process once to set up the cache
	cache := make(backendconfig.DestinationCache)
	for s := range config.Sources {
		for d := range config.Sources[s].Destinations {
			dest := &config.Sources[s].Destinations[d]
			dest.UpdateDerivedFields(cache)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reuse the same cache for subsequent runs
		for s := range config.Sources {
			for d := range config.Sources[s].Destinations {
				dest := &config.Sources[s].Destinations[d]
				dest.UpdateDerivedFields(cache)
			}
		}
	}
}

// Benchmark for processing dynamic config using the utility function
// This simulates the first run where all configs need to be processed
func BenchmarkProcessDestinationsInSources_FirstRun(b *testing.B) {
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
		cache := make(backendconfig.DestinationCache)
		backendconfig.ProcessDestinationsInSources(config.Sources, cache)
	}
}

// Benchmark for processing dynamic config using the utility function
// This simulates subsequent runs where no configs have changed
func BenchmarkProcessDestinationsInSources_NoChanges(b *testing.B) {
	config := createTestConfig(5, 10, true)

	// Process once to set up the cache
	cache := make(backendconfig.DestinationCache)
	backendconfig.ProcessDestinationsInSources(config.Sources, cache)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reuse the same cache for subsequent runs
		backendconfig.ProcessDestinationsInSources(config.Sources, cache)
	}
}

// Benchmark for processing dynamic config using the utility function with a large config
func BenchmarkProcessDestinationsInSources_LargeConfig(b *testing.B) {
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
		cache := make(backendconfig.DestinationCache)
		backendconfig.ProcessDestinationsInSources(config.Sources, cache)
	}
}

// TestComputeOAuthInfo tests the ComputeOAuthInfo method of DestinationT
func TestSetOAuthFlags(t *testing.T) {
	tests := []struct {
		name                    string
		destination             *backendconfig.DestinationT
		expectedDeliveryByOAuth bool
		expectedDeleteByOAuth   bool
	}{
		{
			name: "OAuth enabled via delivery account definition",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-1",
				Name: "Test Destination 1",
				DeliveryAccount: &backendconfig.Account{
					ID: "delivery-account-1",
					AccountDefinition: &backendconfig.AccountDefinition{
						Name: "test-account-def",
						Config: map[string]interface{}{
							"refreshOAuthToken": true,
						},
					},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-1",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type": "OAuth",
						},
					},
				},
			},
			expectedDeliveryByOAuth: true,
			expectedDeleteByOAuth:   true, // Falls back to destination definition which defaults to true for all flows
		},
		{
			name: "OAuth enabled via delete account definition",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-2",
				Name: "Test Destination 2",
				DeleteAccount: &backendconfig.Account{
					ID: "delete-account-1",
					AccountDefinition: &backendconfig.AccountDefinition{
						Name: "test-account-def",
						Config: map[string]interface{}{
							"refreshOAuthToken": true,
						},
					},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-2",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type": "OAuth",
						},
					},
				},
			},
			expectedDeliveryByOAuth: true, // Falls back to destination definition
			expectedDeleteByOAuth:   true,
		},
		{
			name: "OAuth enabled via both account definitions",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-3",
				Name: "Test Destination 3",
				DeliveryAccount: &backendconfig.Account{
					ID: "delivery-account-1",
					AccountDefinition: &backendconfig.AccountDefinition{
						Name: "test-account-def",
						Config: map[string]interface{}{
							"refreshOAuthToken": true,
						},
					},
				},
				DeleteAccount: &backendconfig.Account{
					ID: "delete-account-1",
					AccountDefinition: &backendconfig.AccountDefinition{
						Name: "test-account-def",
						Config: map[string]interface{}{
							"refreshOAuthToken": true,
						},
					},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-3",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type": "OAuth",
						},
					},
				},
			},
			expectedDeliveryByOAuth: true,
			expectedDeleteByOAuth:   true,
		},
		{
			name: "OAuth disabled via account definitions",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-4",
				Name: "Test Destination 4",
				DeliveryAccount: &backendconfig.Account{
					ID: "delivery-account-1",
					AccountDefinition: &backendconfig.AccountDefinition{
						Name: "test-account-def",
						Config: map[string]interface{}{
							"refreshOAuthToken": false,
						},
					},
				},
				DeleteAccount: &backendconfig.Account{
					ID: "delete-account-1",
					AccountDefinition: &backendconfig.AccountDefinition{
						Name: "test-account-def",
						Config: map[string]interface{}{
							"refreshOAuthToken": false,
						},
					},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-4",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type": "OAuth",
						},
					},
				},
			},
			expectedDeliveryByOAuth: false,
			expectedDeleteByOAuth:   false,
		},
		{
			name: "OAuth enabled via destination definition with delivery and delete scopes",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-5",
				Name: "Test Destination 5",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-5",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type":         "OAuth",
							"rudderScopes": []interface{}{"delivery", "delete"},
						},
					},
				},
			},
			expectedDeliveryByOAuth: true,
			expectedDeleteByOAuth:   true,
		},
		{
			name: "OAuth enabled via destination definition with delivery scope only",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-6",
				Name: "Test Destination 6",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-6",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type":         "OAuth",
							"rudderScopes": []interface{}{"delivery"},
						},
					},
				},
			},
			expectedDeliveryByOAuth: true,
			expectedDeleteByOAuth:   false,
		},
		{
			name: "OAuth enabled via destination definition with delete scope only",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-7",
				Name: "Test Destination 7",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-7",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type":         "OAuth",
							"rudderScopes": []interface{}{"delete"},
						},
					},
				},
			},
			expectedDeliveryByOAuth: false,
			expectedDeleteByOAuth:   true,
		},
		{
			name: "OAuth enabled via destination definition without rudderScopes (defaults to all flows)",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-8",
				Name: "Test Destination 8",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-8",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type": "OAuth",
						},
					},
				},
			},
			expectedDeliveryByOAuth: true,
			expectedDeleteByOAuth:   true,
		},
		{
			name: "OAuth disabled - non-OAuth auth type",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-9",
				Name: "Test Destination 9",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-9",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type": "ApiKey",
						},
					},
				},
			},
			expectedDeliveryByOAuth: false,
			expectedDeleteByOAuth:   false,
		},
		{
			name: "OAuth disabled - no auth config",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-10",
				Name: "Test Destination 10",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:     "dest-def-10",
					Name:   "Test Destination Definition",
					Config: map[string]interface{}{},
				},
			},
			expectedDeliveryByOAuth: false,
			expectedDeleteByOAuth:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset OAuth flags before testing
			tt.destination.DeliveryByOAuth = false
			tt.destination.DeleteByOAuth = false

			// Call the method under test
			tt.destination.SetOAuthFlags()

			// Verify the results
			assert.Equal(t, tt.expectedDeliveryByOAuth, tt.destination.DeliveryByOAuth, "DeliveryByOAuth should match expected value")
			assert.Equal(t, tt.expectedDeleteByOAuth, tt.destination.DeleteByOAuth, "DeleteByOAuth should match expected value")
		})
	}
}

// TestComputeOAuthInfoEdgeCases tests edge cases for the ComputeOAuthInfo method
func TestSetOAuthFlagsEdgeCases(t *testing.T) {
	tests := []struct {
		name                    string
		destination             *backendconfig.DestinationT
		expectedDeliveryByOAuth bool
		expectedDeleteByOAuth   bool
	}{
		{
			name: "nil delivery account",
			destination: &backendconfig.DestinationT{
				ID:              "test-dest-nil-delivery",
				Name:            "Test Destination Nil Delivery",
				DeliveryAccount: nil,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-nil-delivery",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type": "OAuth",
						},
					},
				},
			},
			expectedDeliveryByOAuth: true, // Falls back to destination definition
			expectedDeleteByOAuth:   true,
		},
		{
			name: "nil delete account",
			destination: &backendconfig.DestinationT{
				ID:            "test-dest-nil-delete",
				Name:          "Test Destination Nil Delete",
				DeleteAccount: nil,
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-nil-delete",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type": "OAuth",
						},
					},
				},
			},
			expectedDeliveryByOAuth: true,
			expectedDeleteByOAuth:   true, // Falls back to destination definition
		},
		{
			name: "nil account definition in delivery account",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-nil-account-def",
				Name: "Test Destination Nil Account Def",
				DeliveryAccount: &backendconfig.Account{
					ID:                "delivery-account-1",
					AccountDefinition: nil,
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-nil-account-def",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type": "OAuth",
						},
					},
				},
			},
			expectedDeliveryByOAuth: true, // Falls back to destination definition
			expectedDeleteByOAuth:   true,
		},
		{
			name: "nil account definition in delete account",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-nil-delete-account-def",
				Name: "Test Destination Nil Delete Account Def",
				DeleteAccount: &backendconfig.Account{
					ID:                "delete-account-1",
					AccountDefinition: nil,
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-nil-delete-account-def",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type": "OAuth",
						},
					},
				},
			},
			expectedDeliveryByOAuth: true,
			expectedDeleteByOAuth:   true, // Falls back to destination definition
		},
		{
			name: "account definition with missing refreshOAuthToken",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-missing-refresh-token",
				Name: "Test Destination Missing Refresh Token",
				DeliveryAccount: &backendconfig.Account{
					ID: "delivery-account-1",
					AccountDefinition: &backendconfig.AccountDefinition{
						Name:   "test-account-def",
						Config: map[string]interface{}{},
					},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-missing-refresh-token",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type": "OAuth",
						},
					},
				},
			},
			expectedDeliveryByOAuth: false, // refreshOAuthToken not present or false
			expectedDeleteByOAuth:   true,  // Falls back to destination definition
		},
		{
			name: "account definition with non-boolean refreshOAuthToken",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-non-bool-refresh-token",
				Name: "Test Destination Non Bool Refresh Token",
				DeliveryAccount: &backendconfig.Account{
					ID: "delivery-account-1",
					AccountDefinition: &backendconfig.AccountDefinition{
						Name: "test-account-def",
						Config: map[string]interface{}{
							"refreshOAuthToken": "true", // String instead of boolean
						},
					},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-non-bool-refresh-token",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type": "OAuth",
						},
					},
				},
			},
			expectedDeliveryByOAuth: false, // Type assertion fails, returns false
			expectedDeleteByOAuth:   true,  // Falls back to destination definition
		},
		{
			name: "destination definition with invalid auth config",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-invalid-auth",
				Name: "Test Destination Invalid Auth",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-invalid-auth",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": "invalid-auth-config", // String instead of map
					},
				},
			},
			expectedDeliveryByOAuth: false,
			expectedDeleteByOAuth:   false,
		},
		{
			name: "destination definition with invalid rudderScopes type",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-invalid-scopes",
				Name: "Test Destination Invalid Scopes",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-invalid-scopes",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type":         "OAuth",
							"rudderScopes": "invalid-scopes", // String instead of slice
						},
					},
				},
			},
			expectedDeliveryByOAuth: true, // When rudderScopes is not []interface{}, it defaults to true for all flows
			expectedDeleteByOAuth:   true, // When rudderScopes is not []interface{}, it defaults to true for all flows
		},
		{
			name: "destination definition with invalid scope item type",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-invalid-scope-item",
				Name: "Test Destination Invalid Scope Item",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-invalid-scope-item",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type":         "OAuth",
							"rudderScopes": []interface{}{123, "delivery"}, // Number instead of string
						},
					},
				},
			},
			expectedDeliveryByOAuth: true,  // Non-string values get converted to empty strings, "delivery" is still present
			expectedDeleteByOAuth:   false, // "delete" is not present in the scopes
		},
		{
			name: "destination definition with invalid scope value",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-invalid-scope-value",
				Name: "Test Destination Invalid Scope Value",
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-invalid-scope-value",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type":         "OAuth",
							"rudderScopes": []interface{}{"invalid", "delivery"}, // Invalid scope value
						},
					},
				},
			},
			expectedDeliveryByOAuth: true,  // "delivery" is present in the scopes
			expectedDeleteByOAuth:   false, // "delete" is not present in the scopes
		},
		{
			name: "account definition present but config is nil",
			destination: &backendconfig.DestinationT{
				ID:   "test-dest-nil-config",
				Name: "Test Destination Nil Config",
				DeliveryAccount: &backendconfig.Account{
					ID: "delivery-account-1",
					AccountDefinition: &backendconfig.AccountDefinition{
						Name:   "test-account-def",
						Config: nil, // Config is nil
					},
				},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					ID:   "dest-def-nil-config",
					Name: "Test Destination Definition",
					Config: map[string]interface{}{
						"auth": map[string]interface{}{
							"type": "OAuth",
						},
					},
				},
			},
			expectedDeliveryByOAuth: false, // Config is nil, so refreshOAuthToken lookup fails
			expectedDeleteByOAuth:   true,  // Falls back to destination definition
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset OAuth flags before testing
			tt.destination.DeliveryByOAuth = false
			tt.destination.DeleteByOAuth = false

			// Call the method under test
			tt.destination.SetOAuthFlags()

			// Verify the results
			assert.Equal(t, tt.expectedDeliveryByOAuth, tt.destination.DeliveryByOAuth, "DeliveryByOAuth should match expected value")
			assert.Equal(t, tt.expectedDeleteByOAuth, tt.destination.DeleteByOAuth, "DeleteByOAuth should match expected value")
		})
	}
}

// TestSetDynamicConfigFlags tests the SetDynamicConfigFlags method directly
func TestSetDynamicConfigFlags(t *testing.T) {
	tests := []struct {
		name                  string
		config                map[string]interface{}
		expectedDynamicConfig bool
	}{
		{
			name: "config with dynamic pattern",
			config: map[string]interface{}{
				"apiKey": "{{ message.context.apiKey || \"default-api-key\" }}",
			},
			expectedDynamicConfig: true,
		},
		{
			name: "config with nested dynamic pattern",
			config: map[string]interface{}{
				"credentials": map[string]interface{}{
					"token": "{{ message.context.token || \"default-token\" }}",
				},
			},
			expectedDynamicConfig: true,
		},
		{
			name: "config with array containing dynamic pattern",
			config: map[string]interface{}{
				"headers": []interface{}{
					map[string]interface{}{
						"value": "{{ message.context.header || \"default\" }}",
					},
				},
			},
			expectedDynamicConfig: true,
		},
		{
			name: "config without dynamic pattern",
			config: map[string]interface{}{
				"apiKey": "static-api-key",
				"nested": map[string]interface{}{
					"value": "static-value",
				},
			},
			expectedDynamicConfig: false,
		},
		{
			name:                  "nil config",
			config:                nil,
			expectedDynamicConfig: false,
		},
		{
			name:                  "empty config",
			config:                map[string]interface{}{},
			expectedDynamicConfig: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dest := &backendconfig.DestinationT{
				ID:     "test-dest",
				Config: tt.config,
			}

			// Call the method under test
			dest.SetDynamicConfigFlags()

			// Verify the result
			assert.Equal(t, tt.expectedDynamicConfig, dest.HasDynamicConfig, "HasDynamicConfig should match expected value")
		})
	}
}
