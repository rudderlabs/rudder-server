package backendconfig

import (
	"testing"
)

// Helper function to create a test config with a specified number of sources and destinations
func createTestConfig(numSources, numDestPerSource int, withDynamicConfig bool) *ConfigT {
	config := &ConfigT{}

	for i := 0; i < numSources; i++ {
		source := SourceT{
			ID:   string(rune('A' + i)),
			Name: "Source " + string(rune('A'+i)),
		}

		for j := 0; j < numDestPerSource; j++ {
			dest := DestinationT{
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
		config.processDynamicConfig()
	}
}

// Benchmark for processing dynamic config with the optimized implementation
// This simulates subsequent runs where no configs have changed
func BenchmarkProcessDynamicConfig_NoChanges(b *testing.B) {
	config := createTestConfig(5, 10, true)

	// Process once to set up the hashes
	config.processDynamicConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		config.processDynamicConfig()
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
		config.processDynamicConfig()
	}
}

// Benchmark for processing dynamic config with a large number of sources and destinations
// with no changes between runs
func BenchmarkProcessDynamicConfig_LargeConfig_NoChanges(b *testing.B) {
	config := createTestConfig(20, 50, true)

	// Process once to set up the hashes
	config.processDynamicConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		config.processDynamicConfig()
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
		config.processDynamicConfig()
	}
}

// Benchmark for processing dynamic config with a config that has no dynamic patterns
// with no changes between runs
func BenchmarkProcessDynamicConfig_NoDynamicConfig_NoChanges(b *testing.B) {
	config := createTestConfig(5, 10, false)

	// Process once to set up the hashes
	config.processDynamicConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		config.processDynamicConfig()
	}
}
