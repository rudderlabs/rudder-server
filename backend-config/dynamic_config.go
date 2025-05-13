package backendconfig

import (
	"github.com/rudderlabs/rudder-server/backend-config/dynamicconfig"
)

// DynamicConfigMapCache is a map-based implementation of DynamicConfigCache.
// The cache is keyed by destination ID and stores the RevisionID and HasDynamicConfig values.
type DynamicConfigMapCache map[string]*dynamicconfig.DestinationRevisionInfo

// Get retrieves a cache entry for a destination ID.
func (c DynamicConfigMapCache) Get(destID string) (*dynamicconfig.DestinationRevisionInfo, bool) {
	info, exists := c[destID]
	return info, exists
}

// Set stores a cache entry for a destination ID.
func (c DynamicConfigMapCache) Set(destID string, info *dynamicconfig.DestinationRevisionInfo) {
	c[destID] = info
}

// Len returns the number of entries in the cache.
func (c DynamicConfigMapCache) Len() int {
	return len(c)
}

// ProcessDestinationsInSources processes all destinations in all sources
// and updates their HasDynamicConfig flag using the provided cache.
// This utility function is used to avoid code duplication across different parts of the codebase.
//
// Parameters:
//   - sources: A slice of SourceT containing destinations to process
//   - cache: A dynamicconfig.Cache implementation to store and retrieve dynamic config information
//
// Usage example:
//
//	ProcessDestinationsInSources(workspace.Sources, dynamicConfigCache)
func ProcessDestinationsInSources(sources []SourceT, cache dynamicconfig.Cache) {
	for i := range sources {
		for j := range sources[i].Destinations {
			dest := &sources[i].Destinations[j]
			dest.UpdateHasDynamicConfig(cache)
		}
	}
}
