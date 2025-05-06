package backendconfig

import (
	"sync"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// DestDynamicConfigCache is a struct to store the cached dynamic config information for a destination.
// It contains the RevisionID and HasDynamicConfig flag for a destination.
// When a destination's RevisionID changes, it indicates a config change, and we need to recompute the flag.
type DestDynamicConfigCache struct {
	RevisionID       string // The revision ID of the destination config
	HasDynamicConfig bool   // Whether the destination config contains dynamic config patterns
}

// Global cache to store the dynamic config information for destinations.
// This cache persists across different instances of the ConfigT struct and across different
// calls to processDynamicConfig, allowing us to avoid redundant computation.
// The cache is keyed by destination ID and protected by a read-write mutex for thread safety.
var (
	dynamicConfigCache     = make(map[string]DestDynamicConfigCache)
	dynamicConfigCacheLock sync.RWMutex
)

// setHasDynamicConfig checks if the destination config contains dynamic config patterns
// and sets the HasDynamicConfig field accordingly.
// It uses a global cache to avoid recomputing the flag for destinations that haven't changed.
// The cache is keyed by destination ID and stores the RevisionID and HasDynamicConfig values.
// When a destination's RevisionID changes, it indicates a config change, and we recompute the flag.
//
// Assumptions:
// - Destination ID is always present in production code
// - RevisionID is always present in production code
func (c *ConfigT) setHasDynamicConfig(dest *DestinationT) {
	// If the config is nil, set HasDynamicConfig to false and return
	// This is mainly for test cases, as in production code Config should never be nil
	if dest.Config == nil {
		dest.HasDynamicConfig = false
		return
	}

	// Check if we have a cached value for this destination
	dynamicConfigCacheLock.RLock()
	cachedInfo, exists := dynamicConfigCache[dest.ID]
	dynamicConfigCacheLock.RUnlock()

	// If the destination's RevisionID matches the cached RevisionID,
	// use the cached HasDynamicConfig value to avoid recomputation
	if exists && dest.RevisionID == cachedInfo.RevisionID {
		dest.HasDynamicConfig = cachedInfo.HasDynamicConfig
		return
	}

	// RevisionID is not in cache or has changed, recompute the dynamic config flag
	dest.HasDynamicConfig = misc.ContainsDynamicConfigPattern(dest.Config)

	// Update the cache with the new value
	// Only update the cache if the destination has an ID (for test cases)
	if dest.ID != "" {
		dynamicConfigCacheLock.Lock()
		dynamicConfigCache[dest.ID] = DestDynamicConfigCache{
			RevisionID:       dest.RevisionID,
			HasDynamicConfig: dest.HasDynamicConfig,
		}
		dynamicConfigCacheLock.Unlock()
	}
}

// processDynamicConfig iterates through all sources and their destinations
// and sets the HasDynamicConfig field based on whether the destination config
// contains dynamic config patterns.
//
// It uses a global cache to avoid recomputing the flag for destinations that haven't changed.
// The cache is keyed by destination ID and stores the RevisionID and HasDynamicConfig values.
// When a destination's RevisionID changes, it indicates a config change, and we recompute the flag.
//
// This optimization is particularly important for the backend-config module, which is called
// frequently but where configurations change infrequently.
func (c *ConfigT) processDynamicConfig() {
	for i := range c.Sources {
		for j := range c.Sources[i].Destinations {
			dest := &c.Sources[i].Destinations[j]
			c.setHasDynamicConfig(dest)
		}
	}
}
