package backendconfig

import (
	"github.com/rudderlabs/rudder-server/backend-config/dynamicconfig"
)

// DynamicConfigMapCache is a map-based implementation of DynamicConfigCache.
// The cache is keyed by destination ID and stores the RevisionID and HasDynamicConfig values.
type DynamicConfigMapCache map[string]dynamicconfig.DestinationRevisionInfo

// Get retrieves a cache entry for a destination ID.
func (c DynamicConfigMapCache) Get(destID string) (*dynamicconfig.DestinationRevisionInfo, bool) {
	info, exists := c[destID]
	if !exists {
		return nil, false
	}
	return &info, true
}

// Set stores a cache entry for a destination ID.
func (c DynamicConfigMapCache) Set(destID string, info dynamicconfig.DestinationRevisionInfo) {
	c[destID] = info
}

// Len returns the number of entries in the cache.
func (c DynamicConfigMapCache) Len() int {
	return len(c)
}
