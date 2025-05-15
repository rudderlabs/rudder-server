package dynamicconfig

//go:generate mockgen -destination=../mocks/backend-config/dynamicconfig/mock_dynamic_config.go -package=mock_backendconfig github.com/rudderlabs/rudder-server/backend-config/dynamicconfig Cache

// DestinationRevisionInfo is a struct to store the cached dynamic config information for a destination.
// It contains the RevisionID and HasDynamicConfig flag for a destination.
// When a destination's RevisionID changes, it indicates a config change, and we need to recompute the flag.
type DestinationRevisionInfo struct {
	RevisionID       string // The revision ID of the destination config
	HasDynamicConfig bool   // Whether the destination config contains dynamic config patterns
}

// Cache is an interface for the dynamic config cache.
// It defines methods to get, set, and check for the existence of cache entries.
type Cache interface {
	// Get retrieves a cache entry for a destination ID.
	// It returns a pointer to the entry (nil if not found) and a boolean indicating if the entry exists.
	Get(destID string) (*DestinationRevisionInfo, bool)

	// Set stores a cache entry for a destination ID.
	Set(destID string, info *DestinationRevisionInfo)

	// Len returns the number of entries in the cache.
	Len() int
}
