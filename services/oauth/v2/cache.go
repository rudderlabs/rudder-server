package v2

import "sync"

type Cache interface {
	// Load retrieves a value for the given key from the cache.
	// If the value doesn't exist, it returns (nil, false).
	Load(key any) (any, bool)

	// Store stores a key-value pair in the cache.
	Store(key, value any)

	// Delete removes a key-value pair from the cache.
	Delete(key any)
}

func NewCache() Cache {
	return &sync.Map{}
}
