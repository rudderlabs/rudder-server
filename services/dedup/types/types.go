package types

import (
	"errors"
)

// BatchKey represents a key in a batch
type BatchKey struct {
	// Index is the index of the key in the batch (used for discriminating between keys with the same value)
	Index int
	// Key is the value of the key
	Key string
}

// DB is the interface that all dedup database implementations must satisfy
type DB interface {
	// Get returns a map of keys that exist in the database
	Get(keys []string) (map[string]bool, error)
	// Set stores keys in the database with a TTL
	Set(keys []string) error
	// Close closes the database connection
	Close()
}

// Dedup is the interface for deduplication service
type Dedup interface {
	// Allowed returns a map containing all keys which are being encountered for the first time, i.e. not present in the deduplication database.
	// Keys that are not allowed are not present in the map.
	Allowed(keys ...BatchKey) (map[BatchKey]bool, error)

	// Commit commits a list of allowed keys to the deduplication service
	Commit(keys []string) error

	// Close closes the deduplication service
	Close()
}

// ErrKeyNotSet is returned when trying to commit a key that was not previously allowed
var ErrKeyNotSet = errors.New("key has not been previously set")
