package partitionbuffer

import (
	"iter"
	"maps"
)

// readOnlyMap provides a read-only view of a map
type readOnlyMap[K comparable, V any] struct {
	data map[K]V
}

// newReadOnlyMap creates a new readOnlyMap from an existing map
// The original map is copied to prevent external modifications
func newReadOnlyMap[K comparable, V any](data map[K]V) *readOnlyMap[K, V] {
	return &readOnlyMap[K, V]{
		data: data,
	}
}

// Get retrieves a value by key
func (r *readOnlyMap[K, V]) Get(key K) (V, bool) {
	val, ok := r.data[key]
	return val, ok
}

// Has checks if a key exists
func (r *readOnlyMap[K, V]) Has(key K) bool {
	_, ok := r.data[key]
	return ok
}

// Len returns the number of elements
func (r *readOnlyMap[K, V]) Len() int {
	return len(r.data)
}

// Keys returns all keys
func (r *readOnlyMap[K, V]) Keys() iter.Seq[K] {
	return maps.Keys(r.data)
}

// Values returns all values
func (r *readOnlyMap[K, V]) Values() iter.Seq[V] {
	return maps.Values(r.data)
}

// ForEach iterates over all key-value pairs
func (r *readOnlyMap[K, V]) ForEach(fn func(K, V)) {
	for k, v := range r.data {
		fn(k, v)
	}
}

// Append returns a new readOnlyMap with the provided entries added
func (r *readOnlyMap[K, V]) Append(entries map[K]V) *readOnlyMap[K, V] {
	newMap := maps.Clone(r.data)
	for k, v := range entries {
		newMap[k] = v
	}
	return &readOnlyMap[K, V]{data: newMap}
}
