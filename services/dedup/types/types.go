package types

// BatchKey represents a key in a batch
type BatchKey struct {
	// Index is the index of the key in the batch (used for discriminating between keys with the same value)
	Index int
	// Key is the value of the key
	Key string
}
