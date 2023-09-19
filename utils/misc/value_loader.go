package misc

// SingleValueLoader returns a ValueLoader that always returns the same value.
func SingleValueLoader[T any](v T) ValueLoader[T] {
	return &loader[T]{v}
}

// ValueLoader is an interface that can be used to load a value.
type ValueLoader[T any] interface {
	Load() T
}

// loader is a ValueLoader that always returns the same value.
type loader[T any] struct {
	v T
}

// Load returns the value.
func (l *loader[T]) Load() T {
	return l.v
}
