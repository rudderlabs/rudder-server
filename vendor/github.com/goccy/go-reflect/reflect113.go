// +build go1.13

package reflect

// IsZero reports whether v is the zero value for its type.
// It panics if the argument is invalid.
func (v Value) IsZero() bool {
	return toRV(v).IsZero()
}
