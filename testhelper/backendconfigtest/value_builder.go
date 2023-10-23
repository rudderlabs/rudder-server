package backendconfigtest

type valueBuilder[V any] struct {
	v *V
}

// Build builds the value
func (b *valueBuilder[V]) Build() V {
	return *b.v
}
