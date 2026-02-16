package repo

// UpdateField is a function type that returns an UpdateKeyValue for a given value.
type UpdateField func(v any) UpdateKeyValue

// UpdateKeyValue is an interface for key-value update pairs.
type UpdateKeyValue interface {
	key() string
	value() any
}

// keyValue is a concrete implementation of UpdateKeyValue.
type keyValue struct {
	k string
	v any
}

func (u keyValue) key() string { return u.k }
func (u keyValue) value() any  { return u.v }
