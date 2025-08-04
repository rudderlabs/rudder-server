package repo

// UpdateField is a function type that returns an UpdateKeyValue for a given value.
type UpdateField func(v interface{}) UpdateKeyValue

// UpdateKeyValue is an interface for key-value update pairs.
type UpdateKeyValue interface {
	key() string
	value() interface{}
}

// keyValue is a concrete implementation of UpdateKeyValue.
type keyValue struct {
	k string
	v interface{}
}

func (u keyValue) key() string        { return u.k }
func (u keyValue) value() interface{} { return u.v }
