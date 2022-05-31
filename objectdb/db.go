package objectdb

type Objectdb interface {
	GetOrPut(interface{}) (interface{}, error)
}
