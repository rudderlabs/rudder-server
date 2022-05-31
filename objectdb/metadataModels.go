//go:generate go run github.com/objectbox/objectbox-go/cmd/objectbox-gogen
package objectdb

//eventMetadata types
type (
	EventType struct {
		Name string
		Id   uint64
	}
)
