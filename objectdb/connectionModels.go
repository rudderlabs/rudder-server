//go:generate go run github.com/objectbox/objectbox-go/cmd/objectbox-gogen
package objectdb

// connection types
type (
	SourceID struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
	DestinationID struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
	SourceBatchID struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
	SourceTaskID struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
	SourceTaskRunID struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
	SourceJobID struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
	SourceJobRunID struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
	SourceDefinitionID struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
	DestinationDefinitionID struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
	SourceCategory struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
)
