//go:generate go run github.com/objectbox/objectbox-go/cmd/objectbox-gogen
package objectdb

// generic aggreation attributes
type (
	UserID struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
	CustomVal struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
	JobState struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
	WorkspaceID struct {
		Name string `objectbox:"unique"`
		Id   uint64
	}
	// ErrorCode string
)
