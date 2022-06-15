//go:generate go run github.com/objectbox/objectbox-go/cmd/objectbox-gogen
package objectdb

// generic aggreation attributes
type (
	UserID struct {
		Name        string `objectbox:"unique"`
		Id          uint64
		WorkspaceID *WorkspaceID `objectbox:"index:link"`
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

func (box *Box) GetOrCreateCustomVal(customVal string) (*CustomVal, error) {
	customValBox := box.customValBox
	customValObj, err := customValBox.Query(CustomVal_.Name.Equals(customVal, true)).Find()
	if err != nil {
		return nil, err
	}
	if len(customValObj) > 0 {
		return customValObj[0], nil
	}
	newCustomVal := &CustomVal{
		Name: customVal,
	}
	_, putError := customValBox.Put(newCustomVal)
	if putError != nil {
		return nil, putError
	}
	CustomValMap[customVal] = newCustomVal
	return newCustomVal, nil
}
