package testhelper

//go:fix inline
func Ptr[T any](v T) *T {
	return new(v)
}
