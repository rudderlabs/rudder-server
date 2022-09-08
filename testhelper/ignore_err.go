package testhelper

// IgnoreErr is a helper function to ignore errors in tests, useful when closing bodies for example
func IgnoreErr(f func() error) {
	_ = f()
}
