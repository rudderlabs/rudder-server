package ro

// Memoize stores the execution result of the provided function in-memory during the first call and uses it as a return value for subsequent calls
func Memoize[R any](f func() R) func() R {
	var result R
	var called bool
	return func() R {
		if called {
			return result
		}
		result = f()
		called = true
		return result
	}
}
