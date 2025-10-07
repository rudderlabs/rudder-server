package v2

import "fmt"

// Helper to construct a StatusCodeError
func NewStatusCodeError(code int, err error) StatusCodeError {
	return &statusCodeError{
		Code: code,
		Err:  err,
	}
}

// StatusCodeError wraps an error with an HTTP-like status code.
type StatusCodeError interface {
	StatusCode() int
	error
}

type statusCodeError struct {
	Code int
	Err  error
}

func (e *statusCodeError) Error() string {
	return fmt.Sprintf("status %d: %v", e.Code, e.Err)
}

func (e *statusCodeError) StatusCode() int {
	return e.Code
}

// Unwrap allows errors.Is and errors.As to work with the wrapped error.
func (e *statusCodeError) Unwrap() error {
	return e.Err
}
