package httputil

import "net/http"

// RetriableStatus returns true if the HTTP status code should be retried.
//
//	We consider retriable status code:
//		* 5xx - all server errors
//		* 408 - request timeout
//		* 429 - too many requests.
func RetriableStatus(statusCode int) bool {
	// 1xx, 2xx, 3xx -- we assume no error and thus no retry should happen
	if statusCode < 400 {
		return false
	}
	// 5xx
	if statusCode >= 500 {
		return true
	}

	// 4xx codes:
	switch statusCode {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return true
	default:
		return false
	}
}
