package httputil

import "net/http"

// RetriableStatus returns true if the HTTP status code should be retried.
// 4xx codes are considered non-retiable except for 408 - request timeout and 429 - too many requests.
func RetriableStatus(statusCode int) bool {
	// 2xx, 3xx
	if statusCode < 400 {
		return true
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
