package httputil

import (
	"io"
	"net/http"
)

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

// CloseResponse closes the response's body. But reads at least some of the body so if it's
// small the underlying TCP connection will be re-used. No need to check for errors: if it
// fails, the Transport won't reuse it anyway.
func CloseResponse(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		const maxBodySlurpSize = 2 << 10 // 2KB
		_, _ = io.CopyN(io.Discard, resp.Body, maxBodySlurpSize)
		_ = resp.Body.Close()
	}
}
