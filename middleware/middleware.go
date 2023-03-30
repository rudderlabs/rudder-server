package middleware

import (
	"net/http"
)

// this is to make sure that we don't have more than `maxClient` in-memory at any point of time. As, having more http client than `maxClient`
// may lead to gateway OOM kill.
func LimitConcurrentRequests(maxRequests int) func(http.Handler) http.Handler {
	requests := make(chan struct{}, maxRequests)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if maxRequests != 0 {
				select {
				case requests <- struct{}{}:
					defer func() {
						<-requests
					}()
				default:
					http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
