package middleware

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/services/stats"
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

func StatMiddleware(ctx context.Context, router *mux.Router) func(http.Handler) http.Handler {
	var concurrentRequests int32
	activeClientCount := stats.Default.NewStat("gateway.concurrent_requests_count", stats.GaugeType)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				activeClientCount.Gauge(atomic.LoadInt32(&concurrentRequests))
			}
		}
	}()

	// getPath retrieves the path from the request.
	// The matched route's template is used if a match is found,
	// otherwise the request's URL path is used instead.
	getPath := func(r *http.Request) string {
		var match mux.RouteMatch
		if router.Match(r, &match) {
			if path, err := match.Route.GetPathTemplate(); err != nil {
				return path
			}
		}
		return r.URL.Path
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := getPath(r)
			latencyStat := stats.Default.NewSampledTaggedStat("gateway.response_time", stats.TimerType, map[string]string{"reqType": path, "method": r.Method})
			latencyStat.Start()
			defer latencyStat.End()

			atomic.AddInt32(&concurrentRequests, 1)
			defer atomic.AddInt32(&concurrentRequests, -1)

			next.ServeHTTP(w, r)
		})
	}
}
