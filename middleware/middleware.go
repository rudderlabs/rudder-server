package middleware

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

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

func StatMiddleware(ctx context.Context) func(http.Handler) http.Handler {
	var concurrentRequests int32
	activeClientCount := stats.NewStat("gateway.concurrent_requests_count", stats.GaugeType)
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

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			latencyStat := stats.DefaultStats.NewSampledTaggedStat("gateway.response_time", stats.TimerType, map[string]string{"reqType": r.URL.Path})
			latencyStat.Start()
			defer latencyStat.End()

			atomic.AddInt32(&concurrentRequests, 1)
			defer atomic.AddInt32(&concurrentRequests, -1)

			next.ServeHTTP(w, r)
		})
	}
}

// ContentType currently sets the content-type only for eventSchemas, health responses.
// Note : responses via http.Error aren't affected. They default to text/plain
func ContentType() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/schemas") || strings.HasPrefix(r.URL.Path, "/health") {
				w.Header().Add("Content-Type", "application/json; charset=utf-8")
			}
			next.ServeHTTP(w, r)
		})
	}
}
