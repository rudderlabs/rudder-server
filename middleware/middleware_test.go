package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/middleware"
)

func TestMaxConcurrentRequestsMiddleware(t *testing.T) {
	// create a handler to use as "next" which will verify the request
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Second * 1)
		w.WriteHeader(http.StatusOK)
	})

	tests := []struct {
		name                  string
		maxConcurrentRequests int
		totalReq              int
	}{
		{
			name:                  "max concurrent request: 2",
			maxConcurrentRequests: 2,
			totalReq:              10,
		},
		{
			name:                  "max concurrent request: 1",
			maxConcurrentRequests: 1,
			totalReq:              10,
		},
		{
			name:                  "max concurrent request: 10",
			maxConcurrentRequests: 10,
			totalReq:              10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			maxConMiddleware := middleware.LimitConcurrentRequests(tt.maxConcurrentRequests)
			handlerToTest := maxConMiddleware(nextHandler)
			var wg sync.WaitGroup
			var resp200, resp503, randomResp int32

			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					respRecorder := httptest.NewRecorder()
					req := httptest.NewRequest("GET", "http://testing", nil)

					handlerToTest.ServeHTTP(respRecorder, req)
					if respRecorder.Code == 200 {
						atomic.AddInt32(&resp200, 1)
					} else if respRecorder.Code == 503 {
						atomic.AddInt32(&resp503, 1)
					} else {
						atomic.AddInt32(&randomResp, 1)
					}
				}()
			}

			wg.Wait()

			require.Equal(t, tt.maxConcurrentRequests, int(resp200), "actual 200 resp different than expected.")
			require.Equal(t, tt.totalReq-tt.maxConcurrentRequests, int(resp503), "actual 503 resp different than expected.")
			require.Equal(t, 0, int(randomResp), "received unexpected response")
		})
	}
}
