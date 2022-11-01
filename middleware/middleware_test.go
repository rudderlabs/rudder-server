package middleware_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/middleware"
	mock_stats "github.com/rudderlabs/rudder-server/mocks/services/stats"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/stretchr/testify/require"
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

func TestStatsMiddleware(t *testing.T) {
	component := "test"
	testCase := func(expectedStatusCode int, pathTemplate, requestPath, expectedReqType, expectedMethod string) func(t *testing.T) {
		return func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockStats := mock_stats.NewMockStats(ctrl)
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(expectedStatusCode)
			})

			measurement := mock_stats.NewMockMeasurement(ctrl)
			mockStats.EXPECT().NewStat(fmt.Sprintf("%s.concurrent_requests_count", component), stats.GaugeType).Return(measurement).Times(1)
			mockStats.EXPECT().NewSampledTaggedStat(fmt.Sprintf("%s.response_time", component), stats.TimerType,
				map[string]string{
					"reqType": expectedReqType,
					"method":  expectedMethod,
					"code":    strconv.Itoa(expectedStatusCode),
				}).Return(measurement).Times(1)
			measurement.EXPECT().Since(gomock.Any()).Times(1)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			router := mux.NewRouter()
			router.Use(
				middleware.StatMiddleware(ctx, router, mockStats, component),
			)
			router.HandleFunc(pathTemplate, handler).Methods(expectedMethod)

			response := httptest.NewRecorder()
			request := httptest.NewRequest("GET", "http://example.com"+requestPath, http.NoBody)
			router.ServeHTTP(response, request)
			require.Equal(t, expectedStatusCode, response.Code)
		}
	}

	t.Run("template with param in path", testCase(http.StatusNotFound, "/v1/{param}", "/v1/abc", "/v1/{param}", "GET"))

	t.Run("template without param in path", testCase(http.StatusNotFound, "/v1/some-other/key", "/v1/some-other/key", "/v1/some-other/key", "GET"))
}
