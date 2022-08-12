package transformer_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	rtTf "github.com/rudderlabs/rudder-server/router/transformer"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// enum for expected Body Types
type expectedBodyTypes string

const (
	JSON = "json"
	STR  = "str"
)

type TcExpected struct {
	// The response body expected from transformer.ProxyRequest
	Body string
	// The expected status code from transformer.ProxyRequest
	StCode      int
	ContentType string
	// The body type we'd get after ProxyRequest method
	BodyType expectedBodyTypes
}

type TcProxy struct {
	// The expected response body from proxy endpoint in transformer
	Response string
	// The response status code from proxy endpoint in transformer
	StCode int
	// The delay in response from proxy endpoint in transformer
	TimeoutValue time.Duration
}
type ProxyContextTc struct {
	Timeout time.Duration
	Cancel  bool
}
type ProxyResponseTc struct {
	// The test-case name for better understanding of what we're trying to do!
	name     string
	destName string
	Expected TcExpected
	Proxy    TcProxy
	// The timeout we have set from router
	// Router will have the timeout of rtTimeout + <timeout_at_router_transform>
	// For http client timeout scenarios, we need to have a proxyTimeout which is > rtTimeout + <timeout_at_router_transform>
	rtTimeout time.Duration
	// Transformed response that needs to be sent to destination
	postParametersT integrations.PostParametersT
	context         ProxyContextTc
}

// error response from destination in transformer proxy
var (
	proxyResponseTcs = []ProxyResponseTc{
		{
			name:     "should pass for good_dest",
			destName: "good_dest",
			Expected: TcExpected{
				StCode:      http.StatusOK,
				Body:        `{"status": 200, "message": "", "destinationResponse":"good_dest"}`,
				ContentType: "application/json",
				BodyType:    JSON,
			},
			Proxy: TcProxy{
				StCode:       http.StatusOK,
				Response:     `{"output": {"status": 200, "message": "", "destinationResponse":"good_dest"}}`,
				TimeoutValue: 0,
			},
			rtTimeout: 10 * time.Millisecond,
			postParametersT: integrations.PostParametersT{
				Type:          "REST",
				URL:           "http://www.good_dest.domain.com",
				RequestMethod: http.MethodPost,
				QueryParams:   map[string]interface{}{},
				Body: map[string]interface{}{
					"JSON": map[string]interface{}{
						"key_1": "val_1",
						"key_2": "val_2",
					},
					"FORM":       map[string]interface{}{},
					"JSON_ARRAY": map[string]interface{}{},
					"XML":        map[string]interface{}{},
				},
				Files: map[string]interface{}{},
			},
		},
		{
			name:     "should throw timeout exception as the timeout in http.client is lower than proxy",
			destName: "good_dest_1",
			Expected: TcExpected{
				StCode:      http.StatusGatewayTimeout,
				Body:        `Post "%s/v0/destinations/good_dest_1/proxy": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`,
				ContentType: "text/plain; charset=utf-8",
				BodyType:    STR,
			},
			Proxy: TcProxy{
				StCode:       http.StatusOK,
				Response:     `{"output": {"status": 200, "message": "", "destinationResponse":"good_dest_1"}}`,
				TimeoutValue: time.Duration(1.2 * 1e9),
			},
			rtTimeout: 8 * time.Millisecond,
			postParametersT: integrations.PostParametersT{
				Type:          "REST",
				URL:           "http://www.good_dest_1.domain.com",
				RequestMethod: http.MethodPost,
				QueryParams:   map[string]interface{}{},
				Body: map[string]interface{}{
					"JSON": map[string]interface{}{
						"key_1": "val_1",
						"key_2": "val_2",
					},
					"FORM":       map[string]interface{}{},
					"JSON_ARRAY": map[string]interface{}{},
					"XML":        map[string]interface{}{},
				},
				Files: map[string]interface{}{},
			},
		},
		{
			name:     "should throw timeout exception due to context getting timedout",
			destName: "ctx_timeout_dest",
			Expected: TcExpected{
				StCode:      http.StatusGatewayTimeout,
				Body:        `Post "%s/v0/destinations/ctx_timeout_dest/proxy": context deadline exceeded`,
				ContentType: "text/plain; charset=utf-8",
				BodyType:    STR,
			},
			Proxy: TcProxy{
				StCode:       http.StatusOK,
				Response:     `{"output": {"status": 200, "message": "", "destinationResponse":"ctx_timeout_dest"}}`,
				TimeoutValue: 4 * time.Millisecond,
			},
			context: ProxyContextTc{
				Timeout: 2 * time.Millisecond,
			},
			postParametersT: integrations.PostParametersT{
				Type:          "REST",
				URL:           "http://www.ctx_timeout_dest.domain.com",
				RequestMethod: http.MethodPost,
				QueryParams:   map[string]interface{}{},
				Body: map[string]interface{}{
					"JSON": map[string]interface{}{
						"key_1": "val_1",
						"key_2": "val_2",
					},
					"FORM":       map[string]interface{}{},
					"JSON_ARRAY": map[string]interface{}{},
					"XML":        map[string]interface{}{},
				},
				Files: map[string]interface{}{},
			},
		},
		{
			name:     "should throw timeout exception due to context getting cancelled immediately",
			destName: "ctx_cancel_dest",
			Expected: TcExpected{
				StCode:      http.StatusInternalServerError,
				Body:        `Post "%s/v0/destinations/ctx_cancel_dest/proxy": context canceled`,
				ContentType: "text/plain; charset=utf-8",
				BodyType:    STR,
			},
			Proxy: TcProxy{
				StCode:   http.StatusOK,
				Response: `{"output": {"status": 200, "message": "", "destinationResponse":"ctx_cancel_dest"}}`,
			},
			context: ProxyContextTc{
				Cancel: true,
			},
			postParametersT: integrations.PostParametersT{
				Type:          "REST",
				URL:           "http://www.ctx_timeout_dest.domain.com",
				RequestMethod: http.MethodPost,
				QueryParams:   map[string]interface{}{},
				Body: map[string]interface{}{
					"JSON": map[string]interface{}{
						"key_1": "val_1",
						"key_2": "val_2",
					},
					"FORM":       map[string]interface{}{},
					"JSON_ARRAY": map[string]interface{}{},
					"XML":        map[string]interface{}{},
				},
				Files: map[string]interface{}{},
			},
		},
		{
			name:     "should fail with not found error for not_found_dest",
			destName: "not_found_dest",
			Expected: TcExpected{
				StCode:      http.StatusNotFound,
				Body:        `post "%s/v0/destinations/not_found_dest/proxy" not found`,
				ContentType: "text/plain; charset=utf-8",
				BodyType:    STR,
			},
			Proxy: TcProxy{
				StCode:   http.StatusNotFound,
				Response: `Not Found`,
			},
			rtTimeout: 10 * time.Millisecond,
			postParametersT: integrations.PostParametersT{
				Type:          "REST",
				URL:           "http://www.not_found_dest.domain.com",
				RequestMethod: http.MethodPost,
				QueryParams:   map[string]interface{}{},
				Body: map[string]interface{}{
					"JSON": map[string]interface{}{
						"key_1": "val_1",
						"key_2": "val_2",
					},
					"FORM":       map[string]interface{}{},
					"JSON_ARRAY": map[string]interface{}{},
					"XML":        map[string]interface{}{},
				},
				Files: map[string]interface{}{},
			},
		},
	}
)

func Initialization() {
	config.Load()
	stats.Init()
	stats.Setup()
	rtTf.Init()
	logger.Init()
}

func TestProxyRequest(t *testing.T) {
	httpClientTimeout := 1 * time.Second
	Initialization()

	for _, tc := range proxyResponseTcs {
		// skip tests for the mentioned destinations
		if tc.destName == "not_found_dest" {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(mockProxyHandler(&tc))
			defer srv.Close()
			tr := rtTf.NewTransformer()
			// Logic for executing test-cases not manipulating test-cases
			if tc.rtTimeout.Milliseconds() > 0 {
				tr.Setup(tc.rtTimeout, httpClientTimeout)
			} else {
				// Just a default value
				tr.Setup(2*time.Millisecond, httpClientTimeout)
			}
			// Logic to include context timing out
			ctx := context.TODO()
			var cancelFunc context.CancelFunc
			if tc.context.Timeout.Milliseconds() > 0 {
				ctx, cancelFunc = context.WithTimeout(context.TODO(), tc.context.Timeout)
				defer cancelFunc()
			} else if tc.context.Cancel {
				ctx, cancelFunc = context.WithCancel(context.TODO())
				cancelFunc()
			}

			reqParams := &rtTf.ProxyRequestParams{
				ResponseData: tc.postParametersT,
				DestName:     tc.destName,
				BaseUrl:      srv.URL,
			}
			stCd, resp, contentType := tr.ProxyRequest(ctx, reqParams)

			assert.Equal(t, tc.Expected.StCode, stCd)
			require.Equal(t, tc.Expected.ContentType, contentType)

			switch tc.Expected.BodyType {
			case JSON:
				require.JSONEq(t, tc.Expected.Body, resp)
			case STR:
				expectedBodyStr := fmt.Sprintf(tc.Expected.Body, srv.URL)
				require.Equal(t, expectedBodyStr, resp)
			}
		})
	}

	// Not found case
	tc := proxyResponseTcs[4]
	t.Run(tc.name, func(t *testing.T) {
		srv := httptest.NewServer(mockProxyHandler(&tc))
		defer srv.Close()

		tr := rtTf.NewTransformer()
		tr.Setup(tc.rtTimeout, httpClientTimeout)
		ctx := context.TODO()
		reqParams := &rtTf.ProxyRequestParams{
			ResponseData: tc.postParametersT,
			DestName:     "not_found_dest",
			BaseUrl:      srv.URL,
		}
		stCd, resp, contentType := tr.ProxyRequest(ctx, reqParams)
		assert.Equal(t, tc.Expected.StCode, stCd)
		require.Equal(t, tc.Expected.ContentType, contentType)
		expectedBodyStr := fmt.Sprintf(tc.Expected.Body, srv.URL)
		require.Equal(t, expectedBodyStr, resp)
	})
}

// A kind of mock for transformer proxy endpoint in transformer
func mockProxyHandler(tc *ProxyResponseTc) *mux.Router {
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/v0/destinations/{destName}/proxy", func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		_, ok := vars["destName"]
		if !ok {
			// This case wouldn't occur I guess
			http.Error(w, "Wrong url being sent", http.StatusInternalServerError)
			return
		}

		// sleep is being used to mimic the waiting in actual transformer response
		if tc.Proxy.TimeoutValue.Milliseconds() > 0 {
			time.Sleep(tc.Proxy.TimeoutValue)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(tc.Proxy.StCode)
		// Lint error fix
		checkAndSendResponse(&w, []byte(tc.Proxy.Response))
	})
	return srvMux
}

func checkAndSendResponse(w *http.ResponseWriter, resp []byte) {
	_, err := (*w).Write(resp)
	if err != nil {
		http.Error(*w, fmt.Sprintf("Provided response is faulty, please check it. Err: %v", err.Error()), http.StatusInternalServerError)
	}
}
