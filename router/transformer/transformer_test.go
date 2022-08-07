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
	"github.com/tidwall/gjson"
)

type ProxyContextTc struct {
	Timeout time.Duration
	Cancel  bool
}
type ProxyResponseTc struct {
	// The test-case name for better understanding of what we're trying to do!
	name string
	// The response body expected from transformer.ProxyRequest
	ExpectedBody string
	// The expected response body from proxy endpoint in transformer
	proxyResponse string
	// The expected status code from transformer.ProxyRequest
	ExpectedStCode int
	// The response status code from proxy endpoint in transformer
	proxyStatusCode int
	// The delay in response from proxy endpoint in transformer
	proxyTimeoutValue time.Duration
	// The timeout we have set from router
	// Router will have the timeout of 2 * rtTimeout
	// For http client timeout scenarios, we need to have a proxyTimeout which is > 2 * rtTimeout
	rtTimeout time.Duration
	// Transformed response that needs to be sent to destination
	postParametersT integrations.PostParametersT
	context         ProxyContextTc
}

// error response from destination in transformer proxy
var proxyResponseTcs = map[string]ProxyResponseTc{
	"good_dest": {
		name:              "should pass for good_dest",
		ExpectedBody:      `{"authErrorCategory": "", "message": "", "destinationResponse":"good_dest"}`,
		proxyResponse:     `{"output": {"status": 200, "message": "", "destinationResponse":"good_dest"}}`,
		ExpectedStCode:    http.StatusOK,
		proxyStatusCode:   http.StatusOK,
		proxyTimeoutValue: 0,
		rtTimeout:         10 * time.Millisecond,
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
	"good_dest_1": {
		name:              "should throw timeout exception as the timeout in http.client is lower than proxy",
		ExpectedStCode:    http.StatusGatewayTimeout,
		ExpectedBody:      `Post "%s/v0/destinations/good_dest_1/proxy": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`,
		proxyStatusCode:   http.StatusOK,
		proxyResponse:     `{"output": {"status": 200, "message": "", "destinationResponse":"good_dest_1"}}`,
		proxyTimeoutValue: 40 * time.Millisecond,
		rtTimeout:         18 * time.Millisecond,
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
	"ctx_timeout_dest": {
		name:              "should throw timeout exception due to context getting timedout",
		ExpectedStCode:    http.StatusGatewayTimeout,
		ExpectedBody:      `Post "%s/v0/destinations/ctx_timeout_dest/proxy": context deadline exceeded`,
		proxyStatusCode:   http.StatusOK,
		proxyResponse:     `{"output": {"status": 200, "message": "", "destinationResponse":"ctx_timeout_dest"}}`,
		proxyTimeoutValue: 4 * time.Millisecond,
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
	"ctx_cancel_dest": {
		name:            "should throw timeout exception due to context getting cancelled immediately",
		ExpectedStCode:  http.StatusInternalServerError,
		ExpectedBody:    `Post "%s/v0/destinations/ctx_cancel_dest/proxy": context canceled`,
		proxyStatusCode: http.StatusOK,
		proxyResponse:   `{"output": {"status": 200, "message": "", "destinationResponse":"ctx_cancel_dest"}}`,
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
}

func Initialization() {
	config.Load()
	stats.Init()
	stats.Setup()
	rtTf.Init()
	logger.Init()
}

func TestProxyRequest(t *testing.T) {
	Initialization()

	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/v0/destinations/{destName}/proxy", proxyHandlerFunc)
	srv := httptest.NewServer(srvMux)
	defer srv.Close()

	for destName, tc := range proxyResponseTcs {
		t.Run(tc.name, func(t *testing.T) {
			tr := rtTf.NewTransformer()
			// Logic for executing test-cases not manipulating test-cases
			if tc.rtTimeout.Milliseconds() > 0 {
				tr.Setup(tc.rtTimeout)
			} else {
				// Just a default value
				tr.Setup(2 * time.Millisecond)
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

			stCd, resp := tr.ProxyRequest(ctx, tc.postParametersT, destName, int64(0), srv.URL)

			assert.Equal(t, tc.ExpectedStCode, stCd)
			if gjson.GetBytes([]byte(resp), "message").Raw != "" {
				require.JSONEq(t, tc.ExpectedBody, resp)
			} else if tc.ExpectedStCode == http.StatusGatewayTimeout {
				expectedBodyStr := fmt.Sprintf(tc.ExpectedBody, srv.URL)
				require.Equal(t, expectedBodyStr, resp)
			}
		})
	}
}

// A kind of mock for transformer proxy endpoint in transformer
func proxyHandlerFunc(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	destName, ok := vars["destName"]
	if !ok {
		// Modify this case ?
		panic(fmt.Errorf("Wrong url being sent"))
	}
	tc := proxyResponseTcs[destName]
	// sleep is being used to mimic the waiting in actual transformer response
	if tc.proxyTimeoutValue.Milliseconds() > 0 {
		time.Sleep(tc.proxyTimeoutValue)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(tc.proxyStatusCode)
	// Lint error fix
	_, err := w.Write([]byte(tc.proxyResponse))
	if err != nil {
		// Is there a better way to handle this ?
		panic(fmt.Errorf("Provided response is faulty, please check it. Err: %v", err))
	}
}
