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

type ProxyResponseTc struct {
	name              string
	ExpectedBody      string
	proxyResponse     string
	ExpectedStCode    int
	proxyStatusCode   int
	proxyTimeoutValue time.Duration
	rtTimeout         time.Duration
	postParametersT   integrations.PostParametersT
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
		rtTimeout:         20 * time.Millisecond,
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

			stCd, resp := tr.ProxyRequest(context.Background(), tc.postParametersT, destName, int64(0), srv.URL)
			// if tc.rtTimeout
			// transformer.Setup(tc.rtTimeout)
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

func proxyHandlerFunc(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	destName, ok := vars["destName"]
	if !ok {
		// TODO: Modify this case
		panic(fmt.Errorf("Wrong url being sent"))
	}
	tc := proxyResponseTcs[destName]
	// sleep is being used to mimic the waiting in actual transformer response
	if tc.proxyTimeoutValue.Milliseconds() > 0 {
		time.Sleep(tc.proxyTimeoutValue)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(tc.proxyStatusCode)
	w.Write([]byte(tc.proxyResponse))
}
