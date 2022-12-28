package transformer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/config"
	mock_stats "github.com/rudderlabs/rudder-server/mocks/services/stats"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/types"
	"github.com/rudderlabs/rudder-server/utils/logger"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProxyRequest(t *testing.T) {
	initMocks(t)

	httpClientTimeout := time.Second

	// enum for expected Body Types
	type expectedBodyType string

	const (
		JSON expectedBodyType = "json"
		STR  expectedBodyType = "str"
	)

	type expectedResponse struct {
		// The response body expected from transformer.ProxyRequest
		body string
		// The expected status code from transformer.ProxyRequest
		code        int
		contentType string
		// The body type we'd get after ProxyRequest method
		bodyType expectedBodyType
	}

	type proxyConfig struct {
		// The delay in response from proxy endpoint in transformer
		timeout time.Duration
		// The expected response body from proxy endpoint in transformer
		response string
		// The response status code from proxy endpoint in transformer
		code int
	}

	type proxyContext struct {
		timeout time.Duration
		cancel  bool
	}

	type testCase struct {
		// The test-case name for better understanding of what we're trying to do!
		name     string
		destName string
		expected expectedResponse
		proxy    proxyConfig
		// The timeout we have set from router
		// Router will have the timeout of rtTimeout + <timeout_at_router_transform>
		// For http client timeout scenarios, we need to have a proxyTimeout which is > rtTimeout + <timeout_at_router_transform>
		rtTimeout time.Duration
		// Transformed response that needs to be sent to destination
		postParameters ProxyRequestPayload
		context        proxyContext
	}

	// error response from destination in transformer proxy

	testCases := []testCase{
		{
			name:     "should pass for good_dest",
			destName: "good_dest",
			expected: expectedResponse{
				code:        http.StatusOK,
				body:        `{"status": 200, "message": "", "destinationResponse":"good_dest"}`,
				contentType: "application/json",
				bodyType:    JSON,
			},
			proxy: proxyConfig{
				code:     http.StatusOK,
				response: `{"output": {"status": 200, "message": "", "destinationResponse":"good_dest"}}`,
				timeout:  0,
			},
			rtTimeout: 10 * time.Millisecond,
			postParameters: ProxyRequestPayload{
				PostParametersT: integrations.PostParametersT{
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
		},
		{
			name:     "should throw timeout exception as the timeout in http.client is lower than proxy",
			destName: "good_dest_1",
			expected: expectedResponse{
				code:        http.StatusGatewayTimeout,
				body:        `Post "%s/v0/destinations/good_dest_1/proxy": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`,
				contentType: "text/plain; charset=utf-8",
				bodyType:    STR,
			},
			proxy: proxyConfig{
				code:     http.StatusOK,
				response: `{"output": {"status": 200, "message": "", "destinationResponse":"good_dest_1"}}`,
				timeout:  time.Duration(1.2 * 1e9),
			},
			rtTimeout: 8 * time.Millisecond,
			postParameters: ProxyRequestPayload{
				PostParametersT: integrations.PostParametersT{
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
		},
		{
			name:     "should throw timeout exception due to context getting timedout",
			destName: "ctx_timeout_dest",
			expected: expectedResponse{
				code:        http.StatusGatewayTimeout,
				body:        `Post "%s/v0/destinations/ctx_timeout_dest/proxy": context deadline exceeded`,
				contentType: "text/plain; charset=utf-8",
				bodyType:    STR,
			},
			proxy: proxyConfig{
				code:     http.StatusOK,
				response: `{"output": {"status": 200, "message": "", "destinationResponse":"ctx_timeout_dest"}}`,
				timeout:  4 * time.Millisecond,
			},
			context: proxyContext{
				timeout: 2 * time.Millisecond,
			},
			postParameters: ProxyRequestPayload{
				PostParametersT: integrations.PostParametersT{
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
		},
		{
			name:     "should throw timeout exception due to context getting cancelled immediately",
			destName: "ctx_cancel_dest",
			expected: expectedResponse{
				code:        http.StatusInternalServerError,
				body:        `Post "%s/v0/destinations/ctx_cancel_dest/proxy": context canceled`,
				contentType: "text/plain; charset=utf-8",
				bodyType:    STR,
			},
			proxy: proxyConfig{
				code:     http.StatusOK,
				response: `{"output": {"status": 200, "message": "", "destinationResponse":"ctx_cancel_dest"}}`,
			},
			context: proxyContext{
				cancel: true,
			},
			postParameters: ProxyRequestPayload{
				PostParametersT: integrations.PostParametersT{
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
		},
		{
			name:     "should fail with not found error for not_found_dest",
			destName: "not_found_dest",
			expected: expectedResponse{
				code:        http.StatusNotFound,
				body:        `post "%s/v0/destinations/not_found_dest/proxy" not found`,
				contentType: "text/plain; charset=utf-8",
				bodyType:    STR,
			},
			proxy: proxyConfig{
				code:     http.StatusNotFound,
				response: `Not Found`,
			},
			rtTimeout: 10 * time.Millisecond,
			postParameters: ProxyRequestPayload{
				PostParametersT: integrations.PostParametersT{
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
		},
	}

	for _, tc := range testCases {
		// skip tests for the mentioned destinations
		if tc.destName == "not_found_dest" {
			t.Run(tc.name, func(t *testing.T) {
				srv := httptest.NewServer(mockProxyHandler(tc.proxy.timeout, tc.proxy.code, tc.proxy.response))
				defer srv.Close()

				tr := NewTransformer(tc.rtTimeout, httpClientTimeout)
				ctx := context.TODO()
				reqParams := &ProxyRequestParams{
					ResponseData: tc.postParameters,
					DestName:     "not_found_dest",
					BaseUrl:      srv.URL,
				}
				stCd, resp, contentType := tr.ProxyRequest(ctx, reqParams)
				assert.Equal(t, tc.expected.code, stCd)
				require.Equal(t, tc.expected.contentType, contentType)
				expectedBodyStr := fmt.Sprintf(tc.expected.body, srv.URL)
				require.Equal(t, expectedBodyStr, resp)
			})
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(mockProxyHandler(tc.proxy.timeout, tc.proxy.code, tc.proxy.response))
			defer srv.Close()
			var tr Transformer
			// Logic for executing test-cases not manipulating test-cases
			if tc.rtTimeout.Milliseconds() > 0 {
				tr = NewTransformer(tc.rtTimeout, httpClientTimeout)
			} else {
				// Just a default value
				tr = NewTransformer(2*time.Millisecond, httpClientTimeout)
			}
			// Logic to include context timing out
			ctx := context.TODO()
			var cancelFunc context.CancelFunc
			if tc.context.timeout.Milliseconds() > 0 {
				ctx, cancelFunc = context.WithTimeout(context.TODO(), tc.context.timeout)
				defer cancelFunc()
			} else if tc.context.cancel {
				ctx, cancelFunc = context.WithCancel(context.TODO())
				cancelFunc()
			}

			reqParams := &ProxyRequestParams{
				ResponseData: tc.postParameters,
				DestName:     tc.destName,
				BaseUrl:      srv.URL,
			}
			stCd, resp, contentType := tr.ProxyRequest(ctx, reqParams)

			assert.Equal(t, tc.expected.code, stCd)
			require.Equal(t, tc.expected.contentType, contentType)

			switch tc.expected.bodyType {
			case JSON:
				require.JSONEq(t, tc.expected.body, resp)
			case STR:
				expectedBodyStr := fmt.Sprintf(tc.expected.body, srv.URL)
				require.Equal(t, expectedBodyStr, resp)
			}
		})
	}
}

// A kind of mock for transformer proxy endpoint in transformer
func mockProxyHandler(timeout time.Duration, code int, response string) *mux.Router {
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
		if timeout > 0 {
			time.Sleep(timeout)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		// Lint error fix
		_, err := w.Write([]byte(response))
		if err != nil {
			http.Error(w, fmt.Sprintf("Provided response is faulty, please check it. Err: %v", err.Error()), http.StatusInternalServerError)
		}
	})
	return srvMux
}

func TestTransformNoValidationErrors(t *testing.T) {
	initMocks(t)
	config.Reset()
	pkgLogger = logger.NOP
	expectedTransformerResponse := []types.DestinationJobT{
		{JobMetadataArray: []types.JobMetadataT{{JobID: 1}}, StatusCode: http.StatusOK},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 2}}, StatusCode: http.StatusOK},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 3}}, StatusCode: http.StatusOK},
	}
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add(apiVersionHeader, strconv.Itoa(utilTypes.SupportedTransformerApiVersion))
		b, err := json.Marshal(expectedTransformerResponse)
		require.NoError(t, err)
		_, err = w.Write(b)
		require.NoError(t, err)
	}))
	defer svr.Close()
	t.Setenv("DEST_TRANSFORM_URL", svr.URL)
	tr := NewTransformer(time.Minute, time.Minute)

	transformMessage := types.TransformMessageT{
		Data: []types.RouterJobT{
			{JobMetadata: types.JobMetadataT{JobID: 1}},
			{JobMetadata: types.JobMetadataT{JobID: 2}},
			{JobMetadata: types.JobMetadataT{JobID: 3}},
		},
	}
	transformerResponse := tr.Transform(BATCH, &transformMessage)
	require.NotNil(t, transformerResponse)
	require.Equal(t, expectedTransformerResponse, transformerResponse)
}

func TestTransformValidationUnmarshallingError(t *testing.T) {
	initMocks(t)
	config.Reset()
	pkgLogger = logger.NOP
	expectedErrorTxt := "Transformer returned invalid response: invalid json for input:"
	expectedTransformerResponse := []types.DestinationJobT{
		{JobMetadataArray: []types.JobMetadataT{{JobID: 1}}, StatusCode: http.StatusInternalServerError, Error: expectedErrorTxt},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 2}}, StatusCode: http.StatusInternalServerError, Error: expectedErrorTxt},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 3}}, StatusCode: http.StatusInternalServerError, Error: expectedErrorTxt},
	}
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add(apiVersionHeader, strconv.Itoa(utilTypes.SupportedTransformerApiVersion))
		_, err := w.Write([]byte("invalid json"))
		require.NoError(t, err)
	}))
	defer svr.Close()
	t.Setenv("DEST_TRANSFORM_URL", svr.URL)
	tr := NewTransformer(time.Minute, time.Minute)

	transformMessage := types.TransformMessageT{
		Data: []types.RouterJobT{
			{JobMetadata: types.JobMetadataT{JobID: 1}},
			{JobMetadata: types.JobMetadataT{JobID: 2}},
			{JobMetadata: types.JobMetadataT{JobID: 3}},
		},
	}
	transformerResponse := tr.Transform(BATCH, &transformMessage)
	normalizeErrors(transformerResponse, expectedErrorTxt)
	require.NotNil(t, transformerResponse)
	require.Equal(t, expectedTransformerResponse, transformerResponse)
}

func TestTransformValidationInOutMismatchError(t *testing.T) {
	initMocks(t)
	config.Reset()
	pkgLogger = logger.NOP
	expectedErrorTxt := "Transformer returned invalid output size: 4 for input size: 3"
	expectedTransformerResponse := []types.DestinationJobT{
		{JobMetadataArray: []types.JobMetadataT{{JobID: 1}}, StatusCode: http.StatusInternalServerError, Error: expectedErrorTxt},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 2}}, StatusCode: http.StatusInternalServerError, Error: expectedErrorTxt},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 3}}, StatusCode: http.StatusInternalServerError, Error: expectedErrorTxt},
	}
	serverResponse := []types.DestinationJobT{
		{JobMetadataArray: []types.JobMetadataT{{JobID: 1}}},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 2}}},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 3}}},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 3}}},
	}
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add(apiVersionHeader, strconv.Itoa(utilTypes.SupportedTransformerApiVersion))
		b, err := json.Marshal(serverResponse)
		require.NoError(t, err)
		_, err = w.Write(b)
		require.NoError(t, err)
	}))
	defer svr.Close()
	t.Setenv("DEST_TRANSFORM_URL", svr.URL)
	tr := NewTransformer(time.Minute, time.Minute)

	transformMessage := types.TransformMessageT{
		Data: []types.RouterJobT{
			{JobMetadata: types.JobMetadataT{JobID: 1}},
			{JobMetadata: types.JobMetadataT{JobID: 2}},
			{JobMetadata: types.JobMetadataT{JobID: 3}},
		},
	}
	transformerResponse := tr.Transform(BATCH, &transformMessage)
	normalizeErrors(transformerResponse, expectedErrorTxt)
	require.NotNil(t, transformerResponse)
	require.Equal(t, expectedTransformerResponse, transformerResponse)
}

func TestTransformValidationJobIDMismatchError(t *testing.T) {
	initMocks(t)
	config.Reset()
	pkgLogger = logger.NOP
	expectedErrorTxt := "Transformer returned invalid jobIDs: [4]"
	expectedTransformerResponse := []types.DestinationJobT{
		{JobMetadataArray: []types.JobMetadataT{{JobID: 1}}, StatusCode: http.StatusInternalServerError, Error: expectedErrorTxt},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 2}}, StatusCode: http.StatusInternalServerError, Error: expectedErrorTxt},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 3}}, StatusCode: http.StatusInternalServerError, Error: expectedErrorTxt},
	}
	serverResponse := []types.DestinationJobT{
		{JobMetadataArray: []types.JobMetadataT{{JobID: 1}}},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 4}}},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 2}}},
	}
	svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add(apiVersionHeader, strconv.Itoa(utilTypes.SupportedTransformerApiVersion))
		b, err := json.Marshal(serverResponse)
		require.NoError(t, err)
		_, err = w.Write(b)
		require.NoError(t, err)
	}))
	defer svr.Close()
	t.Setenv("DEST_TRANSFORM_URL", svr.URL)
	tr := NewTransformer(time.Minute, time.Minute)

	transformMessage := types.TransformMessageT{
		Data: []types.RouterJobT{
			{JobMetadata: types.JobMetadataT{JobID: 1}},
			{JobMetadata: types.JobMetadataT{JobID: 2}},
			{JobMetadata: types.JobMetadataT{JobID: 3}},
		},
	}
	transformerResponse := tr.Transform(BATCH, &transformMessage)
	normalizeErrors(transformerResponse, expectedErrorTxt)
	require.NotNil(t, transformerResponse)
	require.Equal(t, expectedTransformerResponse, transformerResponse)
}

func initMocks(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockRudderStats := mock_stats.NewMockMeasurement(ctrl)

	mockRudderStats.EXPECT().SendTiming(gomock.Any()).AnyTimes()
	mockRudderStats.EXPECT().Increment().AnyTimes()

	pkgLogger = logger.NOP
}

func normalizeErrors(transformerResponse []types.DestinationJobT, prefix string) {
	for i := range transformerResponse {
		job := &transformerResponse[i]
		if strings.HasPrefix(job.Error, prefix) {
			job.Error = prefix
		}
	}
}
