package transformer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/mock_stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksBackendConfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/types"
	testutils "github.com/rudderlabs/rudder-server/utils/tests"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"

	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

type mockAdapter struct {
	url string
}

func (a *mockAdapter) getPayload(proxyReqParams *ProxyRequestParams) ([]byte, error) {
	return []byte(`{}`), nil
}

func (a *mockAdapter) getProxyURL(destType string) (string, error) {
	return url.JoinPath(a.url, "v0", "destinations", strings.ToLower(destType), "proxy")
}

func (a *mockAdapter) getResponse(response []byte, respCode int, metadata []ProxyRequestMetadata) (TransResponse, error) {
	return TransResponse{
		routerJobResponseCodes:       make(map[int64]int),
		routerJobResponseBodys:       make(map[int64]string),
		routerJobDontBatchDirectives: make(map[int64]bool),
		authErrorCategory:            "",
	}, nil
}

func TestProxyRequest(t *testing.T) {
	initMocks(t)

	httpClientTimeout := time.Second

	// enum for expected Body Types
	type expectedBodyType string

	const (
		JSON      expectedBodyType = "json"
		STR       expectedBodyType = "str"
		EXACT_STR expectedBodyType = "exact_str"
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
				Metadata: []ProxyRequestMetadata{
					{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
					},
				},
				DestinationConfig: map[string]interface{}{
					"key_1": "val_1",
					"key_2": "val_2",
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
				Metadata: []ProxyRequestMetadata{
					{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
					},
				},
				DestinationConfig: map[string]interface{}{
					"key_1": "val_1",
					"key_2": "val_2",
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
				Metadata: []ProxyRequestMetadata{
					{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
					},
				},
				DestinationConfig: map[string]interface{}{
					"key_1": "val_1",
					"key_2": "val_2",
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
				Metadata: []ProxyRequestMetadata{
					{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
					},
				},
				DestinationConfig: map[string]interface{}{
					"key_1": "val_1",
					"key_2": "val_2",
				},
			},
		},
		{
			name:     "should fail with not found error for not_found_dest",
			destName: "not_found_dest",
			expected: expectedResponse{
				code:        http.StatusInternalServerError,
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
				Metadata: []ProxyRequestMetadata{
					{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
					},
				},
				DestinationConfig: map[string]interface{}{
					"key_1": "val_1",
					"key_2": "val_2",
				},
			},
		},
		{
			name:     "should fail with no metadata found error",
			destName: "good_dest",
			expected: expectedResponse{
				code:        http.StatusBadRequest,
				body:        `Input metadata is empty`,
				contentType: "text/plain; charset=utf-8",
				bodyType:    EXACT_STR,
			},
			proxy: proxyConfig{
				code:     http.StatusBadRequest,
				response: `Not Found`,
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
				DestinationConfig: map[string]interface{}{
					"key_1": "val_1",
					"key_2": "val_2",
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

				isOAuthV2EnabledLoader := config.SingleValueLoader(false)
				expTimeDiff := config.SingleValueLoader(1 * time.Minute)
				tr := NewTransformer(tc.rtTimeout, httpClientTimeout, nil, isOAuthV2EnabledLoader, expTimeDiff)
				ctx := context.TODO()
				reqParams := &ProxyRequestParams{
					ResponseData: tc.postParameters,
					DestName:     "not_found_dest",
					Adapter:      &mockAdapter{url: srv.URL},
				}
				r := tr.ProxyRequest(ctx, reqParams)
				stCd := r.ProxyRequestStatusCode
				resp := r.ProxyRequestResponseBody
				contentType := r.RespContentType
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
			isOAuthV2EnabledLoader := config.SingleValueLoader(false)
			expTimeDiff := config.SingleValueLoader(1 * time.Minute)
			// Logic for executing test-cases not manipulating test-cases
			if tc.rtTimeout.Milliseconds() > 0 {
				tr = NewTransformer(tc.rtTimeout, httpClientTimeout, nil, isOAuthV2EnabledLoader, expTimeDiff)
			} else {
				// Just a default value
				tr = NewTransformer(2*time.Millisecond, httpClientTimeout, nil, isOAuthV2EnabledLoader, expTimeDiff)
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
				Adapter:      &mockAdapter{url: srv.URL},
			}
			r := tr.ProxyRequest(ctx, reqParams)
			stCd := r.ProxyRequestStatusCode
			resp := r.ProxyRequestResponseBody
			contentType := r.RespContentType

			assert.Equal(t, tc.expected.code, stCd)
			require.Equal(t, tc.expected.contentType, contentType)

			require.NotNil(t, r.RespStatusCodes)
			require.NotNil(t, r.RespBodys)
			require.NotNil(t, r.DontBatchDirectives)

			switch tc.expected.bodyType {
			case JSON:
				require.JSONEq(t, tc.expected.body, resp)
			case STR:
				expectedBodyStr := fmt.Sprintf(tc.expected.body, srv.URL)
				require.Equal(t, expectedBodyStr, resp)
			case EXACT_STR:
				require.Equal(t, tc.expected.body, resp)
			}
		})
	}
}

// A kind of mock for transformer proxy endpoint in transformer
func mockProxyHandler(timeout time.Duration, code int, response string) *chi.Mux {
	srvMux := chi.NewRouter()
	srvMux.HandleFunc("/v0/destinations/{destName}/proxy", func(w http.ResponseWriter, req *http.Request) {
		dName := chi.URLParam(req, "destName")
		if dName == "" {
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
			return
		}
	})
	return srvMux
}

type oauthV2TestCase struct {
	description              string
	cpResponses              []testutils.CpResponseParams
	routerTransformResponses []types.DestinationJobT
	inputEvents              []types.RouterJobT
	expected                 []types.DestinationJobT
}

var oauthDests = []backendconfig.DestinationT{
	{
		ID:          "d1",
		WorkspaceID: "wsp",
		Config: map[string]interface{}{
			"rudderAccountId": "actId",
		},
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: "SALESFORCE_OAUTH",
			Config: map[string]interface{}{
				"auth": map[string]interface{}{
					"type":         "OAuth",
					"rudderScopes": []interface{}{"delivery"},
				},
			},
		},
	},
}

var oauthV2RtTcs = []oauthV2TestCase{
	{
		description: "should only set the jobs with '500' where AuthErrorCategory is defined",
		cpResponses: []testutils.CpResponseParams{
			// fetch token http request
			{
				Code:     200,
				Response: `{"secret": {"access_token": "expired_token","refresh_token":"refresh_token"}}`,
			},
			// refresh token http request
			{
				Code:     200,
				Response: `{"secret": {"access_token": "valid_token","refresh_token":"refresh_token"}}`,
			},
		},
		routerTransformResponses: []types.DestinationJobT{
			{JobMetadataArray: []types.JobMetadataT{{JobID: 1, WorkspaceID: "wsp"}}, StatusCode: http.StatusOK, Destination: oauthDests[0]},
			{JobMetadataArray: []types.JobMetadataT{{JobID: 2, WorkspaceID: "wsp"}}, StatusCode: http.StatusUnauthorized, AuthErrorCategory: common.CategoryRefreshToken, Destination: oauthDests[0]},
			{JobMetadataArray: []types.JobMetadataT{{JobID: 3, WorkspaceID: "wsp"}}, StatusCode: http.StatusOK, Destination: oauthDests[0]},
			{JobMetadataArray: []types.JobMetadataT{{JobID: 4, WorkspaceID: "wsp"}}, StatusCode: http.StatusUnauthorized, AuthErrorCategory: common.CategoryRefreshToken, Destination: oauthDests[0]},
		},
		expected: []types.DestinationJobT{
			{Destination: oauthDests[0], JobMetadataArray: []types.JobMetadataT{{JobID: 1, WorkspaceID: "wsp"}}, StatusCode: http.StatusOK},
			{Destination: oauthDests[0], JobMetadataArray: []types.JobMetadataT{{JobID: 2, WorkspaceID: "wsp"}}, StatusCode: http.StatusInternalServerError, AuthErrorCategory: common.CategoryRefreshToken},
			{Destination: oauthDests[0], JobMetadataArray: []types.JobMetadataT{{JobID: 3, WorkspaceID: "wsp"}}, StatusCode: http.StatusOK},
			{Destination: oauthDests[0], JobMetadataArray: []types.JobMetadataT{{JobID: 4, WorkspaceID: "wsp"}}, StatusCode: http.StatusInternalServerError, AuthErrorCategory: common.CategoryRefreshToken},
		},
		inputEvents: []types.RouterJobT{
			{JobMetadata: types.JobMetadataT{JobID: 1, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
			{JobMetadata: types.JobMetadataT{JobID: 2, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
			{JobMetadata: types.JobMetadataT{JobID: 3, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
			{JobMetadata: types.JobMetadataT{JobID: 4, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
		},
	},
	{
		description: "should only set the jobs with '400' where AuthErrorCategory is defined",
		cpResponses: []testutils.CpResponseParams{
			// fetch token http request
			{
				Code:     200,
				Response: `{"secret": {"access_token": "invalid_grant_access_token","refresh_token":"invalid_grant_refresh_token"}}`,
			},
			// refresh token http request
			{
				Code:     403,
				Response: `{"status":403,"body":{"message":"[google_analytics] \"invalid_grant\" error, refresh token has been revoked","status":403,"code":"ref_token_invalid_grant"},"code":"ref_token_invalid_grant","access_token":"invalid_grant_access_token","refresh_token":"invalid_grant_refresh_token","developer_token":"dev_token"}`,
			},
			// authStatus inactive http request
			{
				Code: 200,
			},
		},
		routerTransformResponses: []types.DestinationJobT{
			{JobMetadataArray: []types.JobMetadataT{{JobID: 1, WorkspaceID: "wsp"}}, StatusCode: http.StatusOK, Destination: oauthDests[0]},
			{JobMetadataArray: []types.JobMetadataT{{JobID: 2, WorkspaceID: "wsp"}}, StatusCode: http.StatusUnauthorized, AuthErrorCategory: common.CategoryRefreshToken, Destination: oauthDests[0]},
			{JobMetadataArray: []types.JobMetadataT{{JobID: 3, WorkspaceID: "wsp"}}, StatusCode: http.StatusOK, Destination: oauthDests[0]},
			{JobMetadataArray: []types.JobMetadataT{{JobID: 4, WorkspaceID: "wsp"}}, StatusCode: http.StatusUnauthorized, AuthErrorCategory: common.CategoryRefreshToken, Destination: oauthDests[0]},
		},
		expected: []types.DestinationJobT{
			{Destination: oauthDests[0], JobMetadataArray: []types.JobMetadataT{{JobID: 1, WorkspaceID: "wsp"}}, StatusCode: http.StatusOK},
			{Error: `[google_analytics] "invalid_grant" error, refresh token has been revoked`, Destination: oauthDests[0], JobMetadataArray: []types.JobMetadataT{{JobID: 2, WorkspaceID: "wsp"}}, StatusCode: http.StatusBadRequest, AuthErrorCategory: common.CategoryRefreshToken},
			{Destination: oauthDests[0], JobMetadataArray: []types.JobMetadataT{{JobID: 3, WorkspaceID: "wsp"}}, StatusCode: http.StatusOK},
			{Error: `[google_analytics] "invalid_grant" error, refresh token has been revoked`, Destination: oauthDests[0], JobMetadataArray: []types.JobMetadataT{{JobID: 4, WorkspaceID: "wsp"}}, StatusCode: http.StatusBadRequest, AuthErrorCategory: common.CategoryRefreshToken},
		},
		inputEvents: []types.RouterJobT{
			{JobMetadata: types.JobMetadataT{JobID: 1, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
			{JobMetadata: types.JobMetadataT{JobID: 2, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
			{JobMetadata: types.JobMetadataT{JobID: 3, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
			{JobMetadata: types.JobMetadataT{JobID: 4, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
		},
	},
	{
		description: "when refreshToken response is not unmarshallable(CP is not responding correctly), should only set the jobs with '500' where AuthErrorCategory is defined",
		cpResponses: []testutils.CpResponseParams{
			// fetch token http request
			{
				Code:     200,
				Response: `{"secret": {"access_token": "expired_token","refresh_token":"refresh_token"}}`,
			},
			// refresh token http request
			{
				Code:     503,
				Response: `Bad Gateway`,
			},
		},
		routerTransformResponses: []types.DestinationJobT{
			{JobMetadataArray: []types.JobMetadataT{{JobID: 1, WorkspaceID: "wsp"}}, StatusCode: http.StatusOK, Destination: oauthDests[0]},
			{Error: "unauthorised", JobMetadataArray: []types.JobMetadataT{{JobID: 2, WorkspaceID: "wsp"}}, StatusCode: http.StatusUnauthorized, AuthErrorCategory: common.CategoryRefreshToken, Destination: oauthDests[0]},
			{JobMetadataArray: []types.JobMetadataT{{JobID: 3, WorkspaceID: "wsp"}}, StatusCode: http.StatusOK, Destination: oauthDests[0]},
			{Error: "unauthorised", JobMetadataArray: []types.JobMetadataT{{JobID: 4, WorkspaceID: "wsp"}}, StatusCode: http.StatusUnauthorized, AuthErrorCategory: common.CategoryRefreshToken, Destination: oauthDests[0]},
		},
		expected: []types.DestinationJobT{
			{Destination: oauthDests[0], JobMetadataArray: []types.JobMetadataT{{JobID: 1, WorkspaceID: "wsp"}}, StatusCode: http.StatusOK},
			{Error: "error occurred while fetching/refreshing account info from CP: Unmarshal of response unsuccessful: Bad Gateway", Destination: oauthDests[0], JobMetadataArray: []types.JobMetadataT{{JobID: 2, WorkspaceID: "wsp"}}, StatusCode: http.StatusInternalServerError, AuthErrorCategory: common.CategoryRefreshToken},
			{Destination: oauthDests[0], JobMetadataArray: []types.JobMetadataT{{JobID: 3, WorkspaceID: "wsp"}}, StatusCode: http.StatusOK},
			{Error: "error occurred while fetching/refreshing account info from CP: Unmarshal of response unsuccessful: Bad Gateway", Destination: oauthDests[0], JobMetadataArray: []types.JobMetadataT{{JobID: 4, WorkspaceID: "wsp"}}, StatusCode: http.StatusInternalServerError, AuthErrorCategory: common.CategoryRefreshToken},
		},
		inputEvents: []types.RouterJobT{
			{JobMetadata: types.JobMetadataT{JobID: 1, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
			{JobMetadata: types.JobMetadataT{JobID: 2, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
			{JobMetadata: types.JobMetadataT{JobID: 3, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
			{JobMetadata: types.JobMetadataT{JobID: 4, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
		},
	},
	{
		// panic test-case
		description: "when transformer response is not unmarshallable for quite sometime, after exhaustion of maxRetries(1) panic should happen",
		cpResponses: []testutils.CpResponseParams{
			// fetch token http request
			{
				Code:     200,
				Response: `{"secret": {"access_token": "valid_token","refresh_token":"refresh_token"}}`,
			},
		},
		inputEvents: []types.RouterJobT{
			{JobMetadata: types.JobMetadataT{JobID: 1, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
			{JobMetadata: types.JobMetadataT{JobID: 2, WorkspaceID: "wsp"}, Destination: oauthDests[0]},
		},
	},
}

type mockIdentifier struct {
	key   string
	token string
}

func (m *mockIdentifier) ID() string                  { return m.key }
func (m *mockIdentifier) BasicAuth() (string, string) { return m.token, "" }
func (*mockIdentifier) Type() deployment.Type         { return "mockType" }

func TestRouterTransformationWithOAuthV2(t *testing.T) {
	initMocks(t)
	config.Reset()
	loggerOverride = logger.NOP

	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)

	mockBackendConfig.EXPECT().AccessToken().AnyTimes()
	mockBackendConfig.EXPECT().Identity().AnyTimes().Return(&mockIdentifier{})

	for _, tc := range oauthV2RtTcs {
		t.Run(tc.description, func(t *testing.T) {
			svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Add(apiVersionHeader, strconv.Itoa(utilTypes.SupportedTransformerApiVersion))
				var err error
				outputJson := []byte("Reset Content")
				statusCode := http.StatusResetContent
				if tc.routerTransformResponses != nil {
					var b []byte
					b, err = json.Marshal(tc.routerTransformResponses)
					outputJson, _ = sjson.SetRawBytes([]byte(`{}`), "output", b)
					statusCode = http.StatusOK
				}
				require.NoError(t, err)
				w.WriteHeader(statusCode)
				_, err = w.Write(outputJson)
				require.NoError(t, err)
			}))

			cpRespProducer := &testutils.CpResponseProducer{
				Responses: tc.cpResponses,
			}

			cfgBeSvr := httptest.NewServer(cpRespProducer.MockCpRequests())

			isOAuthV2EnabledLoader := config.SingleValueLoader(true)
			defer svr.Close()
			defer cfgBeSvr.Close()
			t.Setenv("DEST_TRANSFORM_URL", svr.URL)
			t.Setenv("CONFIG_BACKEND_URL", cfgBeSvr.URL)
			config.Set("Processor.maxRetry", 1) // no retries

			backendconfig.Init()
			expTimeDiff := config.SingleValueLoader(1 * time.Minute)

			tr := NewTransformer(time.Minute, time.Minute, mockBackendConfig, isOAuthV2EnabledLoader, expTimeDiff)

			transformMsg := types.TransformMessageT{
				Data: tc.inputEvents,
			}

			if tc.expected != nil {
				transformerResponse := tr.Transform(ROUTER_TRANSFORM, &transformMsg)
				require.NotNil(t, transformerResponse)

				for i, ex := range tc.expected {
					require.Equalf(t, ex.Batched, transformerResponse[i].Batched, "[%i] Batched assertion failed", i)
					require.Equalf(t, ex.Message, transformerResponse[i].Message, "[%i] Message assertion failed", i)
					require.Equalf(t, ex.AuthErrorCategory, transformerResponse[i].AuthErrorCategory, "[%i] AuthErrorCategory assertion failed", i)
					require.Equalf(t, ex.JobMetadataArray, transformerResponse[i].JobMetadataArray, "[%i] JobMetadataArray assertion failed", i)
					require.Equalf(t, ex.Error, transformerResponse[i].Error, "[%i] Error field assertion failed", i)
					require.Equalf(t, ex.StatusCode, transformerResponse[i].StatusCode, "[%i] StatusCode assertion failed", i)
				}
				return
			}
			assert.Panics(t, func() { tr.Transform(ROUTER_TRANSFORM, &transformMsg) })
		})
	}
}

type oauthv2ProxyTcs struct {
	description string
	// input
	reqPayload ProxyRequestPayload
	destType   string
	// should be either v0 or v1 & depending on it you need to fill up transformerProxyResponse{V0 or V1}
	proxyVersion               string
	transformerProxyResponseV1 ProxyResponseV1
	transformerProxyResponseV0 ProxyResponseV0

	// When to use this field: we made proxy v1 call but got proxyv0 response in such cases this field can be used
	transformerResponse string
	// oauth calls
	cpResponses []testutils.CpResponseParams
	// output
	expected ProxyRequestResponse
	// destination Object for preparing context
	destination backendconfig.DestinationT

	ioReadError bool
	// load-balancer erroring out
	lbError bool
}

var oauthv2ProxyTestCases = []oauthv2ProxyTcs{
	{
		description:  "[v1proxy] when refreshToken succeeds, should have respStatus as 500, respBody should have transformer sent response",
		proxyVersion: "v1",
		transformerProxyResponseV1: ProxyResponseV1{
			Message:           "some message that we got from transformer",
			AuthErrorCategory: common.CategoryRefreshToken,
			Response: []TPDestResponse{
				{
					StatusCode: http.StatusUnauthorized,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         2,
					},
					Error: "token has expired",
				},
				{
					StatusCode: http.StatusUnauthorized,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         1,
					},
					Error: "token has expired",
				},
			},
		},
		destType: "salesforce_oauth", // some destination
		reqPayload: ProxyRequestPayload{
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
			Metadata: []ProxyRequestMetadata{
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         2,
				},
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         1,
				},
			},
			DestinationConfig: oauthDests[0].Config,
		},
		cpResponses: []testutils.CpResponseParams{
			// refresh token http request
			{
				Code:     http.StatusOK,
				Response: `{"secret": {"access_token": "valid_token","refresh_token":"refresh_token"}}`,
			},
		},
		expected: ProxyRequestResponse{
			DontBatchDirectives: map[int64]bool{
				1: false,
				2: false,
			},
			RespBodys: map[int64]string{
				1: "token has expired",
				2: "token has expired",
			},
			RespContentType:          "application/json",
			ProxyRequestResponseBody: `{"message": "some message that we got from transformer","authErrorCategory":"REFRESH_TOKEN","response":[{"statusCode":401,"error":"token has expired","metadata":{"workspaceId":"workspace_id","destinationId":"destination_id","jobId":2}},{"statusCode":401,"error":"token has expired","metadata":{"workspaceId":"workspace_id","destinationId":"destination_id","jobId":1}}]}`,
			ProxyRequestStatusCode:   http.StatusInternalServerError,
			RespStatusCodes: map[int64]int{
				1: http.StatusInternalServerError,
				2: http.StatusInternalServerError,
			},
			OAuthErrorCategory: common.CategoryRefreshToken,
		},
		destination: oauthDests[0],
	},
	{
		description:  "[v1proxy] when refreshToken fails with a string response body, should have respStatus as 500, respBody should have the error thrown",
		proxyVersion: "v1",
		transformerProxyResponseV1: ProxyResponseV1{
			Message:           "some message that we got from transformer",
			AuthErrorCategory: common.CategoryRefreshToken,
			Response: []TPDestResponse{
				{
					StatusCode: http.StatusUnauthorized,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         2,
					},
					Error: "token has expired",
				},
				{
					StatusCode: http.StatusUnauthorized,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         1,
					},
					Error: "token has expired",
				},
			},
		},
		destType: "salesforce_oauth", // some destination
		reqPayload: ProxyRequestPayload{
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
			Metadata: []ProxyRequestMetadata{
				{
					WorkspaceID:   oauthDests[0].WorkspaceID + "1",
					DestinationID: oauthDests[0].ID + "1",
					JobID:         2,
				},
				{
					WorkspaceID:   oauthDests[0].WorkspaceID + "1",
					DestinationID: oauthDests[0].ID + "1",
					JobID:         1,
				},
			},
			DestinationConfig: oauthDests[0].Config,
		},
		cpResponses: []testutils.CpResponseParams{
			// refresh token http request
			{
				Code:     http.StatusInternalServerError,
				Response: `Error occurred in downstream rudder service`, // only sample
			},
		},
		expected: ProxyRequestResponse{
			DontBatchDirectives: map[int64]bool{
				1: false,
				2: false,
			},
			RespBodys: map[int64]string{
				1: "error occurred while fetching/refreshing account info from CP: Unmarshal of response unsuccessful: Error occurred in downstream rudder service",
				2: "error occurred while fetching/refreshing account info from CP: Unmarshal of response unsuccessful: Error occurred in downstream rudder service",
			},
			RespContentType:          "application/json",
			ProxyRequestResponseBody: `error occurred while fetching/refreshing account info from CP: Unmarshal of response unsuccessful: Error occurred in downstream rudder service`,
			ProxyRequestStatusCode:   http.StatusInternalServerError,
			RespStatusCodes: map[int64]int{
				1: http.StatusInternalServerError,
				2: http.StatusInternalServerError,
			},
			OAuthErrorCategory: common.CategoryRefreshToken,
		},
		destination: oauthDests[0],
	},
	{
		description:  "[v1proxy] when authStatusInactive succeeds, should have respStatus as 400, respBody should have the error thrown from transformer",
		proxyVersion: "v1",
		transformerProxyResponseV1: ProxyResponseV1{
			Message:           "some message that we got from transformer",
			AuthErrorCategory: common.CategoryAuthStatusInactive,
			Response: []TPDestResponse{
				{
					StatusCode: http.StatusForbidden,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   oauthDests[0].WorkspaceID,
						DestinationID: oauthDests[0].ID,
						JobID:         2,
					},
					Error: "permission is not present",
				},
				{
					StatusCode: http.StatusForbidden,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   oauthDests[0].WorkspaceID,
						DestinationID: oauthDests[0].ID,
						JobID:         1,
					},
					Error: "permission is not present",
				},
			},
		},
		destType: "salesforce_oauth", // some destination
		reqPayload: ProxyRequestPayload{
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
			Metadata: []ProxyRequestMetadata{
				{
					WorkspaceID:   oauthDests[0].WorkspaceID,
					DestinationID: oauthDests[0].ID,
					JobID:         2,
				},
				{
					WorkspaceID:   oauthDests[0].WorkspaceID,
					DestinationID: oauthDests[0].ID,
					JobID:         1,
				},
			},
			DestinationConfig: oauthDests[0].Config,
		},
		cpResponses: []testutils.CpResponseParams{
			// auth status inactive http request
			{
				Code:     http.StatusBadRequest,
				Response: `{"body":{"code":"ref_token_invalid_grant"}}`, // only sample
			},
		},
		expected: ProxyRequestResponse{
			DontBatchDirectives: map[int64]bool{
				1: false,
				2: false,
			},
			RespBodys: map[int64]string{
				1: "permission is not present",
				2: "permission is not present",
			},
			RespContentType:          "application/json",
			ProxyRequestResponseBody: `{"message": "some message that we got from transformer","authErrorCategory":"AUTH_STATUS_INACTIVE","response":[{"statusCode":403,"error":"permission is not present","metadata":{"workspaceId":"wsp","destinationId":"d1","jobId":2}},{"statusCode":403,"error":"permission is not present","metadata":{"workspaceId":"wsp","destinationId":"d1","jobId":1}}]}`,
			ProxyRequestStatusCode:   http.StatusBadRequest,
			RespStatusCodes: map[int64]int{
				1: http.StatusBadRequest,
				2: http.StatusBadRequest,
			},
			OAuthErrorCategory: common.CategoryAuthStatusInactive,
		},
		destination: oauthDests[0],
	},
	{
		description:  "[v1proxy] when authStatusInactive fails, should have respStatus as 400, respBody should have the error thrown from transformer",
		proxyVersion: "v1",
		transformerProxyResponseV1: ProxyResponseV1{
			Message:           "some message that we got from transformer",
			AuthErrorCategory: common.CategoryAuthStatusInactive,
			Response: []TPDestResponse{
				{
					StatusCode: http.StatusForbidden,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   oauthDests[0].WorkspaceID,
						DestinationID: oauthDests[0].ID,
						JobID:         2,
					},
					Error: "permission is not present",
				},
				{
					StatusCode: http.StatusForbidden,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   oauthDests[0].WorkspaceID,
						DestinationID: oauthDests[0].ID,
						JobID:         1,
					},
					Error: "permission is not present",
				},
			},
		},
		destType: "salesforce_oauth", // some destination
		reqPayload: ProxyRequestPayload{
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
			Metadata: []ProxyRequestMetadata{
				{
					WorkspaceID:   oauthDests[0].WorkspaceID,
					DestinationID: oauthDests[0].ID,
					JobID:         2,
				},
				{
					WorkspaceID:   oauthDests[0].WorkspaceID,
					DestinationID: oauthDests[0].ID,
					JobID:         1,
				},
			},
			DestinationConfig: oauthDests[0].Config,
		},
		cpResponses: []testutils.CpResponseParams{
			// auth status inactive http request
			{
				Code:     http.StatusInternalServerError,
				Response: `{"body":"could not complete update"}`, // only sample
			},
		},
		expected: ProxyRequestResponse{
			DontBatchDirectives: map[int64]bool{
				1: false,
				2: false,
			},
			RespBodys: map[int64]string{
				1: "permission is not present",
				2: "permission is not present",
			},
			RespContentType:          "application/json",
			ProxyRequestResponseBody: `{"message": "some message that we got from transformer","authErrorCategory":"AUTH_STATUS_INACTIVE","response":[{"statusCode":403,"error":"permission is not present","metadata":{"workspaceId":"wsp","destinationId":"d1","jobId":2}},{"statusCode":403,"error":"permission is not present","metadata":{"workspaceId":"wsp","destinationId":"d1","jobId":1}}]}`,
			ProxyRequestStatusCode:   http.StatusBadRequest,
			RespStatusCodes: map[int64]int{
				1: http.StatusBadRequest,
				2: http.StatusBadRequest,
			},
			OAuthErrorCategory: common.CategoryAuthStatusInactive,
		},
		destination: oauthDests[0],
	},
	{
		description:  "[v1proxy] when transformer response body cannot be read, should have respStatus as 500, respBody should have transformer sent response",
		proxyVersion: "v1",
		ioReadError:  true,
		transformerProxyResponseV1: ProxyResponseV1{
			Message:           "some message that we got from transformer",
			AuthErrorCategory: common.CategoryRefreshToken,
			Response: []TPDestResponse{
				{
					StatusCode: http.StatusUnauthorized,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         2,
					},
					Error: "token has expired",
				},
				{
					StatusCode: http.StatusUnauthorized,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         1,
					},
					Error: "token has expired",
				},
			},
		},
		destType: "salesforce_oauth", // some destination
		reqPayload: ProxyRequestPayload{
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
			Metadata: []ProxyRequestMetadata{
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         2,
				},
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         1,
				},
			},
			DestinationConfig: oauthDests[0].Config,
		},
		cpResponses: []testutils.CpResponseParams{
			// refresh token http request
			{
				Code:     http.StatusOK,
				Response: `{"secret": {"access_token": "valid_token","refresh_token":"refresh_token"}}`,
			},
		},
		expected: ProxyRequestResponse{
			DontBatchDirectives: map[int64]bool{
				1: false,
				2: false,
			},
			RespBodys:                map[int64]string{},
			RespContentType:          "text/plain; charset=utf-8",
			ProxyRequestResponseBody: `reading response body post roundTrip: unexpected EOF`, // not full error message
			ProxyRequestStatusCode:   http.StatusInternalServerError,
			RespStatusCodes:          map[int64]int{},
		},
		destination: oauthDests[0],
	},
	{
		description:  "[v1proxy] when transformer does not respond properly",
		proxyVersion: "v1",
		lbError:      true,
		destType:     "salesforce_oauth", // some destination
		reqPayload: ProxyRequestPayload{
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
			Metadata: []ProxyRequestMetadata{
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         2,
				},
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         1,
				},
			},
			DestinationConfig: oauthDests[0].Config,
		},
		cpResponses: []testutils.CpResponseParams{
			// refresh token http request
			{
				Code:     http.StatusOK,
				Response: `{"secret": {"access_token": "valid_token","refresh_token":"refresh_token"}}`,
			},
		},
		expected: ProxyRequestResponse{
			DontBatchDirectives: map[int64]bool{
				1: false,
				2: false,
			},
			RespBodys:       map[int64]string{},
			RespContentType: "text/plain; charset=utf-8",
			// Originally Response Body will look like this "Post \"http://<TF_SERVER>/v1/destinations/salesforce_oauth/proxy\": getting auth error category post roundTrip: LB cannot send to transformer"
			ProxyRequestResponseBody: `getting auth error category post roundTrip: LB cannot send to transformer`,
			ProxyRequestStatusCode:   http.StatusInternalServerError,
			RespStatusCodes:          map[int64]int{},
		},
		destination: oauthDests[0],
	},
	{
		description:  "[v0proxy] when transformer does not respond properly",
		proxyVersion: "v0",
		lbError:      true,
		destType:     "salesforce_oauth", // some destination
		reqPayload: ProxyRequestPayload{
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
			Metadata: []ProxyRequestMetadata{
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         2,
				},
			},
			DestinationConfig: oauthDests[0].Config,
		},
		cpResponses: []testutils.CpResponseParams{
			// fetch token http request
			{
				Code:     http.StatusOK,
				Response: `{"secret": {"access_token": "valid_token","refresh_token":"refresh_token"}}`,
			},
		},
		expected: ProxyRequestResponse{
			DontBatchDirectives: map[int64]bool{
				2: false,
			},
			RespBodys:       map[int64]string{},
			RespContentType: "text/plain; charset=utf-8",
			// Originally Response Body will look like this "Post \"http://<TF_SERVER>/v1/destinations/salesforce_oauth/proxy\": getting auth error category post roundTrip: LB cannot send to transformer"
			ProxyRequestResponseBody: `getting auth error category post roundTrip: LB cannot send to transformer`,
			ProxyRequestStatusCode:   http.StatusInternalServerError,
			RespStatusCodes:          map[int64]int{},
		},
		destination: oauthDests[0],
	},
	{
		description:         "[v1proxy] when v1Proxy endpoint sending v0Proxy response, unmarshal should happen at proxyAdapter.getResponse level",
		transformerResponse: `{"message": ["some other error"]}`,
		destType:            "salesforce_oauth", // some destination
		reqPayload: ProxyRequestPayload{
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
			Metadata: []ProxyRequestMetadata{
				{
					WorkspaceID:   "wsp",
					DestinationID: "d1",
					JobID:         2,
				},
			},
			DestinationConfig: oauthDests[0].Config,
		},
		cpResponses: []testutils.CpResponseParams{
			// refresh token http request
			{
				Code:     http.StatusOK,
				Response: `{"secret": {"access_token": "valid_token","refresh_token":"refresh_token"}}`,
			},
		},
		expected: ProxyRequestResponse{
			DontBatchDirectives: map[int64]bool{
				2: false,
			},
			RespBodys:                map[int64]string{},
			RespContentType:          "text/plain; charset=utf-8",
			ProxyRequestResponseBody: `[TransformerProxy Unmarshalling]:: respData: {"message": ["some other error"]}, err: transformer.ProxyResponseV1.Message: ReadString: expects " or n, but found [, error found in #10 byte of ...|essage": ["some othe|..., bigger context ...|{"message": ["some other error"]}|...`,
			ProxyRequestStatusCode:   http.StatusOK, // transformer returned response
			RespStatusCodes:          map[int64]int{},
		},
		destination: oauthDests[0],
	},
	{
		description:  "[v1proxy] when there are partial failures, should have respStatus as 200, respBody should have the high-level resp sent by transformer and rest of the parameters should correctly encompass partial failures",
		proxyVersion: "v1",
		transformerProxyResponseV1: ProxyResponseV1{
			Message: "some message that we got from transformer",
			Response: []TPDestResponse{
				{
					StatusCode: http.StatusOK,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         2,
					},
					Error: "success",
				},
				{
					StatusCode: http.StatusBadRequest,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         1,
					},
					Error: "UserId for matchId: '12324' cannot be found",
				},
				{
					StatusCode: http.StatusOK,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         4,
					},
					Error: "success",
				},
				{
					StatusCode: http.StatusBadRequest,
					Metadata: ProxyRequestMetadata{
						JobID:         6,
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
					},
					Error: "UserId for matchId: '64676493' can not be found",
				},
			},
		},
		destType: "salesforce_oauth", // some destination
		reqPayload: ProxyRequestPayload{
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
			Metadata: []ProxyRequestMetadata{
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         2,
				},
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         4,
				},
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         1,
				},
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         6,
				},
			},
			DestinationConfig: oauthDests[0].Config,
		},
		cpResponses: []testutils.CpResponseParams{},
		expected: ProxyRequestResponse{
			DontBatchDirectives: map[int64]bool{
				1: false,
				2: false,
				4: false,
				6: false,
			},
			RespBodys: map[int64]string{
				2: "success",
				1: "UserId for matchId: '12324' cannot be found",
				6: "UserId for matchId: '64676493' can not be found",
				4: "success",
			},
			RespContentType:          "application/json",
			ProxyRequestResponseBody: `{"message": "some message that we got from transformer","authErrorCategory":"REFRESH_TOKEN","response":[{"statusCode":401,"error":"token has expired","metadata":{"workspaceId":"workspace_id","destinationId":"destination_id","jobId":2}},{"statusCode":401,"error":"token has expired","metadata":{"workspaceId":"workspace_id","destinationId":"destination_id","jobId":1}}]}`,
			ProxyRequestStatusCode:   http.StatusOK,
			RespStatusCodes: map[int64]int{
				2: http.StatusOK,
				1: http.StatusBadRequest,
				4: http.StatusOK,
				6: http.StatusBadRequest,
			},
		},
		destination: oauthDests[0],
	},

	{
		description:  "[v1proxy] when there are partial failures & no oauth errors, respective jobs should have their statuses",
		proxyVersion: "v1",
		transformerProxyResponseV1: ProxyResponseV1{
			Message: "Request processed successfully",
			Response: []TPDestResponse{
				{
					StatusCode: http.StatusOK,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         12,
					},
					Error: "success",
				},
				{
					StatusCode: http.StatusBadRequest,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         21,
					},
					Error: "client sent bad data, please correct it",
				},
				{
					StatusCode: http.StatusBadGateway,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         34,
					},
					Error: "Something wrong with platform, please try after sometime",
				},
				{
					StatusCode: http.StatusServiceUnavailable,
					Metadata: ProxyRequestMetadata{
						JobID:         46,
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
					},
					Error: "platform has a problem, please try after sometime",
				},
			},
		},
		destType: "salesforce_oauth", // some destination
		reqPayload: ProxyRequestPayload{
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
			Metadata: []ProxyRequestMetadata{
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         12,
				},
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         21,
				},
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         34,
				},
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         46,
				},
			},
			DestinationConfig: oauthDests[0].Config,
		},
		cpResponses: []testutils.CpResponseParams{},
		expected: ProxyRequestResponse{
			DontBatchDirectives: map[int64]bool{
				12: false,
				21: false,
				34: false,
				46: false,
			},
			RespBodys: map[int64]string{
				12: "success",
				21: "client sent bad data, please correct it",
				34: "Something wrong with platform, please try after sometime",
				46: "platform has a problem, please try after sometime",
			},
			RespContentType:          "application/json",
			ProxyRequestResponseBody: "{\"message\":\"Request processed successfully\",\"response\":[{\"statusCode\":200,\"metadata\":{\"jobId\":12,\"attemptNum\":0,\"userId\":\"\",\"sourceId\":\"\",\"destinationId\":\"destination_id\",\"workspaceId\":\"workspace_id\",\"secret\":null,\"dontBatch\":false},\"error\":\"success\"},{\"statusCode\":400,\"metadata\":{\"jobId\":21,\"attemptNum\":0,\"userId\":\"\",\"sourceId\":\"\",\"destinationId\":\"destination_id\",\"workspaceId\":\"workspace_id\",\"secret\":null,\"dontBatch\":false},\"error\":\"client sent bad data, please correct it\"},{\"statusCode\":502,\"metadata\":{\"jobId\":34,\"attemptNum\":0,\"userId\":\"\",\"sourceId\":\"\",\"destinationId\":\"destination_id\",\"workspaceId\":\"workspace_id\",\"secret\":null,\"dontBatch\":false},\"error\":\"Something wrong with platform, please try after sometime\"},{\"statusCode\":503,\"metadata\":{\"jobId\":46,\"attemptNum\":0,\"userId\":\"\",\"sourceId\":\"\",\"destinationId\":\"destination_id\",\"workspaceId\":\"workspace_id\",\"secret\":null,\"dontBatch\":false},\"error\":\"platform has a problem, please try after sometime\"}],\"authErrorCategory\":\"\"}",
			ProxyRequestStatusCode:   http.StatusOK,
			RespStatusCodes: map[int64]int{
				12: http.StatusOK,
				21: http.StatusBadRequest,
				34: http.StatusBadGateway,
				46: http.StatusServiceUnavailable,
			},
		},
		destination: oauthDests[0],
	},
	{
		description:  "[v1proxy] when there are partial failures with oauth error for a single job & CP errors out with 500, all jobs will have same status(500) and errored response from CP",
		proxyVersion: "v1",
		transformerProxyResponseV1: ProxyResponseV1{
			Message:           "Request processed successfully",
			AuthErrorCategory: common.CategoryRefreshToken,
			Response: []TPDestResponse{
				{
					StatusCode: http.StatusOK,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         12,
					},
					Error: "success",
				},
				{
					StatusCode: http.StatusBadRequest,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         21,
					},
					Error: "client sent bad data, please correct it",
				},
				{
					StatusCode: http.StatusUnauthorized,
					Metadata: ProxyRequestMetadata{
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
						JobID:         34,
					},
					Error: "token expired",
				},
				{
					StatusCode: http.StatusServiceUnavailable,
					Metadata: ProxyRequestMetadata{
						JobID:         46,
						WorkspaceID:   "workspace_id",
						DestinationID: "destination_id",
					},
					Error: "platform has a problem, please try after sometime",
				},
			},
		},
		destType: "salesforce_oauth", // some destination
		reqPayload: ProxyRequestPayload{
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
			Metadata: []ProxyRequestMetadata{
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         12,
				},
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         21,
				},
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         34,
				},
				{
					WorkspaceID:   "workspace_id",
					DestinationID: "destination_id",
					JobID:         46,
				},
			},
			DestinationConfig: oauthDests[0].Config,
		},
		cpResponses: []testutils.CpResponseParams{
			{
				Code:     http.StatusInternalServerError,
				Response: "internal error",
			},
		},
		expected: ProxyRequestResponse{
			OAuthErrorCategory: common.CategoryRefreshToken,
			DontBatchDirectives: map[int64]bool{
				12: false,
				21: false,
				34: false,
				46: false,
			},
			RespBodys: map[int64]string{
				12: "error occurred while fetching/refreshing account info from CP: Unmarshal of response unsuccessful: internal error",
				21: "error occurred while fetching/refreshing account info from CP: Unmarshal of response unsuccessful: internal error",
				34: "error occurred while fetching/refreshing account info from CP: Unmarshal of response unsuccessful: internal error",
				46: "error occurred while fetching/refreshing account info from CP: Unmarshal of response unsuccessful: internal error",
			},
			RespContentType:          "application/json",
			ProxyRequestResponseBody: "error occurred while fetching/refreshing account info from CP: Unmarshal of response unsuccessful: internal error",
			ProxyRequestStatusCode:   http.StatusInternalServerError,
			RespStatusCodes: map[int64]int{
				12: http.StatusInternalServerError,
				21: http.StatusInternalServerError,
				34: http.StatusInternalServerError,
				46: http.StatusInternalServerError,
			},
		},
		destination: oauthDests[0],
	},
}

func TestProxyRequestWithOAuthV2(t *testing.T) {
	initMocks(t)
	config.Reset()
	loggerOverride = logger.NOP

	mockCtrl := gomock.NewController(t)
	mockBackendConfig := mocksBackendConfig.NewMockBackendConfig(mockCtrl)

	mockBackendConfig.EXPECT().AccessToken().AnyTimes()
	mockBackendConfig.EXPECT().Identity().AnyTimes().Return(&mockIdentifier{})

	for _, tc := range oauthv2ProxyTestCases {
		t.Run(tc.description, func(t *testing.T) {
			svr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Add(apiVersionHeader, strconv.Itoa(utilTypes.SupportedTransformerApiVersion))
				b := []byte("LB cannot send to transformer")
				statusCode := http.StatusBadGateway
				var err error
				if tc.lbError {
					w.WriteHeader(statusCode)
					_, _ = w.Write(b)
					return
				}
				if tc.proxyVersion == "v1" {
					b, _ = json.Marshal(tc.transformerProxyResponseV1)
				} else if tc.proxyVersion == "v0" {
					b, _ = json.Marshal(tc.transformerProxyResponseV0)
				} else {
					b = []byte(tc.transformerResponse)
				}
				outputJson, _ := sjson.SetRawBytes([]byte(`{}`), "output", b)
				if tc.ioReadError {
					w.Header().Add("Content-Length", strconv.Itoa(len(string(outputJson))+10))
					// return less bytes, which will result in an "unexpected EOF" from ioutil.ReadAll()
					_, err = w.Write([]byte("a"))
				} else {
					_, err = w.Write(outputJson)
				}

				require.NoError(t, err)
			}))

			cpRespProducer := &testutils.CpResponseProducer{
				Responses: tc.cpResponses,
			}

			cfgBeSvr := httptest.NewServer(cpRespProducer.MockCpRequests())

			isOAuthV2EnabledLoader := config.SingleValueLoader(true)
			defer svr.Close()
			defer cfgBeSvr.Close()
			t.Setenv("DEST_TRANSFORM_URL", svr.URL)
			t.Setenv("CONFIG_BACKEND_URL", cfgBeSvr.URL)

			backendconfig.Init()
			expTimeDiff := config.SingleValueLoader(1 * time.Minute)

			tr := NewTransformer(time.Minute, time.Minute, mockBackendConfig, isOAuthV2EnabledLoader, expTimeDiff)

			var adapter transformerProxyAdapter
			adapter = NewTransformerProxyAdapter("v1", loggerOverride)
			if tc.proxyVersion == "v0" {
				adapter = NewTransformerProxyAdapter("v0", loggerOverride)
			}

			destinationInfo := &v2.DestinationInfo{
				Config:           tc.destination.Config,
				DefinitionConfig: tc.destination.DestinationDefinition.Config,
				WorkspaceID:      tc.reqPayload.Metadata[0].WorkspaceID,
				DefinitionName:   tc.destination.DestinationDefinition.Name,
				ID:               tc.destination.DestinationDefinition.ID,
			}
			reqParams := &ProxyRequestParams{
				ResponseData: tc.reqPayload,
				DestName:     tc.destType,
				Adapter:      adapter,
				DestInfo:     destinationInfo,
			}

			proxyResp := tr.ProxyRequest(context.Background(), reqParams)

			require.NotNil(t, proxyResp)
			require.Equal(t, tc.expected.DontBatchDirectives, proxyResp.DontBatchDirectives)
			require.Equal(t, tc.expected.OAuthErrorCategory, proxyResp.OAuthErrorCategory)
			require.Equal(t, tc.expected.RespBodys, proxyResp.RespBodys)
			require.Equal(t, tc.expected.RespStatusCodes, proxyResp.RespStatusCodes)
			require.Equal(t, tc.expected.RespContentType, proxyResp.RespContentType)

			require.Equal(t, tc.expected.ProxyRequestStatusCode, proxyResp.ProxyRequestStatusCode)
			// Assert of ProxyRequestPayload
			var expectedPrxResp, actualPrxResp ProxyRequestPayload
			e1 := json.Unmarshal([]byte(tc.expected.ProxyRequestResponseBody), &expectedPrxResp)
			e2 := json.Unmarshal([]byte(proxyResp.ProxyRequestResponseBody), &actualPrxResp)
			if e1 == nil && e2 == nil {
				require.Equal(t, actualPrxResp, expectedPrxResp)
			} else {
				require.Contains(t, proxyResp.ProxyRequestResponseBody, tc.expected.ProxyRequestResponseBody)
			}
		})
	}
}

func TestTransformNoValidationErrors(t *testing.T) {
	initMocks(t)
	config.Reset()
	loggerOverride = logger.NOP
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
	isOAuthV2EnabledLoader := config.SingleValueLoader(false)
	defer svr.Close()
	t.Setenv("DEST_TRANSFORM_URL", svr.URL)
	expTimeDiff := config.SingleValueLoader(1 * time.Minute)
	tr := NewTransformer(time.Minute, time.Minute, nil, isOAuthV2EnabledLoader, expTimeDiff)

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
	loggerOverride = logger.NOP
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
	isOAuthV2EnabledLoader := config.SingleValueLoader(false)
	expTimeDiff := config.SingleValueLoader(1 * time.Minute)
	tr := NewTransformer(time.Minute, time.Minute, nil, isOAuthV2EnabledLoader, expTimeDiff)

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
	loggerOverride = logger.NOP
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
	isOAuthV2EnabledLoader := config.SingleValueLoader(false)
	expTimeDiff := config.SingleValueLoader(1 * time.Minute)
	tr := NewTransformer(time.Minute, time.Minute, nil, isOAuthV2EnabledLoader, expTimeDiff)

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
	loggerOverride = logger.NOP
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
	isOAuthV2EnabledLoader := config.SingleValueLoader(false)
	expTimeDiff := config.SingleValueLoader(1 * time.Minute)
	tr := NewTransformer(time.Minute, time.Minute, nil, isOAuthV2EnabledLoader, expTimeDiff)

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

func TestDehydrateHydrate(t *testing.T) {
	initMocks(t)
	config.Reset()

	transformMessage := types.TransformMessageT{
		Data: []types.RouterJobT{
			{JobMetadata: types.JobMetadataT{JobID: 1, JobT: &jobsdb.JobT{JobID: 1}}},
			{JobMetadata: types.JobMetadataT{JobID: 2, JobT: &jobsdb.JobT{JobID: 2}}},
			{JobMetadata: types.JobMetadataT{JobID: 3, JobT: &jobsdb.JobT{JobID: 3}}},
		},
	}

	serverResponse := []types.DestinationJobT{
		{JobMetadataArray: []types.JobMetadataT{{JobID: 1}}},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 2}}},
		{JobMetadataArray: []types.JobMetadataT{{JobID: 3}}},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload types.TransformMessageT
		err := json.NewDecoder(r.Body).Decode(&payload)
		require.NoError(t, err)
		for i := range payload.Data {
			require.Nil(t, payload.Data[i].JobMetadata.JobT, "JobT should be nil")
		}
		w.Header().Add(apiVersionHeader, strconv.Itoa(utilTypes.SupportedTransformerApiVersion))
		b, err := json.Marshal(serverResponse)
		require.NoError(t, err)
		_, err = w.Write(b)
		require.NoError(t, err)
	}))
	config.Set("DEST_TRANSFORM_URL", srv.URL)
	isOAuthV2EnabledLoader := config.SingleValueLoader(false)
	expTimeDiff := config.SingleValueLoader(1 * time.Minute)
	tr := NewTransformer(time.Minute, time.Minute, nil, isOAuthV2EnabledLoader, expTimeDiff)

	transformerResponse := tr.Transform(BATCH, &transformMessage)

	require.NotNil(t, transformerResponse)
	require.Equal(t, 3, len(transformerResponse))
	for i := range transformerResponse {
		require.Equal(t, 1, len(transformerResponse[i].JobMetadataArray))
		require.EqualValues(t, i+1, transformerResponse[i].JobMetadataArray[0].JobID)
		require.NotNil(t, transformerResponse[i].JobMetadataArray[0].JobT, "JobT should not be nil")
		require.EqualValues(t, i+1, transformerResponse[i].JobMetadataArray[0].JobT.JobID)
	}
}

func initMocks(t *testing.T) {
	ctrl := gomock.NewController(t)

	mockRudderStats := mock_stats.NewMockMeasurement(ctrl)

	mockRudderStats.EXPECT().SendTiming(gomock.Any()).AnyTimes()
	mockRudderStats.EXPECT().Increment().AnyTimes()

	loggerOverride = logger.NOP
}

func normalizeErrors(transformerResponse []types.DestinationJobT, prefix string) {
	for i := range transformerResponse {
		job := &transformerResponse[i]
		if strings.HasPrefix(job.Error, prefix) {
			job.Error = prefix
		}
	}
}
