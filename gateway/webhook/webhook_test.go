package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/bytesize"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	gwStats "github.com/rudderlabs/rudder-server/gateway/internal/stats"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	mockWebhook "github.com/rudderlabs/rudder-server/gateway/mocks"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	sampleWriteKey = "SampleWriteKey"
	sourceDefName  = "webhook"
	sampleError    = "someError"
	sampleJson     = "{\"hello\":\"world\"}"
)

var (
	once            sync.Once
	outputToGateway = map[string]interface{}{"hello": "world"}
	outputToWebhook = &outputToSource{
		Body:        []byte(sampleJson),
		ContentType: "application/json",
	}
)

func initWebhook() {
	once.Do(func() {
		config.Reset()
		logger.Reset()
		misc.Init()
		config.Set("Gateway.webhook.maxTransformerProcess", 1)
		config.Set("WriteTimeout", "1s")
	})
}

func TestWebhookMaxRequestSize(t *testing.T) {
	initWebhook()

	ctrl := gomock.NewController(t)

	mockGW := mockWebhook.NewMockGateway(ctrl)
	mockGW.EXPECT().TrackRequestMetrics(gomock.Any()).Times(1)
	mockGW.EXPECT().NewSourceStat(gomock.Any(), gomock.Any()).Return(&gwStats.SourceStat{}).AnyTimes()

	maxReqSizeInKB := 1

	webhookHandler := Setup(mockGW, stats.NOP)
	webhookHandler.config.maxReqSize = config.SingleValueLoader(maxReqSizeInKB)
	t.Cleanup(func() {
		_ = webhookHandler.Shutdown()
	})

	webhookHandler.Register(sourceDefName)

	payload := fmt.Sprintf(`{"hello":"world", "data": %q}`, strings.Repeat("a", 2*maxReqSizeInKB*int(bytesize.KB)))
	require.Greater(t, len(payload), maxReqSizeInKB*int(bytesize.KB))

	req := httptest.NewRequest(http.MethodPost, "/v1/webhook", bytes.NewBufferString(payload))
	resp := httptest.NewRecorder()

	reqCtx := context.WithValue(req.Context(), gwtypes.CtxParamCallType, "webhook")
	reqCtx = context.WithValue(reqCtx, gwtypes.CtxParamAuthRequestContext, &gwtypes.AuthRequestContext{
		SourceDefName: sourceDefName,
	})

	webhookHandler.RequestHandler(resp, req.WithContext(reqCtx))
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.Result().StatusCode)
}

func TestWebhookBlockTillFeaturesAreFetched(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mockWebhook.NewMockGateway(ctrl)
	webhookHandler := Setup(mockGW, stats.NOP)

	mockGW.EXPECT().TrackRequestMetrics(gomock.Any()).Times(1)
	mockGW.EXPECT().NewSourceStat(gomock.Any(), gomock.Any()).Return(&gwStats.SourceStat{}).AnyTimes()
	arctx := &gwtypes.AuthRequestContext{
		SourceDefName: sourceDefName,
		WriteKey:      sampleWriteKey,
	}
	webhookHandler.Register(sourceDefName)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook", bytes.NewBufferString(sampleJson))
	w := httptest.NewRecorder()
	ctx := context.WithValue(req.Context(), gwtypes.CtxParamCallType, "webhook")
	ctx = context.WithValue(ctx, gwtypes.CtxParamAuthRequestContext, arctx)
	req = req.WithContext(ctx)
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, http.StatusGatewayTimeout, w.Result().StatusCode)
	assert.Contains(t, strings.TrimSpace(w.Body.String()), "Gateway timeout")
	_ = webhookHandler.Shutdown()
}

func TestWebhookRequestHandlerWithTransformerBatchGeneralError(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mockWebhook.NewMockGateway(ctrl)

	webhookHandler := Setup(mockGW, stats.NOP)

	mockGW.EXPECT().TrackRequestMetrics(gomock.Any()).Times(1)
	mockGW.EXPECT().NewSourceStat(gomock.Any(), gomock.Any()).Return(&gwStats.SourceStat{}).Times(2)
	mockGW.EXPECT().SaveWebhookFailures(gomock.Any()).Return(nil).Times(1)
	arctx := &gwtypes.AuthRequestContext{
		SourceDefName: sourceDefName,
		WriteKey:      sampleWriteKey,
	}
	webhookHandler.Register(sourceDefName)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook", bytes.NewBufferString(sampleJson))
	w := httptest.NewRecorder()
	ctx := context.WithValue(req.Context(), gwtypes.CtxParamCallType, "webhook")
	ctx = context.WithValue(ctx, gwtypes.CtxParamAuthRequestContext, arctx)
	req = req.WithContext(ctx)
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Result().StatusCode)
	assert.Contains(t, strings.TrimSpace(w.Body.String()), "source Transformer returned non-success status")
	_ = webhookHandler.Shutdown()
}

func TestWebhookRequestHandlerWithTransformerBatchPayloadLengthMismatchError(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mockWebhook.NewMockGateway(ctrl)

	webhookHandler := Setup(mockGW, stats.NOP)

	mockGW.EXPECT().TrackRequestMetrics(gomock.Any()).Times(1)
	mockGW.EXPECT().NewSourceStat(gomock.Any(), gomock.Any()).Return(&gwStats.SourceStat{}).Times(2)
	mockGW.EXPECT().SaveWebhookFailures(gomock.Any()).Return(nil).Times(1)

	webhookHandler.Register(sourceDefName)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook?writeKey="+sampleWriteKey, bytes.NewBufferString(sampleJson))
	w := httptest.NewRecorder()
	ctx := context.WithValue(req.Context(), gwtypes.CtxParamCallType, "webhook")
	ctx = context.WithValue(ctx, gwtypes.CtxParamAuthRequestContext, &gwtypes.AuthRequestContext{
		SourceDefName: sourceDefName,
	})
	req = req.WithContext(ctx)
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Result().StatusCode)
	assert.Contains(t, strings.TrimSpace(w.Body.String()), response.SourceTransformerInvalidResponseFormat)
	_ = webhookHandler.Shutdown()
}

func TestWebhookRequestHandlerWithTransformerRequestError(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mockWebhook.NewMockGateway(ctrl)
	webhookHandler := Setup(mockGW, stats.NOP)

	mockGW.EXPECT().TrackRequestMetrics(gomock.Any()).Times(1)
	mockGW.EXPECT().NewSourceStat(gomock.Any(), gomock.Any()).Return(&gwStats.SourceStat{}).Times(2)
	mockGW.EXPECT().SaveWebhookFailures(gomock.Any()).Return(nil).Times(1)

	webhookHandler.Register(sourceDefName)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook?writeKey="+sampleWriteKey, bytes.NewBufferString(sampleJson))
	w := httptest.NewRecorder()
	ctx := context.WithValue(req.Context(), gwtypes.CtxParamCallType, "webhook")
	ctx = context.WithValue(ctx, gwtypes.CtxParamAuthRequestContext, &gwtypes.AuthRequestContext{
		SourceDefName: sourceDefName,
	})
	req = req.WithContext(ctx)
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Result().StatusCode)
	assert.Contains(t, sampleError, strings.TrimSpace(w.Body.String()))
	_ = webhookHandler.Shutdown()
}

func TestWebhookRequestHandlerWithOutputToSource(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mockWebhook.NewMockGateway(ctrl)

	webhookHandler := Setup(mockGW, stats.NOP)
	mockGW.EXPECT().TrackRequestMetrics("").Times(1)
	mockGW.EXPECT().NewSourceStat(gomock.Any(), gomock.Any()).Return(&gwStats.SourceStat{}).Times(1)

	webhookHandler.Register(sourceDefName)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook?writeKey="+sampleWriteKey, bytes.NewBufferString(sampleJson))
	w := httptest.NewRecorder()
	ctx := context.WithValue(req.Context(), gwtypes.CtxParamCallType, "webhook")
	ctx = context.WithValue(ctx, gwtypes.CtxParamAuthRequestContext, &gwtypes.AuthRequestContext{
		SourceDefName: sourceDefName,
	})
	req = req.WithContext(ctx)
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Result().StatusCode)
	assert.Equal(t, sampleJson, strings.TrimSpace(w.Body.String()))
	_ = webhookHandler.Shutdown()
}

func TestWebhookRequestHandlerWithOutputToGateway(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mockWebhook.NewMockGateway(ctrl)
	outputToGateway := map[string]interface{}{"text": "hello world"}
	webhookHandler := Setup(mockGW, stats.NOP)
	mockGW.EXPECT().TrackRequestMetrics("").Times(1)
	mockGW.EXPECT().NewSourceStat(gomock.Any(), gomock.Any()).Return(&gwStats.SourceStat{}).Times(1)

	gwPayload, _ := jsonrs.Marshal(outputToGateway)
	arctx := &gwtypes.AuthRequestContext{
		WriteKey:      sampleWriteKey,
		SourceDefName: sourceDefName,
	}
	mockGW.EXPECT().ProcessWebRequest(gomock.Any(), gomock.Any(), "batch", gwPayload, arctx).Times(1)

	webhookHandler.Register(sourceDefName)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook", bytes.NewBufferString(sampleJson))
	w := httptest.NewRecorder()
	ctx := context.WithValue(req.Context(), gwtypes.CtxParamCallType, "webhook")
	ctx = context.WithValue(ctx, gwtypes.CtxParamAuthRequestContext, arctx)
	req = req.WithContext(ctx)
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Result().StatusCode)
	assert.Equal(t, response.Ok, strings.TrimSpace(w.Body.String()))
	_ = webhookHandler.Shutdown()
}

func TestWebhookRequestHandlerWithOutputToGatewayAndSource(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mockWebhook.NewMockGateway(ctrl)

	webhookHandler := Setup(mockGW, stats.NOP)
	mockGW.EXPECT().TrackRequestMetrics("").Times(1)
	mockGW.EXPECT().NewSourceStat(gomock.Any(), gomock.Any()).Return(&gwStats.SourceStat{}).Times(1)

	gwPayload, _ := jsonrs.Marshal(outputToGateway)
	arctx := &gwtypes.AuthRequestContext{
		WriteKey:      sampleWriteKey,
		SourceDefName: sourceDefName,
	}
	mockGW.EXPECT().ProcessWebRequest(gomock.Any(), gomock.Any(), "batch", gwPayload, arctx).Times(1)

	webhookHandler.Register(sourceDefName)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook", bytes.NewBufferString(sampleJson))
	w := httptest.NewRecorder()
	ctx := context.WithValue(req.Context(), gwtypes.CtxParamCallType, "webhook")
	ctx = context.WithValue(ctx, gwtypes.CtxParamAuthRequestContext, arctx)
	req = req.WithContext(ctx)
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Result().StatusCode)
	assert.Equal(t, sampleJson, strings.TrimSpace(w.Body.String()))
	_ = webhookHandler.Shutdown()
}

func TestRecordWebhookErrors(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mockWebhook.NewMockGateway(ctrl)
	statsStore, err := memstats.New()
	require.NoError(t, err)
	webhookHandler := Setup(mockGW, statsStore)
	reqs := []*webhookT{
		{authContext: &gwtypes.AuthRequestContext{WriteKey: "w1"}},
		{authContext: &gwtypes.AuthRequestContext{WriteKey: "w2"}},
		{authContext: &gwtypes.AuthRequestContext{WriteKey: "w1"}},
		{authContext: &gwtypes.AuthRequestContext{WriteKey: "w3"}},
		{authContext: &gwtypes.AuthRequestContext{WriteKey: "w2"}},
		{authContext: &gwtypes.AuthRequestContext{WriteKey: "w1"}},
	}
	mockGW.EXPECT().NewSourceStat(gomock.Any(), gomock.Any()).DoAndReturn(func(arctx *gwtypes.AuthRequestContext, reqType string) *gwStats.SourceStat {
		switch arctx.WriteKey {
		case "w1":
			return &gwStats.SourceStat{
				Source:      "source1",
				SourceID:    "sourceID1",
				WriteKey:    arctx.WriteKey,
				ReqType:     reqType,
				WorkspaceID: "workspaceID1",
				SourceType:  "webhook1",
			}
		case "w2":
			return &gwStats.SourceStat{
				Source:      "source2",
				SourceID:    "sourceID2",
				WriteKey:    arctx.WriteKey,
				ReqType:     reqType,
				WorkspaceID: "workspaceID2",
				SourceType:  "webhook2",
			}
		case "w3":
			return &gwStats.SourceStat{
				Source:      "source3",
				SourceID:    "sourceID3",
				WriteKey:    arctx.WriteKey,
				ReqType:     reqType,
				WorkspaceID: "workspaceID3",
				SourceType:  "webhook3",
			}
		}
		return nil
	}).Times(3)

	webhookHandler.recordWebhookErrors("cio", "err1", reqs, 400)

	m := statsStore.Get("webhook_num_errors", stats.Tags{
		"writeKey":    "w1",
		"workspaceId": "workspaceID1",
		"sourceID":    "sourceID1",
		"statusCode":  "400",
		"sourceType":  "cio",
		"reason":      "err1",
	})
	require.EqualValues(t, m.LastValue(), 3)
	m = statsStore.Get("webhook_num_errors", stats.Tags{
		"writeKey":    "w2",
		"workspaceId": "workspaceID2",
		"sourceID":    "sourceID2",
		"statusCode":  "400",
		"sourceType":  "cio",
		"reason":      "err1",
	})
	require.EqualValues(t, m.LastValue(), 2)
	m = statsStore.Get("webhook_num_errors", stats.Tags{
		"writeKey":    "w3",
		"workspaceId": "workspaceID3",
		"sourceID":    "sourceID3",
		"statusCode":  "400",
		"sourceType":  "cio",
		"reason":      "err1",
	})
	require.EqualValues(t, m.LastValue(), 1)
}

func TestPrepareTransformerRequest(t *testing.T) {
	type requestOpts struct {
		method  string
		target  string
		body    io.Reader
		params  map[string]string
		headers map[string]string
	}

	createRequest := func(reqOpts requestOpts) *http.Request {
		r := httptest.NewRequest(reqOpts.method, reqOpts.target, reqOpts.body)
		for k, v := range reqOpts.headers {
			r.Header.Set(k, v)
		}

		q := r.URL.Query()
		for k, v := range reqOpts.params {
			q.Add(k, v)
		}
		r.URL.RawQuery = q.Encode()
		return r
	}

	testCases := []struct {
		name               string
		req                *http.Request
		includeQueryParams bool
		wantError          bool
		expectedResponse   string
	}{
		{
			name:             "Empty request body with no query parameters and no headers",
			req:              createRequest(requestOpts{method: http.MethodPost, target: "http://example.com"}),
			expectedResponse: `{"method":"POST","url":"/","proto":"HTTP/1.1","headers":{},"body":"{}","query_parameters":{}}`,
		},
		{
			name:             "Empty request body with query parameters and no headers",
			req:              createRequest(requestOpts{method: http.MethodPost, target: "http://example.com", params: map[string]string{"key": "value"}}),
			expectedResponse: `{"method":"POST","url":"/?key=value","proto":"HTTP/1.1","headers":{},"body":"{}","query_parameters":{"key":["value"]}}`,
		},
		{
			name:             "Some payload with no query parameters and no headers",
			req:              createRequest(requestOpts{method: http.MethodPost, target: "http://example.com", body: strings.NewReader(`{"key":"value"}`)}),
			expectedResponse: `{"method":"POST","url":"/","proto":"HTTP/1.1","headers":{},"body":"{\"key\":\"value\"}","query_parameters":{}}`,
		},
		{
			name:             "Empty request body with headers and no query parameters",
			req:              createRequest(requestOpts{method: http.MethodPost, target: "http://example.com", headers: map[string]string{"content-type": "application/json"}}),
			expectedResponse: `{"method":"POST","url":"/","proto":"HTTP/1.1","headers":{"Content-Type":["application/json"]},"body":"{}","query_parameters":{}}`,
		},
		{
			name:             "Empty request body with headers and query parameters",
			req:              createRequest(requestOpts{method: http.MethodPost, target: "http://example.com", params: map[string]string{"key": "value"}, headers: map[string]string{"content-type": "application/json"}}),
			expectedResponse: `{"method":"POST","url":"/?key=value","proto":"HTTP/1.1","headers":{"Content-Type":["application/json"]},"body":"{}","query_parameters":{"key":["value"]}}`,
		},
		{
			name:             "Some payload with headers and no parameters",
			req:              createRequest(requestOpts{method: http.MethodPost, target: "http://example.com", body: strings.NewReader(`{"key":"value"}`), headers: map[string]string{"content-type": "application/json"}}),
			expectedResponse: `{"method":"POST","url":"/","proto":"HTTP/1.1","headers":{"Content-Type":["application/json"]},"body":"{\"key\":\"value\"}","query_parameters":{}}`,
		},
		{
			name:             "Some payload with parameters and no headers",
			req:              createRequest(requestOpts{method: http.MethodPost, target: "http://example.com", body: strings.NewReader(`{"key":"value"}`), params: map[string]string{"key": "value"}}),
			expectedResponse: `{"method":"POST","url":"/?key=value","proto":"HTTP/1.1","headers":{},"body":"{\"key\":\"value\"}","query_parameters":{"key":["value"]}}`,
		},
		{
			name:             "Error reading request body",
			req:              createRequest(requestOpts{method: http.MethodPost, target: "http://example.com", body: iotest.ErrReader(errors.New("some error"))}),
			wantError:        true,
			expectedResponse: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := prepareTransformerRequestBody(tc.req)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expectedResponse, string(result))
		})
	}
}

func TestAllowGetReqForWebhookSrc(t *testing.T) {
	cases := []struct {
		name                       string
		forwardGetRequestForSrcMap map[string]struct{}
		method                     string
		srcDef                     string
		expected                   bool
	}{
		{
			name:   "should allow get request for adjust",
			method: http.MethodGet,
			forwardGetRequestForSrcMap: map[string]struct{}{
				"adjust": {},
			},
			srcDef:   "adjust",
			expected: false,
		},
		{
			name:   "should allow post request for adjust",
			method: http.MethodPost,
			forwardGetRequestForSrcMap: map[string]struct{}{
				"adjust": {},
			},
			srcDef:   "adjust",
			expected: false,
		},
		{
			name: "should not allow get request for shopify",
			forwardGetRequestForSrcMap: map[string]struct{}{
				"adjust":     {},
				"customerio": {},
			},
			method:   http.MethodGet,
			srcDef:   "Shopify",
			expected: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wbh := HandleT{}
			wbh.config.forwardGetRequestForSrcMap = tc.forwardGetRequestForSrcMap

			isGetAndNotAllow := wbh.IsGetAndNotAllow(tc.method, tc.srcDef)
			require.Equal(t, tc.expected, isGetAndNotAllow)
		})
	}
}
