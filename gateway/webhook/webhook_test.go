package webhook

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/gateway/response"
	mock_webhook "github.com/rudderlabs/rudder-server/mocks/gateway/webhook"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/assert"
)

const (
	sampleWriteKey = "SampleWriteKey"
	sourceDefName  = "webhook"
	sampleError    = "someError"
	sampleJson     = "{\"hello\":\"world\"}"
)

var (
	whStats         *webhookStatsT
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
		Init()
		maxTransformerProcess = 1
		whStats = newWebhookStats()
	})
}

func TestWebhookRequestHandlerWithTransformerBatchGeneralError(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mock_webhook.NewMockGatewayI(ctrl)
	transformerServer := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, sampleError, http.StatusBadRequest)
		}))
	webhookHandler := Setup(mockGW, func(bt *batchWebhookTransformerT) {
		bt.sourceTransformerURL = transformerServer.URL
	})

	mockGW.EXPECT().IncrementRecvCount(gomock.Any()).Times(1)
	mockGW.EXPECT().IncrementAckCount(gomock.Any()).Times(1)
	mockGW.EXPECT().GetWebhookSourceDefName(sampleWriteKey).Return(sourceDefName, true)
	mockGW.EXPECT().TrackRequestMetrics(gomock.Any()).Times(1)

	webhookHandler.Register(sourceDefName)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook?writeKey="+sampleWriteKey, bytes.NewBufferString(sampleJson))
	w := httptest.NewRecorder()
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Result().StatusCode)
	assert.Contains(t, strings.TrimSpace(w.Body.String()), "source Transformer returned non-success status")
	_ = webhookHandler.Shutdown()
}

func TestWebhookRequestHandlerWithTransformerBatchPayloadLengthMismatchError(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mock_webhook.NewMockGatewayI(ctrl)
	transformerServer := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			body, _ := io.ReadAll(r.Body)
			var requests []interface{}
			_ = json.Unmarshal(body, &requests)
			responses := []transformerResponse{}
			// return payload of length = len(requests) + 1
			for i := 0; i < len(requests)+1; i++ {
				responses = append(responses, transformerResponse{
					Err:        sampleError,
					StatusCode: http.StatusBadRequest,
				})
			}
			respBody, _ := json.Marshal(responses)
			_, _ = w.Write(respBody)
		}))
	webhookHandler := Setup(mockGW, func(bt *batchWebhookTransformerT) {
		bt.sourceTransformerURL = transformerServer.URL
	})

	mockGW.EXPECT().IncrementRecvCount(gomock.Any()).Times(1)
	mockGW.EXPECT().IncrementAckCount(gomock.Any()).Times(1)
	mockGW.EXPECT().GetWebhookSourceDefName(sampleWriteKey).Return(sourceDefName, true)
	mockGW.EXPECT().TrackRequestMetrics(gomock.Any()).Times(1)

	webhookHandler.Register(sourceDefName)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook?writeKey="+sampleWriteKey, bytes.NewBufferString(sampleJson))
	w := httptest.NewRecorder()
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Result().StatusCode)
	assert.Contains(t, strings.TrimSpace(w.Body.String()), response.SourceTransformerInvalidResponseFormat)
	_ = webhookHandler.Shutdown()
}

func TestWebhookRequestHandlerWithTransformerRequestError(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mock_webhook.NewMockGatewayI(ctrl)
	transformerServer := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			body, _ := io.ReadAll(r.Body)
			var requests []interface{}
			_ = json.Unmarshal(body, &requests)
			responses := []transformerResponse{}
			for i := 0; i < len(requests); i++ {
				responses = append(responses, transformerResponse{
					Err:        sampleError,
					StatusCode: http.StatusBadRequest,
				})
			}
			respBody, _ := json.Marshal(responses)
			_, _ = w.Write(respBody)
		}))
	webhookHandler := Setup(mockGW, func(bt *batchWebhookTransformerT) {
		bt.sourceTransformerURL = transformerServer.URL
	})

	mockGW.EXPECT().IncrementRecvCount(gomock.Any()).Times(1)
	mockGW.EXPECT().IncrementAckCount(gomock.Any()).Times(1)
	mockGW.EXPECT().GetWebhookSourceDefName(sampleWriteKey).Return(sourceDefName, true)
	mockGW.EXPECT().TrackRequestMetrics(gomock.Any()).Times(1)

	webhookHandler.Register(sourceDefName)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook?writeKey="+sampleWriteKey, bytes.NewBufferString(sampleJson))
	w := httptest.NewRecorder()
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Result().StatusCode)
	assert.Contains(t, sampleError, strings.TrimSpace(w.Body.String()))
	_ = webhookHandler.Shutdown()
}

func TestWebhookRequestHandlerWithOutputToSource(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mock_webhook.NewMockGatewayI(ctrl)
	transformerServer := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			body, _ := io.ReadAll(r.Body)
			var requests []interface{}
			_ = json.Unmarshal(body, &requests)
			responses := []transformerResponse{}
			for i := 0; i < len(requests); i++ {
				responses = append(responses, transformerResponse{
					OutputToSource: outputToWebhook,
					StatusCode:     http.StatusOK,
				})
			}
			respBody, _ := json.Marshal(responses)
			_, _ = w.Write(respBody)
		}))
	webhookHandler := Setup(mockGW, func(bt *batchWebhookTransformerT) {
		bt.sourceTransformerURL = transformerServer.URL
	})
	mockGW.EXPECT().IncrementRecvCount(gomock.Any()).Times(1)
	mockGW.EXPECT().IncrementAckCount(gomock.Any()).Times(1)
	mockGW.EXPECT().GetWebhookSourceDefName(sampleWriteKey).Return(sourceDefName, true)
	mockGW.EXPECT().TrackRequestMetrics("").Times(1)

	webhookHandler.Register(sourceDefName)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook?writeKey="+sampleWriteKey, bytes.NewBufferString(sampleJson))
	w := httptest.NewRecorder()
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Result().StatusCode)
	assert.Equal(t, sampleJson, strings.TrimSpace(w.Body.String()))
	_ = webhookHandler.Shutdown()
}

func TestWebhookRequestHandlerWithOutputToGateway(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mock_webhook.NewMockGatewayI(ctrl)
	outputToGateway := map[string]interface{}{"text": "hello world"}
	transformerServer := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			body, _ := io.ReadAll(r.Body)
			var requests []interface{}
			_ = json.Unmarshal(body, &requests)
			responses := []transformerResponse{}
			for i := 0; i < len(requests); i++ {
				responses = append(responses, transformerResponse{
					Output:     outputToGateway,
					StatusCode: http.StatusOK,
				})
			}
			respBody, _ := json.Marshal(responses)
			_, _ = w.Write(respBody)
		}))
	webhookHandler := Setup(mockGW, func(bt *batchWebhookTransformerT) {
		bt.sourceTransformerURL = transformerServer.URL
	})
	mockGW.EXPECT().IncrementRecvCount(gomock.Any()).Times(1)
	mockGW.EXPECT().IncrementAckCount(gomock.Any()).Times(1)
	mockGW.EXPECT().GetWebhookSourceDefName(sampleWriteKey).Return(sourceDefName, true)
	mockGW.EXPECT().TrackRequestMetrics("").Times(1)
	gwPayload, _ := json.Marshal(outputToGateway)
	mockGW.EXPECT().ProcessWebRequest(gomock.Any(), gomock.Any(), "batch", gwPayload, sampleWriteKey).Times(1)

	webhookHandler.Register(sourceDefName)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook?writeKey="+sampleWriteKey, bytes.NewBufferString(sampleJson))
	w := httptest.NewRecorder()
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Result().StatusCode)
	assert.Equal(t, response.Ok, strings.TrimSpace(w.Body.String()))
	_ = webhookHandler.Shutdown()
}

func TestWebhookRequestHandlerWithOutputToGatewayAndSource(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mock_webhook.NewMockGatewayI(ctrl)
	transformerServer := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()
			body, _ := io.ReadAll(r.Body)
			var requests []interface{}
			_ = json.Unmarshal(body, &requests)
			responses := []transformerResponse{}
			for i := 0; i < len(requests); i++ {
				responses = append(responses, transformerResponse{
					Output:         outputToGateway,
					OutputToSource: outputToWebhook,
					StatusCode:     http.StatusOK,
				})
			}
			respBody, _ := json.Marshal(responses)
			_, _ = w.Write(respBody)
		}))
	webhookHandler := Setup(mockGW, func(bt *batchWebhookTransformerT) {
		bt.sourceTransformerURL = transformerServer.URL
	})
	mockGW.EXPECT().IncrementRecvCount(gomock.Any()).Times(1)
	mockGW.EXPECT().IncrementAckCount(gomock.Any()).Times(1)
	mockGW.EXPECT().GetWebhookSourceDefName(sampleWriteKey).Return(sourceDefName, true)
	mockGW.EXPECT().TrackRequestMetrics("").Times(1)
	gwPayload, _ := json.Marshal(outputToGateway)
	mockGW.EXPECT().ProcessWebRequest(gomock.Any(), gomock.Any(), "batch", gwPayload, sampleWriteKey).Times(1)

	webhookHandler.Register(sourceDefName)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook?writeKey="+sampleWriteKey, bytes.NewBufferString(sampleJson))
	w := httptest.NewRecorder()
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, http.StatusOK, w.Result().StatusCode)
	assert.Equal(t, sampleJson, strings.TrimSpace(w.Body.String()))
	_ = webhookHandler.Shutdown()
}
