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
	"github.com/hashicorp/go-retryablehttp"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/gateway/response"
	mock_webhook "github.com/rudderlabs/rudder-server/mocks/gateway/webhook"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/assert"
)

const (
	sampleWriteKey = "SampleWriteKey"
	sourceDefName  = "webhook"
	sampleError    = "someError"
	sampleOutput   = "{\"output\":true}"
)

var (
	whStats           *webhookStatsT
	once              sync.Once
	transformerServer = httptest.NewServer(http.HandlerFunc(transformMockHandler))
)

func initWebhook() {
	once.Do(func() {
		config.Load()
		logger.Init()
		misc.Init()
		Init()
		stats.DefaultStats = &stats.HandleT{}
		whStats = newWebhookStats()
		sourceTransformerURL = transformerServer.URL
	})
}

func createWebhookHandler(gwHandle GatewayI) *HandleT {
	return &HandleT{
		requestQ:      map[string]chan *webhookT{},
		batchRequestQ: make(chan *batchWebhookT),
		netClient:     retryablehttp.NewClient(),
		gwHandle:      gwHandle,
	}
}

func transformMockHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	reqBody, _ := io.ReadAll(r.Body)
	_, err := w.Write(reqBody)
	if err != nil {
		pkgLogger.Error(err)
	}
}

func TestWebhookRequestHandlerErrorCase(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mock_webhook.NewMockGatewayI(ctrl)
	webhookHandler := createWebhookHandler(mockGW)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook?writeKey="+sampleWriteKey, nil)
	w := httptest.NewRecorder()
	mockGW.EXPECT().IncrementRecvCount(gomock.Any()).Times(1)
	mockGW.EXPECT().IncrementAckCount(gomock.Any()).Times(1)
	mockGW.EXPECT().GetWebhookSourceDefName(sampleWriteKey).Return(sourceDefName, true)
	webhookHandler.requestQ[sourceDefName] = make(chan *webhookT)

	// Error case
	go func() {
		whReq := <-webhookHandler.requestQ[sourceDefName]
		whReq.done <- transformerResponse{
			Err:        sampleError,
			StatusCode: http.StatusBadRequest,
		}
	}()
	mockGW.EXPECT().TrackRequestMetrics(sampleError).Times(1)
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, w.Result().StatusCode, http.StatusBadRequest)
	assert.Equal(t, strings.TrimSpace(w.Body.String()), sampleError)
}

func TestWebhookRequestHandlerNonEmptyOutputCase(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mock_webhook.NewMockGatewayI(ctrl)
	webhookHandler := createWebhookHandler(mockGW)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook?writeKey="+sampleWriteKey, nil)
	w := httptest.NewRecorder()
	mockGW.EXPECT().IncrementRecvCount(gomock.Any()).Times(1)
	mockGW.EXPECT().IncrementAckCount(gomock.Any()).Times(1)
	mockGW.EXPECT().GetWebhookSourceDefName(sampleWriteKey).Return(sourceDefName, true)
	webhookHandler.requestQ[sourceDefName] = make(chan *webhookT)

	go func() {
		whReq := <-webhookHandler.requestQ[sourceDefName]
		whReq.done <- transformerResponse{
			OutputToSource: &outputToSource{
				Body:        []byte(sampleOutput),
				ContentType: "application/json",
			},
		}
	}()
	mockGW.EXPECT().TrackRequestMetrics("").Times(1)
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, w.Result().StatusCode, http.StatusOK)
	assert.Equal(t, strings.TrimSpace(w.Body.String()), sampleOutput)
}

func TestWebhookRequestHandlerEmptyOutputCase(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mock_webhook.NewMockGatewayI(ctrl)
	webhookHandler := createWebhookHandler(mockGW)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhook?writeKey="+sampleWriteKey, nil)
	w := httptest.NewRecorder()
	mockGW.EXPECT().IncrementRecvCount(gomock.Any()).Times(1)
	mockGW.EXPECT().IncrementAckCount(gomock.Any()).Times(1)
	mockGW.EXPECT().GetWebhookSourceDefName(sampleWriteKey).Return(sourceDefName, true)
	webhookHandler.requestQ[sourceDefName] = make(chan *webhookT)

	go func() {
		whReq := <-webhookHandler.requestQ[sourceDefName]
		whReq.done <- transformerResponse{}
	}()
	mockGW.EXPECT().TrackRequestMetrics("").Times(1)
	webhookHandler.RequestHandler(w, req)

	assert.Equal(t, w.Result().StatusCode, http.StatusOK)
	assert.Equal(t, strings.TrimSpace(w.Body.String()), response.Ok)
}

func TestBatchTransformLoopWithError(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mock_webhook.NewMockGatewayI(ctrl)
	webhookHandler := createWebhookHandler(mockGW)
	w := httptest.NewRecorder()
	bt := batchWebhookTransformerT{
		webhook: webhookHandler,
		stats:   whStats,
	}

	var whResp transformerResponse
	whInputResp := transformerResponse{
		Err:        sampleError,
		StatusCode: http.StatusBadRequest,
	}

	go func() {
		done := make(chan transformerResponse)
		sampleInput, _ := json.Marshal(whInputResp)
		req := httptest.NewRequest(http.MethodPost, "/transform", bytes.NewBuffer(sampleInput))
		whReq := &webhookT{
			request:    req,
			writer:     w,
			done:       done,
			sourceType: sourceDefName,
			writeKey:   sampleWriteKey,
		}
		bt.webhook.batchRequestQ <- &batchWebhookT{
			batchRequest: []*webhookT{whReq},
			sourceType:   sourceDefName,
		}
		defer close(bt.webhook.batchRequestQ)
		whResp = <-done
	}()

	bt.batchTransformLoop()
	assert.Equal(t, whInputResp, whResp)
}

func TestBatchTransformLoopWithOutput(t *testing.T) {
	initWebhook()
	ctrl := gomock.NewController(t)
	mockGW := mock_webhook.NewMockGatewayI(ctrl)
	webhookHandler := createWebhookHandler(mockGW)

	w := httptest.NewRecorder()

	bt := batchWebhookTransformerT{
		webhook: webhookHandler,
		stats:   whStats,
	}

	var whResp transformerResponse
	whInputResp := transformerResponse{
		Output: map[string]interface{}{"messgage": "hello world"},
		OutputToSource: &outputToSource{
			Body:        []byte(sampleOutput),
			ContentType: "application/json",
		},
	}
	go func() {
		done := make(chan transformerResponse)
		sampleInput, _ := json.Marshal(whInputResp)
		req := httptest.NewRequest(http.MethodPost, "/transform", bytes.NewBuffer(sampleInput))
		whReq := &webhookT{
			request:    req,
			writer:     w,
			done:       done,
			sourceType: sourceDefName,
			writeKey:   sampleWriteKey,
		}
		bt.webhook.batchRequestQ <- &batchWebhookT{
			batchRequest: []*webhookT{whReq},
			sourceType:   sourceDefName,
		}
		defer close(bt.webhook.batchRequestQ)
		whResp = <-done
	}()
	mockGW.EXPECT().ProcessWebRequest(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)

	bt.batchTransformLoop()
	assert.Equal(t, whInputResp, whResp)
}
