package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/retryablehttp"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/gateway/response"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/types"
	"github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type webhookT struct {
	request     *http.Request
	writer      http.ResponseWriter
	done        chan<- transformerResponse
	sourceID    string
	sourceType  string
	authContext *gwtypes.AuthRequestContext
}

type batchWebhookT struct {
	batchRequest []*webhookT
	sourceType   string
}

//go:generate mockgen -destination=../../mocks/gateway/webhook.go -package=mocks_gateway github.com/rudderlabs/rudder-server/gateway/webhook WebhookRequestHandler
type WebhookRequestHandler interface {
	// RequestHandler handles the incoming webhook request
	RequestHandler(w http.ResponseWriter, r *http.Request)
	// Register registers a new webhook source type and starts a goroutine to process requests for that source type
	Register(name string)
	// Shutdown shuts down the webhook handler, closing all channels and waiting for goroutines to finish
	Shutdown() error
}

type HandleT struct {
	logger        logger.Logger
	requestQMu    sync.RWMutex
	requestQ      map[string]chan *webhookT
	batchRequestQ chan *batchWebhookT
	gwHandle      Gateway
	stats         stats.Stats
	ackCount      atomic.Uint64
	recvCount     atomic.Uint64

	batchRequestsWg  sync.WaitGroup
	backgroundWait   func() error
	backgroundCancel context.CancelFunc

	config struct {
		maxReqSize                 config.ValueLoader[int]
		webhookBatchTimeout        config.ValueLoader[time.Duration]
		maxWebhookBatchSize        config.ValueLoader[int]
		sourceListForParsingParams []string
		forwardGetRequestForSrcMap map[string]struct{}
		webhookV2HandlerEnabled    bool
	}
	statReporterCreator StatReporterCreator
	httpClient          retryablehttp.HttpClient
}

type webhookSourceStatT struct {
	id              string
	numEvents       stats.Measurement
	numOutputEvents stats.Measurement
	sourceTransform stats.Measurement
}

type webhookStatsT struct {
	sentStat           stats.Measurement
	receivedStat       stats.Measurement
	failedStat         stats.Measurement
	transformTimerStat stats.Measurement
	sourceStats        map[string]*webhookSourceStatT
}

type batchWebhookTransformerT struct {
	webhook                *HandleT
	stats                  *webhookStatsT
	statsFactory           stats.Stats
	sourceTransformAdapter func(ctx context.Context) (sourceTransformAdapter, error)
}

type batchTransformerOption func(bt *batchWebhookTransformerT)

func (webhook *HandleT) failRequest(w http.ResponseWriter, r *http.Request, reason string, code int) {
	statusCode := http.StatusBadRequest
	if code != 0 {
		statusCode = code
	}
	webhook.logger.Infon("IP -- Response",
		logger.NewStringField("ip", kithttputil.GetRequestIP(r)),
		logger.NewStringField("path", r.URL.Path),
		logger.NewIntField("code", int64(code)),
		logger.NewStringField("reason", reason))
	http.Error(w, reason, statusCode)
}

func (wb *HandleT) IsGetAndNotAllow(reqMethod, sourceDefName string) bool {
	_, ok := wb.config.forwardGetRequestForSrcMap[strings.ToLower(sourceDefName)]
	return reqMethod == http.MethodGet && !ok
}

func (webhook *HandleT) RequestHandler(w http.ResponseWriter, r *http.Request) {
	reqType := r.Context().Value(gwtypes.CtxParamCallType).(string)
	arctx := r.Context().Value(gwtypes.CtxParamAuthRequestContext).(*gwtypes.AuthRequestContext)
	webhook.logger.LogRequest(r)
	webhook.recvCount.Add(1)
	sourceDefName := arctx.SourceDefName

	var postFrom url.Values
	var multipartForm *multipart.Form

	if webhook.IsGetAndNotAllow(r.Method, sourceDefName) {
		return
	}
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(strings.ToLower(contentType), "application/x-www-form-urlencoded") {
		if err := r.ParseForm(); err != nil {
			stat := webhook.statReporterCreator(arctx, reqType)
			stat.RequestFailed("couldNotParseForm")
			stat.Report(webhook.stats)

			webhook.failRequest(
				w,
				r,
				response.GetStatus(response.ErrorInParseForm),
				response.GetErrorStatusCode(response.ErrorInParseForm),
			)
			webhook.ackCount.Add(1)
			return
		}
		postFrom = r.PostForm
	} else if strings.Contains(strings.ToLower(contentType), "multipart/form-data") {
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			stat := webhook.statReporterCreator(arctx, reqType)
			stat.RequestFailed("couldNotParseMultiform")
			stat.Report(webhook.stats)

			webhook.failRequest(
				w,
				r,
				response.GetStatus(response.ErrorInParseMultiform),
				response.GetErrorStatusCode(response.ErrorInParseMultiform),
			)
			webhook.ackCount.Add(1)
			return
		}
		multipartForm = r.MultipartForm
	}

	var jsonByte []byte
	var err error

	if r.MultipartForm != nil {
		jsonByte, err = jsonrs.Marshal(multipartForm)
		if err != nil {
			stat := webhook.statReporterCreator(arctx, reqType)
			stat.RequestFailed("couldNotMarshal")
			stat.Report(webhook.stats)
			webhook.failRequest(
				w,
				r,
				response.GetStatus(response.ErrorInMarshal),
				response.GetErrorStatusCode(response.ErrorInMarshal),
			)
			webhook.ackCount.Add(1)
			return
		}
	} else if len(postFrom) != 0 {
		jsonByte, err = jsonrs.Marshal(postFrom)
		if err != nil {
			stat := webhook.statReporterCreator(arctx, reqType)
			stat.RequestFailed("couldNotMarshal")
			stat.Report(webhook.stats)
			webhook.failRequest(
				w,
				r,
				response.GetStatus(response.ErrorInMarshal),
				response.GetErrorStatusCode(response.ErrorInMarshal),
			)
			webhook.ackCount.Add(1)
			return
		}
	}

	if len(jsonByte) != 0 {
		r.Body = io.NopCloser(bytes.NewReader(jsonByte))
		r.Header.Set("Content-Type", "application/json")
	}

	done := make(chan transformerResponse)
	req := webhookT{
		request:     r,
		writer:      w,
		done:        done,
		sourceType:  sourceDefName,
		sourceID:    arctx.SourceID,
		authContext: arctx,
	}
	if webhook.config.webhookV2HandlerEnabled {
		webhook.Register(sourceDefName)
	}
	webhook.requestQMu.RLock()
	requestQ := webhook.requestQ[sourceDefName]
	requestQ <- &req
	webhook.requestQMu.RUnlock()

	// Wait for batcher process to be done
	resp := <-done
	webhook.ackCount.Add(1)
	webhook.gwHandle.TrackRequestMetrics(resp.Err)

	ss := webhook.statReporterCreator(arctx, reqType)

	if resp.Err != "" {
		code := http.StatusBadRequest
		if resp.StatusCode != 0 {
			code = resp.StatusCode
		}
		webhook.logger.Infon("IP -- Response",
			logger.NewStringField("ip", kithttputil.GetRequestIP(r)),
			logger.NewStringField("path", r.URL.Path),
			logger.NewIntField("code", int64(code)),
			logger.NewStringField("error", resp.Err))
		http.Error(w, resp.Err, code)
		if resp.StatusCode == http.StatusTooManyRequests {
			ss.RequestDropped()
		} else {
			failureReason := getWebhookFailureReason(resp.Err, resp.StatusCode)
			ss.RequestFailed(failureReason)
		}
		ss.Report(webhook.stats)
		return
	}

	payload := []byte(response.Ok)
	if resp.OutputToSource != nil {
		payload = resp.OutputToSource.Body
		w.Header().Set("Content-Type", resp.OutputToSource.ContentType)
	}
	webhook.logger.Debugn("IP -- Response: 200",
		logger.NewStringField("ip", kithttputil.GetRequestIP(r)),
		logger.NewStringField("path", r.URL.Path),
		logger.NewStringField("status", response.GetStatus(response.Ok)))
	_, _ = w.Write(payload)
	ss.RequestSucceeded()
	ss.Report(webhook.stats)
}

func getWebhookFailureReason(errMsg string, statusCode int) string {
	switch {
	case errMsg == response.RequestBodyTooLarge:
		return response.RequestBodyTooLarge
	case statusCode != 0:
		return response.SourceTransformerNonSuccessResponse
	case errMsg != "":
		return response.SourceTransformerResponseError
	default:
		return response.SourceTransformerResponseError
	}
}

func (webhook *HandleT) batchRequests(sourceDef string, requestQ chan *webhookT) {
	reqBuffer := make([]*webhookT, 0)
	timeout := time.After(webhook.config.webhookBatchTimeout.Load())
	for {
		select {
		case req, hasMore := <-requestQ:
			if !hasMore {
				if len(reqBuffer) > 0 {
					// If there are requests in the buffer, send them to the batcher
					breq := batchWebhookT{batchRequest: reqBuffer, sourceType: sourceDef}
					webhook.batchRequestQ <- &breq
				}
				return
			}

			// Append to request buffer
			reqBuffer = append(reqBuffer, req)
			if len(reqBuffer) == webhook.config.maxWebhookBatchSize.Load() {
				breq := batchWebhookT{batchRequest: reqBuffer, sourceType: sourceDef}
				webhook.batchRequestQ <- &breq
				reqBuffer = make([]*webhookT, 0)
			}
		case <-timeout:
			timeout = time.After(webhook.config.webhookBatchTimeout.Load())
			if len(reqBuffer) > 0 {
				breq := batchWebhookT{batchRequest: reqBuffer, sourceType: sourceDef}
				webhook.batchRequestQ <- &breq
				reqBuffer = make([]*webhookT, 0)
			}
		}
	}
}

// TODO : return back immediately for blank request body. its waiting till timeout
func (bt *batchWebhookTransformerT) batchTransformLoop() {
	for breq := range bt.webhook.batchRequestQ {
		// If unable to fetch features from transformer, send GatewayTimeout to all requests
		// TODO: Remove timeout from here after timeout handler is added in gateway
		ctx, cancel := context.WithTimeout(context.Background(), config.GetDurationVar(10, time.Second, "WriteTimeout", "WriteTimeOutInSec"))
		sourceTransformAdapter, err := bt.sourceTransformAdapter(ctx)
		if err != nil {
			bt.webhook.logger.Errorn("webhook source transformation failed",
				obskit.SourceType(breq.sourceType),
				obskit.Error(err))
			bt.webhook.recordWebhookErrors(breq.sourceType, err.Error(), breq.batchRequest, response.GetErrorStatusCode(err.Error()))
			for _, req := range breq.batchRequest {
				req.done <- transformerResponse{StatusCode: response.GetErrorStatusCode(response.GatewayTimeout), Err: response.GetStatus(response.GatewayTimeout)}
			}
			cancel()
			continue
		}
		cancel()

		transformerURL, err := sourceTransformAdapter.getTransformerURL(breq.sourceType)
		if err != nil {
			bt.webhook.logger.Errorn("webhook source transformation failed",
				obskit.SourceType(breq.sourceType),
				obskit.Error(err))
			bt.webhook.recordWebhookErrors(breq.sourceType, err.Error(), breq.batchRequest, response.GetErrorStatusCode(response.ServiceUnavailable))
			for _, req := range breq.batchRequest {
				req.done <- transformerResponse{StatusCode: response.GetErrorStatusCode(response.ServiceUnavailable), Err: response.GetStatus(response.ServiceUnavailable)}
			}
			continue
		}

		var payloadArr [][]byte
		var webRequests []*webhookT
		for _, req := range breq.batchRequest {
			var payload []byte
			var eventRequest []byte

			if sourceTransformAdapter.getAdapterVersion() == transformer.V1 {
				eventRequest, err = prepareTransformerEventRequestV1(req.request, breq.sourceType, bt.webhook.config.sourceListForParsingParams)
			} else {
				eventRequest, err = prepareTransformerEventRequestV2(req.request)
			}

			if err == nil {
				eventRequest, err = misc.SanitizeJSON(eventRequest)
				if err != nil {
					bt.webhook.logger.Errorn("invalid payload", obskit.Error(err), obskit.SourceType(breq.sourceType), obskit.SourceID(req.sourceID))
					err = errors.New(response.InvalidJSON)
				}
			}

			if err == nil && !json.Valid(eventRequest) {
				err = errors.New(response.InvalidJSON)
			}
			if err == nil && len(eventRequest) > bt.webhook.config.maxReqSize.Load() {
				err = errors.New(response.RequestBodyTooLarge)
			}
			if err == nil {
				payload, err = sourceTransformAdapter.getTransformerEvent(req.authContext, eventRequest)
			}

			if err != nil {
				bt.webhook.logger.Errorn("webhook source transformation failed for sourceID",
					obskit.SourceType(req.sourceType),
					obskit.SourceID(req.sourceID),
					obskit.Error(err))
				bt.webhook.countWebhookErrors(breq.sourceType, req.authContext, err.Error(), response.GetErrorStatusCode(err.Error()), 1)
				req.done <- transformerResponse{Err: response.GetStatus(err.Error()), StatusCode: response.GetErrorStatusCode(err.Error())}
				continue
			}

			payloadArr = append(payloadArr, payload)

			webRequests = append(webRequests, req)
		}

		if len(payloadArr) == 0 {
			continue
		}

		if _, ok := bt.stats.sourceStats[breq.sourceType]; !ok {
			bt.stats.sourceStats[breq.sourceType] = bt.newWebhookStat(breq.sourceType)
		}

		// stats
		bt.stats.sourceStats[breq.sourceType].numEvents.Count(len(payloadArr))

		transformStart := time.Now()
		batchResponse := bt.transform(payloadArr, transformerURL)

		// stats
		bt.stats.sourceStats[breq.sourceType].sourceTransform.Since(transformStart)

		var reason string
		if batchResponse.batchError == nil && len(batchResponse.responses) != len(payloadArr) {
			batchResponse.batchError = errors.New("webhook batch transform response events size does not equal sent events size")
			reason = "in out mismatch"
			bt.webhook.logger.Errorn("webhook batch transform response events size does not equal sent events size",
				obskit.Error(batchResponse.batchError))
		}
		if batchResponse.batchError != nil {
			if reason == "" {
				reason = "batch response error"
			}
			statusCode := http.StatusInternalServerError
			if batchResponse.statusCode != 0 {
				statusCode = batchResponse.statusCode
			}
			bt.webhook.logger.Errorn("webhook source transformation failed with error and status code",
				obskit.SourceType(breq.sourceType),
				logger.NewIntField("statusCode", int64(statusCode)),
				obskit.Error(batchResponse.batchError))
			bt.webhook.recordWebhookErrors(breq.sourceType, reason, webRequests, statusCode)

			for _, req := range breq.batchRequest {
				req.done <- transformerResponse{StatusCode: statusCode, Err: batchResponse.batchError.Error()}
			}
			continue
		}

		bt.stats.sourceStats[breq.sourceType].numOutputEvents.Count(len(batchResponse.responses))

		for idx, resp := range batchResponse.responses {
			webRequest := webRequests[idx]
			if resp.Err == "" && resp.Output != nil {
				var errMessage, reason string
				outputPayload, err := jsonrs.Marshal(resp.Output)
				if err != nil {
					errMessage = response.SourceTransformerInvalidOutputFormatInResponse
					reason = "marshal error"
				} else {
					errMessage = bt.webhook.enqueueInGateway(webRequest, outputPayload)
					reason = "enqueueInGateway failed"
				}
				if errMessage != "" {
					bt.webhook.logger.Errorn("webhook source transformation failed",
						obskit.SourceType(breq.sourceType),
						logger.NewStringField("errorMessage", errMessage))
					bt.webhook.countWebhookErrors(breq.sourceType, webRequest.authContext, bt.getWebhookFailureReason(errMessage, reason), response.GetErrorStatusCode(errMessage), 1)
					webRequest.done <- bt.markResponseFail(errMessage)
					continue
				}
			} else if resp.StatusCode != http.StatusOK {
				bt.webhook.logger.Errorn("webhook source transformation failed", obskit.SourceType(breq.sourceType), logger.NewStringField("errorMsg", resp.Err), logger.NewIntField("statusCode", int64(resp.StatusCode)))
				bt.webhook.countWebhookErrors(breq.sourceType, webRequest.authContext, getWebhookFailureReason(resp.Err, resp.StatusCode), resp.StatusCode, 1)
			}
			webRequest.done <- resp
		}
	}
}

func (bt *batchWebhookTransformerT) getWebhookFailureReason(errMessage, reason string) string {
	if reason == "enqueueInGateway failed" {
		switch errMessage {
		case response.TooManyRequests:
			return response.TooManyRequests
		case response.RequestBodyTooLarge:
			return response.RequestBodyTooLarge
		default:
			return reason
		}
	}
	return reason
}

func (webhook *HandleT) enqueueInGateway(req *webhookT, payload []byte) string {
	// replace body with transformed event (it comes in a batch format)
	req.request.Body = io.NopCloser(bytes.NewReader(payload))
	payload, err := io.ReadAll(req.request.Body)
	_ = req.request.Body.Close()
	if err != nil {
		return err.Error()
	}
	return webhook.gwHandle.ProcessTransformedWebhookRequest(&req.writer, req.request, "batch", payload, req.authContext)
}

func (webhook *HandleT) Register(name string) {
	webhook.requestQMu.RLock()
	_, ok := webhook.requestQ[name]
	if ok {
		webhook.requestQMu.RUnlock()
		return
	}
	webhook.requestQMu.RUnlock()

	webhook.requestQMu.Lock()
	defer webhook.requestQMu.Unlock()
	if _, ok := webhook.requestQ[name]; !ok {
		requestQ := make(chan *webhookT)
		webhook.requestQ[name] = requestQ

		webhook.batchRequestsWg.Add(1)
		go (func() {
			defer webhook.batchRequestsWg.Done()
			webhook.batchRequests(name, requestQ)
		})()
	}
}

func (webhook *HandleT) Shutdown() error {
	webhook.backgroundCancel()
	webhook.requestQMu.Lock()
	defer webhook.requestQMu.Unlock()
	for _, q := range webhook.requestQ {
		close(q)
	}
	webhook.batchRequestsWg.Wait()
	close(webhook.batchRequestQ)
	webhook.requestQ = make(map[string]chan *webhookT)

	return webhook.backgroundWait()
}

func (webhook *HandleT) countWebhookErrors(sourceType string, arctx *gwtypes.AuthRequestContext, reason string, statusCode, count int) {
	webhook.stats.NewTaggedStat("webhook_num_errors", stats.CountType, stats.Tags{
		"writeKey":    arctx.WriteKey,
		"workspaceId": arctx.WorkspaceID,
		"sourceID":    arctx.SourceID,
		"statusCode":  strconv.Itoa(statusCode),
		"sourceType":  sourceType,
		"reason":      reason,
	}).Count(count)
}

func (webhook *HandleT) recordWebhookErrors(sourceType, reason string, reqs []*webhookT, statusCode int) {
	authCtxs := lo.SliceToMap(reqs, func(request *webhookT) (string, *gwtypes.AuthRequestContext) {
		return request.authContext.WriteKey, request.authContext
	})
	reqsGroupedBySource := lo.GroupBy(reqs, func(request *webhookT) string {
		return request.authContext.WriteKey
	})
	for writeKey, reqs := range reqsGroupedBySource {
		webhook.countWebhookErrors(sourceType, authCtxs[writeKey], reason, statusCode, len(reqs))
	}
}

// TODO: Check if correct
func (bt *batchWebhookTransformerT) newWebhookStat(sourceType string) *webhookSourceStatT {
	tags := map[string]string{
		"sourceType": sourceType,
	}
	numEvents := bt.statsFactory.NewTaggedStat("webhook_num_events", stats.CountType, tags)
	numOutputEvents := bt.statsFactory.NewTaggedStat("webhook_num_output_events", stats.CountType, tags)
	sourceTransform := bt.statsFactory.NewTaggedStat("webhook_dest_transform", stats.TimerType, tags)
	return &webhookSourceStatT{
		id:              sourceType,
		numEvents:       numEvents,
		numOutputEvents: numOutputEvents,
		sourceTransform: sourceTransform,
	}
}

func (webhook *HandleT) printStats(ctx context.Context) {
	var lastRecvCount, lastAckCount uint64
	for {
		if lastRecvCount != webhook.recvCount.Load() || lastAckCount != webhook.ackCount.Load() {
			lastRecvCount = webhook.recvCount.Load()
			lastAckCount = webhook.ackCount.Load()
			webhook.logger.Debugn("WebhookRequestHandler Recv/Ack ",
				logger.NewIntField("received", int64(webhook.recvCount.Load())),
				logger.NewIntField("ack", int64(webhook.ackCount.Load())))
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}
}
