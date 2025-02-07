package transformer

//go:generate mockgen -destination=../../mocks/router/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/router/transformer Transformer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/sync"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/types"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	cntx "github.com/rudderlabs/rudder-server/services/oauth/v2/context"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
	oauthv2httpclient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	BATCH            = "BATCH"
	ROUTER_TRANSFORM = "ROUTER_TRANSFORM"
	apiVersionHeader = "apiVersion"
)

// handle is the handle for this class
type handle struct {
	tr *http.Transport
	// http client for router transformation request
	client *http.Client
	// Mockable http.client for transformer proxy request
	proxyClient sysUtils.HTTPClientI
	// http client timeout for transformer proxy request
	destinationTimeout time.Duration
	// http client timeout for server-transformer request
	transformTimeout          time.Duration
	transformRequestTimerStat stats.Measurement
	logger                    logger.Logger

	// clientOAuthV2 is the HTTP client for router transformation requests using OAuth V2.
	clientOAuthV2 *http.Client
	// proxyClientOAuthV2 is the mockable HTTP client for transformer proxy requests using OAuth V2.
	proxyClientOAuthV2 sysUtils.HTTPClientI
	// oAuthV2EnabledLoader dynamically loads the OAuth V2 enabled status.
	oAuthV2EnabledLoader config.ValueLoader[bool]
	// expirationTimeDiff holds the configured time difference for token expiration.
	expirationTimeDiff config.ValueLoader[time.Duration]
}

type ProxyRequestMetadata struct {
	JobID         int64           `json:"jobId"`
	AttemptNum    int             `json:"attemptNum"`
	UserID        string          `json:"userId"`
	SourceID      string          `json:"sourceId"`
	DestinationID string          `json:"destinationId"`
	WorkspaceID   string          `json:"workspaceId"`
	Secret        json.RawMessage `json:"secret"`
	DestInfo      json.RawMessage `json:"destInfo,omitempty"`
	DontBatch     bool            `json:"dontBatch"`
}

type ProxyRequestPayload struct {
	integrations.PostParametersT
	Metadata          []ProxyRequestMetadata `json:"metadata"`
	DestinationConfig map[string]interface{} `json:"destinationConfig"`
}

type ProxyRequestParams struct {
	ResponseData ProxyRequestPayload
	DestName     string
	Adapter      transformerProxyAdapter
	DestInfo     *oauthv2.DestinationInfo
	Connection   backendconfig.Connection `json:"connection"`
}

type ProxyRequestResponse struct {
	ProxyRequestStatusCode   int
	ProxyRequestResponseBody string
	RespContentType          string
	RespStatusCodes          map[int64]int
	RespBodys                map[int64]string
	DontBatchDirectives      map[int64]bool
	OAuthErrorCategory       string
}

// Transformer provides methods to transform events
type Transformer interface {
	Transform(transformType string, transformMessage *types.TransformMessageT) []types.DestinationJobT
	ProxyRequest(ctx context.Context, proxyReqParams *ProxyRequestParams) ProxyRequestResponse
}

// NewTransformer creates a new transformer
func NewTransformer(destinationTimeout, transformTimeout time.Duration, backendConfig backendconfig.BackendConfig, oauthV2Enabled config.ValueLoader[bool], expirationTimeDiff config.ValueLoader[time.Duration]) Transformer {
	cache := oauthv2.NewCache()
	oauthLock := kitsync.NewPartitionRWLocker()
	handle := &handle{
		oAuthV2EnabledLoader: oauthV2Enabled,
		expirationTimeDiff:   expirationTimeDiff,
	}
	handle.setup(destinationTimeout, transformTimeout, &cache, oauthLock, backendConfig)
	return handle
}

var loggerOverride logger.Logger

// Transform transforms router jobs to destination jobs
func (trans *handle) Transform(transformType string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
	// Early validation of transform type
	url, err := trans.getTransformURL(transformType)
	if err != nil {
		return []types.DestinationJobT{}
	}

	// Prepare request payload
	transformMessageCopy, jobs := transformMessage.Dehydrate()
	rawJSON, err := trans.marshalTransformMessage(transformMessageCopy)
	if err != nil {
		return []types.DestinationJobT{}
	}

	// Make request to transformer service
	resp, respData, err := trans.makeTransformRequest(url, rawJSON, transformMessageCopy)
	if err != nil {
		return trans.handleTransformError(err, transformMessage)
	}
	defer httputil.CloseResponse(resp)

	// Handle non-200 responsess
	if resp.StatusCode != http.StatusOK {
		return trans.handleNon200Response(resp, respData, transformMessage)
	}

	// Process successful response
	destinationJobs, err := trans.processTransformResponse(resp, respData, transformType, transformMessage)
	if err != nil {
		return trans.handleTransformError(err, transformMessage)
	}

	destinationJobs.Hydrate(jobs)
	return destinationJobs
}

func (trans *handle) getTransformURL(transformType string) (string, error) {
	switch transformType {
	case BATCH:
		return getBatchURL(), nil
	case ROUTER_TRANSFORM:
		return getRouterTransformURL(), nil
	default:
		return "", fmt.Errorf("unexpected transform type: %s", transformType)
	}
}

func (trans *handle) marshalTransformMessage(msg interface{}) ([]byte, error) {
	rawJSON, err := jsonfast.Marshal(msg)
	if err != nil {
		trans.logger.Errorf("problematic input for marshalling: %#v", msg)
		return nil, fmt.Errorf("failed to marshal transform message: %w", err)
	}
	trans.logger.Debugf("[Router Transformer] :: input payload : %s", string(rawJSON))
	return rawJSON, nil
}

func (trans *handle) makeTransformRequest(url string, payload []byte, transformMessage interface{}) (*http.Response, []byte, error) {
	const maxRetries = 30
	var resp *http.Response
	var respData []byte

	for retryCount := 0; retryCount <= maxRetries; retryCount++ {
		req, err := trans.createTransformRequest(url, payload)
		if err != nil {
			return nil, nil, err
		}

		if trans.oAuthV2EnabledLoader.Load() {
			resp, respData, err = trans.makeOAuthRequest(req, transformMessage)
		} else {
			resp, respData, err = trans.makeStandardRequest(req)
		}

		if err == nil {
			return resp, respData, nil
		}

		if retryCount == maxRetries {
			return nil, nil, fmt.Errorf("max retries exceeded: %w", err)
		}

		trans.logger.Errorn(
			"JS HTTP connection error",
			logger.NewErrorField(err),
			logger.NewStringField("URL", url),
		)

		time.Sleep(config.GetDurationVar(100, time.Millisecond, "Processor.retrySleep", "Processor.retrySleepInMS"))
		if resp != nil {
			httputil.CloseResponse(resp)
		}
	}

	return nil, nil, fmt.Errorf("failed to make transform request after retries")
}

func (trans *handle) createTransformRequest(url string, payload []byte) (*http.Request, error) {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("X-Feature-Gzip-Support", "?1")
	req.Header.Set("X-Feature-Filter-Code", "?1")
	return req, nil
}

func (trans *handle) processTransformResponse(resp *http.Response, respData []byte, transformType string, transformMessage *types.TransformMessageT) (types.DestinationJobs, error) {
	// Verify API version
	if err := trans.verifyTransformerVersion(resp); err != nil {
		return nil, err
	}

	var destinationJobs types.DestinationJobs
	var err error

	// Handle OAuth response if enabled
	if trans.oAuthV2EnabledLoader.Load() {
		respData = trans.handleOAuthResponse(respData)
	}

	// Parse response based on transform type
	if transformType == BATCH {
		integrations.CollectIntgTransformErrorStats(respData)
		err = jsonfast.Unmarshal(respData, &destinationJobs)
	} else {
		rawResp := []byte(gjson.GetBytes(respData, "output").Raw)
		integrations.CollectIntgTransformErrorStats(rawResp)
		err = jsonfast.Unmarshal(rawResp, &destinationJobs)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// Validate response
	if err := trans.validateTransformResponse(destinationJobs, transformMessage); err != nil {
		return trans.createErrorResponse(transformMessage, err.Error()), nil
	}

	return destinationJobs, nil
}

func (trans *handle) handleTransformError(err error, transformMessage *types.TransformMessageT) []types.DestinationJobT {
	trans.logger.Error(err.Error())
	stats.Default.NewTaggedStat(`router.transformer.invalid.response`, stats.CountType, stats.Tags{
		"destType": transformMessage.DestType,
		"reason":   "unknown",
	}).Increment()

	statusCode := 500
	if respErr, ok := err.(net.Error); ok && respErr.Timeout() {
		statusCode = http.StatusGatewayTimeout
	}

	destinationJobs := []types.DestinationJobT{}
	for i := range transformMessage.Data {
		routerJob := &transformMessage.Data[i]
		resp := types.DestinationJobT{
			Message:          routerJob.Message,
			JobMetadataArray: []types.JobMetadataT{routerJob.JobMetadata},
			Destination:      routerJob.Destination,
			Connection:       routerJob.Connection,
			StatusCode:       statusCode,
			Error:            err.Error(),
		}
		destinationJobs = append(destinationJobs, resp)
	}
	return destinationJobs
}

func (trans *handle) handleNon200Response(resp *http.Response, respData []byte, transformMessage *types.TransformMessageT) []types.DestinationJobT {
	trans.logger.Errorf("[Router Transfomrer] :: Transformer returned status code: %v reason: %v", resp.StatusCode, resp.Status)

	statusCode := 500
	if resp.StatusCode == http.StatusNotFound {
		statusCode = 404
	}

	destinationJobs := []types.DestinationJobT{}
	for i := range transformMessage.Data {
		routerJob := &transformMessage.Data[i]
		resp := types.DestinationJobT{
			Message:          routerJob.Message,
			JobMetadataArray: []types.JobMetadataT{routerJob.JobMetadata},
			Destination:      routerJob.Destination,
			Connection:       routerJob.Connection,
			StatusCode:       statusCode,
			Error:            string(respData),
		}
		destinationJobs = append(destinationJobs, resp)
	}
	return destinationJobs
}

func (trans *handle) verifyTransformerVersion(resp *http.Response) error {
	transformerAPIVersion, convErr := strconv.Atoi(resp.Header.Get(apiVersionHeader))
	if convErr != nil {
		transformerAPIVersion = 0
	}
	if utilTypes.SupportedTransformerApiVersion != transformerAPIVersion {
		trans.logger.Errorf("Incompatible transformer version: Expected: %d Received: %d, URL: %v", utilTypes.SupportedTransformerApiVersion, transformerAPIVersion, resp.Request.URL)
		return fmt.Errorf("Incompatible transformer version: Expected: %d Received: %d, URL: %v", utilTypes.SupportedTransformerApiVersion, transformerAPIVersion, resp.Request.URL)
	}
	return nil
}

func (trans *handle) handleOAuthResponse(respData []byte) []byte {
	var transResp oauthv2.TransportResponse
	err := json.Unmarshal(respData, &transResp)
	if err == nil && transResp.OriginalResponse != "" {
		respData = []byte(transResp.OriginalResponse)
	}
	return respData
}

func (trans *handle) validateTransformResponse(destinationJobs types.DestinationJobs, transformMessage *types.TransformMessageT) error {
	in := transformMessage.JobIDs()
	var out []int64
	invalid := make(map[int64]struct{})
	for i := range destinationJobs {
		for k, v := range destinationJobs[i].JobIDs() {
			out = append(out, k)
			if _, ok := in[k]; !ok {
				invalid[k] = v
			}
		}
	}
	var invalidResponseReason, invalidResponseError string
	if len(in) != len(out) {
		invalidResponseReason = "in out mismatch"
		invalidResponseError = fmt.Sprintf("Transformer returned invalid output size: %d for input size: %d", len(out), len(in))
	} else if len(invalid) > 0 {
		var invalidSlice []int64
		for k := range invalid {
			invalidSlice = append(invalidSlice, k)
		}
		invalidResponseReason = "invalid jobIDs"
		invalidResponseError = fmt.Sprintf("Transformer returned invalid jobIDs: %v", invalidSlice)
	}

	if invalidResponseReason != "" {
		trans.logger.Error(invalidResponseError)
		stats.Default.NewTaggedStat(`router.transformer.invalid.response`, stats.CountType, stats.Tags{
			"destType": transformMessage.DestType,
			"reason":   invalidResponseReason,
		}).Increment()

		statusCode := 500
		destinationJobs = []types.DestinationJobT{}
		for i := range transformMessage.Data {
			routerJob := &transformMessage.Data[i]
			resp := types.DestinationJobT{
				Message:          routerJob.Message,
				JobMetadataArray: []types.JobMetadataT{routerJob.JobMetadata},
				Destination:      routerJob.Destination,
				Connection:       routerJob.Connection,
				StatusCode:       statusCode,
				Error:            invalidResponseError,
			}
			destinationJobs = append(destinationJobs, resp)
		}
		return fmt.Errorf(invalidResponseError)
	}
	return nil
}

func (trans *handle) createErrorResponse(transformMessage *types.TransformMessageT, reason string) types.DestinationJobs {
	destinationJobs := []types.DestinationJobT{}
	for i := range transformMessage.Data {
		routerJob := &transformMessage.Data[i]
		resp := types.DestinationJobT{
			Message:          routerJob.Message,
			JobMetadataArray: []types.JobMetadataT{routerJob.JobMetadata},
			Destination:      routerJob.Destination,
			Connection:       routerJob.Connection,
			StatusCode:       500,
			Error:            reason,
		}
		destinationJobs = append(destinationJobs, resp)
	}
	return destinationJobs
}

func (trans *handle) ProxyRequest(ctx context.Context, proxyReqParams *ProxyRequestParams) ProxyRequestResponse {
	routerJobResponseCodes := make(map[int64]int)
	routerJobResponseBodys := make(map[int64]string)
	routerJobDontBatchDirectives := make(map[int64]bool)

	if len(proxyReqParams.ResponseData.Metadata) == 0 {
		trans.logger.Warnf(`[TransformerProxy] (Dest-%[1]v) Input metadata is empty`, proxyReqParams.DestName)
		return ProxyRequestResponse{
			ProxyRequestStatusCode:   http.StatusBadRequest,
			ProxyRequestResponseBody: "Input metadata is empty",
			RespContentType:          "text/plain; charset=utf-8",
			RespStatusCodes:          routerJobResponseCodes,
			RespBodys:                routerJobResponseBodys,
			DontBatchDirectives:      routerJobDontBatchDirectives,
		}
	}

	for _, m := range proxyReqParams.ResponseData.Metadata {
		routerJobDontBatchDirectives[m.JobID] = m.DontBatch
	}

	stats.Default.NewTaggedStat("transformer_proxy.delivery_request", stats.CountType, stats.Tags{
		"destType":      proxyReqParams.DestName,
		"workspaceId":   proxyReqParams.ResponseData.Metadata[0].WorkspaceID,
		"destinationId": proxyReqParams.ResponseData.Metadata[0].DestinationID,
	}).Increment()
	trans.logger.Debugf(`[TransformerProxy] (Dest-%[1]v) Proxy Request starts - %[1]v`, proxyReqParams.DestName)

	payload, err := proxyReqParams.Adapter.getPayload(proxyReqParams)
	if err != nil {
		return ProxyRequestResponse{
			ProxyRequestStatusCode:   http.StatusInternalServerError,
			ProxyRequestResponseBody: "Payload preparation failed",
			RespContentType:          "text/plain; charset=utf-8",
			RespStatusCodes:          routerJobResponseCodes,
			RespBodys:                routerJobResponseBodys,
			DontBatchDirectives:      routerJobDontBatchDirectives,
		}
	}
	proxyURL, err := proxyReqParams.Adapter.getProxyURL(proxyReqParams.DestName)
	if err != nil {
		return ProxyRequestResponse{
			ProxyRequestStatusCode:   http.StatusInternalServerError,
			ProxyRequestResponseBody: "ProxyURL preparation failed",
			RespContentType:          "text/plain; charset=utf-8",
			RespStatusCodes:          routerJobResponseCodes,
			RespBodys:                routerJobResponseBodys,
			DontBatchDirectives:      routerJobDontBatchDirectives,
		}
	}

	rdlTime := time.Now()
	httpPrxResp := trans.doProxyRequest(ctx, proxyURL, proxyReqParams, payload)
	respData, respCode, requestError := httpPrxResp.respData, httpPrxResp.statusCode, httpPrxResp.err

	reqSuccessStr := strconv.FormatBool(requestError == nil)
	stats.Default.NewTaggedStat("transformer_proxy.request_latency", stats.TimerType, stats.Tags{"requestSuccess": reqSuccessStr, "destType": proxyReqParams.DestName}).SendTiming(time.Since(rdlTime))
	stats.Default.NewTaggedStat("transformer_proxy.request_result", stats.CountType, stats.Tags{"requestSuccess": reqSuccessStr, "destType": proxyReqParams.DestName}).Increment()

	if requestError != nil {
		return ProxyRequestResponse{
			ProxyRequestStatusCode:   respCode,
			ProxyRequestResponseBody: requestError.Error(),
			RespContentType:          "text/plain; charset=utf-8",
			RespStatusCodes:          routerJobResponseCodes,
			RespBodys:                routerJobResponseBodys,
			DontBatchDirectives:      routerJobDontBatchDirectives,
		}
	}

	/*
		respData will be in ProxyResponseV0 or ProxyResponseV1
	*/
	var transportResponse oauthv2.TransportResponse // response that we get from oauth-interceptor in postRoundTrip
	if trans.oAuthV2EnabledLoader.Load() {
		_ = json.Unmarshal(respData, &transportResponse)
		// unmarshal unsuccessful scenarios
		// if respData is not a valid json
		if transportResponse.OriginalResponse != "" {
			respData = []byte(transportResponse.OriginalResponse)
		}

		if transportResponse.InterceptorResponse.StatusCode > 0 {
			respCode = transportResponse.InterceptorResponse.StatusCode
		}
	}
	/**

		Structure of TransformerProxy Response:
		{
			output: {
				status: [destination status compatible with server]
				message: [ generic message for jobs_db payload]
				destinationResponse: [actual response payload from destination] <-- v0
				response: [actual response payload from destination] <-- v1
			}
		}
	**/
	trans.logger.Debugf("ProxyResponseData: %s\n", string(respData))
	respData = []byte(gjson.GetBytes(respData, "output").Raw)
	integrations.CollectDestErrorStats(respData)

	transResp, err := proxyReqParams.Adapter.getResponse(respData, respCode, proxyReqParams.ResponseData.Metadata)
	if err != nil {
		return ProxyRequestResponse{
			ProxyRequestStatusCode:   respCode,
			ProxyRequestResponseBody: err.Error(),
			RespContentType:          "text/plain; charset=utf-8",
			RespStatusCodes:          routerJobResponseCodes,
			RespBodys:                routerJobResponseBodys,
			DontBatchDirectives:      routerJobDontBatchDirectives,
		}
	}

	if trans.oAuthV2EnabledLoader.Load() {
		for _, metadata := range proxyReqParams.ResponseData.Metadata {
			// Conditions for which InterceptorResponse.StatusCode/Response will not be empty
			// 1. authErrorCategory == CategoryRefreshToken
			// 2. authErrorCategory == CategoryAuthStatusInactive
			// 3. Any error occurred while performing authStatusInactive / RefreshToken
			// Under these conditions, we will have to propagate the response from interceptor to JobsDB
			if transportResponse.InterceptorResponse.StatusCode > 0 {
				transResp.routerJobResponseCodes[metadata.JobID] = transportResponse.InterceptorResponse.StatusCode
			}
			if transportResponse.InterceptorResponse.Response != "" {
				transResp.routerJobResponseBodys[metadata.JobID] = transportResponse.InterceptorResponse.Response
			}
		}
	}
	if trans.oAuthV2EnabledLoader.Load() && transportResponse.InterceptorResponse.Response != "" {
		respData = []byte(transportResponse.InterceptorResponse.Response)
	}

	return ProxyRequestResponse{
		ProxyRequestStatusCode:   respCode,
		ProxyRequestResponseBody: string(respData),
		RespContentType:          "application/json",
		RespStatusCodes:          transResp.routerJobResponseCodes,
		RespBodys:                transResp.routerJobResponseBodys,
		DontBatchDirectives:      transResp.routerJobDontBatchDirectives,
		OAuthErrorCategory:       transResp.authErrorCategory,
	}
}

func (trans *handle) setup(destinationTimeout, transformTimeout time.Duration, cache *oauthv2.Cache, locker *sync.PartitionRWLocker, backendConfig backendconfig.BackendConfig) {
	if loggerOverride == nil {
		trans.logger = logger.NewLogger().Child("router").Child("transformer")
	} else {
		trans.logger = loggerOverride
	}

	trans.tr = &http.Transport{
		DisableKeepAlives:   config.GetBool("Transformer.Client.disableKeepAlives", true),
		MaxConnsPerHost:     config.GetInt("Transformer.Client.maxHTTPConnections", 100),
		MaxIdleConnsPerHost: config.GetInt("Transformer.Client.maxHTTPIdleConnections", 10),
		IdleConnTimeout:     30 * time.Second,
	}
	// The timeout between server and transformer
	// Basically this timeout is more for communication between transformer and server
	trans.transformTimeout = transformTimeout
	// Destination API timeout
	// Basically this timeout we will configure when we make final call to destination to send event
	trans.destinationTimeout = destinationTimeout
	// This client is used for Router Transformation
	trans.client = &http.Client{Transport: trans.tr, Timeout: trans.transformTimeout}
	optionalArgs := &oauthv2httpclient.HttpClientOptionalArgs{
		Locker:             locker,
		Augmenter:          extensions.RouterBodyAugmenter,
		ExpirationTimeDiff: (trans.expirationTimeDiff).Load(),
		Logger:             logger.NewLogger().Child("TransformerHttpClient"),
	}
	// This client is used for Router Transformation using oauthV2
	trans.clientOAuthV2 = oauthv2httpclient.NewOAuthHttpClient(&http.Client{Transport: trans.tr, Timeout: trans.transformTimeout}, common.RudderFlowDelivery, cache, backendConfig, GetAuthErrorCategoryFromTransformResponse, optionalArgs)

	proxyClientOptionalArgs := &oauthv2httpclient.HttpClientOptionalArgs{
		Locker:             locker,
		ExpirationTimeDiff: (trans.expirationTimeDiff).Load(),
		Logger:             logger.NewLogger().Child("TransformerProxyHttpClient"),
	}
	// This client is used for Transformer Proxy(delivered from transformer to destination)
	trans.proxyClient = &http.Client{Transport: trans.tr, Timeout: trans.destinationTimeout + trans.transformTimeout}
	// This client is used for Transformer Proxy(delivered from transformer to destination) using oauthV2
	trans.proxyClientOAuthV2 = oauthv2httpclient.NewOAuthHttpClient(&http.Client{Transport: trans.tr, Timeout: trans.destinationTimeout + trans.transformTimeout}, common.RudderFlowDelivery, cache, backendConfig, GetAuthErrorCategoryFromTransformProxyResponse, proxyClientOptionalArgs)
	trans.transformRequestTimerStat = stats.Default.NewStat("router.transformer_request_time", stats.TimerType)
}

type httpProxyResponse struct {
	respData   []byte
	statusCode int
	err        error
}

func (trans *handle) doProxyRequest(ctx context.Context, proxyUrl string, proxyReqParams *ProxyRequestParams, payload []byte) httpProxyResponse {
	var respData []byte
	destName := proxyReqParams.DestName
	trans.logger.Debugf(`[TransformerProxy] (Dest-%[1]v) Proxy Request payload - %[2]s`, destName, string(payload))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, proxyUrl, bytes.NewReader(payload))
	if err != nil {
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) NewRequestWithContext Failed for %[1]v, with %[3]v`, destName, err.Error())
		return httpProxyResponse{
			respData:   []byte{},
			statusCode: http.StatusInternalServerError,
			err:        err,
		}
	}
	req.Header.Set("Content-Type", "application/json")
	trans.logger.Debugf("[TransformerProxy] Timeout for %[1]s = %[2]v ms \n", destName, strconv.FormatInt((trans.destinationTimeout+trans.transformTimeout).Milliseconds(), 10))
	// Make use of this header to set timeout in the transfomer's http client
	// The header name may be worked out ?
	req.Header.Set("RdProxy-Timeout", strconv.FormatInt(trans.destinationTimeout.Milliseconds(), 10))
	httpReqStTime := time.Now()
	var resp *http.Response
	if trans.oAuthV2EnabledLoader.Load() {
		trans.logger.Debugn("[router delivery]", logger.NewBoolField("oauthV2Enabled", true))
		req = req.WithContext(cntx.CtxWithDestInfo(req.Context(), proxyReqParams.DestInfo))
		req = req.WithContext(cntx.CtxWithSecret(req.Context(), proxyReqParams.ResponseData.Metadata[0].Secret))
		resp, err = trans.proxyClientOAuthV2.Do(req)
	} else {
		resp, err = trans.proxyClient.Do(req)
	}
	reqRoundTripTime := time.Since(httpReqStTime)
	// This stat will be useful in understanding the round trip time taken for the http req
	// between server and transformer
	stats.Default.NewTaggedStat("transformer_proxy.req_round_trip_time", stats.TimerType, stats.Tags{
		"destType": destName,
	}).SendTiming(reqRoundTripTime)

	if os.IsTimeout(err) {
		// A timeout error occurred
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) Client.Do Failure for %[1]v, with %[2]v`, destName, err.Error())
		return httpProxyResponse{
			respData:   []byte{},
			statusCode: http.StatusGatewayTimeout,
			err:        err,
		}
	} else if err != nil {
		// This was an error, but not a timeout
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) Client.Do Failure for %[1]v, with %[2]v`, destName, err.Error())
		return httpProxyResponse{
			respData:   []byte{},
			statusCode: http.StatusInternalServerError,
			err:        err,
		}
	} else if resp.StatusCode == http.StatusNotFound {
		// Actually Router wouldn't send any destination to proxy unless it already exists
		// But if accidentally such a request is sent, failing instead of aborting
		notFoundErr := fmt.Errorf(`post "%s" not found`, req.URL)
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) Client.Do Failure for %[1]v, with %[2]v`, destName, notFoundErr)
		return httpProxyResponse{
			respData:   []byte{},
			statusCode: http.StatusInternalServerError,
			err:        notFoundErr,
		}
	}

	// error handling if body is missing
	if resp.Body == nil {
		errStr := "empty response body"
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) Failed with statusCode: %[2]v, message: %[3]v`, destName, http.StatusInternalServerError, string(respData))
		return httpProxyResponse{
			respData:   []byte{},
			statusCode: http.StatusInternalServerError,
			err:        errors.New(errStr),
		}
	}

	respData, err = io.ReadAll(resp.Body)
	defer func() { httputil.CloseResponse(resp) }()
	// error handling while reading from resp.Body
	if err != nil {
		respData = []byte(fmt.Sprintf(`failed to read response body, Error:: %+v`, err))
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) Failed with statusCode: %[2]v, message: %[3]v`, destName, http.StatusBadRequest, string(respData))
		return httpProxyResponse{
			respData:   []byte{}, // sending this as it is not getting sent at all
			statusCode: http.StatusInternalServerError,
			err:        err,
		}
	}

	trans.logger.Debugf(`[TransformerProxy] (Dest-%[1]v) Proxy Request response - %[2]s`, destName, string(respData))

	return httpProxyResponse{
		respData:   respData,
		statusCode: resp.StatusCode,
	}
}

func getBatchURL() string {
	return strings.TrimSuffix(config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/batch"
}

func getRouterTransformURL() string {
	return strings.TrimSuffix(config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/routerTransform"
}

type transformerResponse struct {
	AuthErrorCategory string `json:"authErrorCategory"`
}

// GetAuthErrorCategoryFromTransformResponse parses the response data from a transformerResponse
// to extract the authentication error category.
// {input: [{}]}
// {input: [{}, {}, {}, {}]}
// {input: [{}, {}, {}, {}]} -> {output: [{200}, {200}, {401,authErr}, {401,authErr}]}
func GetAuthErrorCategoryFromTransformResponse(respData []byte) (string, error) {
	var transformedJobs []transformerResponse
	err := jsonfast.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &transformedJobs)
	if err != nil {
		return "", err
	}
	tfJob, found := lo.Find(transformedJobs, func(item transformerResponse) bool {
		return oauthv2.IsValidAuthErrorCategory(item.AuthErrorCategory)
	})
	if !found {
		// can be a valid scenario
		return "", nil
	}
	return tfJob.AuthErrorCategory, nil
}

func GetAuthErrorCategoryFromTransformProxyResponse(respData []byte) (string, error) {
	var transformedJobs transformerResponse
	err := jsonfast.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &transformedJobs)
	if err != nil {
		return "", err
	}
	return transformedJobs.AuthErrorCategory, nil
}

func (trans *handle) makeOAuthRequest(req *http.Request, transformMessage interface{}) (*http.Response, []byte, error) {
	// Cast the transform message to access required fields
	msg, ok := transformMessage.(*types.TransformMessageT)
	if !ok || len(msg.Data) == 0 {
		return nil, nil, fmt.Errorf("invalid transform message format for OAuth request")
	}

	// Prepare destination info for OAuth context
	destinationInfo := &oauthv2.DestinationInfo{
		Config:           msg.Data[0].Destination.Config,
		DefinitionConfig: msg.Data[0].Destination.DestinationDefinition.Config,
		WorkspaceID:      msg.Data[0].JobMetadata.WorkspaceID,
		DefinitionName:   msg.Data[0].Destination.DestinationDefinition.Name,
		ID:               msg.Data[0].Destination.ID,
	}

	// Add destination info to request context
	req = req.WithContext(cntx.CtxWithDestInfo(req.Context(), destinationInfo))

	// Start timing the request
	start := time.Now()
	resp, err := trans.clientOAuthV2.Do(req)
	trans.transformRequestTimerStat.SendTiming(time.Since(start))

	if err != nil {
		return nil, nil, fmt.Errorf("OAuth request failed: %w", err)
	}

	// Read response body
	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		httputil.CloseResponse(resp)
		return nil, nil, fmt.Errorf("failed to read OAuth response body: %w", err)
	}

	return resp, respData, nil
}

func (trans *handle) makeStandardRequest(req *http.Request) (*http.Response, []byte, error) {
	// Start timing the request
	start := time.Now()
	resp, err := trans.client.Do(req)
	trans.transformRequestTimerStat.SendTiming(time.Since(start))

	if err != nil {
		return nil, nil, fmt.Errorf("standard request failed: %w", err)
	}

	// Read response body
	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		httputil.CloseResponse(resp)
		return nil, nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return resp, respData, nil
}
