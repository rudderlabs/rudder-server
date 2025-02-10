package transformer

//go:generate mockgen -destination=../../mocks/router/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/router/transformer Transformer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	transformerclient "github.com/rudderlabs/rudder-server/internal/transformer-client"
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
	client sysUtils.HTTPClientI
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
	var destinationJobs types.DestinationJobs
	transformMessageCopy, jobs := transformMessage.Dehydrate()

	// Call remote transformation
	rawJSON, err := jsonfast.Marshal(&transformMessageCopy)
	if err != nil {
		trans.logger.Errorf("problematic input for marshalling: %#v", transformMessage)
		panic(err)
	}
	trans.logger.Debugf("[Router Transformer] :: input payload : %s", string(rawJSON))

	retryCount := 0
	var resp *http.Response
	var respData []byte
	// We should rarely have error communicating with our JS
	reqFailed := false

	var url string
	if transformType == BATCH {
		url = getBatchURL()
	} else if transformType == ROUTER_TRANSFORM {
		url = getRouterTransformURL()
	} else {
		// Unexpected transformType returning empty
		return []types.DestinationJobT{}
	}

	for {
		s := time.Now()
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(rawJSON))
		if err != nil {
			// No point in retrying if we can't even create a request. Panicking as per convention.
			panic(fmt.Errorf("JS HTTP request creation error: URL: %v Error: %+v", url, err))
		}

		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		req.Header.Set("X-Feature-Gzip-Support", "?1")
		// Header to let transformer know that the client understands event filter code
		req.Header.Set("X-Feature-Filter-Code", "?1")
		if trans.oAuthV2EnabledLoader.Load() {
			trans.logger.Debugn("[router transform]", logger.NewBoolField("oauthV2Enabled", true))
			destinationInfo := &oauthv2.DestinationInfo{
				Config:           transformMessageCopy.Data[0].Destination.Config,
				DefinitionConfig: transformMessageCopy.Data[0].Destination.DestinationDefinition.Config,
				WorkspaceID:      transformMessageCopy.Data[0].JobMetadata.WorkspaceID,
				DefinitionName:   transformMessageCopy.Data[0].Destination.DestinationDefinition.Name,
				ID:               transformMessageCopy.Data[0].Destination.ID,
			}
			req = req.WithContext(cntx.CtxWithDestInfo(req.Context(), destinationInfo))
			resp, err = trans.clientOAuthV2.Do(req)
		} else {
			resp, err = trans.client.Do(req)
		}

		if err == nil {
			// If no err returned by client.Post, reading body.
			// If reading body fails, retrying.
			respData, err = io.ReadAll(resp.Body)
		}

		if err != nil {
			trans.transformRequestTimerStat.SendTiming(time.Since(s))
			reqFailed = true
			trans.logger.Errorn(
				"JS HTTP connection error",
				logger.NewErrorField(err),
				logger.NewStringField("URL", url),
			)
			if retryCount > config.GetInt("Processor.maxRetry", 30) {
				panic(fmt.Errorf("JS HTTP connection error: URL: %v Error: %+v", url, err))
			}
			retryCount++
			time.Sleep(config.GetDurationVar(100, time.Millisecond, "Processor.retrySleep", "Processor.retrySleepInMS"))
			// Refresh the connection
			httputil.CloseResponse(resp)
			continue
		}

		if reqFailed {
			trans.logger.Errorn(
				"Failed request succeeded",
				logger.NewStringField("URL", url),
				logger.NewIntField("RetryCount", int64(retryCount)),
			)
		}

		trans.transformRequestTimerStat.SendTiming(time.Since(s))
		break
	}

	if resp.StatusCode != http.StatusOK {
		trans.logger.Errorf("[Router Transfomrer] :: Transformer returned status code: %v reason: %v", resp.StatusCode, resp.Status)
	}

	var transResp oauthv2.TransportResponse
	if trans.oAuthV2EnabledLoader.Load() {
		// We don't need to handle it, as we can receive a string response even before executing OAuth operations like Refresh Token or Auth Status Toggle.
		// It's acceptable if the structure of respData doesn't match the oauthv2.TransportResponse struct.
		err = json.Unmarshal(respData, &transResp)
		if err == nil && transResp.OriginalResponse != "" {
			respData = []byte(transResp.OriginalResponse) // re-assign originalResponse
		}
	}

	if resp.StatusCode == http.StatusOK {
		transformerAPIVersion, convErr := strconv.Atoi(resp.Header.Get(apiVersionHeader))
		if convErr != nil {
			transformerAPIVersion = 0
		}
		if utilTypes.SupportedTransformerApiVersion != transformerAPIVersion {
			trans.logger.Errorf("Incompatible transformer version: Expected: %d Received: %d, URL: %v", utilTypes.SupportedTransformerApiVersion, transformerAPIVersion, url)
			panic(fmt.Errorf("Incompatible transformer version: Expected: %d Received: %d, URL: %v", utilTypes.SupportedTransformerApiVersion, transformerAPIVersion, url))
		}

		trans.logger.Debugf("[Router Transfomrer] :: output payload : %s", string(respData))

		if transformType == BATCH {
			integrations.CollectIntgTransformErrorStats(respData)
			err = jsonfast.Unmarshal(respData, &destinationJobs)
		} else if transformType == ROUTER_TRANSFORM {
			rawResp := []byte(gjson.GetBytes(respData, "output").Raw)
			integrations.CollectIntgTransformErrorStats(rawResp)
			err = jsonfast.Unmarshal(rawResp, &destinationJobs)
		}

		// Validate the response received from the transformer
		in := transformMessage.JobIDs()
		var out []int64
		invalid := make(map[int64]struct{}) // invalid jobIDs are the ones that are in the response but were not included in the request
		for i := range destinationJobs {
			if oauthv2.IsValidAuthErrorCategory(destinationJobs[i].AuthErrorCategory) {
				if transResp.InterceptorResponse.StatusCode > 0 {
					destinationJobs[i].StatusCode = transResp.InterceptorResponse.StatusCode
				}
				if transResp.InterceptorResponse.Response != "" {
					destinationJobs[i].Error = transResp.InterceptorResponse.Response
				}
			}
			for k, v := range destinationJobs[i].JobIDs() {
				out = append(out, k)
				if _, ok := in[k]; !ok {
					invalid[k] = v
				}
			}
		}
		var invalidResponseReason, invalidResponseError string
		if err != nil {
			invalidResponseReason = "unmarshal error"
			invalidResponseError = fmt.Sprintf("Transformer returned invalid response: %s for input: %s", string(respData), string(rawJSON))
		} else if len(in) != len(out) {
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

			// Retrying. Go and fix transformer.
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
		}
	} else {
		statusCode := 500
		if resp.StatusCode == http.StatusNotFound {
			statusCode = 404
		}
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
	}
	func() { httputil.CloseResponse(resp) }()

	destinationJobs.Hydrate(jobs)
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
	transformerClientConfig := &transformerclient.ClientConfig{
		ClientTimeout: trans.transformTimeout,
		ClientTTL:     config.GetDuration("Transformer.Client.ttl", 10, time.Second),
		ClientType:    config.GetString("Transformer.Client.type", "stdlib"),
		PickerType:    config.GetString("Transformer.Client.httplb.pickerType", "power_of_two"),
	}
	transformerClientConfig.TransportConfig.DisableKeepAlives = config.GetBool("Transformer.Client.disableKeepAlives", true)
	transformerClientConfig.TransportConfig.MaxConnsPerHost = config.GetInt("Transformer.Client.maxHTTPConnections", 100)
	transformerClientConfig.TransportConfig.MaxIdleConnsPerHost = config.GetInt("Transformer.Client.maxHTTPIdleConnections", 10)
	transformerClientConfig.TransportConfig.IdleConnTimeout = 30 * time.Second
	trans.client = transformerclient.NewClient(transformerClientConfig)
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
	transformerClientConfig.ClientTimeout = trans.destinationTimeout + trans.transformTimeout
	trans.proxyClient = transformerclient.NewClient(transformerClientConfig)
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
