package transformer

//go:generate mockgen -destination=../../mocks/router/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/router/transformer Transformer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/sync"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	transformerclient "github.com/rudderlabs/rudder-server/internal/transformer-client"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/types"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	cntx "github.com/rudderlabs/rudder-server/services/oauth/v2/context"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
	oauthv2httpclient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
	transformerfs "github.com/rudderlabs/rudder-server/services/transformer"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
)

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

	stats stats.Stats

	// clientOAuthV2 is the HTTP client for router transformation requests using OAuth V2.
	clientOAuthV2 *http.Client
	// proxyClientOAuthV2 is the mockable HTTP client for transformer proxy requests using OAuth V2.
	proxyClientOAuthV2 sysUtils.HTTPClientI
	// expirationTimeDiff holds the configured time difference for token expiration.
	expirationTimeDiff config.ValueLoader[time.Duration]

	compactionSupported bool
}

type ProxyRequestMetadata struct {
	JobID         int64           `json:"jobId"`
	AttemptNum    int             `json:"attemptNum"`
	UserID        string          `json:"userId"`
	SourceID      string          `json:"sourceId"`
	DestinationID string          `json:"destinationId"`
	WorkspaceID   string          `json:"workspaceId"`
	Secret        json.RawMessage `json:"secret"`             // Receives OAuth destination secrets in the transformer and holds the current token during refresh token flows
	DestInfo      json.RawMessage `json:"destInfo,omitempty"` // Used by the transformer; potentially removable
	DontBatch     bool            `json:"dontBatch"`
}

type ProxyRequestPayload struct {
	integrations.PostParametersT
	Metadata           []ProxyRequestMetadata `json:"metadata"`
	DestinationConfig  map[string]any         `json:"destinationConfig"`
	DestinationVersion int                    `json:"destinationVersion,omitempty"`
}

type ProxyRequestParams struct {
	ResponseData ProxyRequestPayload
	DestName     string
	Adapter      transformerProxyAdapter
	Destination  *backendconfig.DestinationT
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

// NewTransformer creates a new transformer.
// If a nil [featuresService] is provided, the transformer will not use message compaction, even though transformation service might support it.
func NewTransformer(
	destType string,
	destinationTimeout, transformTimeout time.Duration,
	backendConfig backendconfig.BackendConfig,
	expirationTimeDiff config.ValueLoader[time.Duration],
	featuresService transformerfs.FeaturesService,
	conf *config.Config,
) Transformer {
	cache := oauthv2.NewOauthTokenCache()
	oauthLock := sync.NewPartitionRWLocker()
	handle := &handle{
		expirationTimeDiff: expirationTimeDiff,
	}
	handle.setup(destType, destinationTimeout, transformTimeout, &cache, oauthLock, backendConfig, featuresService, conf)
	return handle
}

var loggerOverride logger.Logger

// Add transformerMetricLabels struct and methods
type transformerMetricLabels struct {
	Endpoint        string // hostname of the service
	DestinationType string // BQ, etc.
	SourceType      string // webhook
	Stage           string // processor, router, gateway
	WorkspaceID     string // workspace identifier
	SourceID        string // source identifier
	DestinationID   string // destination identifier
}

// ToStatsTag converts transformerMetricLabels to stats.Tags
func (t transformerMetricLabels) ToStatsTag() stats.Tags {
	tags := stats.Tags{
		"endpoint":        t.Endpoint,
		"destinationType": t.DestinationType,
		"sourceType":      t.SourceType,
		"stage":           t.Stage,
		"workspaceId":     t.WorkspaceID,
		"destinationId":   t.DestinationID,
		"sourceId":        t.SourceID,

		// Legacy tags: to be removed
		"destType": t.DestinationType,
	}
	return tags
}

// Transform transforms router jobs to destination jobs
func (trans *handle) Transform(transformType string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
	start := time.Now()
	var destinationJobs types.DestinationJobs
	transformMessageCopy, preservedData := transformMessage.Dehydrate()
	compactRequestPayloads := trans.compactionSupported // consistent state for the entire request

	rawJSON, err := trans.getRequestPayload(transformMessageCopy, compactRequestPayloads)
	if err != nil {
		trans.logger.Errorn("problematic input for marshalling", obskit.Error(err))
		panic(err)
	}

	var url string
	switch transformType {
	case BATCH:
		url = getBatchURL()
	case ROUTER_TRANSFORM:
		url = getRouterTransformURL()
	default:
		return []types.DestinationJobT{}
	}

	// Create metric labels
	labels := transformerMetricLabels{
		Endpoint:        getEndpointFromURL(url),
		Stage:           "router",
		DestinationType: transformMessage.Data[0].Destination.DestinationDefinition.Name,
		SourceType:      transformMessage.Data[0].JobMetadata.SourceCategory,
		WorkspaceID:     transformMessage.Data[0].JobMetadata.WorkspaceID,
		SourceID:        transformMessage.Data[0].JobMetadata.SourceID,
		DestinationID:   transformMessage.Data[0].Destination.ID,
	}

	// Record request metrics
	trans.stats.NewTaggedStat("transformer_client_request_total_bytes", stats.CountType, labels.ToStatsTag()).Count(len(rawJSON))

	retryCount := 0
	var resp *http.Response
	var respData []byte
	// We should rarely have error communicating with our JS
	reqFailed := false

	for {
		s := time.Now()
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(rawJSON))
		if err != nil {
			// No point in retrying if we can't even create a request. Panicking as per convention.
			panic(fmt.Errorf("JS HTTP request creation error: URL: %v Error: %+v", url, err))
		}

		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		if compactRequestPayloads {
			req.Header.Set("X-Content-Format", "json+compactedv1")
		}
		req.Header.Set("X-Feature-Gzip-Support", "?1")
		// Header to let transformer know that the client understands event filter code
		req.Header.Set("X-Feature-Filter-Code", "?1")

		dest := &transformMessageCopy.Data[0].Destination
		req = req.WithContext(cntx.CtxWithDestination(req.Context(), dest))
		resp, err = trans.clientOAuthV2.Do(req)

		duration := time.Since(s)
		trans.stats.NewTaggedStat("transformer_client_total_durations_seconds", stats.CountType, labels.ToStatsTag()).Count(int(duration.Seconds()))

		if err == nil {
			// If no err returned by client.Post, reading body.
			// If reading body fails, retrying.
			respData, err = io.ReadAll(resp.Body)
			trans.stats.NewTaggedStat("transformer_client_response_total_bytes", stats.CountType, labels.ToStatsTag()).Count(len(respData))

		}

		if err != nil {
			trans.transformRequestTimerStat.SendTiming(duration)
			reqFailed = true
			trans.logger.Errorn(
				"JS HTTP connection error",
				obskit.Error(err),
				logger.NewStringField("URL", url),
			)
			if retryCount > config.GetIntVar(30, 1, "Processor.maxRetry") {
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

		trans.transformRequestTimerStat.SendTiming(duration)
		break
	}

	if resp.StatusCode != http.StatusOK {
		trans.logger.Errorn("[Router Transfomrer] :: Transformer returned status code and reason",
			logger.NewIntField("statusCode", int64(resp.StatusCode)),
			logger.NewStringField("reason", resp.Status))
	}

	var transResp oauthv2.TransportResponse

	// We don't need to handle it, as we can receive a string response even before executing OAuth operations like Refresh Token.
	// It's acceptable if the structure of respData doesn't match the oauthv2.TransportResponse struct.
	err = jsonrs.Unmarshal(respData, &transResp)
	if err == nil && transResp.OriginalResponse != "" {
		respData = []byte(transResp.OriginalResponse) // re-assign originalResponse
	}

	if resp.StatusCode == http.StatusOK {
		transformerAPIVersion, convErr := strconv.Atoi(resp.Header.Get(apiVersionHeader))
		if convErr != nil {
			transformerAPIVersion = 0
		}
		if utilTypes.SupportedTransformerApiVersion != transformerAPIVersion {
			trans.logger.Errorn("Incompatible transformer version",
				logger.NewIntField("expectedVersion", int64(utilTypes.SupportedTransformerApiVersion)),
				logger.NewIntField("receivedVersion", int64(transformerAPIVersion)),
				logger.NewStringField("url", url))
			panic(fmt.Errorf("incompatible transformer version: Expected: %d Received: %d, URL: %v", utilTypes.SupportedTransformerApiVersion, transformerAPIVersion, url))
		}

		switch transformType {
		case BATCH:
			err = jsonrs.Unmarshal(respData, &destinationJobs)
		case ROUTER_TRANSFORM:
			rawResp := []byte(gjson.GetBytes(respData, "output").Raw)
			err = jsonrs.Unmarshal(rawResp, &destinationJobs)
		}
		for _, destinationJob := range destinationJobs {
			integrations.CollectIntegrationFailureDetailedStats(trans.stats, destinationJob.StatTags)
		}

		// Validate the response received from the transformer
		in := transformMessage.JobIDs()
		var out []int64
		invalid := make(map[int64]struct{}) // invalid jobIDs are the ones that are in the response but were not included in the request
		emptyMetadataCount := 0
		for i := range destinationJobs {
			if oauthv2.IsValidAuthErrorCategory(destinationJobs[i].AuthErrorCategory) {
				if transResp.InterceptorResponse.StatusCode > 0 {
					destinationJobs[i].StatusCode = transResp.InterceptorResponse.StatusCode
				}
				if transResp.InterceptorResponse.Response != "" {
					destinationJobs[i].Error = transResp.InterceptorResponse.Response
				}
			}
			if len(destinationJobs[i].JobMetadataArray) == 0 {
				emptyMetadataCount++
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
		} else if emptyMetadataCount > 0 {
			invalidResponseReason = "empty metadata array"
			invalidResponseError = fmt.Sprintf("Transformer returned %d destination job(s) with an empty metadata array for input: %s", emptyMetadataCount, string(rawJSON))
		} else if len(invalid) > 0 {
			var invalidSlice []int64
			for k := range invalid {
				invalidSlice = append(invalidSlice, k)
			}
			invalidResponseReason = "invalid jobIDs"
			invalidResponseError = fmt.Sprintf("Transformer returned invalid jobIDs: %v", invalidSlice)
		}

		if invalidResponseReason != "" {
			trans.stats.NewTaggedStat(`router_transformer_invalid_response`, stats.CountType, stats.Tags{
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

	destinationJobs.Hydrate(preservedData)
	trans.stats.NewTaggedStat("transformer_client_request_total_events", stats.CountType, labels.ToStatsTag()).Count(len(transformMessage.Data))
	trans.stats.NewTaggedStat("transformer_client_response_total_events", stats.CountType, labels.ToStatsTag()).Count(len(destinationJobs))
	trans.stats.NewTaggedStat("transformer_client_total_time", stats.TimerType, labels.ToStatsTag()).SendTiming(time.Since(start))
	return destinationJobs
}

// proxyJobIDs returns the jobIDs in a proxy request batch — what a responder needs to look the
// affected events up in jobsDB.
func proxyJobIDs(metadata []ProxyRequestMetadata) []int64 {
	return lo.Map(metadata, func(m ProxyRequestMetadata, _ int) int64 {
		return m.JobID
	})
}

// emitProxyInvalidResponse records a transformer-proxy contract breach and logs the jobIDs needed
// to inspect the affected events. Every breach reason goes through here so the metric keeps a single
// tag set and a single stats handle. The response body is deliberately not logged: these are
// debugged by reading the events out of jobsDB by jobID.
//
// Takes proxyReqParams rather than the destType/destinationId strings so the two cannot be
// transposed at a call site and silently mis-tag the metric. Callers must be past ProxyRequest's
// empty-metadata guard, which every breach site is.
func (trans *handle) emitProxyInvalidResponse(proxyReqParams *ProxyRequestParams, reason, msg string, fields ...logger.Field) {
	destinationID := proxyReqParams.ResponseData.Metadata[0].DestinationID
	trans.stats.NewTaggedStat(`router.transformerproxy.invalid.response`, stats.CountType, stats.Tags{
		"reason":        reason,
		"destType":      proxyReqParams.DestName,
		"destinationId": destinationID,
	}).Increment()
	trans.logger.Warnn(msg, append([]logger.Field{obskit.DestinationID(destinationID)}, fields...)...)
}

// truncatedBody bounds a body embedded in an error string: nothing reads ProxyRequestResponseBody
// and a batch response can be multi-MB.
func truncatedBody(b []byte) string {
	const maxBodyBytes = int(10 * bytesize.KB)
	if len(b) <= maxBodyBytes {
		return string(b)
	}
	// A cut can land mid-rune and render as U+FFFD; acceptable, since nothing reads this field.
	return string(b[:maxBodyBytes]) + "...[truncated]"
}

// proxyErrorResponse builds the error-path response shared by every early return in ProxyRequest.
// RespStatusCodes/RespBodys are always empty here by construction: per-job results only exist once
// the adapter has parsed a response, which has not happened on any path that bails out early.
func proxyErrorResponse(code int, body string, dontBatch map[int64]bool) ProxyRequestResponse {
	return ProxyRequestResponse{
		ProxyRequestStatusCode:   code,
		ProxyRequestResponseBody: body,
		RespContentType:          "text/plain; charset=utf-8",
		RespStatusCodes:          map[int64]int{},
		RespBodys:                map[int64]string{},
		DontBatchDirectives:      dontBatch,
	}
}

func (trans *handle) ProxyRequest(ctx context.Context, proxyReqParams *ProxyRequestParams) ProxyRequestResponse {
	start := time.Now()
	routerJobDontBatchDirectives := make(map[int64]bool)

	if len(proxyReqParams.ResponseData.Metadata) == 0 {
		trans.logger.Warnn("[TransformerProxy] Input metadata is empty",
			logger.NewStringField("destination", proxyReqParams.DestName))
		return proxyErrorResponse(http.StatusBadRequest, "Input metadata is empty", routerJobDontBatchDirectives)
	}

	for _, m := range proxyReqParams.ResponseData.Metadata {
		routerJobDontBatchDirectives[m.JobID] = m.DontBatch
	}

	trans.logger.Debugn("[TransformerProxy] Proxy Request starts",
		logger.NewStringField("destination", proxyReqParams.DestName))

	payload, err := proxyReqParams.Adapter.getPayload(proxyReqParams)
	if err != nil {
		return proxyErrorResponse(http.StatusInternalServerError, "Payload preparation failed", routerJobDontBatchDirectives)
	}

	proxyURL, err := proxyReqParams.Adapter.getProxyURL(proxyReqParams.DestName)
	if err != nil {
		return proxyErrorResponse(http.StatusInternalServerError, "ProxyURL preparation failed", routerJobDontBatchDirectives)
	}

	// Create metric labels
	labels := transformerMetricLabels{
		Endpoint:        getEndpointFromURL(proxyURL),
		Stage:           "router_proxy",
		DestinationType: proxyReqParams.DestName,
		WorkspaceID:     proxyReqParams.ResponseData.Metadata[0].WorkspaceID,
		DestinationID:   proxyReqParams.ResponseData.Metadata[0].DestinationID,
	}.ToStatsTag()

	// Record request metrics
	trans.stats.NewTaggedStat("transformer_proxy_delivery_request", stats.CountType, labels).Increment()
	trans.stats.NewTaggedStat("transformer_client_request_total_bytes", stats.CountType, labels).Count(len(payload))

	rdlTime := time.Now()
	httpPrxResp := trans.doProxyRequest(ctx, proxyURL, proxyReqParams, payload)
	respData, respCode, requestError := httpPrxResp.respData, httpPrxResp.statusCode, httpPrxResp.err

	duration := time.Since(rdlTime)

	trans.stats.NewTaggedStat("transformer_client_total_durations_seconds", stats.CountType, labels).Count(int(duration.Seconds()))
	trans.stats.NewTaggedStat("transformer_client_response_total_bytes", stats.CountType, labels).Count(len(respData))

	labelsWithSuccess := lo.Assign(labels, stats.Tags{"requestSuccess": strconv.FormatBool(requestError == nil)})
	trans.stats.NewTaggedStat("transformer_proxy_request_latency", stats.TimerType, labelsWithSuccess).SendTiming(duration)
	trans.stats.NewTaggedStat("transformer_proxy_request_result", stats.CountType, labelsWithSuccess).Increment()

	if requestError != nil {
		return proxyErrorResponse(respCode, requestError.Error(), routerJobDontBatchDirectives)
	}

	// Infrastructure, not a contract breach: the response never came from the transformer. Only the
	// transformer sets apiVersion - an oauth-transport-synthesized error and an ingress/LB error page
	// both lack it. Status is deliberately not part of this: v0 mirrors the destination's delivery
	// status, so a destination 5xx would relabel a genuine breach as infra.
	infraFailure := !httpPrxResp.fromTransformer

	/*
		respData will be in ProxyResponseV0 or ProxyResponseV1
	*/
	var transportResponse oauthv2.TransportResponse // response that we get from oauth-interceptor in postRoundTrip
	// A non-oauth destination hands back the raw upstream body, so a non-JSON body here is either infra
	// or a corrupt transformer response.
	if err := jsonrs.Unmarshal(respData, &transportResponse); err != nil {
		reason, msg := "unmarshal error", "[TransformerProxy] proxy response unmarshal failed"
		if infraFailure {
			reason, msg = "transport error", "[TransformerProxy] transport-synthesized response failed envelope unmarshal"
		}
		trans.emitProxyInvalidResponse(proxyReqParams, reason, msg,
			logger.NewIntField("statusCode", int64(httpPrxResp.statusCode)),
			logger.NewIntSliceField("jobIDs", proxyJobIDs(proxyReqParams.ResponseData.Metadata)))
		// Not JSON, so `output` cannot exist; continuing would blank respData and fail on an empty body.
		return proxyErrorResponse(respCode, fmt.Sprintf("[TransformerProxy] response is not valid JSON: %s, err: %v", truncatedBody(respData), err), routerJobDontBatchDirectives)
	}
	if transportResponse.OriginalResponse != "" {
		respData = []byte(transportResponse.OriginalResponse)
	}

	if transportResponse.InterceptorResponse.StatusCode > 0 {
		respCode = transportResponse.InterceptorResponse.StatusCode
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
	output := gjson.GetBytes(respData, "output")
	// gjson reports an explicit JSON null as existing (Exists is `Type != Null || len(Raw) != 0`, and
	// a literal null has Raw == "null"), so the type has to be checked too. Otherwise
	// `{"output":null}` slips through, unmarshals into an empty response set, and gets reported as an
	// "in out mismatch" - sending the runbook down the data-corruption branch instead of this one.
	if !output.Exists() || output.Type == gjson.Null {
		// A transport-synthesized body has no output either, so it keeps its own reason.
		reason, msg := "missing output", "[TransformerProxy] proxy response missing output"
		if infraFailure {
			reason, msg = "transport error", "[TransformerProxy] transport-synthesized response has no output field"
		}
		trans.emitProxyInvalidResponse(proxyReqParams, reason, msg,
			logger.NewIntField("statusCode", int64(httpPrxResp.statusCode)),
			logger.NewIntSliceField("jobIDs", proxyJobIDs(proxyReqParams.ResponseData.Metadata)))
		// No per-job results to apply without `output`.
		return proxyErrorResponse(respCode, fmt.Sprintf("[TransformerProxy] response has no output field: %s", truncatedBody(respData)), routerJobDontBatchDirectives)
	}
	respData = []byte(output.Raw)

	transResp, err := proxyReqParams.Adapter.getResponse(respData, respCode, proxyReqParams.ResponseData.Metadata)
	if err != nil {
		// output was present but its payload will not parse - same "unmarshal error" reason as the
		// outer envelope.
		reason, msg := "unmarshal error", "[TransformerProxy] proxy response output unmarshal failed"
		if infraFailure {
			reason, msg = "transport error", "[TransformerProxy] transport-synthesized response output unmarshal failed"
		}
		trans.emitProxyInvalidResponse(proxyReqParams, reason, msg,
			logger.NewIntField("statusCode", int64(httpPrxResp.statusCode)),
			logger.NewIntSliceField("jobIDs", proxyJobIDs(proxyReqParams.ResponseData.Metadata)))
		return proxyErrorResponse(respCode, err.Error(), routerJobDontBatchDirectives)
	}
	// Compared as sets: a request can legitimately carry the same JobID twice (router/worker.go only
	// dedupes when building final responses). Only meaningful for v1 - v0 keys the map by the request
	// metadata, so the sets are equal by construction.
	jobIDsInMetadata := lo.Uniq(proxyJobIDs(proxyReqParams.ResponseData.Metadata))
	slices.Sort(jobIDsInMetadata)
	jobIDsInResponse := slices.Sorted(maps.Keys(transResp.routerJobResponseCodes))
	// Non-fatal: the results were applied, but may be attached to the wrong jobs. Duplicate response
	// entries collapse to one map key, silently dropping a status - a breach the set check cannot see.
	duplicateResponses := transResp.responseEntriesCount > len(transResp.routerJobResponseCodes)
	if !slices.Equal(jobIDsInMetadata, jobIDsInResponse) || duplicateResponses {
		trans.emitProxyInvalidResponse(proxyReqParams, "in out mismatch",
			"[TransformerProxy] JobIDs in out mismatch",
			logger.NewIntSliceField("jobIDsInMetadata", jobIDsInMetadata),
			logger.NewIntSliceField("jobIDsInResponse", jobIDsInResponse),
			logger.NewIntField("responseEntries", int64(transResp.responseEntriesCount)))
	}
	integrations.CollectIntegrationFailureDetailedStats(trans.stats, transResp.statTags)

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

	if transportResponse.InterceptorResponse.Response != "" {
		respData = []byte(transportResponse.InterceptorResponse.Response)
	}

	trans.stats.NewTaggedStat("transformer_client_request_total_events", stats.CountType, labels).Count(len(proxyReqParams.ResponseData.Metadata))
	trans.stats.NewTaggedStat("transformer_client_response_total_events", stats.CountType, labels).Count(len(transResp.routerJobResponseCodes))
	trans.stats.NewTaggedStat("transformer_client_total_time", stats.TimerType, labels).SendTiming(time.Since(start))

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

func (trans *handle) setup(destType string, destinationTimeout, transformTimeout time.Duration, cache *oauthv2.OauthTokenCache, locker *sync.PartitionRWLocker, backendConfig backendconfig.BackendConfig, featuresService transformerfs.FeaturesService, conf *config.Config) {
	if loggerOverride == nil {
		trans.logger = logger.NewLogger().Child("router").Child("transformer")
	} else {
		trans.logger = loggerOverride
	}

	trans.tr = &http.Transport{
		DisableKeepAlives:   config.GetBoolVar(true, "Transformer.Client.disableKeepAlives"),
		MaxConnsPerHost:     config.GetIntVar(100, 1, "Transformer.Client.maxHTTPConnections"),
		MaxIdleConnsPerHost: config.GetIntVar(10, 1, "Transformer.Client.maxHTTPIdleConnections"),
		IdleConnTimeout:     30 * time.Second,
	}
	// The timeout between server and transformer
	// Basically this timeout is more for communication between transformer and server
	trans.transformTimeout = transformTimeout
	// Destination API timeout
	// Basically this timeout we will configure when we make final call to destination to send event
	trans.destinationTimeout = destinationTimeout
	// This client is used for Router Transformation
	trans.client = transformerclient.NewClient("RouterTransformer", trans.transformerClientConfig())
	optionalArgs := &oauthv2httpclient.HttpClientOptionalArgs{
		Locker:              locker,
		Augmenter:           extensions.RouterHeaderAugmenter,
		ExpirationTimeDiff:  trans.expirationTimeDiff.Load(),
		Logger:              logger.NewLogger().Child("TransformerHttpClient"),
		OAuthBreakerOptions: oauthv2.ConfigToOauthBreakerOptions("Router."+destType, conf),
	}
	// This client is used for Router Transformation using oauthV2
	trans.clientOAuthV2 = oauthv2httpclient.NewOAuthHttpClient(&http.Client{Transport: trans.tr, Timeout: trans.transformTimeout}, common.RudderFlowDelivery, cache, backendConfig, GetAuthErrorCategoryFromTransformResponse, optionalArgs)

	proxyClientOptionalArgs := &oauthv2httpclient.HttpClientOptionalArgs{
		Locker:              locker,
		ExpirationTimeDiff:  trans.expirationTimeDiff.Load(),
		Logger:              logger.NewLogger().Child("TransformerProxyHttpClient"),
		OAuthBreakerOptions: oauthv2.ConfigToOauthBreakerOptions("Router."+destType, conf),
	}
	// This client is used for Transformer Proxy(delivered from transformer to destination)
	trans.proxyClient = transformerclient.NewClient("TransformerProxy", trans.transformerClientConfig())
	// This client is used for Transformer Proxy(delivered from transformer to destination) using oauthV2
	trans.proxyClientOAuthV2 = oauthv2httpclient.NewOAuthHttpClient(&http.Client{Transport: trans.tr, Timeout: trans.destinationTimeout + trans.transformTimeout}, common.RudderFlowDelivery, cache, backendConfig, GetAuthErrorCategoryFromTransformProxyResponse, proxyClientOptionalArgs)
	trans.stats = stats.Default
	trans.transformRequestTimerStat = trans.stats.NewStat("router_transformer_request_time", stats.TimerType)
	if featuresService != nil {
		go func() {
			<-featuresService.Wait()
			trans.compactionSupported = featuresService.SupportDestTransformCompactedPayloadV1()
		}()
	}
}

func (trans *handle) transformerClientConfig() *transformerclient.ClientConfig {
	transformerClientConfig := &transformerclient.ClientConfig{
		ClientTimeout: config.GetDurationVar(600, time.Second, "HttpClient.backendProxy.timeout", "HttpClient.routerTransformer.timeout"),
		ClientTTL:     config.GetDurationVar(10, time.Second, "Transformer.Client.ttl"),
		ClientType:    config.GetStringVar("stdlib", "Transformer.Client.type"),
		PickerType:    config.GetStringVar("power_of_two", "Transformer.Client.httplb.pickerType"),
	}
	transformerClientConfig.TransportConfig.DisableKeepAlives = config.GetBoolVar(true, "Transformer.Client.disableKeepAlives")
	transformerClientConfig.TransportConfig.MaxConnsPerHost = config.GetIntVar(100, 1, "Transformer.Client.maxHTTPConnections")
	transformerClientConfig.TransportConfig.MaxIdleConnsPerHost = config.GetIntVar(1, 1, "Transformer.Client.maxHTTPIdleConnections")
	transformerClientConfig.TransportConfig.IdleConnTimeout = config.GetDurationVar(5, time.Second, "Transformer.Client.maxIdleConnDuration")
	transformerClientConfig.Recycle = config.GetBoolVar(false, "Transformer.Client.DestinationTransformer.recycle", "Transformer.Client.recycle")
	transformerClientConfig.RecycleTTL = config.GetDurationVar(60, time.Second, "Transformer.Client.DestinationTransformer.recycleTTL", "Transformer.Client.recycleTTL")
	transformerClientConfig.RetryRudderErrors.Enabled = config.GetBoolVar(true, fmt.Sprintf("Transformer.Client.%s.retryRudderErrors.enabled", "Router"), "Transformer.Client.retryRudderErrors.enabled")
	transformerClientConfig.RetryRudderErrors.MaxRetry = config.GetIntVar(-1, 1, fmt.Sprintf("Transformer.Client.%s.retryRudderErrors.maxRetry", "Router"), "Transformer.Client.retryRudderErrors.maxRetry")
	transformerClientConfig.RetryRudderErrors.InitialInterval = config.GetDurationVar(1, time.Second, fmt.Sprintf("Transformer.Client.%s.retryRudderErrors.initialInterval", "Router"), "Transformer.Client.retryRudderErrors.initialInterval")
	transformerClientConfig.RetryRudderErrors.MaxInterval = config.GetDurationVar(30, time.Second, fmt.Sprintf("Transformer.Client.%s.retryRudderErrors.maxInterval", "Router"), "Transformer.Client.retryRudderErrors.maxInterval")
	transformerClientConfig.RetryRudderErrors.MaxElapsedTime = config.GetDurationVar(0, time.Second, fmt.Sprintf("Transformer.Client.%s.retryRudderErrors.maxElapsedTime", "Router"), "Transformer.Client.retryRudderErrors.maxElapsedTime")
	transformerClientConfig.RetryRudderErrors.Multiplier = config.GetFloat64Var(2.0, fmt.Sprintf("Transformer.Client.%s.retryRudderErrors.multiplier", "Router"), "Transformer.Client.retryRudderErrors.multiplier")

	return transformerClientConfig
}

type httpProxyResponse struct {
	respData   []byte
	statusCode int
	err        error
	// fromTransformer reports that the response carried the apiVersion header, which only the
	// transformer sets. An oauth-transport-synthesized error or an ingress/LB error page carries none,
	// and is infrastructure rather than a transformer contract breach.
	fromTransformer bool
}

func (trans *handle) doProxyRequest(ctx context.Context, proxyUrl string, proxyReqParams *ProxyRequestParams, payload []byte) httpProxyResponse {
	var respData []byte
	destName := proxyReqParams.DestName
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, proxyUrl, bytes.NewReader(payload))
	if err != nil {
		trans.logger.Errorn("[TransformerProxy] NewRequestWithContext Failed",
			logger.NewStringField("destination", destName),
			obskit.Error(err))
		return httpProxyResponse{
			respData:   []byte{},
			statusCode: http.StatusInternalServerError,
			err:        err,
		}
	}
	req.Header.Set("Content-Type", "application/json")
	trans.logger.Debugn("[TransformerProxy] Timeout for destination",
		logger.NewStringField("destination", destName),
		logger.NewIntField("timeoutMs", (trans.destinationTimeout+trans.transformTimeout).Milliseconds()))
	// Make use of this header to set timeout in the transfomer's http client
	// The header name may be worked out ?
	req.Header.Set("RdProxy-Timeout", strconv.FormatInt(trans.destinationTimeout.Milliseconds(), 10))
	httpReqStTime := time.Now()
	var resp *http.Response
	req = req.WithContext(cntx.CtxWithDestination(req.Context(), proxyReqParams.Destination))
	req = req.WithContext(cntx.CtxWithSecret(req.Context(), proxyReqParams.ResponseData.Metadata[0].Secret))
	resp, err = trans.proxyClientOAuthV2.Do(req)

	reqRoundTripTime := time.Since(httpReqStTime)
	// This stat will be useful in understanding the round trip time taken for the http req
	// between server and transformer
	trans.stats.NewTaggedStat("transformer_proxy_req_round_trip_time", stats.TimerType, stats.Tags{
		"destType": destName,
	}).SendTiming(reqRoundTripTime)

	if os.IsTimeout(err) {
		// A timeout error occurred
		trans.logger.Errorn("[TransformerProxy] Client.Do Failure",
			logger.NewStringField("destination", destName),
			obskit.Error(err))
		return httpProxyResponse{
			respData:   []byte{},
			statusCode: http.StatusGatewayTimeout,
			err:        err,
		}
	} else if err != nil {
		// This was an error, but not a timeout
		trans.logger.Errorn("[TransformerProxy] Client.Do Failure",
			logger.NewStringField("destination", destName),
			obskit.Error(err))
		return httpProxyResponse{
			respData:   []byte{},
			statusCode: http.StatusInternalServerError,
			err:        err,
		}
	} else if resp.StatusCode == http.StatusNotFound {
		// Actually Router wouldn't send any destination to proxy unless it already exists
		// But if accidentally such a request is sent, failing instead of aborting
		notFoundErr := fmt.Errorf(`post "%s" not found`, req.URL)
		trans.logger.Errorn("[TransformerProxy] Client.Do Failure",
			logger.NewStringField("destination", destName),
			obskit.Error(notFoundErr))
		return httpProxyResponse{
			respData:   []byte{},
			statusCode: http.StatusInternalServerError,
			err:        notFoundErr,
		}
	}

	// error handling if body is missing
	if resp.Body == nil {
		errStr := "empty response body"
		trans.logger.Errorn(`[TransformerProxy] empty response body`, logger.NewIntField("statusCode", http.StatusInternalServerError))
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
		trans.logger.Errorn("[TransformerProxy] Failure",
			logger.NewIntField("statusCode", http.StatusBadRequest),
			obskit.Error(err))
		return httpProxyResponse{
			respData:   []byte{}, // sending this as it is not getting sent at all
			statusCode: http.StatusInternalServerError,
			err:        err,
		}
	}

	return httpProxyResponse{
		respData:        respData,
		statusCode:      resp.StatusCode,
		fromTransformer: resp.Header.Get(apiVersionHeader) != "",
	}
}

func getBatchURL() string {
	return strings.TrimSuffix(config.GetStringVar("http://localhost:9090", "DEST_TRANSFORM_URL"), "/") + "/batch"
}

func getRouterTransformURL() string {
	return strings.TrimSuffix(config.GetStringVar("http://localhost:9090", "DEST_TRANSFORM_URL"), "/") + "/routerTransform"
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
	err := jsonrs.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &transformedJobs)
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
	err := jsonrs.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &transformedJobs)
	if err != nil {
		return "", err
	}
	return transformedJobs.AuthErrorCategory, nil
}

// Helper function to get endpoint from URL
func getEndpointFromURL(urlStr string) string {
	if parsedURL, err := url.Parse(urlStr); err == nil {
		return parsedURL.Host
	}
	return ""
}

func (trans *handle) getRequestPayload(data *types.TransformMessageT, compactRequestPayloads bool) ([]byte, error) {
	if compactRequestPayloads {
		return jsonrs.Marshal(data.Compacted())
	}
	return jsonrs.Marshal(&data)
}
