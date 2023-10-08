package transformer

//go:generate mockgen -destination=../../mocks/router/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/router/transformer Transformer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/types"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
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
}

type ProxyRequestPayload struct {
	integrations.PostParametersT
	Metadata ProxyRequestMetadata `json:"metadata,omitempty"`
}
type ProxyRequestParams struct {
	ResponseData ProxyRequestPayload
	DestName     string
	JobID        int64
	BaseUrl      string
}

// Transformer provides methods to transform events
type Transformer interface {
	Transform(transformType string, transformMessage *types.TransformMessageT) []types.DestinationJobT
	ProxyRequest(ctx context.Context, proxyReqParams *ProxyRequestParams) (statusCode int, respBody, contentType string)
}

// NewTransformer creates a new transformer
func NewTransformer(destinationTimeout, transformTimeout time.Duration) Transformer {
	handle := &handle{}
	handle.setup(destinationTimeout, transformTimeout)
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
		// Header to let transformer know that the client understands event filter code
		req.Header.Set("X-Feature-Filter-Code", "?1")

		resp, err = trans.client.Do(req)

		if err == nil {
			// If no err returned by client.Post, reading body.
			// If reading body fails, retrying.
			respData, err = io.ReadAll(resp.Body)
		}

		if err != nil {
			trans.transformRequestTimerStat.SendTiming(time.Since(s))
			reqFailed = true
			trans.logger.Errorf("JS HTTP connection error: URL: %v Error: %+v", url, err)
			if retryCount > config.GetInt("Processor.maxRetry", 30) {
				panic(fmt.Errorf("JS HTTP connection error: URL: %v Error: %+v", url, err))
			}
			retryCount++
			time.Sleep(config.GetDurationVar(100, time.Millisecond, "Processor.retrySleep", "Processor.retrySleepInMS"))
			// Refresh the connection
			continue
		}
		if reqFailed {
			trans.logger.Errorf("Failed request succeeded after %v retries, URL: %v", retryCount, url)
		}

		trans.transformRequestTimerStat.SendTiming(time.Since(s))
		break
	}

	if resp.StatusCode != http.StatusOK {
		trans.logger.Errorf("[Router Transfomrer] :: Transformer returned status code: %v reason: %v", resp.StatusCode, resp.Status)
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
			integrations.CollectIntgTransformErrorStats([]byte(gjson.GetBytes(respData, "output").Raw))
			err = jsonfast.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &destinationJobs)
		}

		// Validate the response received from the transformer
		in := transformMessage.JobIDs()
		var out []int64
		invalid := make(map[int64]struct{}) // invalid jobIDs are the ones that are in the response but were not included in the request
		for i := range destinationJobs {
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
				resp := types.DestinationJobT{Message: routerJob.Message, JobMetadataArray: []types.JobMetadataT{routerJob.JobMetadata}, Destination: routerJob.Destination, StatusCode: statusCode, Error: invalidResponseError}
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
			resp := types.DestinationJobT{Message: routerJob.Message, JobMetadataArray: []types.JobMetadataT{routerJob.JobMetadata}, Destination: routerJob.Destination, StatusCode: statusCode, Error: string(respData)}
			destinationJobs = append(destinationJobs, resp)
		}
	}
	func() { httputil.CloseResponse(resp) }()

	destinationJobs.Hydrate(jobs)
	return destinationJobs
}

func (trans *handle) ProxyRequest(ctx context.Context, proxyReqParams *ProxyRequestParams) (int, string, string) {
	stats.Default.NewTaggedStat("transformer_proxy.delivery_request", stats.CountType, stats.Tags{
		"destType":      proxyReqParams.DestName,
		"workspaceId":   proxyReqParams.ResponseData.Metadata.WorkspaceID,
		"destinationId": proxyReqParams.ResponseData.Metadata.DestinationID,
	}).Increment()
	trans.logger.Debugf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Proxy Request starts - %[1]v`, proxyReqParams.DestName, proxyReqParams.JobID)

	rdlTime := time.Now()
	httpPrxResp := trans.doProxyRequest(ctx, proxyReqParams)
	respData, respCode, requestError := httpPrxResp.respData, httpPrxResp.statusCode, httpPrxResp.err
	reqSuccessStr := strconv.FormatBool(requestError == nil)
	stats.Default.NewTaggedStat("transformer_proxy.request_latency", stats.TimerType, stats.Tags{"requestSuccess": reqSuccessStr, "destType": proxyReqParams.DestName}).SendTiming(time.Since(rdlTime))
	stats.Default.NewTaggedStat("transformer_proxy.request_result", stats.CountType, stats.Tags{"requestSuccess": reqSuccessStr, "destType": proxyReqParams.DestName}).Increment()

	if requestError != nil {
		return respCode, requestError.Error(), "text/plain; charset=utf-8"
	}

	/**

		Structure of TransformerProxy Response:
		{
			output: {
				status: [destination status compatible with server]
				message: [ generic message for jobs_db payload]
				destinationResponse: [actual response payload from destination]
			}
		}
	**/
	transformerResponse := integrations.TransResponseT{
		Message: "[TransformerProxy]:: Default Message TransResponseT",
	}
	respData = []byte(gjson.GetBytes(respData, "output").Raw)
	integrations.CollectDestErrorStats(respData)
	err := jsonfast.Unmarshal(respData, &transformerResponse)
	// unmarshal failure
	if err != nil {
		errStr := string(respData) + " [TransformerProxy Unmarshaling]::" + err.Error()
		trans.logger.Errorf(errStr)
		respCode = http.StatusInternalServerError
		return respCode, errStr, "text/plain; charset=utf-8"
	}

	return respCode, string(respData), "application/json"
}

func (trans *handle) setup(destinationTimeout, transformTimeout time.Duration) {
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
	// This client is used for Transformer Proxy(delivered from transformer to destination)
	trans.proxyClient = &http.Client{Transport: trans.tr, Timeout: trans.destinationTimeout + trans.transformTimeout}
	trans.transformRequestTimerStat = stats.Default.NewStat("router.transformer_request_time", stats.TimerType)
}

type httpProxyResponse struct {
	respData   []byte
	statusCode int
	err        error
}

func (trans *handle) doProxyRequest(ctx context.Context, proxyReqParams *ProxyRequestParams) httpProxyResponse {
	var respData []byte

	baseUrl := proxyReqParams.BaseUrl
	destName := proxyReqParams.DestName
	jobID := proxyReqParams.JobID

	payload, err := jsonfast.Marshal(proxyReqParams.ResponseData)
	if err != nil {
		panic(err)
	}
	proxyUrl := getProxyURL(destName, baseUrl)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, proxyUrl, bytes.NewReader(payload))
	if err != nil {
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} NewRequestWithContext Failed for %[1]v, with %[3]v`, destName, jobID, err.Error())
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
	resp, err := trans.proxyClient.Do(req)
	reqRoundTripTime := time.Since(httpReqStTime)
	// This stat will be useful in understanding the round trip time taken for the http req
	// between server and transformer
	stats.Default.NewTaggedStat("transformer_proxy.req_round_trip_time", stats.TimerType, stats.Tags{
		"destType": destName,
	}).SendTiming(reqRoundTripTime)

	if os.IsTimeout(err) {
		// A timeout error occurred
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Client.Do Failure for %[1]v, with %[3]v`, destName, jobID, err.Error())
		return httpProxyResponse{
			respData:   []byte{},
			statusCode: http.StatusGatewayTimeout,
			err:        err,
		}
	} else if err != nil {
		// This was an error, but not a timeout
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Client.Do Failure for %[1]v, with %[3]v`, destName, jobID, err.Error())
		return httpProxyResponse{
			respData:   []byte{},
			statusCode: http.StatusInternalServerError,
			err:        err,
		}
	} else if resp.StatusCode == http.StatusNotFound {
		// Actually Router wouldn't send any destination to proxy unless it already exists
		// But if accidentally such a request is sent, we'd probably need to handle for better
		// understanding of the response
		notFoundErr := fmt.Errorf(`post "%s" not found`, req.URL)
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Client.Do Failure for %[1]v, with %[3]v`, destName, jobID, notFoundErr)
		return httpProxyResponse{
			respData:   []byte{},
			statusCode: resp.StatusCode,
			err:        notFoundErr,
		}
	}

	// error handling if body is missing
	if resp.Body == nil {
		errStr := "empty response body"
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Failed with statusCode: %[3]v, message: %[4]v`, destName, jobID, http.StatusBadRequest, string(respData))
		return httpProxyResponse{
			respData:   []byte{},
			statusCode: http.StatusInternalServerError,
			err:        fmt.Errorf(errStr),
		}
	}

	respData, err = io.ReadAll(resp.Body)
	defer func() { httputil.CloseResponse(resp) }()
	// error handling while reading from resp.Body
	if err != nil {
		respData = []byte(fmt.Sprintf(`failed to read response body, Error:: %+v`, err))
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Failed with statusCode: %[3]v, message: %[4]v`, destName, jobID, http.StatusBadRequest, string(respData))
		return httpProxyResponse{
			respData:   []byte{}, // sending this as it is not getting sent at all
			statusCode: http.StatusInternalServerError,
			err:        err,
		}
	}

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

func getProxyURL(destName, baseUrl string) string {
	if !router_utils.IsNotEmptyString(baseUrl) { // empty string check
		baseUrl = strings.TrimSuffix(config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"), "/")
	}
	return baseUrl + "/v0/destinations/" + strings.ToLower(destName) + "/proxy"
}
