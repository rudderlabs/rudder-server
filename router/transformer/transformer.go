package transformer

//go:generate mockgen -destination=../../mocks/router/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/router/transformer Transformer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/types"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
	"github.com/tidwall/gjson"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	BATCH            = "BATCH"
	ROUTER_TRANSFORM = "ROUTER_TRANSFORM"
)

// HandleT is the handle for this class
type HandleT struct {
	tr *http.Transport
	// http client for router transformation request
	client *http.Client
	// Mockable http.client for transformer proxy request
	tfProxyClient sysUtils.HTTPClientI
	// http client timeout for transformer proxy request
	tfProxyTimeout time.Duration
	// http client timeout for server-transformer request
	serverTfTimeout                    time.Duration
	transformRequestTimerStat          stats.RudderStats
	transformerNetworkRequestTimerStat stats.RudderStats
	transformerProxyRequestTime        stats.RudderStats
	logger                             logger.LoggerI
}

type ProxyRequestParams struct {
	ResponseData integrations.PostParametersT
	DestName     string
	JobID        int64
	BaseUrl      string
}

type httpProxyResponse struct {
	respData   []byte
	statusCode int
	err        error
}

// Transformer provides methods to transform events
type Transformer interface {
	Setup(timeout, srvTfTimeout time.Duration)
	Transform(transformType string, transformMessage *types.TransformMessageT) []types.DestinationJobT
	ProxyRequest(ctx context.Context, proxyReqParams *ProxyRequestParams) (statusCode int, respBody, contentType string)
}

// NewTransformer creates a new transformer
func NewTransformer() *HandleT {
	return &HandleT{}
}

var (
	maxRetry   int
	retrySleep time.Duration
	pkgLogger  logger.LoggerI
)

func loadConfig() {
	config.RegisterIntConfigVariable(30, &maxRetry, true, 1, "Processor.maxRetry")
	config.RegisterDurationConfigVariable(100, &retrySleep, true, time.Millisecond, []string{"Processor.retrySleep", "Processor.retrySleepInMS"}...)
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("router").Child("transformer")
}

// Transform transforms router jobs to destination jobs
func (trans *HandleT) Transform(transformType string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
	// Call remote transformation
	rawJSON, err := jsonfast.Marshal(transformMessage)
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
		resp, err = trans.client.Post(url, "application/json; charset=utf-8",
			bytes.NewBuffer(rawJSON))

		if err == nil {
			// If no err returned by client.Post, reading body.
			// If reading body fails, retrying.
			respData, err = io.ReadAll(resp.Body)
		}

		if err != nil {
			trans.transformRequestTimerStat.SendTiming(time.Since(s))
			reqFailed = true
			trans.logger.Errorf("JS HTTP connection error: URL: %v Error: %+v", url, err)
			if retryCount > maxRetry {
				panic(fmt.Errorf("JS HTTP connection error: URL: %v Error: %+v", url, err))
			}
			retryCount++
			time.Sleep(retrySleep)
			// Refresh the connection
			continue
		}
		if reqFailed {
			trans.logger.Errorf("Failed request succeeded after %v retries, URL: %v", retryCount, url)
		}

		trans.transformRequestTimerStat.SendTiming(time.Since(s))
		break
	}

	// Remove Assertion?
	if resp.StatusCode != http.StatusOK {
		trans.logger.Errorf("[Router Transfomrer] :: Transformer returned status code: %v reason: %v", resp.StatusCode, resp.Status)
	}

	var destinationJobs []types.DestinationJobT
	if resp.StatusCode == http.StatusOK {
		transformerAPIVersion, convErr := strconv.Atoi(resp.Header.Get("apiVersion"))
		if convErr != nil {
			transformerAPIVersion = 0
		}
		if utilTypes.SUPPORTED_TRANSFORMER_API_VERSION != transformerAPIVersion {
			trans.logger.Errorf("Incompatible transformer version: Expected: %d Received: %d, URL: %v", utilTypes.SUPPORTED_TRANSFORMER_API_VERSION, transformerAPIVersion, url)
			panic(fmt.Errorf("Incompatible transformer version: Expected: %d Received: %d, URL: %v", utilTypes.SUPPORTED_TRANSFORMER_API_VERSION, transformerAPIVersion, url))
		}

		trans.logger.Debugf("[Router Transfomrer] :: output payload : %s", string(respData))

		if transformType == BATCH {
			integrations.CollectIntgTransformErrorStats(respData)
			err = jsonfast.Unmarshal(respData, &destinationJobs)
		} else if transformType == ROUTER_TRANSFORM {
			integrations.CollectIntgTransformErrorStats([]byte(gjson.GetBytes(respData, "output").Raw))
			err = jsonfast.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &destinationJobs)
		}
		// This is returned by our JS engine so should  be parsable
		// but still handling it
		if err != nil {
			// NOTE: Transformer failed to give response in the right format
			// Retrying. Go and fix transformer.
			destinationJobs = []types.DestinationJobT{}
			statusCode := 500
			errorResp := fmt.Sprintf("Transformer returned invalid response: %s for input: %s", string(respData), string(rawJSON))
			trans.logger.Error(errorResp)
			for _, routerJob := range transformMessage.Data {
				resp := types.DestinationJobT{Message: routerJob.Message, JobMetadataArray: []types.JobMetadataT{routerJob.JobMetadata}, Destination: routerJob.Destination, Batched: false, StatusCode: statusCode, Error: errorResp}
				destinationJobs = append(destinationJobs, resp)
			}
		}
	} else {
		statusCode := 500
		if resp.StatusCode == http.StatusNotFound {
			statusCode = 404
		}
		for _, routerJob := range transformMessage.Data {
			resp := types.DestinationJobT{Message: routerJob.Message, JobMetadataArray: []types.JobMetadataT{routerJob.JobMetadata}, Destination: routerJob.Destination, Batched: false, StatusCode: statusCode, Error: string(respData)}
			destinationJobs = append(destinationJobs, resp)
		}
	}
	resp.Body.Close()

	return destinationJobs
}

func (trans *HandleT) ProxyRequest(ctx context.Context, proxyReqParams *ProxyRequestParams) (int, string, string) {
	stats.NewTaggedStat("transformer_proxy.delivery_request", stats.CountType, stats.Tags{"destination": proxyReqParams.DestName}).Increment()
	trans.logger.Debugf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Proxy Request starts - %[1]v`, proxyReqParams.DestName, proxyReqParams.JobID)

	rdlTime := time.Now()
	httpPrxResp := trans.makeTfProxyRequest(ctx, proxyReqParams)
	respData, respCode, requestError := httpPrxResp.respData, httpPrxResp.statusCode, httpPrxResp.err
	reqSuccessStr := strconv.FormatBool(requestError == nil)
	stats.NewTaggedStat("transformer_proxy.request_latency", stats.TimerType, stats.Tags{"requestSuccess": reqSuccessStr, "destination": proxyReqParams.DestName}).SendTiming(time.Since(rdlTime))
	stats.NewTaggedStat("transformer_proxy.request_result", stats.CountType, stats.Tags{"requestSuccess": reqSuccessStr, "destination": proxyReqParams.DestName}).Increment()

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

func (trans *HandleT) Setup(netClientTimeout, serverTfTimeout time.Duration) {
	trans.logger = pkgLogger
	trans.tr = &http.Transport{}
	// The timeout between server and transformer
	// Basically this timeout is more for communication between transformer and server
	trans.serverTfTimeout = serverTfTimeout
	// Destination API timeout
	// Basically this timeout we will configure when we make final call to destination to send event
	trans.tfProxyTimeout = netClientTimeout
	// This client is used for Router Transformation
	trans.client = &http.Client{Transport: trans.tr, Timeout: trans.serverTfTimeout}
	// This client is used for Transformer Proxy(delivered from transformer to destination)
	trans.tfProxyClient = &http.Client{Transport: trans.tr, Timeout: trans.tfProxyTimeout + trans.serverTfTimeout}
	trans.transformRequestTimerStat = stats.DefaultStats.NewStat("router.transformer_request_time", stats.TimerType)
	trans.transformerNetworkRequestTimerStat = stats.DefaultStats.NewStat("router.transformer_network_request_time", stats.TimerType)
	trans.transformerProxyRequestTime = stats.DefaultStats.NewStat("router.transformer_response_transform_time", stats.TimerType)
}

func (trans *HandleT) makeTfProxyRequest(ctx context.Context, proxyReqParams *ProxyRequestParams) httpProxyResponse {
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
	trans.logger.Debugf("[TransformerProxy] Timeout for %[1]s = %[2]v ms \n", destName, strconv.FormatInt((trans.tfProxyTimeout+trans.serverTfTimeout).Milliseconds(), 10))
	// Make use of this header to set timeout in the transfomer's http client
	// The header name may be worked out ?
	req.Header.Set("RdProxy-Timeout", strconv.FormatInt(trans.tfProxyTimeout.Milliseconds(), 10))

	httpReqStTime := time.Now()
	resp, err := trans.tfProxyClient.Do(req)
	reqRoundTripTime := time.Since(httpReqStTime)
	// This stat will be useful in understanding the round trip time taken for the http req
	// between server and transformer
	stats.NewTaggedStat("transformer_proxy.req_round_trip_time", stats.TimerType, stats.Tags{
		"destination": destName,
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
	defer resp.Body.Close()
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
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/batch"
}

func getRouterTransformURL() string {
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/routerTransform"
}

func getProxyURL(destName, baseUrl string) string {
	if !router_utils.IsNotEmptyString(baseUrl) { // empty string check
		baseUrl = strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/")
	}
	return baseUrl + "/v0/destinations/" + strings.ToLower(destName) + "/proxy"
}
