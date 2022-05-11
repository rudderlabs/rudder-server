package transformer

//go:generate mockgen -destination=../../mocks/router/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/router/transformer Transformer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/types"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
	"github.com/tidwall/gjson"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	BATCH            = "BATCH"
	ROUTER_TRANSFORM = "ROUTER_TRANSFORM"
)

//HandleT is the handle for this class
type HandleT struct {
	tr                                 *http.Transport
	client                             *http.Client
	transformRequestTimerStat          stats.RudderStats
	transformerNetworkRequestTimerStat stats.RudderStats
	transformerProxyRequestTime        stats.RudderStats
	logger                             logger.LoggerI
}

type LogStats struct {
	Stat        string
	Latency     int64
	CurrentTime time.Time
}

//Transformer provides methods to transform events
type Transformer interface {
	Setup()
	Transform(transformType string, transformMessage *types.TransformMessageT) []types.DestinationJobT
	ProxyRequest(ctx context.Context, responseData integrations.PostParametersT, destName string, mockDestLatencyTime int) (statusCode int, respBody string, stats []LogStats)
}

//NewTransformer creates a new transformer
func NewTransformer() *HandleT {
	return &HandleT{}
}

var (
	maxRetry              int
	retrySleep            time.Duration
	timeoutDuration       time.Duration
	retryWithBackoffCount int64
	pkgLogger             logger.LoggerI
)

func loadConfig() {
	config.RegisterIntConfigVariable(30, &maxRetry, true, 1, "Processor.maxRetry")
	config.RegisterDurationConfigVariable(100, &retrySleep, true, time.Millisecond, []string{"Processor.retrySleep", "Processor.retrySleepInMS"}...)
	config.RegisterDurationConfigVariable(30, &timeoutDuration, false, time.Second, []string{"HttpClient.timeout"}...)
	config.RegisterInt64ConfigVariable(15, &retryWithBackoffCount, true, 1, "Router.transformerProxyRetryCount")
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("router").Child("transformer")

}

//Transform transforms router jobs to destination jobs
func (trans *HandleT) Transform(transformType string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
	//Call remote transformation
	rawJSON, err := jsonfast.Marshal(transformMessage)
	if err != nil {
		trans.logger.Errorf("problematic input for marshalling: %#v", transformMessage)
		panic(err)
	}
	trans.logger.Debugf("[Router Transformer] :: input payload : %s", string(rawJSON))

	retryCount := 0
	var resp *http.Response
	var respData []byte
	//We should rarely have error communicating with our JS
	reqFailed := false

	var url string
	if transformType == BATCH {
		url = getBatchURL()
	} else if transformType == ROUTER_TRANSFORM {
		url = getRouterTransformURL()
	} else {
		//Unexpected transformType returning empty
		return []types.DestinationJobT{}
	}

	for {
		s := time.Now()
		resp, err = trans.client.Post(url, "application/json; charset=utf-8",
			bytes.NewBuffer(rawJSON))

		if err == nil {
			//If no err returned by client.Post, reading body.
			//If reading body fails, retrying.
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
			//Refresh the connection
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
		//This is returned by our JS engine so should  be parsable
		//but still handling it
		if err != nil {
			//NOTE: Transformer failed to give response in the right format
			//Retrying. Go and fix transformer.
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

func (trans *HandleT) ProxyRequest(ctx context.Context, responseData integrations.PostParametersT, destName string, mockDestLatencyTime int) (int, string, []LogStats) {
	statsMap := []LogStats{}
	marshStTime := time.Now()
	rawJSON, err := jsonfast.Marshal(responseData)
	if err != nil {
		panic(err)
	}
	statsMap = append(statsMap, LogStats{
		Stat:        "proxy_resp_data_marshal_time",
		Latency:     time.Since(marshStTime).Milliseconds(),
		CurrentTime: time.Now(),
	})

	var respData []byte
	var respCode int

	url := getProxyURL(destName)
	payload := []byte(rawJSON)

	operation := func() error {
		var requestError error
		//start
		rdl_time := time.Now()
		respData, respCode, requestError = trans.makeHTTPRequest(ctx, url, payload, mockDestLatencyTime, &statsMap)
		// if requestError != nil {
		// 	stats.NewTaggedStat("transformer_proxy.request_latency", stats.TimerType, stats.Tags{"requestSuccess": "false"}).SendTiming(time.Since(rdl_time))
		// 	stats.NewTaggedStat("transformer_proxy.request_result", stats.CountType, stats.Tags{"requestSuccess": "false"}).Increment()
		// } else {
		// 	stats.NewTaggedStat("transformer_proxy.request_latency", stats.TimerType, stats.Tags{"requestSuccess": "true"}).SendTiming(time.Since(rdl_time))
		// 	stats.NewTaggedStat("transformer_proxy.request_result", stats.CountType, stats.Tags{"requestSuccess": "true"}).Increment()
		// }
		httpReqDuration := time.Since(rdl_time)
		reqSuccessStr := strconv.FormatBool(requestError != nil)
		statsMap = append(statsMap, LogStats{
			Stat:        "proxy_make_http_req_time",
			Latency:     httpReqDuration.Milliseconds(),
			CurrentTime: time.Now(),
		})
		stats.NewTaggedStat("transformer_proxy.request_latency", stats.TimerType, stats.Tags{"requestSuccess": reqSuccessStr, "destination": destName}).SendTiming(httpReqDuration)
		stats.NewTaggedStat("transformer_proxy.request_result", stats.CountType, stats.Tags{"requestSuccess": reqSuccessStr, "destination": destName}).Increment()
		//end
		return requestError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(retryWithBackoffCount))
	err = backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf("[Transformer Proxy] Request for proxy to URL:: %v, Error:: %+v retrying after:: %v,", url, err, t)
		stats.NewTaggedStat("transformer_proxy.retry_metric", stats.CountType, stats.Tags{"destination": destName}).Increment()
	})

	if err != nil {
		panic(fmt.Errorf("[Transformer Proxy] Proxy request failed after max retries Error:: %+v", err))
	}
	gjsonProcStTime := time.Now()
	//Detecting content type of the respBody
	contentTypeHeader := strings.ToLower(http.DetectContentType(respData))
	//If content type is not of type "*text*", overriding it with empty string
	if !(strings.Contains(contentTypeHeader, "text") ||
		strings.Contains(contentTypeHeader, "application/json") ||
		strings.Contains(contentTypeHeader, "application/xml")) {
		respData = []byte("")
	}

	transformerResponse := integrations.TransResponseT{
		Message: "[Transformer Proxy]:: Default Message TransResponseT",
	}
	respData = []byte(gjson.GetBytes(respData, "output").Raw)
	statsMap = append(statsMap, LogStats{
		Stat:        "transformer_proxy_gjson_proc_time",
		Latency:     time.Since(gjsonProcStTime).Milliseconds(),
		CurrentTime: time.Now(),
	})
	unmarshStTime := time.Now()
	integrations.CollectDestErrorStats(respData)
	err = jsonfast.Unmarshal(respData, &transformerResponse)
	statsMap = append(statsMap, LogStats{
		Stat:        "transformer_proxy_resp_unmarshal_time",
		Latency:     time.Since(unmarshStTime).Milliseconds(),
		CurrentTime: time.Now(),
	})
	// unmarshal failure
	if err != nil {
		unmarshErrStTime := time.Now()
		errStr := string(respData) + " [Transformer Proxy Unmarshaling]::" + err.Error()
		trans.logger.Errorf(errStr)
		respData = []byte(errStr)
		respCode = http.StatusBadRequest
		statsMap = append(statsMap, LogStats{
			Stat:        "transformer_proxy_resp_error",
			Latency:     time.Since(unmarshErrStTime).Milliseconds(),
			CurrentTime: time.Now(),
		})
		return respCode, string(respData), statsMap
	}
	respMarshStTime := time.Now()
	// unmarshal success
	respData, err = jsonfast.Marshal(transformerResponse)
	if err != nil {
		panic(fmt.Errorf("[Transformer Proxy]:: failed to Marshal proxy response : %+v", err))
	}
	statsMap = append(statsMap, LogStats{
		Stat:        "transformer_proxy_resp_marshal_time",
		Latency:     time.Since(respMarshStTime).Milliseconds(),
		CurrentTime: time.Now(),
	})
	return respCode, string(respData), statsMap
}

//is it ok to use same client for network and transformer calls? need to understand timeout setup in router
func (trans *HandleT) Setup() {
	trans.logger = pkgLogger
	trans.tr = &http.Transport{}
	trans.client = &http.Client{Transport: trans.tr, Timeout: timeoutDuration}
	trans.transformRequestTimerStat = stats.NewStat("router.processor.transformer_request_time", stats.TimerType)
	trans.transformerNetworkRequestTimerStat = stats.NewStat("router.transformer_network_request_time", stats.TimerType)
	trans.transformerProxyRequestTime = stats.NewStat("router.transformer_response_transform_time", stats.TimerType)

}

func (trans *HandleT) makeHTTPRequest(ctx context.Context, url string, payload []byte, mockDestTime int, statsPtr *[]LogStats) ([]byte, int, error) {
	var respData []byte
	var respCode int
	reqBuildStTime := time.Now()
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		return []byte{}, http.StatusBadRequest, err
	}
	req.Header.Set("Content-Type", "application/json")

	if mockDestTime > 0 {
		q := req.URL.Query()
		q.Add("mockDestTime", strconv.Itoa(mockDestTime))
		req.URL.RawQuery = q.Encode()
	}
	*statsPtr = append(*statsPtr, LogStats{
		Stat:        "transformer_proxy_req_build_time",
		Latency:     time.Since(reqBuildStTime).Milliseconds(),
		CurrentTime: time.Now(),
	})

	reqStTime := time.Now()
	resp, err := trans.client.Do(req)
	*statsPtr = append(*statsPtr, LogStats{
		Stat:        "transformer_proxy_req_round_trip_time",
		Latency:     time.Since(reqStTime).Milliseconds(),
		CurrentTime: time.Now(),
	})

	respProcStTime := time.Now()
	if err != nil {
		return []byte{}, http.StatusBadRequest, err
	}

	// error handling if body is missing
	if resp.Body == nil {
		respData = []byte("[Transformer Proxy] :: transformer returned empty response body")
		respCode = http.StatusBadRequest
		return respData, respCode, fmt.Errorf("[Transformer Proxy] :: transformer returned empty response body")
	}

	respData, err = io.ReadAll(resp.Body)
	defer resp.Body.Close()
	defer func() {
		*statsPtr = append(*statsPtr, LogStats{
			Stat:        "transformer_proxy_resp_checks_io_readall_time",
			Latency:     time.Since(respProcStTime).Milliseconds(),
			CurrentTime: time.Now(),
		})
	}()
	// error handling while reading from resp.Body
	if err != nil {
		respData = []byte(fmt.Sprintf(`[Transformer Proxy] :: failed to read response body, Error:: %+v`, err))
		respCode = http.StatusBadRequest
		return respData, respCode, err
	}
	respCode = resp.StatusCode
	return respData, respCode, nil
}

func getBatchURL() string {
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/batch"
}

func getRouterTransformURL() string {
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/routerTransform"
}

func getProxyURL(destName string) string {
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/v0/destinations/" + strings.ToLower(destName) + "/proxy"
}
