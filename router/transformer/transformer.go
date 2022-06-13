package transformer

//go:generate mockgen -destination=../../mocks/router/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/router/transformer Transformer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
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

//Transformer provides methods to transform events
type Transformer interface {
	Setup()
	Transform(transformType string, transformMessage *types.TransformMessageT) []types.DestinationJobT
	ProxyRequest(ctx context.Context, responseData integrations.PostParametersT, destName string, jobId int64) (statusCode int, respBody string)
	ProxyRequestTest(ctx context.Context, responseData integrations.PostParametersT, destName string, jobId int64) (statusCode int, respBody string)
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

func (trans *HandleT) ProxyRequest(ctx context.Context, responseData integrations.PostParametersT, destName string, jobId int64) (int, string) {
	stats.NewTaggedStat("transformer_proxy.delivery_request", stats.CountType, stats.Tags{"destination": destName}).Increment()
	trans.logger.Debugf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Proxy Request starts - %[1]v`, destName, jobId)
	rawJSON, err := jsonfast.Marshal(responseData)
	if err != nil {
		panic(err)
	}

	var respData []byte
	var respCode int

	url := getProxyURL(destName)
	payload := []byte(rawJSON)

	operation := func() error {
		var requestError error
		trans.logger.Debugf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Proxy Request operation method - %[1]v`, destName, jobId)
		//start
		rdl_time := time.Now()
		respData, respCode, requestError = trans.makeHTTPRequest(ctx, url, payload, destName, jobId)
		reqSuccessStr := strconv.FormatBool(requestError == nil)
		stats.NewTaggedStat("transformer_proxy.request_latency", stats.TimerType, stats.Tags{"requestSuccess": reqSuccessStr, "destination": destName}).SendTiming(time.Since(rdl_time))
		stats.NewTaggedStat("transformer_proxy.request_result", stats.CountType, stats.Tags{"requestSuccess": reqSuccessStr, "destination": destName}).Increment()
		trans.logger.Debugf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} RespData - %[3]v, RespCode - %[4]v `, destName, jobId, string(respData), respCode)
		trans.logger.Debugf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Proxy Request operation ended - %[1]v`, destName, jobId)
		//end
		return requestError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(retryWithBackoffCount))
	err = backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf("[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Request for proxy to URL:: %[3]v, Error:: %+[4]v retrying after:: %[5]v,", destName, jobId, url, err, t)
		stats.NewTaggedStat("transformer_proxy.retries", stats.CountType, stats.Tags{"destination": destName}).Increment()
	})

	if err != nil {
		panic(fmt.Errorf("[TransformerProxy] Proxy request failed after max retries Error:: %+v", err))
	}

	//Detecting content type of the respBody
	contentTypeHeader := strings.ToLower(http.DetectContentType(respData))
	//If content type is not of type "*text*", overriding it with empty string
	if !(strings.Contains(contentTypeHeader, "text") ||
		strings.Contains(contentTypeHeader, "application/json") ||
		strings.Contains(contentTypeHeader, "application/xml")) {
		respData = []byte("")
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
	err = jsonfast.Unmarshal(respData, &transformerResponse)
	// unmarshal failure
	if err != nil {
		errStr := string(respData) + " [TransformerProxy Unmarshaling]::" + err.Error()
		trans.logger.Errorf(errStr)
		respData = []byte(errStr)
		respCode = http.StatusBadRequest
		return respCode, string(respData)
	}
	// unmarshal success
	respData, err = jsonfast.Marshal(transformerResponse)
	if err != nil {
		panic(fmt.Errorf("[TransformerProxy]:: failed to Marshal proxy response : %+v", err))
	}

	return respCode, string(respData)
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

func (trans *HandleT) makeHTTPRequest(ctx context.Context, url string, payload []byte, destName string, jobId int64) ([]byte, int, error) {
	var respData []byte
	var respCode int
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} NewRequestWithContext Failed for %[1]v, with %[3]v`, destName, jobId, err.Error())
		return []byte{}, http.StatusBadRequest, err
	}
	req.Header.Set("Content-Type", "application/json")

	httpReqStTime := time.Now()
	resp, err := trans.client.Do(req)
	reqRoundTripTime := time.Since(httpReqStTime)
	// This stat will be useful in understanding the round trip time taken for the http req
	// between server and transformer
	stats.NewTaggedStat("transformer_proxy.req_round_trip_time", stats.TimerType, stats.Tags{
		"destination": destName,
	}).SendTiming(reqRoundTripTime)

	if err != nil {
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Client.Do Failure for %[1]v, with %[3]v`, destName, jobId, err.Error())
		return []byte{}, http.StatusBadRequest, err
	}

	// error handling if body is missing
	if resp.Body == nil {
		respData = []byte("[TransformerProxy] :: transformer returned empty response body")
		respCode = http.StatusBadRequest
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Failed with statusCode: %[3]v, message: %[4]v`, destName, jobId, respCode, string(respData))
		return respData, respCode, fmt.Errorf("[Transformer Proxy] :: transformer returned empty response body")
	}

	respData, err = io.ReadAll(resp.Body)
	defer resp.Body.Close()
	// error handling while reading from resp.Body
	if err != nil {
		respData = []byte(fmt.Sprintf(`[TransformerProxy] :: failed to read response body, Error:: %+v`, err))
		respCode = http.StatusBadRequest
		trans.logger.Errorf(`[TransformerProxy] (Dest-%[1]v) {Job - %[2]v} Failed with statusCode: %[3]v, message: %[4]v`, destName, jobId, respCode, respCode, string(respData))
		return respData, respCode, err
	}
	respCode = resp.StatusCode
	return respData, respCode, nil
}

func (trans *HandleT) MakeDeliveryRequest(ctx context.Context, structData integrations.PostParametersT) (interface{}, error) {
	postInfo := structData
	isRest := postInfo.Type == "REST"

	isMultipart := len(postInfo.Files) > 0

	// going forward we may want to support GraphQL and multipart requests
	// the files key in the response is specifically to handle the multipart usecase
	// for type GraphQL may need to support more keys like expected response format etc
	// in future it's expected that we will build on top of this response type
	// so, code addition should be done here instead of version bumping of response.
	if isRest && !isMultipart {
		requestMethod := postInfo.RequestMethod
		requestBody := postInfo.Body
		requestQueryParams := postInfo.QueryParams
		var bodyFormat string
		var bodyValue map[string]interface{}
		for k, v := range requestBody {
			if len(v.(map[string]interface{})) > 0 {
				bodyFormat = k
				bodyValue = v.(map[string]interface{})
				break
			}
		}

		var payload io.Reader
		// support for JSON and FORM body type
		if len(bodyValue) > 0 {
			switch bodyFormat {
			case "JSON":
				jsonValue, err := jsonfast.Marshal(bodyValue)
				if err != nil {
					// panic(err)
					return nil, fmt.Errorf("400 Unable to parse bodyValue. Unexpected transformer response: %+v", bodyValue)
				}
				payload = strings.NewReader(string(jsonValue))
			case "JSON_ARRAY":
				// support for JSON ARRAY
				jsonListStr, ok := bodyValue["batch"].(string)
				if !ok {
					trans.logger.Error("400 Unable to parse json list. Unexpected transformer response")
					return nil, fmt.Errorf("400 Unable to parse json list. Unexpected transformer response")
					// 	return &utils.SendPostResponse{
					// 		StatusCode:   400,
					// 		ResponseBody: []byte("400 Unable to parse json list. Unexpected transformer response"),
					// 	}
				}
				payload = strings.NewReader(jsonListStr)
			case "XML":
				strValue, ok := bodyValue["payload"].(string)
				if !ok {
					trans.logger.Error("400 Unable to construct xml payload. Unexpected transformer response")
					return nil, fmt.Errorf("400 Unable to construct xml payload. Unexpected transformer response")
					// 	return &utils.SendPostResponse{
					// 		StatusCode:   400,
					// 		ResponseBody: []byte("400 Unable to construct xml payload. Unexpected transformer response"),
					// 	}
				}
				payload = strings.NewReader(strValue)
			case "FORM":
				formValues := url.Values{}
				for key, val := range bodyValue {
					formValues.Set(key, fmt.Sprint(val)) // transformer ensures top level string values, still val.(string) would be restrictive
				}
				payload = strings.NewReader(formValues.Encode())
			default:
				return nil, fmt.Errorf("bodyFormat: %s is not supported", bodyFormat)
			}
		}

		payloadBytes, err := ioutil.ReadAll(payload)
		if err != nil {
			return nil, fmt.Errorf("problem in ReadAll: %s", err)
		}

		// return string(payloadBytes), nil

		// THINK: Shall return before forming the http.Request object ?
		req, err := http.NewRequestWithContext(ctx, requestMethod, postInfo.URL, payload)
		if err != nil {
			errStr := fmt.Sprintf(`400 Unable to construct "%s" request for URL : "%s"`, requestMethod, postInfo.URL)
			trans.logger.Error(errStr)
			return nil, fmt.Errorf(errStr)
			// return &utils.SendPostResponse{
			// 	StatusCode:   400,
			// 	ResponseBody: []byte(fmt.Sprintf(`400 Unable to construct "%s" request for URL : "%s"`, requestMethod, postInfo.URL)),
			// }
		}

		// add queryparams to the url
		// support of array type in params is handled if the
		// response from transformers are "," seperated
		queryParams := req.URL.Query()
		for key, val := range requestQueryParams {

			// list := strings.Split(valString, ",")
			// for _, listItem := range list {
			// 	queryParams.Add(key, fmt.Sprint(listItem))
			// }
			formattedVal := handleQueryParam(val)
			queryParams.Add(key, formattedVal)
		}

		req.URL.RawQuery = queryParams.Encode()
		headerKV := postInfo.Headers
		for key, val := range headerKV {
			req.Header.Add(key, val.(string))
		}

		req.Header.Add("User-Agent", "RudderLabs")

		// This is being done to facilitate compatible comparison
		// As map[string][]string is the data-type for headers in golang
		// But headers is an object in Javascript, hence we need to level the plane for effective comparison
		headersMap := make(map[string]string)
		for k, v := range req.Header {
			headersMap[strings.ToLower(k)] = string(v[0])
		}

		rtDelPayload := RouterDelPayload{
			Data:     string(payloadBytes),
			Method:   requestMethod,
			Params:   req.URL.Query(),
			Endpoint: postInfo.URL,
			Headers:  headersMap,
		}

		return rtDelPayload, nil
	}
	return nil, fmt.Errorf("this type of request currently is not supported")
}

//temp solution for handling complex query params
func handleQueryParam(param interface{}) string {
	switch p := param.(type) {
	case string:
		return p
	case map[string]interface{}:
		temp, err := jsonfast.Marshal(p)
		if err != nil {
			return fmt.Sprint(p)
		}

		jsonParam := string(temp)
		return jsonParam
	default:
		return fmt.Sprint(param)
	}
}

type RouterDelPayload struct {
	// The data of the router payload after some changes are applied while preparing the event delivery request
	Data interface{} `json:"data"`
	// The event delivery request method
	Method string `json:"method"`
	// The query parameters for the event delivery
	Params interface{} `json:"params"`
	// The url to which the event has to be sent
	Endpoint string `json:"endpoint"`
	// The headers for the event delivery request
	Headers interface{} `json:"headers"`
}

type ProxyTestReqBody struct {
	// The event delivery request payload formed by Router
	RouterDeliveryPayload RouterDelPayload
	// The payload which will be used by transformer to form event delivery request payload
	ProxyRequestPayload integrations.PostParametersT
}

func (trans *HandleT) ProxyRequestTest(ctx context.Context, responseData integrations.PostParametersT, destName string, jobId int64) (statusCode int, respBody string) {
	proxyReqBody := ProxyTestReqBody{}
	var delErr error
	var routerDeliveryRequest interface{}
	routerDeliveryRequest, delErr = trans.MakeDeliveryRequest(ctx, responseData)
	if delErr != nil {
		trans.logger.Errorf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} MakeRouterDelivery Error, with %[3]v`, destName, jobId, delErr.Error())
		return http.StatusOK, delErr.Error()
	}
	// proxyReqBody.RouterDeliveryPayload, delErr = jsonfast.Marshal(*routerDelPayload)
	// if delErr != nil {
	// 	trans.logger.Errorf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} RouterDeliveryMarshal Error, with %[3]v`, destName, jobId, delErr.Error())
	// 	return http.StatusOK, delErr.Error()
	// }
	// respPayload, respMarshErr := jsonfast.Marshal(responseData)
	// if respMarshErr != nil {
	// 	trans.logger.Errorf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} ProxyReqBody Marshal Error, with %[3]v`, destName, jobId, respMarshErr.Error())
	// 	return http.StatusOK, respMarshErr.Error()
	// }

	proxyReqBody.RouterDeliveryPayload = routerDeliveryRequest.(RouterDelPayload)
	proxyReqBody.ProxyRequestPayload = responseData

	payload, marshErr := jsonfast.Marshal(proxyReqBody)
	if marshErr != nil {
		trans.logger.Errorf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} ProxyReqBody Marshal Error, with %[3]v`, destName, jobId, marshErr.Error())
		return http.StatusOK, marshErr.Error()
	}
	// ProxyTest Url
	url := getProxyTestURL(destName)
	// New Request for ProxyTest endpoint
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		trans.logger.Errorf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} NewRequestWithContext Failed for %[1]v, with %[3]v`, destName, jobId, err.Error())
		return http.StatusOK, err.Error()
	}
	req.Header.Set("Content-Type", "application/json")

	_, err = trans.client.Do(req)
	if err != nil {
		trans.logger.Errorf(`[TransformerProxyTest] (Dest-%[1]v) {Job - %[2]v} Error in response or request to proxyTest with Error: %[3]v`, destName, jobId, err.Error())
		return http.StatusOK, err.Error()
	}

	return http.StatusOK, "ProxyTest Request Success [OK]"
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

func getProxyTestURL(destName string) string {
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/v0/destinations/" + strings.ToLower(destName) + "/proxyTest"
}
