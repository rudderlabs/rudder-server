package transformer

//go:generate mockgen -destination=../../mocks/router/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/router/transformer Transformer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/router/types"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	utilTypes "github.com/rudderlabs/rudder-server/utils/types"
	"github.com/tidwall/gjson"
)

const (
	BATCH            = "BATCH"
	ROUTER_TRANSFORM = "ROUTER_TRANSFORM"
)

//HandleT is the handle for this class
type HandleT struct {
	tr                                      *http.Transport
	client                                  *http.Client
	transformRequestTimerStat               stats.RudderStats
	transformerNetworkRequestTimerStat      stats.RudderStats
	transformerResponseTransformRequestTime stats.RudderStats
	logger                                  logger.LoggerI
}

//Transformer provides methods to transform events
type Transformer interface {
	Setup()
	Transform(transformType string, transformMessage *types.TransformMessageT) []types.DestinationJobT
	ProxyRequest(ctx context.Context, responseData integrations.PostParametersT, destName string) (statusCode int, respBody string)
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
	config.RegisterDurationConfigVariable(time.Duration(100), &retrySleep, true, time.Millisecond, []string{"Processor.retrySleep", "Processor.retrySleepInMS"}...)
	config.RegisterDurationConfigVariable(time.Duration(600), &timeoutDuration, true, time.Second, []string{"Processor.timeoutDuration", "Processor.timeoutDurationInSecond"}...)
	config.RegisterInt64ConfigVariable(15, &retryWithBackoffCount, true, 1, "Router.responseTransformRetryCount")
}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("router").Child("transformer")

}

//Transform transforms router jobs to destination jobs
func (trans *HandleT) Transform(transformType string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
	//Call remote transformation
	rawJSON, err := json.Marshal(transformMessage)
	if err != nil {
		panic(err)
	}
	trans.logger.Debugf("[Router Transfomrer] :: input payload : %s", string(rawJSON))

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
			err = json.Unmarshal(respData, &destinationJobs)
		} else if transformType == ROUTER_TRANSFORM {
			integrations.CollectIntgTransformErrorStats([]byte(gjson.GetBytes(respData, "output").Raw))
			err = json.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &destinationJobs)
		}
		//This is returned by our JS engine so should  be parsable
		//but still handling it
		if err != nil {
			panic(err)
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

func (trans *HandleT) ProxyRequest(ctx context.Context, responseData integrations.PostParametersT, destName string) (int, string) {
	rawJSON, err := json.Marshal(responseData)
	if err != nil {
		panic(err)
	}

	var respData []byte
	var respCode int

	url := getTransformerProxyURL(destName)
	payload := []byte(rawJSON)

	operation := func() error {
		var requestError error
		respData, respCode, requestError = trans.makeHTTPRequest(ctx, url, payload)
		return requestError
	}

	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(retryWithBackoffCount))
	err = backoff.RetryNotify(operation, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf("[Transformer Proxy] Request for proxy to URL:: %v, Error:: %+v retrying after:: %v,", url, err, t)
		stats.NewStat("responnse_transformer.retry_metric", stats.CountType).Increment()
	})

	if err != nil {
		panic(fmt.Errorf("[Transformer Proxy] Proxy request failed after max retries Error:: %+v", err))
	}

	//Detecting content type of the respBody
	contentTypeHeader := strings.ToLower(http.DetectContentType(respData))
	//If content type is not of type "*text*", overriding it with empty string
	if !(strings.Contains(contentTypeHeader, "text") ||
		strings.Contains(contentTypeHeader, "application/json") ||
		strings.Contains(contentTypeHeader, "application/xml")) {
		respData = []byte("")
	}

	/*
		Response Transsformer Response payload:
		{
			output: {
				status: [destination status compatible with server]
				message: [ generic message for jobs_db payload]
				destinationResponse: [actual response payload from destination]
			}
		}
	*/
	// response transform success extract the value of output, marshal to TransResponseT Type

	transformerResponse := integrations.TransResponseT{
		Message: "[Transformer Proxy]:: Default Message TransResponseT",
	}
	respData = []byte(gjson.GetBytes(respData, "output").Raw)
	integrations.CollectDestErrorStats(respData)
	err = json.Unmarshal(respData, &transformerResponse)
	// unmarshal failure
	if err != nil {
		errStr := string(respData) + " [Transformer Proxy Unmarshaling::]" + err.Error()
		trans.logger.Errorf(errStr)
		respData = []byte(errStr)
		respCode = http.StatusBadRequest
		return respCode, string(respData)
	}
	// unmarshal success
	respData, err = json.Marshal(transformerResponse)
	if err != nil {
		panic(fmt.Errorf("[Transformer Proxy:: failed to Marshal proxy response : %+v", err))
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
	trans.transformerResponseTransformRequestTime = stats.NewStat("router.transformer_response_transform_time", stats.TimerType)
}

func (trans *HandleT) makeHTTPRequest(ctx context.Context, url string, payload []byte) ([]byte, int, error) {
	var respData []byte
	var respCode int
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(payload))
	if err != nil {
		return []byte{}, http.StatusBadRequest, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := trans.client.Do(req)

	if err != nil {
		return []byte{}, http.StatusBadRequest, err
	}

	// error handling if body is missing
	if resp.Body == nil {
		respData = []byte("[Response Transform] :: transformer returned empty response body")
		respCode = http.StatusBadRequest
		return respData, respCode, fmt.Errorf("[Response Transform] :: transformer returned empty response body")
	}

	respData, err = io.ReadAll(resp.Body)
	defer resp.Body.Close()
	// error handling while reading from resp.Body
	if err != nil {
		respData = []byte(fmt.Sprintf(`[Response Transform] :: failed to read response body, Error:: %+v`, err))
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

func getTransformerProxyURL(destName string) string {
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/v0/destinations/" + strings.ToLower(destName) + "/proxy"
}
