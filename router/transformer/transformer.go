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
	ResponseTransform(ctx context.Context, responseData integrations.DeliveryResponseT, destName string) (statusCode int, respBody string)
}

//NewTransformer creates a new transformer
func NewTransformer() *HandleT {
	return &HandleT{}
}

var (
	maxRetry        int
	retrySleep      time.Duration
	timeoutDuration time.Duration
	pkgLogger       logger.LoggerI
)

func loadConfig() {
	config.RegisterIntConfigVariable(30, &maxRetry, true, 1, "Processor.maxRetry")
	config.RegisterDurationConfigVariable(time.Duration(100), &retrySleep, true, time.Millisecond, []string{"Processor.retrySleep", "Processor.retrySleepInMS"}...)
	config.RegisterDurationConfigVariable(time.Duration(30), &timeoutDuration, true, time.Second, []string{"Processor.timeoutDuration", "Processor.timeoutDurationInSecond"}...)
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

func (trans *HandleT) ResponseTransform(ctx context.Context, responseData integrations.DeliveryResponseT, destName string) (statusCode int, respBody string) {
	rawJSON, err := json.Marshal(responseData)
	requestFailed := false
	retryCount := 0
	if err != nil {
		panic(err)
	}
	var resp *http.Response
	var respData []byte
	var respCode int
	var tempRespData []byte
	url := getResponseTransformURL(destName)
	payload := strings.NewReader(string(rawJSON))
	for {
		payload.Seek(0, io.SeekStart)
		req, err := http.NewRequestWithContext(ctx, "POST", url, payload)
		if err != nil {
			trans.logger.Error(fmt.Sprintf(`400 Unable to construct POST request for URL : "%s"`, url))
			return 400, fmt.Sprintf(`400 Unable to construct POST request for URL : "%s"`, url)
		}
		req.Header.Add("Content-Type", "application/json")
		s := time.Now()
		resp, err = trans.client.Do(req)
		trans.transformerResponseTransformRequestTime.SendTiming(time.Since(s))
		// retry in case of err
		if err != nil {
			trans.logger.Errorf("[Transformer Response Transform request failed] :: %+v", err)
			requestFailed = true
			if retryCount > maxRetry {
				panic(fmt.Errorf("Transformer HTTP connection error: URL: %v Error: %+v", url, err))
			}
			retryCount++
			time.Sleep(retrySleep)
			//Refresh the connection
			continue
		}

		if resp != nil && resp.Body != nil {
			tempRespData, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
		}
		//Detecting content type of the respBody
		contentTypeHeader := http.DetectContentType(tempRespData)
		//If content type is not of type "*text*", overriding it with empty string
		if !(strings.Contains(strings.ToLower(contentTypeHeader), "text") ||
			strings.Contains(strings.ToLower(contentTypeHeader), "application/json") ||
			strings.Contains(strings.ToLower(contentTypeHeader), "application/xml")) {
			tempRespData = []byte("")
		}

		if requestFailed {
			trans.logger.Errorf("Failed request succeeded after %v retries, URL: %v", retryCount, url)
		}

		if resp != nil {
			//handling for 404
			if resp.StatusCode == 404 {
				panic(fmt.Errorf("[Response transform doesnot exist for URL: URL: %v", url))
			}
			// handling for 5xx
			if resp.StatusCode >= 500 && resp.StatusCode < 600 {
				var transError integrations.TransErrorT
				rawTResponse := []byte(gjson.GetBytes(tempRespData, "output").Raw)
				// if the response is not containing "output" is not an explicit error from transformer
				if len(rawTResponse) != 0 {
					err = json.Unmarshal(rawTResponse, &transError)
					if err != nil {
						respData = []byte("Failed to parse response transform at server error: " + err.Error())
						respCode = 400
						break
					}
					// error is explicitly lodged from transformer marking a destination side failure
					if transError.ErrorDetailed.ResponseTransformFailure {
						integrations.CollectDestErrorStats(rawTResponse)
						respData = tempRespData
						respCode = resp.StatusCode
						break
					}
				}
				// network level 5xx error we need to retry response transform request
				trans.logger.Errorf("[Transformer Response Transform request failed] :: %+v", err)
				requestFailed = true
				if retryCount > maxRetry {
					panic(fmt.Errorf("Transformer HTTP connection error: URL: %v Error: %+v", url, err))
				}
				retryCount++
				time.Sleep(retrySleep)
				//Refresh the connection
				continue
			}
			rawTResponse := []byte(gjson.GetBytes(tempRespData, "output").Raw)
			integrations.CollectDestErrorStats(rawTResponse)
			respData = tempRespData
			respCode = resp.StatusCode
			break
		}
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

func getBatchURL() string {
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/batch"
}

func getRouterTransformURL() string {
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/routerTransform"
}

func getResponseTransformURL(destName string) string {
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/transform/" + strings.ToLower(destName) + "/response"
}
