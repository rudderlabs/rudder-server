package transformer

//go:generate mockgen -destination=../../mocks/processor/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/processor/transformer Transformer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

const (
	UserTransformerStage        = "user_transformer"
	DestTransformerStage        = "dest_transformer"
	TrackingPlanValidationStage = "trackingPlan_validation"
)
const supportedTransformerAPIVersion = 1

type MetadataT struct {
	SourceID            string                            `json:"sourceId"`
	WorkspaceID         string                            `json:"workspaceId"`
	Namespace           string                            `json:"namespace"`
	InstanceID          string                            `json:"instanceId"`
	SourceType          string                            `json:"sourceType"`
	SourceCategory      string                            `json:"sourceCategory"`
	TrackingPlanId      string                            `json:"trackingPlanId"`
	TrackingPlanVersion int                               `json:"trackingPlanVersion"`
	SourceTpConfig      map[string]map[string]interface{} `json:"sourceTpConfig"`
	MergedTpConfig      map[string]interface{}            `json:"mergedTpConfig"`
	DestinationID       string                            `json:"destinationId"`
	JobRunID            string                            `json:"jobRunId"`
	JobID               int64                             `json:"jobId"`
	SourceBatchID       string                            `json:"sourceBatchId"`
	SourceJobID         string                            `json:"sourceJobId"`
	SourceJobRunID      string                            `json:"sourceJobRunId"`
	SourceTaskID        string                            `json:"sourceTaskId"`
	SourceTaskRunID     string                            `json:"sourceTaskRunId"`
	RecordID            interface{}                       `json:"recordId"`
	DestinationType     string                            `json:"destinationType"`
	MessageID           string                            `json:"messageId"`
	// set by user_transformer to indicate transformed event is part of group indicated by messageIDs
	MessageIDs              []string `json:"messageIds"`
	RudderID                string   `json:"rudderId"`
	SessionID               string   `json:"sessionId,omitempty"`
	ReceivedAt              string   `json:"receivedAt"`
	EventName               string   `json:"eventName"`
	EventType               string   `json:"eventType"`
	SourceDefinitionID      string   `json:"sourceDefinitionId"`
	DestinationDefinitionID string   `json:"destinationDefinitionId"`
}

type TransformerEventT struct {
	Message     types.SingularEventT       `json:"message"`
	Metadata    MetadataT                  `json:"metadata"`
	Destination backendconfig.DestinationT `json:"destination"`
	SessionID   string                     `json:"session_id,omitempty"`
	Libraries   []backendconfig.LibraryT   `json:"libraries"`
}

//HandleT is the handle for this class
type HandleT struct {
	perfStats                 *misc.PerfStats
	sentStat                  stats.RudderStats
	receivedStat              stats.RudderStats
	failedStat                stats.RudderStats
	transformTimerStat        stats.RudderStats
	transformRequestTimerStat stats.RudderStats

	logger logger.LoggerI

	Client *http.Client

	guardConcurrency chan struct{}
}

//Transformer provides methods to transform events
type Transformer interface {
	Setup()
	Transform(clientEvents []TransformerEventT, url string, batchSize int) ResponseT
	Validate(clientEvents []TransformerEventT, url string, batchSize int) ResponseT
}

//NewTransformer creates a new transformer
func NewTransformer() *HandleT {
	return &HandleT{}
}

var (
	maxConcurrency, maxHTTPConnections, maxHTTPIdleConnections, maxRetry int
	retrySleep                                                           time.Duration
	pkgLogger                                                            logger.LoggerI
)

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("processor").Child("transformer")
}

func loadConfig() {
	config.RegisterIntConfigVariable(200, &maxConcurrency, false, 1, "Processor.maxConcurrency")
	config.RegisterIntConfigVariable(100, &maxHTTPConnections, false, 1, "Processor.maxHTTPConnections")
	config.RegisterIntConfigVariable(50, &maxHTTPIdleConnections, false, 1, "Processor.maxHTTPIdleConnections")

	config.RegisterIntConfigVariable(30, &maxRetry, true, 1, "Processor.maxRetry")
	config.RegisterDurationConfigVariable(time.Duration(100), &retrySleep, true, time.Millisecond, []string{"Processor.retrySleep", "Processor.retrySleepInMS"}...)
}

type TransformerResponseT struct {
	// Not marking this Singular Event, since this not a RudderEvent
	Output           map[string]interface{} `json:"output"`
	Metadata         MetadataT              `json:"metadata"`
	StatusCode       int                    `json:"statusCode"`
	Error            string                 `json:"error"`
	ValidationErrors []ValidationErrorT     `json:"validationErrors"`
}

type ValidationErrorT struct {
	Type    string            `json:"type"`
	Message string            `json:"message"`
	Meta    map[string]string `json:"meta"`
}

//Setup initializes this class
func (trans *HandleT) Setup() {
	trans.logger = pkgLogger
	trans.sentStat = stats.NewStat("processor.transformer_sent", stats.CountType)
	trans.receivedStat = stats.NewStat("processor.transformer_received", stats.CountType)
	trans.failedStat = stats.NewStat("processor.transformer_failed", stats.CountType)
	trans.transformTimerStat = stats.NewStat("processor.transformation_time", stats.TimerType)
	trans.transformRequestTimerStat = stats.NewStat("processor.transformer_request_time", stats.TimerType)

	trans.guardConcurrency = make(chan struct{}, maxConcurrency)
	trans.perfStats = &misc.PerfStats{}
	trans.perfStats.Setup("JS Call")

	if trans.Client == nil {
		trans.Client = &http.Client{
			Transport: &http.Transport{
				MaxConnsPerHost:     maxHTTPConnections,
				MaxIdleConnsPerHost: maxHTTPIdleConnections,
				IdleConnTimeout:     time.Minute,
			},
		}
	}
}

//ResponseT represents a Transformer response
type ResponseT struct {
	Events       []TransformerResponseT
	FailedEvents []TransformerResponseT
}

//GetVersion gets the transformer version by asking it on /transfomerBuildVersion. if there is any error it returns empty string
func GetVersion() (transformerBuildVersion string) {
	transformerBuildVersion = "Not an official release. Get the latest release from dockerhub."
	url := integrations.GetTransformerURL() + "/transformerBuildVersion"
	resp, err := http.Get(url)
	if err != nil {
		pkgLogger.Errorf("Unable to make a transfomer build version call with error : %s", err.Error())
		return

	}
	if resp == nil {
		transformerBuildVersion = fmt.Sprintf("No response from transformer. %s", transformerBuildVersion)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			pkgLogger.Errorf("Unable to read response into bytes with error : %s", err.Error())
			transformerBuildVersion = "Unable to read response from transformer."
			return
		}
		transformerBuildVersion = string(bodyBytes)
	}
	return
}

//Transform function is used to invoke transformer API
func (trans *HandleT) Transform(clientEvents []TransformerEventT,
	url string, batchSize int) ResponseT {

	s := time.Now()
	defer func() {
		trans.transformTimerStat.SendTiming(time.Since(s))
	}()

	batchCount := len(clientEvents) / batchSize
	if len(clientEvents)%batchSize != 0 {
		batchCount += 1
	}
	transformResponse := make([][]TransformerResponseT, batchCount)

	wg := sync.WaitGroup{}
	wg.Add(len(transformResponse))
	for i := range transformResponse {
		i := i
		from := i * batchSize
		to := (i + 1) * batchSize
		if to > len(clientEvents) {
			to = len(clientEvents)
		}
		trans.guardConcurrency <- struct{}{}
		go func() {
			transformResponse[i] = trans.request(url, clientEvents[from:to])
			<-trans.guardConcurrency
			wg.Done()
		}()
	}
	wg.Wait()

	var outClientEvents []TransformerResponseT
	var failedEvents []TransformerResponseT

	for _, batch := range transformResponse {
		if batch == nil {
			continue
		}

		//Transform is one to many mapping so returned
		//response for each is an array. We flatten it out
		for _, transformerResponse := range batch {
			if transformerResponse.StatusCode != 200 {
				failedEvents = append(failedEvents, transformerResponse)
				continue
			}
			outClientEvents = append(outClientEvents, transformerResponse)
		}
	}

	trans.receivedStat.Count(len(outClientEvents))
	trans.failedStat.Count(len(failedEvents))
	trans.perfStats.Rate(len(clientEvents), time.Since(s))
	trans.perfStats.Print()

	return ResponseT{
		Events:       outClientEvents,
		FailedEvents: failedEvents,
	}
}

func (trans *HandleT) Validate(clientEvents []TransformerEventT,
	url string, batchSize int) ResponseT {
	return trans.Transform(clientEvents, url, batchSize)
}





var defaultTransformerFeatures = `{
	"routerTransform": {
	  "MARKETO": true,
	  "HS": true
	}
  }`


type transfomerFeatures struct {
	RouterTransform map[string]bool `json:"routerTransform"`
}

type defaultTransfomerFeatures struct {
	RouterTransform map[string]bool `json:"routerTransform"`
}

func (trans *HandleT) Features(ctx context.Context, transformerURL string) (transfomerFeatures, error) {
	url := transformerURL + "/features"

	features := json.RawMessage(defaultTransformerFeatures)

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)
	err := backoff.RetryNotify(func() error {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return err
		}

		res, err := trans.Client.Do(req)
		if err != nil {
			return err
		}
		if res.StatusCode == 404 {
			return nil
		}
		if res.StatusCode != 200 {
			return fmt.Errorf("unexpected error code: %d", res.StatusCode)
		}

		body, err := io.ReadAll(res.Body)
		if err == nil {
			return err
		}
		defer res.Body.Close()
		features = json.RawMessage(body)

		return nil
	}, b, func(err error, t time.Duration) {
		trans.logger.Warnf("Error fetching transformer features: %s", err)
	})

	return features, err
}

func (trans *HandleT) request(url string, data []TransformerEventT) []TransformerResponseT {
	//Call remote transformation
	rawJSON, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	retryCount := 0
	var resp *http.Response
	var respData []byte
	//We should rarely have error communicating with our JS
	reqFailed := false

	for {
		s := time.Now()
		resp, err = trans.Client.Post(url, "application/json; charset=utf-8", bytes.NewBuffer(rawJSON))

		if err == nil {
			//If no err returned by client.Post, reading body.
			//If reading body fails, retrying.
			respData, err = io.ReadAll(resp.Body)
			resp.Body.Close()
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

		// perform version compatability check only on success
		if resp.StatusCode == http.StatusOK {
			transformerAPIVersion, convErr := strconv.Atoi(resp.Header.Get("apiVersion"))
			if convErr != nil {
				transformerAPIVersion = 0
			}
			if supportedTransformerAPIVersion != transformerAPIVersion {
				trans.logger.Errorf("Incompatible transformer version: Expected: %d Received: %d, URL: %v", supportedTransformerAPIVersion, transformerAPIVersion, url)
				panic(fmt.Errorf("Incompatible transformer version: Expected: %d Received: %d, URL: %v", supportedTransformerAPIVersion, transformerAPIVersion, url))
			}
		}

		trans.transformRequestTimerStat.SendTiming(time.Since(s))
		break
	}

	// Remove Assertion?
	if !(resp.StatusCode == http.StatusOK ||
		resp.StatusCode == http.StatusBadRequest ||
		resp.StatusCode == http.StatusNotFound ||
		resp.StatusCode == http.StatusRequestEntityTooLarge) {
		trans.logger.Errorf("Transformer returned status code: %v", resp.StatusCode)
	}

	var transformerResponses []TransformerResponseT
	if resp.StatusCode == http.StatusOK {
		err = json.Unmarshal(respData, &transformerResponses)
		//This is returned by our JS engine so should  be parsable
		//but still handling it
		if err != nil {
			trans.logger.Errorf("Data sent to transformer : %v", string(rawJSON))
			trans.logger.Errorf("Transformer returned : %v", string(respData))
			respData = []byte(fmt.Sprintf("Failed to unmarshal transformer response: %s", string(respData)))
			transformerResponses = nil
			resp.StatusCode = 400
		}
	}

	if resp.StatusCode != http.StatusOK {
		for i := range data {
			transformEvent := &data[i]
			resp := TransformerResponseT{StatusCode: resp.StatusCode, Error: string(respData), Metadata: transformEvent.Metadata}
			transformerResponses = append(transformerResponses, resp)
		}
	}
	return transformerResponses
}
