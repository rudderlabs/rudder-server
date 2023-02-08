package transformer

//go:generate mockgen -destination=../../mocks/processor/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/processor/transformer Transformer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime/trace"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/types"
)

const (
	UserTransformerStage        = "user_transformer"
	EventFilterStage            = "event_filter"
	DestTransformerStage        = "dest_transformer"
	TrackingPlanValidationStage = "trackingPlan_validation"
)

const (
	StatusCPDown              = 809
	TransformerRequestFailure = 909
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

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
	JobID               int64                             `json:"jobId"`
	SourceJobID         string                            `json:"sourceJobId"`
	SourceJobRunID      string                            `json:"sourceJobRunId"`
	SourceTaskRunID     string                            `json:"sourceTaskRunId"`
	RecordID            interface{}                       `json:"recordId"`
	DestinationType     string                            `json:"destinationType"`
	MessageID           string                            `json:"messageId"`
	OAuthAccessToken    string                            `json:"oauthAccessToken"`
	// set by user_transformer to indicate transformed event is part of group indicated by messageIDs
	MessageIDs              []string `json:"messageIds"`
	RudderID                string   `json:"rudderId"`
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
	Libraries   []backendconfig.LibraryT   `json:"libraries"`
}

// HandleT is the handle for this class
type HandleT struct {
	sentStat     stats.Measurement
	receivedStat stats.Measurement
	cpDownGauge  stats.Measurement

	logger logger.Logger

	Client *http.Client

	guardConcurrency chan struct{}
}

// Transformer provides methods to transform events
type Transformer interface {
	Setup()
	Transform(ctx context.Context, clientEvents []TransformerEventT, url string, batchSize int) ResponseT
	Validate(clientEvents []TransformerEventT, url string, batchSize int) ResponseT
}

// NewTransformer creates a new transformer
func NewTransformer() *HandleT {
	return &HandleT{}
}

var (
	maxConcurrency, maxHTTPConnections, maxHTTPIdleConnections, maxRetry int
	retrySleep                                                           time.Duration
	timeoutDuration                                                      time.Duration
	pkgLogger                                                            logger.Logger
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
	config.RegisterDurationConfigVariable(100, &retrySleep, true, time.Millisecond, []string{"Processor.retrySleep", "Processor.retrySleepInMS"}...)
	config.RegisterDurationConfigVariable(30, &timeoutDuration, false, time.Second, "HttpClient.procTransformer.timeout")
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

// Setup initializes this class
func (trans *HandleT) Setup() {
	trans.logger = pkgLogger
	trans.sentStat = stats.Default.NewStat("processor.transformer_sent", stats.CountType)
	trans.receivedStat = stats.Default.NewStat("processor.transformer_received", stats.CountType)
	trans.cpDownGauge = stats.Default.NewStat("processor.control_plane_down", stats.GaugeType)

	trans.guardConcurrency = make(chan struct{}, maxConcurrency)

	if trans.Client == nil {
		trans.Client = &http.Client{
			Transport: &http.Transport{
				MaxConnsPerHost:     maxHTTPConnections,
				MaxIdleConnsPerHost: maxHTTPIdleConnections,
				IdleConnTimeout:     time.Minute,
			},
			Timeout: timeoutDuration,
		}
	}
}

// ResponseT represents a Transformer response
type ResponseT struct {
	Events       []TransformerResponseT
	FailedEvents []TransformerResponseT
}

// GetVersion gets the transformer version by asking it on /transfomerBuildVersion. if there is any error it returns empty string
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
	defer func() { httputil.CloseResponse(resp) }()
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

// Transform function is used to invoke transformer API
func (trans *HandleT) Transform(ctx context.Context, clientEvents []TransformerEventT,
	url string, batchSize int,
) ResponseT {
	if len(clientEvents) == 0 {
		return ResponseT{}
	}

	sTags := statsTags(clientEvents[0])

	batchCount := len(clientEvents) / batchSize
	if len(clientEvents)%batchSize != 0 {
		batchCount += 1
	}

	stats.Default.NewTaggedStat(
		"processor.transformer_request_batch_count",
		stats.HistogramType,
		sTags,
	).Observe(float64(batchCount))
	trace.Logf(ctx, "request", "batch_count: %d", batchCount)

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
			trace.WithRegion(ctx, "request", func() {
				transformResponse[i] = trans.request(ctx, url, clientEvents[from:to])
			})
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

		// Transform is one to many mapping so returned
		// response for each is an array. We flatten it out
		for _, transformerResponse := range batch {
			if transformerResponse.StatusCode != 200 {
				failedEvents = append(failedEvents, transformerResponse)
				continue
			}
			outClientEvents = append(outClientEvents, transformerResponse)
		}
	}

	trans.receivedStat.Count(len(outClientEvents))

	return ResponseT{
		Events:       outClientEvents,
		FailedEvents: failedEvents,
	}
}

func (trans *HandleT) Validate(clientEvents []TransformerEventT,
	url string, batchSize int,
) ResponseT {
	return trans.Transform(context.TODO(), clientEvents, url, batchSize)
}

func (*HandleT) requestTime(s stats.Tags, d time.Duration) {
	stats.Default.NewTaggedStat("processor.transformer_request_time", stats.TimerType, s).SendTiming(d)
}

func statsTags(event TransformerEventT) stats.Tags {
	return stats.Tags{
		"dest_type": event.Destination.DestinationDefinition.Name,
		"dest_id":   event.Destination.ID,
		"src_id":    event.Metadata.SourceID,
	}
}

func (trans *HandleT) request(ctx context.Context, url string, data []TransformerEventT) []TransformerResponseT {
	// Call remote transformation
	var (
		rawJSON []byte
		err     error
	)

	trace.WithRegion(ctx, "marshal", func() {
		rawJSON, err = jsonfast.Marshal(data)
	})
	trace.Logf(ctx, "marshal", "request raw body size: %d", len(rawJSON))
	if err != nil {
		panic(err)
	}

	if len(data) == 0 {
		return nil
	}

	var (
		respData   []byte
		statusCode int
	)

	// endless retry if transformer-controlplane connection is down
	endlessBackoff := backoff.NewExponentialBackOff()
	endlessBackoff.MaxElapsedTime = 0 // no max time -> ends only when no error
	// endless backoff loop, only nil error or panics inside
	_ = backoff.RetryNotify(
		func() error {
			respData, statusCode = trans.doPost(ctx, rawJSON, url, statsTags(data[0]))
			if statusCode == StatusCPDown {
				trans.cpDownGauge.Gauge(1)
				return fmt.Errorf("control plane not reachable")
			}
			trans.cpDownGauge.Gauge(0)
			return nil
		},
		endlessBackoff,
		func(err error, t time.Duration) {
			var transformationID, transformationVersionID string
			if len(data[0].Destination.Transformations) > 0 {
				transformationID = data[0].Destination.Transformations[0].ID
				transformationVersionID = data[0].Destination.Transformations[0].VersionID
			}
			trans.logger.Errorf("JS HTTP connection error: URL: %v Error: %+v. WorkspaceID: %s, sourceID: %s, destinationID: %s, transformationID: %s, transformationVersionID: %s",
				url, err, data[0].Metadata.WorkspaceID, data[0].Metadata.SourceID, data[0].Metadata.DestinationID,
				transformationID, transformationVersionID)
		})
	// control plane back up

	switch statusCode {
	case http.StatusOK,
		http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusRequestEntityTooLarge:
	default:
		trans.logger.Errorf("Transformer returned status code: %v", statusCode)
	}

	var transformerResponses []TransformerResponseT
	if statusCode == http.StatusOK {
		integrations.CollectIntgTransformErrorStats(respData)

		trace.Logf(ctx, "Unmarshal", "response raw size: %d", len(respData))
		trace.WithRegion(ctx, "Unmarshal", func() {
			err = jsonfast.Unmarshal(respData, &transformerResponses)
		})
		// This is returned by our JS engine so should  be parsable
		// but still handling it
		if err != nil {
			trans.logger.Errorf("Data sent to transformer : %v", string(rawJSON))
			trans.logger.Errorf("Transformer returned : %v", string(respData))
			respData = []byte(fmt.Sprintf("Failed to unmarshal transformer response: %s", string(respData)))
			transformerResponses = nil
			statusCode = 400
		}
	}

	if statusCode != http.StatusOK {
		for i := range data {
			transformEvent := &data[i]
			resp := TransformerResponseT{StatusCode: statusCode, Error: string(respData), Metadata: transformEvent.Metadata}
			transformerResponses = append(transformerResponses, resp)
		}
	}
	return transformerResponses
}

func (trans *HandleT) doPost(ctx context.Context, rawJSON []byte, url string, tags stats.Tags) ([]byte, int) {
	var (
		retryCount int
		resp       *http.Response
		respData   []byte
	)

	err := backoff.RetryNotify(
		func() error {
			var reqErr error
			s := time.Now()
			trace.WithRegion(ctx, "request/post", func() {
				resp, reqErr = trans.Client.Post(url, "application/json; charset=utf-8", bytes.NewBuffer(rawJSON))
			})
			trans.requestTime(tags, time.Since(s))
			if reqErr != nil {
				return reqErr
			}
			defer func() { httputil.CloseResponse(resp) }()
			respData, reqErr = io.ReadAll(resp.Body)
			return reqErr
		},
		backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(maxRetry)),
		func(err error, t time.Duration) {
			retryCount++
			trans.logger.Warnf("JS HTTP connection error: URL: %v Error: %+v after %v tries", url, err, retryCount)
		})
	if err != nil {
		if config.GetBool("Processor.Transformer.failOnError", false) {
			return []byte(fmt.Sprintf("transformer request failed: %s", err)), TransformerRequestFailure
		} else {
			panic(err)
		}
	}

	// perform version compatibility check only on success
	if resp.StatusCode == http.StatusOK {
		transformerAPIVersion, convErr := strconv.Atoi(resp.Header.Get("apiVersion"))
		if convErr != nil {
			transformerAPIVersion = 0
		}
		if types.SupportedTransformerApiVersion != transformerAPIVersion {
			unexpectedVersionError := fmt.Errorf("incompatible transformer version: Expected: %d Received: %d, URL: %v", types.SupportedTransformerApiVersion, transformerAPIVersion, url)
			trans.logger.Error(unexpectedVersionError)
			panic(unexpectedVersionError)
		}
	}

	return respData, resp.StatusCode
}
