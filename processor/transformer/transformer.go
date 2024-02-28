package transformer

//go:generate mockgen -destination=../../mocks/processor/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/processor/transformer Transformer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/cenkalti/backoff"
	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

const (
	userTransformerStage        = "user_transformer"
	destTransformerStage        = "dest_transformer"
	trackingPlanValidationStage = "trackingPlan_validation"
)

const (
	StatusCPDown              = 809
	TransformerRequestFailure = 909
	TransformerRequestTimeout = 919
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Metadata struct {
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
	TraceParent         string                            `json:"traceparent"`
	// set by user_transformer to indicate transformed event is part of group indicated by messageIDs
	MessageIDs              []string `json:"messageIds"`
	RudderID                string   `json:"rudderId"`
	ReceivedAt              string   `json:"receivedAt"`
	EventName               string   `json:"eventName"`
	EventType               string   `json:"eventType"`
	SourceDefinitionID      string   `json:"sourceDefinitionId"`
	DestinationDefinitionID string   `json:"destinationDefinitionId"`
	TransformationID        string   `json:"transformationId"`
	TransformationVersionID string   `json:"transformationVersionId"`
	SourceDefinitionType    string   `json:"-"`
}

func (m Metadata) GetMessagesIDs() []string {
	if len(m.MessageIDs) > 0 {
		return m.MessageIDs
	}
	return []string{m.MessageID}
}

type TransformerEvent struct {
	Message     types.SingularEventT       `json:"message"`
	Metadata    Metadata                   `json:"metadata"`
	Destination backendconfig.DestinationT `json:"destination"`
	Libraries   []backendconfig.LibraryT   `json:"libraries"`
}

func isJobTerminated(status int) bool {
	if status == http.StatusTooManyRequests || status == http.StatusRequestTimeout {
		return false
	}
	return status >= http.StatusOK && status < http.StatusInternalServerError
}

type TransformerResponse struct {
	// Not marking this Singular Event, since this not a RudderEvent
	Output           map[string]interface{} `json:"output"`
	Metadata         Metadata               `json:"metadata"`
	StatusCode       int                    `json:"statusCode"`
	Error            string                 `json:"error"`
	ValidationErrors []ValidationError      `json:"validationErrors"`
}

type ValidationError struct {
	Type    string            `json:"type"`
	Message string            `json:"message"`
	Meta    map[string]string `json:"meta"`
}

// Response represents a Transformer response
type Response struct {
	Events       []TransformerResponse
	FailedEvents []TransformerResponse
}

// GetVersion gets the transformer version by asking it on /transformerBuildVersion. if there is any error it returns empty string
func GetVersion() string {
	defaultBuildVersion := "Not an official release. Get the latest release from dockerhub."

	transformURL := config.GetString("DEST_TRANSFORM_URL", "http://localhost:9090") + "/transformerBuildVersion"

	resp, err := http.Get(transformURL)
	if err != nil {
		fmt.Printf("Unable to make a transformer build version call with error: %s", err.Error())
		return defaultBuildVersion
	}
	if resp == nil {
		return fmt.Sprintf("No response from transformer. %s", defaultBuildVersion)
	}
	defer func() { httputil.CloseResponse(resp) }()

	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("Unable to read response into bytes with error : ", err.Error())
			return "Unable to read response from transformer."
		}

		return string(bodyBytes)
	}
	return defaultBuildVersion
}

type Opt func(*handle)

func WithClient(client *http.Client) Opt {
	return func(s *handle) {
		s.client = client
	}
}

// Transformer provides methods to transform events
type Transformer interface {
	Transform(ctx context.Context, clientEvents []TransformerEvent, batchSize int) Response
	UserTransform(ctx context.Context, clientEvents []TransformerEvent, batchSize int) Response
	Validate(ctx context.Context, clientEvents []TransformerEvent, batchSize int) Response
}

// handle is the handle for this class
type handle struct {
	sentStat     stats.Measurement
	receivedStat stats.Measurement
	cpDownGauge  stats.Measurement

	conf   *config.Config
	logger logger.Logger
	stat   stats.Stats

	client *http.Client

	guardConcurrency chan struct{}

	config struct {
		maxConcurrency         int
		maxHTTPConnections     int
		maxHTTPIdleConnections int
		disableKeepAlives      bool

		timeoutDuration time.Duration

		maxRetry                   misc.ValueLoader[int]
		failOnUserTransformTimeout misc.ValueLoader[bool]
		failOnError                misc.ValueLoader[bool]

		destTransformationURL string
		userTransformationURL string
	}
}

// NewTransformer creates a new transformer
func NewTransformer(conf *config.Config, log logger.Logger, stat stats.Stats, opts ...Opt) Transformer {
	trans := handle{}

	trans.conf = conf
	trans.logger = log.Child("transformer")
	trans.stat = stat

	trans.sentStat = stat.NewStat("processor.transformer_sent", stats.CountType)
	trans.receivedStat = stat.NewStat("processor.transformer_received", stats.CountType)
	trans.cpDownGauge = stat.NewStat("processor.control_plane_down", stats.GaugeType)

	trans.config.maxConcurrency = conf.GetInt("Processor.maxConcurrency", 200)
	trans.config.maxHTTPConnections = conf.GetInt("Transformer.Client.maxHTTPConnections", 100)
	trans.config.maxHTTPIdleConnections = conf.GetInt("Transformer.Client.maxHTTPIdleConnections", 10)
	trans.config.disableKeepAlives = conf.GetBool("Transformer.Client.disableKeepAlives", true)
	trans.config.timeoutDuration = conf.GetDuration("HttpClient.procTransformer.timeout", 600, time.Second)
	trans.config.destTransformationURL = conf.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	trans.config.userTransformationURL = conf.GetString("USER_TRANSFORM_URL", trans.config.destTransformationURL)

	trans.config.maxRetry = conf.GetReloadableIntVar(30, 1, "Processor.maxRetry")
	trans.config.failOnUserTransformTimeout = conf.GetReloadableBoolVar(false, "Processor.Transformer.failOnUserTransformTimeout")
	trans.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.Transformer.failOnError")

	trans.guardConcurrency = make(chan struct{}, trans.config.maxConcurrency)

	if trans.client == nil {
		trans.client = &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives:   trans.config.disableKeepAlives,
				MaxConnsPerHost:     trans.config.maxHTTPConnections,
				MaxIdleConnsPerHost: trans.config.maxHTTPIdleConnections,
				IdleConnTimeout:     30 * time.Second,
			},
			Timeout: trans.config.timeoutDuration,
		}
	}

	for _, opt := range opts {
		opt(&trans)
	}

	return &trans
}

// Transform function is used to invoke destination transformer API
func (trans *handle) Transform(ctx context.Context, clientEvents []TransformerEvent, batchSize int) Response {
	if len(clientEvents) == 0 {
		return Response{}
	}
	destType := clientEvents[0].Destination.DestinationDefinition.Name
	transformURL := trans.destTransformURL(destType)
	return trans.transform(ctx, clientEvents, transformURL, batchSize, destTransformerStage)
}

// UserTransform function is used to invoke user transformer API
func (trans *handle) UserTransform(ctx context.Context, clientEvents []TransformerEvent, batchSize int) Response {
	return trans.transform(ctx, clientEvents, trans.userTransformURL(), batchSize, userTransformerStage)
}

// Validate function is used to invoke tracking plan validation API
func (trans *handle) Validate(ctx context.Context, clientEvents []TransformerEvent, batchSize int) Response {
	return trans.transform(ctx, clientEvents, trans.trackingPlanValidationURL(), batchSize, trackingPlanValidationStage)
}

func (trans *handle) transform(
	ctx context.Context,
	clientEvents []TransformerEvent,
	url string,
	batchSize int,
	stage string,
) Response {
	if len(clientEvents) == 0 {
		return Response{}
	}
	sTags := statsTags(&clientEvents[0])

	var trackWg sync.WaitGroup
	defer trackWg.Wait()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	trackWg.Add(1)
	go func() {
		var loggerCtx []interface{}
		for k, v := range sTags {
			loggerCtx = append(loggerCtx, k, v)
		}
		trackLongRunningTransformation(ctx, stage, trans.config.timeoutDuration, trans.logger.With(loggerCtx...))
		trackWg.Done()
	}()

	batches := lo.Chunk(clientEvents, batchSize)

	trans.stat.NewTaggedStat(
		"processor.transformer_request_batch_count",
		stats.HistogramType,
		sTags,
	).Observe(float64(len(batches)))
	trace.Logf(ctx, "request", "batch_count: %d", len(batches))

	transformResponse := make([][]TransformerResponse, len(batches))

	var wg sync.WaitGroup
	wg.Add(len(batches))

	lo.ForEach(
		batches,
		func(batch []TransformerEvent, i int) {
			trans.guardConcurrency <- struct{}{}
			go func() {
				trace.WithRegion(ctx, "request", func() {
					transformResponse[i] = trans.request(ctx, url, stage, batch)
				})
				<-trans.guardConcurrency
				wg.Done()
			}()
		},
	)
	wg.Wait()

	var outClientEvents []TransformerResponse
	var failedEvents []TransformerResponse

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

	return Response{
		Events:       outClientEvents,
		FailedEvents: failedEvents,
	}
}

func statsTags(event *TransformerEvent) stats.Tags {
	return stats.Tags{
		"dest_type": event.Destination.DestinationDefinition.Name,
		"dest_id":   event.Destination.ID,
		"src_id":    event.Metadata.SourceID,
	}
}

func (trans *handle) requestTime(s stats.Tags, d time.Duration) {
	trans.stat.NewTaggedStat("processor.transformer_request_time", stats.TimerType, s).SendTiming(d)
}

func (trans *handle) request(ctx context.Context, url, stage string, data []TransformerEvent) []TransformerResponse {
	// Call remote transformation
	var (
		rawJSON []byte
		err     error
	)

	trace.WithRegion(ctx, "marshal", func() {
		rawJSON, err = json.Marshal(data)
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

	// endless retry if transformer-control plane connection is down
	endlessBackoff := backoff.NewExponentialBackOff()
	endlessBackoff.MaxElapsedTime = 0 // no max time -> ends only when no error

	// endless backoff loop, only nil error or panics inside
	_ = backoff.RetryNotify(
		func() error {
			respData, statusCode = trans.doPost(ctx, rawJSON, url, stage, statsTags(&data[0]))
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
				transformationID, transformationVersionID,
			)
		},
	)
	// control plane back up

	switch statusCode {
	case http.StatusOK,
		http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusRequestEntityTooLarge:
	default:
		trans.logger.Errorf("Transformer returned status code: %v", statusCode)
	}

	var transformerResponses []TransformerResponse
	if statusCode == http.StatusOK {
		integrations.CollectIntgTransformErrorStats(respData)

		trace.Logf(ctx, "Unmarshal", "response raw size: %d", len(respData))
		trace.WithRegion(ctx, "Unmarshal", func() {
			err = json.Unmarshal(respData, &transformerResponses)
		})
		// This is returned by our JS engine so should  be parsable
		// but still handling it
		if err != nil {
			trans.logger.Errorf("Data sent to transformer : %v", string(rawJSON))
			trans.logger.Errorf("Transformer returned : %v", string(respData))

			respData = []byte(fmt.Sprintf("Failed to unmarshal transformer response: %s", string(respData)))

			transformerResponses = nil
			statusCode = http.StatusBadRequest
		}
	}

	if statusCode != http.StatusOK {
		for i := range data {
			transformEvent := &data[i]
			resp := TransformerResponse{StatusCode: statusCode, Error: string(respData), Metadata: transformEvent.Metadata}
			transformerResponses = append(transformerResponses, resp)
		}
	}
	return transformerResponses
}

func (trans *handle) doPost(ctx context.Context, rawJSON []byte, url, stage string, tags stats.Tags) ([]byte, int) {
	var (
		retryCount int
		resp       *http.Response
		respData   []byte
	)

	err := backoff.RetryNotify(
		func() error {
			var reqErr error
			requestStartTime := time.Now()

			trace.WithRegion(ctx, "request/post", func() {
				var req *http.Request
				req, reqErr = http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(rawJSON))
				if reqErr != nil {
					return
				}

				req.Header.Set("Content-Type", "application/json; charset=utf-8")
				req.Header.Set("X-Feature-Gzip-Support", "?1")
				// Header to let transformer know that the client understands event filter code
				req.Header.Set("X-Feature-Filter-Code", "?1")

				resp, reqErr = trans.client.Do(req)
			})
			trans.requestTime(tags, time.Since(requestStartTime))
			if reqErr != nil {
				return reqErr
			}

			defer func() { httputil.CloseResponse(resp) }()

			if !isJobTerminated(resp.StatusCode) && resp.StatusCode != StatusCPDown {
				return fmt.Errorf("transformer returned status code: %v", resp.StatusCode)
			}

			respData, reqErr = io.ReadAll(resp.Body)
			return reqErr
		},
		backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(trans.config.maxRetry.Load())),
		func(err error, t time.Duration) {
			retryCount++
			trans.logger.Warnn(
				"JS HTTP connection error",
				logger.NewStringField("URL", url),
				logger.NewErrorField(err),
				logger.NewIntField("attempts", int64(retryCount)),
			)
		},
	)
	if err != nil {
		if trans.config.failOnUserTransformTimeout.Load() && stage == userTransformerStage && os.IsTimeout(err) {
			return []byte(fmt.Sprintf("transformer request timed out: %s", err)), TransformerRequestTimeout
		} else if trans.config.failOnError.Load() {
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

func (trans *handle) destTransformURL(destType string) string {
	destinationEndPoint := fmt.Sprintf("%s/v0/destinations/%s", trans.config.destTransformationURL, strings.ToLower(destType))

	if _, ok := warehouseutils.WarehouseDestinationMap[destType]; ok {
		whSchemaVersionQueryParam := fmt.Sprintf("whSchemaVersion=%s&whIDResolve=%v", trans.conf.GetString("Warehouse.schemaVersion", "v1"), warehouseutils.IDResolutionEnabled())
		if destType == warehouseutils.RS {
			return destinationEndPoint + "?" + whSchemaVersionQueryParam
		}
		if destType == warehouseutils.CLICKHOUSE {
			enableArraySupport := fmt.Sprintf("chEnableArraySupport=%s", fmt.Sprintf("%v", trans.conf.GetBool("Warehouse.clickhouse.enableArraySupport", false)))
			return destinationEndPoint + "?" + whSchemaVersionQueryParam + "&" + enableArraySupport
		}
		return destinationEndPoint + "?" + whSchemaVersionQueryParam
	}
	return destinationEndPoint
}

func (trans *handle) userTransformURL() string {
	return trans.config.userTransformationURL + "/customTransform"
}

func (trans *handle) trackingPlanValidationURL() string {
	return trans.config.destTransformationURL + "/v0/validate"
}

func trackLongRunningTransformation(ctx context.Context, stage string, timeout time.Duration, log logger.Logger) {
	start := time.Now()
	t := time.NewTimer(timeout)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			log.Errorw("Long running transformation detected",
				"stage", stage,
				"duration", time.Since(start).String())
		}
	}
}
