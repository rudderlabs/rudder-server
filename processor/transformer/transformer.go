package transformer

//go:generate mockgen -destination=../../mocks/processor/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/processor/transformer Transformer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bufbuild/httplb"
	"github.com/bufbuild/httplb/resolver"
	"github.com/cenkalti/backoff"
	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/utils/types"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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
	SourceName          string                            `json:"sourceName"`
	OriginalSourceID    string                            `json:"originalSourceId"`
	WorkspaceID         string                            `json:"workspaceId"`
	Namespace           string                            `json:"namespace"`
	InstanceID          string                            `json:"instanceId"`
	SourceType          string                            `json:"sourceType"`
	SourceCategory      string                            `json:"sourceCategory"`
	TrackingPlanID      string                            `json:"trackingPlanId"`
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
	DestinationName     string                            `json:"destinationName"`
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
	Connection  backendconfig.Connection   `json:"connection"`
	Libraries   []backendconfig.LibraryT   `json:"libraries"`
	Credentials []Credential               `json:"credentials"`
}

// GetVersionsOnly removes the connection and credentials from the event
// along with pruning the destination to only include the transformation versionID
// before sending it to the transformer thereby reducing the payload size
func (e *TransformerEvent) GetVersionsOnly() *TransformerEvent {
	tmCopy := *e
	transformations := make([]backendconfig.TransformationT, 0, len(e.Destination.Transformations))
	for _, t := range e.Destination.Transformations {
		transformations = append(transformations, backendconfig.TransformationT{
			VersionID: t.VersionID,
		})
	}
	tmCopy.Destination = backendconfig.DestinationT{
		Transformations: transformations,
	}
	tmCopy.Connection = backendconfig.Connection{}
	return &tmCopy
}

type Credential struct {
	ID       string `json:"id"`
	Key      string `json:"key"`
	Value    string `json:"value"`
	IsSecret bool   `json:"isSecret"`
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
	StatTags         map[string]string      `json:"statTags"`
}

type ValidationError struct {
	Type     string            `json:"type"`
	Message  string            `json:"message"`
	Meta     map[string]string `json:"meta"`
	Property string            `json:"property"`
}

// Response represents a Transformer response
type Response struct {
	Events       []TransformerResponse
	FailedEvents []TransformerResponse
}

type Opt func(*handle)

func WithClient(client HTTPDoer) Opt {
	return func(s *handle) {
		s.httpClient = client
	}
}

// Transformer provides methods to transform events
type Transformer interface {
	Transform(ctx context.Context, clientEvents []TransformerEvent, batchSize int) Response
	UserTransform(ctx context.Context, clientEvents []TransformerEvent, batchSize int) Response
	Validate(ctx context.Context, clientEvents []TransformerEvent, batchSize int) Response
}

type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// handle is the handle for this class
type handle struct {
	sentStat     stats.Measurement
	receivedStat stats.Measurement
	cpDownGauge  stats.Measurement

	conf   *config.Config
	logger logger.Logger
	stat   stats.Stats

	httpClient HTTPDoer

	guardConcurrency chan struct{}

	config struct {
		maxConcurrency         int
		maxHTTPConnections     int
		maxHTTPIdleConnections int
		maxIdleConnDuration    time.Duration
		disableKeepAlives      bool

		timeoutDuration time.Duration

		maxRetry                   config.ValueLoader[int]
		failOnUserTransformTimeout config.ValueLoader[bool]
		failOnError                config.ValueLoader[bool]
		maxRetryBackoffInterval    config.ValueLoader[time.Duration]

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
	trans.config.maxIdleConnDuration = conf.GetDuration("Transformer.Client.maxIdleConnDuration", 30, time.Second)
	trans.config.disableKeepAlives = conf.GetBool("Transformer.Client.disableKeepAlives", true)
	trans.config.timeoutDuration = conf.GetDuration("HttpClient.procTransformer.timeout", 600, time.Second)
	trans.config.destTransformationURL = conf.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	trans.config.userTransformationURL = conf.GetString("USER_TRANSFORM_URL", trans.config.destTransformationURL)

	trans.config.maxRetry = conf.GetReloadableIntVar(30, 1, "Processor.maxRetry")
	trans.config.failOnUserTransformTimeout = conf.GetReloadableBoolVar(false, "Processor.Transformer.failOnUserTransformTimeout")
	trans.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.Transformer.failOnError")

	trans.config.maxRetryBackoffInterval = conf.GetReloadableDurationVar(30, time.Second, "Processor.Transformer.maxRetryBackoffInterval")

	trans.guardConcurrency = make(chan struct{}, trans.config.maxConcurrency)

	clientType := conf.GetString("Transformer.Client.type", "stdlib")

	transport := &http.Transport{
		DisableKeepAlives:   trans.config.disableKeepAlives,
		MaxConnsPerHost:     trans.config.maxHTTPConnections,
		MaxIdleConnsPerHost: trans.config.maxHTTPIdleConnections,
		IdleConnTimeout:     trans.config.maxIdleConnDuration,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   trans.config.timeoutDuration,
	}

	switch clientType {
	case "stdlib":
		trans.httpClient = client
	case "recycled":
		trans.httpClient = sysUtils.NewRecycledHTTPClient(func() *http.Client {
			return client
		}, config.GetDuration("Transformer.Client.ttl", 120, time.Second))
	case "httplb":
		trans.httpClient = httplb.NewClient(
			httplb.WithTransport("http", &HTTPLBTransport{
				Transport: transport,
			}),
			httplb.WithResolver(
				resolver.NewDNSResolver(
					net.DefaultResolver,
					resolver.PreferIPv6,
					config.GetDuration("Transformer.Client.ttl", 120, time.Second), // TTL value
				),
			),
		)
	default:
		panic(fmt.Sprintf("unknown transformer client type: %s", clientType))
	}

	for _, opt := range opts {
		opt(&trans)
	}

	return &trans
}

// Transform function is used to invoke destination transformer API
func (trans *handle) Transform(ctx context.Context, clientEvents []TransformerEvent, batchSize int) Response {
	return trans.transform(ctx, clientEvents, trans.destTransformURL(clientEvents[0].Destination.DestinationDefinition.Name), batchSize, destTransformerStage)
}

// UserTransform function is used to invoke user transformer API
func (trans *handle) UserTransform(ctx context.Context, clientEvents []TransformerEvent, batchSize int) Response {
	var dehydratedClientEvents []TransformerEvent
	for _, clientEvent := range clientEvents {
		dehydratedClientEvent := clientEvent.GetVersionsOnly()
		dehydratedClientEvents = append(dehydratedClientEvents, *dehydratedClientEvent)
	}

	return trans.transform(ctx, dehydratedClientEvents, trans.userTransformURL(), batchSize, userTransformerStage)
}

// Validate function is used to invoke tracking plan validation API
func (trans *handle) Validate(ctx context.Context, clientEvents []TransformerEvent, batchSize int) Response {
	return trans.transform(ctx, clientEvents, trans.trackingPlanValidationURL(), batchSize, trackingPlanValidationStage)
}

type HTTPLBTransport struct {
	*http.Transport
}

func (t *HTTPLBTransport) NewRoundTripper(scheme, target string, config httplb.TransportConfig) httplb.RoundTripperResult {
	return httplb.RoundTripperResult{RoundTripper: t.Transport, Close: t.CloseIdleConnections}
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
	// flip sourceID and originalSourceID if it's a replay source for the purpose of any user transformation
	// flip back afterwards
	for i := range clientEvents {
		if clientEvents[i].Metadata.OriginalSourceID != "" {
			clientEvents[i].Metadata.OriginalSourceID, clientEvents[i].Metadata.SourceID = clientEvents[i].Metadata.SourceID, clientEvents[i].Metadata.OriginalSourceID
		}
	}
	sTags := stats.Tags{
		"dest_type": clientEvents[0].Destination.DestinationDefinition.Name,
		"dest_id":   clientEvents[0].Destination.ID,
		"src_id":    clientEvents[0].Metadata.SourceID,
		"stage":     stage,
	}

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
		// Transform is one to many mapping so returned
		// response for each is an array. We flatten it out
		for _, transformerResponse := range batch {
			if transformerResponse.Metadata.OriginalSourceID != "" {
				transformerResponse.Metadata.SourceID, transformerResponse.Metadata.OriginalSourceID = transformerResponse.Metadata.OriginalSourceID, transformerResponse.Metadata.SourceID
			}
			switch transformerResponse.StatusCode {
			case http.StatusOK:
				outClientEvents = append(outClientEvents, transformerResponse)
			default:
				failedEvents = append(failedEvents, transformerResponse)
			}
		}
	}

	trans.sentStat.Count(len(clientEvents))
	trans.receivedStat.Count(len(outClientEvents))

	return Response{
		Events:       outClientEvents,
		FailedEvents: failedEvents,
	}
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
	endlessBackoff.MaxInterval = trans.config.maxRetryBackoffInterval.Load()

	// endless backoff loop, only nil error or panics inside
	_ = backoff.RetryNotify(
		func() error {
			transformationID := ""
			if len(data[0].Destination.Transformations) > 0 {
				transformationID = data[0].Destination.Transformations[0].ID
			}

			respData, statusCode = trans.doPost(ctx, rawJSON, url, stage, stats.Tags{
				"destinationType":  data[0].Destination.DestinationDefinition.Name,
				"destinationId":    data[0].Destination.ID,
				"sourceId":         data[0].Metadata.SourceID,
				"transformationId": transformationID,
				"stage":            stage,

				// Legacy tags: to be removed
				"dest_type": data[0].Destination.DestinationDefinition.Name,
				"dest_id":   data[0].Destination.ID,
				"src_id":    data[0].Metadata.SourceID,
			})
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
		http.StatusRequestEntityTooLarge:
	default:
		trans.logger.Errorf("Transformer returned status code: %v", statusCode)
	}

	var transformerResponses []TransformerResponse
	switch statusCode {
	case http.StatusOK:
		integrations.CollectIntgTransformErrorStats(respData)

		trace.Logf(ctx, "Unmarshal", "response raw size: %d", len(respData))
		trace.WithRegion(ctx, "Unmarshal", func() {
			err = json.Unmarshal(respData, &transformerResponses)
		})
		// This is returned by our JS engine so should  be parsable
		// Panic the processor to avoid replays
		if err != nil {
			trans.logger.Errorf("Data sent to transformer : %v", string(rawJSON))
			trans.logger.Errorf("Transformer returned : %v", string(respData))
			panic(err)
		}
	default:
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
	retryStrategy := backoff.NewExponentialBackOff()
	// MaxInterval caps the RetryInterval
	retryStrategy.MaxInterval = trans.config.maxRetryBackoffInterval.Load()

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

				resp, reqErr = trans.httpClient.Do(req)
			})
			trans.stat.NewTaggedStat("processor.transformer_request_time", stats.TimerType, tags).SendTiming(time.Since(requestStartTime))
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
		backoff.WithMaxRetries(retryStrategy, uint64(trans.config.maxRetry.Load())),
		func(err error, t time.Duration) {
			retryCount++
			trans.logger.Warnn(
				"JS HTTP connection error",
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
		transformerAPIVersion, _ := strconv.Atoi(resp.Header.Get("apiVersion"))
		if types.SupportedTransformerApiVersion != transformerAPIVersion {
			unexpectedVersionError := fmt.Errorf("incompatible transformer version: Expected: %d Received: %s, URL: %v", types.SupportedTransformerApiVersion, resp.Header.Get("apiVersion"), url)
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
		switch destType {
		case warehouseutils.RS:
			return destinationEndPoint + "?" + whSchemaVersionQueryParam
		case warehouseutils.CLICKHOUSE:
			enableArraySupport := fmt.Sprintf("chEnableArraySupport=%s", fmt.Sprintf("%v", trans.conf.GetBool("Warehouse.clickhouse.enableArraySupport", false)))
			return destinationEndPoint + "?" + whSchemaVersionQueryParam + "&" + enableArraySupport
		default:
			return destinationEndPoint + "?" + whSchemaVersionQueryParam
		}
	}
	if destType == warehouseutils.SnowpipeStreaming {
		return fmt.Sprintf("%s?whSchemaVersion=%s&whIDResolve=%t", destinationEndPoint, trans.conf.GetString("Warehouse.schemaVersion", "v1"), warehouseutils.IDResolutionEnabled())
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
