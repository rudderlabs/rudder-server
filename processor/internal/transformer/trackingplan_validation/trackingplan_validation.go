package trackingplan_validation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/cenkalti/backoff/v5"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	transformerclient "github.com/rudderlabs/rudder-server/internal/transformer-client"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	transformerutils "github.com/rudderlabs/rudder-server/processor/internal/transformer"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/backoffvoid"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

type Opt func(*Client)

func WithClient(client transformerclient.Client) Opt {
	return func(s *Client) {
		s.client = client
	}
}

func New(conf *config.Config, log logger.Logger, stat stats.Stats, opts ...Opt) *Client {
	handle := &Client{}
	handle.conf = conf
	handle.log = log
	handle.stat = stat
	handle.client = transformerclient.NewClient(transformerutils.TransformerClientConfig(conf, "TrackingPlanValidation"))
	handle.config.destTransformationURL = handle.conf.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	handle.config.maxRetry = conf.GetReloadableIntVar(30, 1, "Processor.TrackingPlanValidation.maxRetry", "Processor.maxRetry")
	handle.config.timeoutDuration = conf.GetDuration("HttpClient.procTransformer.timeout", 600, time.Second)
	handle.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.TrackingPlanValidation.failOnError", "Processor.Transformer.failOnError")
	handle.config.maxRetryBackoffInterval = conf.GetReloadableDurationVar(30, time.Second, "Processor.TrackingPlanValidation.maxRetryBackoffInterval", "Processor.maxRetryBackoffInterval")
	handle.config.batchSize = conf.GetReloadableIntVar(200, 1, "Processor.TrackingPlanValidation.batchSize", "Processor.userTransformBatchSize")

	for _, opt := range opts {
		opt(handle)
	}

	return handle
}

type Client struct {
	config struct {
		destTransformationURL   string
		maxRetry                config.ValueLoader[int]
		failOnError             config.ValueLoader[bool]
		maxRetryBackoffInterval config.ValueLoader[time.Duration]
		timeoutDuration         time.Duration
		batchSize               config.ValueLoader[int]
	}
	conf   *config.Config
	log    logger.Logger
	stat   stats.Stats
	client transformerclient.Client
}

func (t *Client) Validate(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
	batchSize := t.config.batchSize.Load()
	if len(clientEvents) == 0 {
		return types.Response{}
	}

	validationURL := t.trackingPlanValidationURL()
	labels := types.TransformerMetricLabels{
		Endpoint:    transformerutils.GetEndpointFromURL(validationURL),
		Stage:       "trackingPlan_validation",
		SourceID:    clientEvents[0].Metadata.SourceID,
		WorkspaceID: clientEvents[0].Metadata.WorkspaceID,
		SourceType:  clientEvents[0].Metadata.SourceType,
	}

	var trackWg sync.WaitGroup
	defer trackWg.Wait()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	trackWg.Go(func() {
		l := t.log.Withn(labels.ToLoggerFields()...)
		transformerutils.TrackLongRunningTransformation(ctx, "trackingPlan_validation", t.config.timeoutDuration, l)
	})

	batches := lo.Chunk(clientEvents, batchSize)

	t.stat.NewTaggedStat("processor.transformer_request_batch_count", stats.HistogramType, labels.ToStatsTag()).Observe(float64(len(batches)))

	transformResponse := make([][]types.TransformerResponse, len(batches))

	var wg sync.WaitGroup
	wg.Add(len(batches))

	lo.ForEach(
		batches,
		func(batch []types.TransformerEvent, i int) {
			go func() {
				transformResponse[i] = t.sendBatch(ctx, t.trackingPlanValidationURL(), labels, batch)
				wg.Done()
			}()
		},
	)
	wg.Wait()

	var outClientEvents []types.TransformerResponse
	var failedEvents []types.TransformerResponse

	for _, batch := range transformResponse {
		// Transform is one to many mapping so returned
		// response for each is an array. We flatten it out
		for _, transformerResponse := range batch {
			switch transformerResponse.StatusCode {
			case http.StatusOK:
				outClientEvents = append(outClientEvents, transformerResponse)
			default:
				failedEvents = append(failedEvents, transformerResponse)
			}
		}
	}

	t.stat.NewStat("processor_transformer_sent", stats.CountType).Count(len(clientEvents))
	t.stat.NewStat("processor_transformer_received", stats.CountType).Count(len(outClientEvents))

	return types.Response{
		Events:       outClientEvents,
		FailedEvents: failedEvents,
	}
}

func (t *Client) sendBatch(ctx context.Context, url string, labels types.TransformerMetricLabels, clientEvents []types.TransformerEvent) []types.TransformerResponse {
	data := lo.Map(clientEvents, func(clientEvent types.TransformerEvent, _ int) types.TrackingPlanValidationEvent {
		return *clientEvent.ToTrackingPlanValidationEvent()
	})
	var (
		rawJSON []byte
		err     error
	)
	start := time.Now()

	rawJSON, err = jsonrs.Marshal(data)
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
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = t.config.maxRetryBackoffInterval.Load()

	// endless backoff loop, only nil error or panics inside
	_ = backoffvoid.Retry(context.Background(),
		func() error {
			respData, statusCode, err = t.doPost(ctx, rawJSON, url, labels)
			if err != nil {
				panic(err)
			}
			if statusCode == transformerutils.StatusCPDown {
				t.stat.NewStat("processor_control_plane_down", stats.GaugeType).Gauge(1)
				return fmt.Errorf("control plane not reachable")
			}
			t.stat.NewStat("processor_control_plane_down", stats.GaugeType).Gauge(0)
			return nil
		},
		backoff.WithBackOff(bo),
		backoff.WithMaxElapsedTime(0), // disable max elapsed time --> endless
		backoff.WithNotify(func(err error, time time.Duration) {
			var transformationID, transformationVersionID string
			if len(clientEvents[0].Destination.Transformations) > 0 {
				transformationID = clientEvents[0].Destination.Transformations[0].ID
				transformationVersionID = clientEvents[0].Destination.Transformations[0].VersionID
			}
			t.log.Errorn("JS HTTP connection error",
				obskit.Error(err),
				obskit.SourceID(data[0].Metadata.SourceID),
				obskit.WorkspaceID(data[0].Metadata.WorkspaceID),
				obskit.DestinationID(data[0].Metadata.DestinationID),
				logger.NewStringField("url", url),
				logger.NewStringField("transformationID", transformationID),
				logger.NewStringField("transformationVersionID", transformationVersionID),
			)
		}),
	)
	// control plane back up

	switch statusCode {
	case http.StatusOK,
		http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusRequestEntityTooLarge:
	default:
		t.log.Errorn("Transformer returned status code", logger.NewStringField("statusCode", strconv.Itoa(statusCode)))
	}

	var transformerResponses []types.TransformerResponse
	switch statusCode {
	case http.StatusOK:
		integrations.CollectIntgTransformErrorStats(respData)
		err = jsonrs.Unmarshal(respData, &transformerResponses)
		// This is returned by our JS engine so should  be parsable
		// Panic the processor to avoid replays
		if err != nil {
			t.log.Errorn("Data sent to transformer", logger.NewStringField("payload", string(rawJSON)))
			t.log.Errorn("Transformer returned", logger.NewStringField("payload", string(respData)))
			panic(err)
		}
	default:
		for i := range data {
			transformEvent := &data[i]
			resp := types.TransformerResponse{StatusCode: statusCode, Error: string(respData), Metadata: transformEvent.Metadata}
			transformerResponses = append(transformerResponses, resp)
		}
	}
	t.stat.NewTaggedStat("transformer_client_request_total_events", stats.CountType, labels.ToStatsTag()).Count(len(clientEvents))
	t.stat.NewTaggedStat("transformer_client_response_total_events", stats.CountType, labels.ToStatsTag()).Count(len(transformerResponses))
	t.stat.NewTaggedStat("transformer_client_total_time", stats.TimerType, labels.ToStatsTag()).SendTiming(time.Since(start))
	return transformerResponses
}

func (t *Client) doPost(ctx context.Context, rawJSON []byte, url string, labels types.TransformerMetricLabels) ([]byte, int, error) {
	var (
		retryCount int
		resp       *http.Response
		respData   []byte
	)
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = t.config.maxRetryBackoffInterval.Load()

	err := backoffvoid.Retry(
		ctx,
		transformerutils.WithProcTransformReqTimeStat(func() error {
			var reqErr error
			requestStartTime := time.Now()

			var req *http.Request
			req, reqErr = http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(rawJSON))
			if reqErr != nil {
				return reqErr
			}

			req.Header.Set("Content-Type", "application/json; charset=utf-8")
			req.Header.Set("X-Feature-Gzip-Support", "?1")
			// Header to let transformer know that the client understands event filter code
			req.Header.Set("X-Feature-Filter-Code", "?1")

			resp, reqErr = t.client.Do(req)
			defer func() { httputil.CloseResponse(resp) }()
			// Record metrics with labels
			tags := labels.ToStatsTag()
			duration := time.Since(requestStartTime)
			t.stat.NewTaggedStat("transformer_client_request_total_bytes", stats.CountType, tags).Count(len(rawJSON))
			t.stat.NewTaggedStat("transformer_client_total_durations_seconds", stats.CountType, tags).Count(int(duration.Seconds()))
			if reqErr != nil {
				return reqErr
			}

			if !transformerutils.IsJobTerminated(resp.StatusCode) && resp.StatusCode != transformerutils.StatusCPDown {
				return fmt.Errorf("transformer returned status code: %v", resp.StatusCode)
			}

			respData, reqErr = io.ReadAll(resp.Body)
			if reqErr == nil {
				t.stat.NewTaggedStat("transformer_client_response_total_bytes", stats.CountType, tags).Count(len(respData))
				// We'll count response events after unmarshaling in the request method
			}
			return reqErr
		}, t.stat, labels),
		backoff.WithBackOff(bo),
		backoff.WithMaxTries(uint(t.config.maxRetry.Load()+1)),
		backoff.WithNotify(func(err error, time time.Duration) {
			retryCount++
			t.log.Warnn(
				"JS HTTP connection error",
				obskit.Error(err),
				logger.NewIntField("attempts", int64(retryCount)),
			)
		}),
	)
	if err != nil {
		if t.config.failOnError.Load() {
			return []byte(fmt.Sprintf("transformer request failed: %s", err)), transformerutils.TransformerRequestFailure, nil
		} else {
			return nil, 0, err
		}
	}

	// perform version compatibility check only on success
	if resp.StatusCode == http.StatusOK {
		transformerAPIVersion, _ := strconv.Atoi(resp.Header.Get("apiVersion"))
		if reportingtypes.SupportedTransformerApiVersion != transformerAPIVersion {
			unexpectedVersionError := fmt.Errorf("incompatible transformer version: Expected: %d Received: %s, URL: %v", reportingtypes.SupportedTransformerApiVersion, resp.Header.Get("apiVersion"), url)
			t.log.Errorn("Unexpected version", obskit.Error(unexpectedVersionError))
			return nil, 0, unexpectedVersionError
		}
	}

	return respData, resp.StatusCode, nil
}

func (t *Client) trackingPlanValidationURL() string {
	return t.config.destTransformationURL + "/v0/validate"
}
