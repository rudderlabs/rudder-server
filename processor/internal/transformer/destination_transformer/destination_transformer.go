package destination_transformer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/jsonrs"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/cenkalti/backoff"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	transformerclient "github.com/rudderlabs/rudder-server/internal/transformer-client"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	transformerutils "github.com/rudderlabs/rudder-server/processor/internal/transformer"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/pubsub"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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
	handle.client = transformerclient.NewClient(transformerutils.TransformerClientConfig(conf, "DestinationTransformer"))
	handle.config.maxConcurrency = conf.GetInt("Processor.maxConcurrency", 200)
	handle.guardConcurrency = make(chan struct{}, handle.config.maxConcurrency)
	handle.config.destTransformationURL = handle.conf.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	handle.config.timeoutDuration = conf.GetDuration("HttpClient.procTransformer.timeout", 600, time.Second)
	handle.config.maxRetry = conf.GetReloadableIntVar(30, 1, "Processor.DestinationTransformer.maxRetry", "Processor.maxRetry")
	handle.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.DestinationTransformer.failOnError", "Processor.Transformer.failOnError")
	handle.config.maxRetryBackoffInterval = conf.GetReloadableDurationVar(30, time.Second, "Processor.DestinationTransformer.maxRetryBackoffInterval", "Processor.maxRetryBackoffInterval")
	handle.config.batchSize = conf.GetReloadableIntVar(100, 1, "Processor.DestinationTransformer.batchSize", "Processor.transformBatchSize")

	handle.config.maxLoggedEvents = conf.GetReloadableIntVar(10000, 1, "Processor.DestinationTransformer.maxLoggedEvents")

	handle.stats.comparisonTime = handle.stat.NewStat("embedded_destination_transform_comparison_time", stats.TimerType)
	handle.stats.matchedEvents = handle.stat.NewStat("embedded_destination_transform_matched_events", stats.CountType)
	handle.stats.mismatchedEvents = handle.stat.NewStat("embedded_destination_transform_mismatched_events", stats.CountType)

	handle.loggedEvents = 0
	handle.loggedEventsMu = sync.Mutex{}
	handle.loggedFileName = generateLogFileName()

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
		maxConcurrency          int
		maxRetryBackoffInterval config.ValueLoader[time.Duration]
		timeoutDuration         time.Duration
		batchSize               config.ValueLoader[int]

		maxLoggedEvents config.ValueLoader[int]
	}
	guardConcurrency chan struct{}
	conf             *config.Config
	log              logger.Logger
	stat             stats.Stats
	client           transformerclient.Client

	stats struct {
		comparisonTime   stats.Timer
		matchedEvents    stats.Counter
		mismatchedEvents stats.Counter
	}

	loggedEventsMu sync.Mutex
	loggedEvents   int64
	loggedFileName string
}

func (d *Client) transform(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
	batchSize := d.config.batchSize.Load()
	if len(clientEvents) == 0 {
		return types.Response{}
	}

	destinationType := clientEvents[0].Destination.DestinationDefinition.Name
	destURL := d.destTransformURL(destinationType)

	labels := types.TransformerMetricLabels{
		Endpoint:        transformerutils.GetEndpointFromURL(destURL),
		Stage:           "dest_transformer",
		DestinationType: destinationType,
		DestinationID:   clientEvents[0].Destination.ID,
		SourceID:        clientEvents[0].Metadata.SourceID,
		WorkspaceID:     clientEvents[0].Metadata.WorkspaceID,
		SourceType:      clientEvents[0].Metadata.SourceType,
	}

	var trackWg sync.WaitGroup
	defer trackWg.Wait()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	trackWg.Add(1)
	go func() {
		l := d.log.Withn(labels.ToLoggerFields()...)
		transformerutils.TrackLongRunningTransformation(ctx, labels.Stage, d.config.timeoutDuration, l)
		trackWg.Done()
	}()

	batches := lo.Chunk(clientEvents, batchSize)

	d.stat.NewTaggedStat(
		"processor.transformer_request_batch_count",
		stats.HistogramType,
		labels.ToStatsTag(),
	).Observe(float64(len(batches)))

	transformResponse := make([][]types.TransformerResponse, len(batches))

	var wg sync.WaitGroup
	wg.Add(len(batches))
	var err error
	var foundError bool
	lo.ForEach(
		batches,
		func(batch []types.TransformerEvent, i int) {
			d.guardConcurrency <- struct{}{}
			go func() {
				transformResponse[i], err = d.sendBatch(ctx, destURL, labels, batch)
				if err != nil {
					foundError = true
				}
				<-d.guardConcurrency
				wg.Done()
			}()
		},
	)
	wg.Wait()

	if foundError {
		panic(err)
	}
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

	d.stat.NewStat("processor.transformer_sent", stats.CountType).Count(len(clientEvents))
	d.stat.NewStat("processor.transformer_received", stats.CountType).Count(len(outClientEvents))

	return types.Response{
		Events:       outClientEvents,
		FailedEvents: failedEvents,
	}
}

func (d *Client) sendBatch(ctx context.Context, url string, labels types.TransformerMetricLabels, data []types.TransformerEvent) ([]types.TransformerResponse, error) {
	d.stat.NewTaggedStat("transformer_client_request_total_events", stats.CountType, labels.ToStatsTag()).Count(len(data))
	// Call remote transformation
	var (
		rawJSON []byte
		err     error
	)

	rawJSON, err = jsonrs.Marshal(data)
	if err != nil {
		panic(err)
	}

	if len(data) == 0 {
		return nil, nil
	}

	var (
		respData   []byte
		statusCode int
	)

	respData, statusCode, err = d.doPost(ctx, rawJSON, url, labels)
	if err != nil {
		return nil, err
	}

	switch statusCode {
	case http.StatusOK,
		http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusRequestEntityTooLarge:
	default:
		d.log.Errorn("Transformer returned status code", logger.NewStringField("statusCode", strconv.Itoa(statusCode)))
	}

	var transformerResponses []types.TransformerResponse
	switch statusCode {
	case http.StatusOK:
		integrations.CollectIntgTransformErrorStats(respData)

		err = jsonrs.Unmarshal(respData, &transformerResponses)
		// This is returned by our JS engine so should  be parsable
		// Panic the processor to avoid replays
		if err != nil {
			return nil, err
		}
		d.stat.NewTaggedStat("transformer_client_response_total_events", stats.CountType, labels.ToStatsTag()).Count(len(transformerResponses))
	default:
		for i := range data {
			transformEvent := &data[i]
			resp := types.TransformerResponse{StatusCode: statusCode, Error: string(respData), Metadata: transformEvent.Metadata}
			transformerResponses = append(transformerResponses, resp)
		}
		d.stat.NewTaggedStat("transformer_client_response_total_events", stats.CountType, labels.ToStatsTag()).Count(len(data))
	}
	return transformerResponses, nil
}

func (d *Client) doPost(ctx context.Context, rawJSON []byte, url string, labels types.TransformerMetricLabels) ([]byte, int, error) {
	var (
		retryCount int
		resp       *http.Response
		respData   []byte
	)
	retryStrategy := backoff.NewExponentialBackOff()
	// MaxInterval caps the RetryInterval
	retryStrategy.MaxInterval = d.config.maxRetryBackoffInterval.Load()

	err := backoff.RetryNotify(
		func() error {
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

			resp, reqErr = d.client.Do(req)
			defer func() { httputil.CloseResponse(resp) }()
			// Record metrics with labels
			tags := labels.ToStatsTag()
			duration := time.Since(requestStartTime)
			d.stat.NewTaggedStat("transformer_client_request_total_bytes", stats.CountType, tags).Count(len(rawJSON))

			d.stat.NewTaggedStat("transformer_client_total_durations_seconds", stats.CountType, tags).Count(int(duration.Seconds()))
			d.stat.NewTaggedStat("processor.transformer_request_time", stats.TimerType, labels.ToStatsTag()).SendTiming(duration)

			if reqErr != nil {
				return reqErr
			}

			if !transformerutils.IsJobTerminated(resp.StatusCode) {
				return fmt.Errorf("transformer returned status code: %v", resp.StatusCode)
			}

			respData, reqErr = io.ReadAll(resp.Body)
			if reqErr == nil {
				d.stat.NewTaggedStat("transformer_client_response_total_bytes", stats.CountType, tags).Count(len(respData))
				// We'll count response events after unmarshaling in the request method
			}
			return reqErr
		},
		backoff.WithMaxRetries(retryStrategy, uint64(d.config.maxRetry.Load())),
		func(err error, t time.Duration) {
			retryCount++
			d.log.Warnn(
				"JS HTTP connection error",
				logger.NewErrorField(err),
				logger.NewIntField("attempts", int64(retryCount)),
			)
		},
	)
	if err != nil {
		if d.config.failOnError.Load() {
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
			d.log.Errorn("Unexpected version", obskit.Error(unexpectedVersionError))
			return nil, 0, unexpectedVersionError
		}
	}

	return respData, resp.StatusCode, nil
}

func (d *Client) destTransformURL(destType string) string {
	destinationEndPoint := fmt.Sprintf("%s/v0/destinations/%s", d.config.destTransformationURL, strings.ToLower(destType))

	if _, ok := warehouseutils.WarehouseDestinationMap[destType]; ok {
		whSchemaVersionQueryParam := fmt.Sprintf("whIDResolve=%t", d.conf.GetBool("Warehouse.enableIDResolution", false))
		switch destType {
		case warehouseutils.RS:
			return destinationEndPoint + "?" + whSchemaVersionQueryParam
		case warehouseutils.CLICKHOUSE:
			enableArraySupport := fmt.Sprintf("chEnableArraySupport=%s", fmt.Sprintf("%v", d.conf.GetBool("Warehouse.clickhouse.enableArraySupport", false)))
			return destinationEndPoint + "?" + whSchemaVersionQueryParam + "&" + enableArraySupport
		default:
			return destinationEndPoint + "?" + whSchemaVersionQueryParam
		}
	}
	if destType == warehouseutils.SnowpipeStreaming {
		return fmt.Sprintf("%s?whIDResolve=%t", destinationEndPoint, d.conf.GetBool("Warehouse.enableIDResolution", false))
	}
	return destinationEndPoint
}

type transformer func(ctx context.Context, clientEvents []types.TransformerEvent) types.Response

var embeddedTransformerImpls = map[string]transformer{
	"GOOGLEPUBSUB": pubsub.Transform,
}

func (c *Client) Transform(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
	if len(clientEvents) == 0 {
		return types.Response{}
	}

	destType := clientEvents[0].Destination.DestinationDefinition.Name
	impl, ok := embeddedTransformerImpls[destType]
	if !ok {
		return c.transform(ctx, clientEvents)
	}
	if !c.conf.GetBoolVar(false, "Processor.Transformer.Embedded."+destType+".Enabled") {
		return c.transform(ctx, clientEvents)
	}
	if c.conf.GetBoolVar(true, "Processor.Transformer.Embedded."+destType+".Verify") {
		legacyTransformerResponse := c.transform(ctx, clientEvents)
		embeddedTransformerResponse := impl(ctx, clientEvents)

		c.CompareAndLog(embeddedTransformerResponse, legacyTransformerResponse)

		return legacyTransformerResponse
	}
	return impl(ctx, clientEvents)
}
