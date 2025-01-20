package trackingplan_validation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime/trace"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer_utils"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	reportingTypes "github.com/rudderlabs/rudder-server/utils/types"

	"github.com/rudderlabs/rudder-server/processor/types"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/processor/internal/http_client"
)

type TPValidator struct {
	config struct {
		destTransformationURL   string
		maxRetry                config.ValueLoader[int]
		failOnError             config.ValueLoader[bool]
		maxConcurrency          int
		maxRetryBackoffInterval config.ValueLoader[time.Duration]
		timeoutDuration         time.Duration
	}
	conf             *config.Config
	log              logger.Logger
	stat             stats.Stats
	guardConcurrency chan struct{}
	client           http_client.HTTPDoer
}

func (t *TPValidator) SendRequest(ctx context.Context, clientEvents []types.TransformerEvent, batchSize int) types.Response {
	return t.transform(ctx, clientEvents, t.trackingPlanValidationURL(), batchSize)
}

func NewTPValidator(conf *config.Config, log logger.Logger, stat stats.Stats) *TPValidator {
	handle := &TPValidator{}
	handle.conf = conf
	handle.log = log
	handle.stat = stat
	handle.client = http_client.NewHTTPClient(conf)
	handle.config.destTransformationURL = handle.conf.GetString("Warehouse.destTransformationURL", "http://localhost:9090")
	handle.config.maxConcurrency = conf.GetInt("Processor.maxConcurrency", 200)
	handle.guardConcurrency = make(chan struct{}, handle.config.maxConcurrency)
	handle.config.maxRetry = conf.GetReloadableIntVar(30, 1, "Processor.maxRetry")
	handle.config.timeoutDuration = conf.GetDuration("HttpClient.procTransformer.timeout", 600, time.Second)
	handle.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.Transformer.failOnError")
	handle.config.maxRetryBackoffInterval = conf.GetReloadableDurationVar(30, time.Second, "Processor.maxRetryBackoffInterval")
	return handle
}

func (t *TPValidator) transform(
	ctx context.Context,
	clientEvents []types.TransformerEvent,
	url string,
	batchSize int,
) types.Response {
	if len(clientEvents) == 0 {
		return types.Response{}
	}
	// flip sourceID and originalSourceID if it's a replay source for the purpose of any user transformation
	// flip back afterward
	for i := range clientEvents {
		if clientEvents[i].Metadata.OriginalSourceID != "" {
			clientEvents[i].Metadata.OriginalSourceID, clientEvents[i].Metadata.SourceID = clientEvents[i].Metadata.SourceID, clientEvents[i].Metadata.OriginalSourceID
		}
	}
	sTags := stats.Tags{
		"dest_type": clientEvents[0].Destination.DestinationDefinition.Name,
		"dest_id":   clientEvents[0].Destination.ID,
		"src_id":    clientEvents[0].Metadata.SourceID,
		"stage":     "dest_transformer",
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
		transformer_utils.TrackLongRunningTransformation(ctx, "dest_transformer", t.config.timeoutDuration, t.log.With(loggerCtx...))
		trackWg.Done()
	}()

	batches := lo.Chunk(clientEvents, batchSize)

	t.stat.NewTaggedStat(
		"processor.transformer_request_batch_count",
		stats.HistogramType,
		sTags,
	).Observe(float64(len(batches)))
	trace.Logf(ctx, "request", "batch_count: %d", len(batches))

	transformResponse := make([][]types.TransformerResponse, len(batches))

	var wg sync.WaitGroup
	wg.Add(len(batches))

	lo.ForEach(
		batches,
		func(batch []types.TransformerEvent, i int) {
			t.guardConcurrency <- struct{}{}
			go func() {
				trace.WithRegion(ctx, "request", func() {
					transformResponse[i] = t.request(ctx, url, "dest_transformer", batch)
				})
				<-t.guardConcurrency
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

	t.stat.NewStat("processor.transformer_sent", stats.CountType).Count(len(clientEvents))
	t.stat.NewStat("processor.transformer_received", stats.CountType).Count(len(outClientEvents))

	return types.Response{
		Events:       outClientEvents,
		FailedEvents: failedEvents,
	}
}

func (t *TPValidator) request(ctx context.Context, url, stage string, data []types.TransformerEvent) []types.TransformerResponse {
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
	endlessBackoff.MaxInterval = t.config.maxRetryBackoffInterval.Load()

	// endless backoff loop, only nil error or panics inside
	_ = backoff.RetryNotify(
		func() error {
			transformationID := ""
			if len(data[0].Destination.Transformations) > 0 {
				transformationID = data[0].Destination.Transformations[0].ID
			}

			respData, statusCode = t.doPost(ctx, rawJSON, url, stats.Tags{
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
			return nil
		},
		endlessBackoff,
		func(err error, time time.Duration) {
			var transformationID, transformationVersionID string
			if len(data[0].Destination.Transformations) > 0 {
				transformationID = data[0].Destination.Transformations[0].ID
				transformationVersionID = data[0].Destination.Transformations[0].VersionID
			}
			t.log.Errorf("JS HTTP connection error: URL: %v Error: %+v. WorkspaceID: %s, sourceID: %s, destinationID: %s, transformationID: %s, transformationVersionID: %s",
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
		t.log.Errorf("Transformer returned status code: %v", statusCode)
	}

	var transformerResponses []types.TransformerResponse
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
			t.log.Errorf("Data sent to transformer : %v", string(rawJSON))
			t.log.Errorf("Transformer returned : %v", string(respData))
			panic(err)
		}
	default:
		for i := range data {
			transformEvent := &data[i]
			resp := types.TransformerResponse{StatusCode: statusCode, Error: string(respData), Metadata: transformEvent.Metadata}
			transformerResponses = append(transformerResponses, resp)
		}
	}
	return transformerResponses
}

func (t *TPValidator) doPost(ctx context.Context, rawJSON []byte, url string, tags stats.Tags) ([]byte, int) {
	var (
		retryCount int
		resp       *http.Response
		respData   []byte
	)
	retryStrategy := backoff.NewExponentialBackOff()
	// MaxInterval caps the RetryInterval
	retryStrategy.MaxInterval = t.config.maxRetryBackoffInterval.Load()

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

				resp, reqErr = t.client.Do(req)
				defer func() { httputil.CloseResponse(resp) }()
			})
			t.stat.NewTaggedStat("processor.transformer_request_time", stats.TimerType, tags).SendTiming(time.Since(requestStartTime))
			if reqErr != nil {
				return reqErr
			}

			if !transformer_utils.IsJobTerminated(resp.StatusCode) && resp.StatusCode != transformer_utils.StatusCPDown {
				return fmt.Errorf("transformer returned status code: %v", resp.StatusCode)
			}

			respData, reqErr = io.ReadAll(resp.Body)
			return reqErr
		},
		backoff.WithMaxRetries(retryStrategy, uint64(t.config.maxRetry.Load())),
		func(err error, time time.Duration) {
			retryCount++
			t.log.Warnn(
				"JS HTTP connection error",
				logger.NewErrorField(err),
				logger.NewIntField("attempts", int64(retryCount)),
			)
		},
	)
	if err != nil {
		if t.config.failOnError.Load() {
			return []byte(fmt.Sprintf("transformer request failed: %s", err)), transformer_utils.TransformerRequestFailure
		} else {
			panic(err)
		}
	}

	// perform version compatibility check only on success
	if resp.StatusCode == http.StatusOK {
		transformerAPIVersion, _ := strconv.Atoi(resp.Header.Get("apiVersion"))
		if reportingTypes.SupportedTransformerApiVersion != transformerAPIVersion {
			unexpectedVersionError := fmt.Errorf("incompatible transformer version: Expected: %d Received: %s, URL: %v", reportingTypes.SupportedTransformerApiVersion, resp.Header.Get("apiVersion"), url)
			t.log.Error(unexpectedVersionError)
			panic(unexpectedVersionError)
		}
	}

	return respData, resp.StatusCode
}

func (t *TPValidator) trackingPlanValidationURL() string {
	return t.config.destTransformationURL + "/v0/validate"
}
