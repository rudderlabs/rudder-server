package user_transformer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime/trace"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/internal/http_client"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer_utils"
	"github.com/rudderlabs/rudder-server/processor/types"
	reportingTypes "github.com/rudderlabs/rudder-server/utils/types"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type UserTransformer struct {
	config struct {
		userTransformationURL      string
		maxRetry                   config.ValueLoader[int]
		failOnUserTransformTimeout config.ValueLoader[bool]
		failOnError                config.ValueLoader[bool]
		maxRetryBackoffInterval    config.ValueLoader[time.Duration]
		timeoutDuration            time.Duration
		maxConcurrency             int
	}
	conf             *config.Config
	log              logger.Logger
	stat             stats.Stats
	client           http_client.HTTPDoer
	guardConcurrency chan struct{}
}

func (u *UserTransformer) SendRequest(ctx context.Context, clientEvents []types.TransformerEvent, batchSize int) types.Response {
	var dehydratedClientEvents []types.TransformerEvent
	for _, clientEvent := range clientEvents {
		dehydratedClientEvent := clientEvent.GetVersionsOnly()
		dehydratedClientEvents = append(dehydratedClientEvents, *dehydratedClientEvent)
	}
	return u.transform(ctx, dehydratedClientEvents, u.userTransformURL(), batchSize)
}

func NewUserTransformer(conf *config.Config, log logger.Logger, stat stats.Stats) *UserTransformer {
	handle := &UserTransformer{}
	handle.conf = conf
	handle.log = log.Child("user_transformer")
	handle.stat = stat
	handle.client = http_client.NewHTTPClient(conf)
	handle.config.maxConcurrency = conf.GetInt("Processor.maxConcurrency", 200)
	handle.guardConcurrency = make(chan struct{}, handle.config.maxConcurrency)
	handle.config.userTransformationURL = handle.conf.GetString("USER_TRANSFORM_URL", "http://localhost:9090")
	handle.config.timeoutDuration = conf.GetDuration("HttpClient.procTransformer.timeout", 600, time.Second)
	handle.config.failOnUserTransformTimeout = conf.GetReloadableBoolVar(false, "Processor.Transformer.failOnUserTransformTimeout")
	handle.config.maxRetry = conf.GetReloadableIntVar(30, 1, "Processor.maxRetry")
	handle.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.Transformer.failOnError")
	handle.config.maxRetryBackoffInterval = conf.GetReloadableDurationVar(30, time.Second, "Processor.maxRetryBackoffInterval")
	return handle
}

func (u *UserTransformer) transform(
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
		"stage":     "user_transformer",
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
		transformer_utils.TrackLongRunningTransformation(ctx, "user_transformer", u.config.timeoutDuration, u.log.With(loggerCtx...))
		trackWg.Done()
	}()

	batches := lo.Chunk(clientEvents, batchSize)

	u.stat.NewTaggedStat(
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
			u.guardConcurrency <- struct{}{}
			go func() {
				trace.WithRegion(ctx, "request", func() {
					transformResponse[i] = u.request(ctx, url, "user_transformer", batch)
				})
				<-u.guardConcurrency
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

	u.stat.NewStat("processor.transformer_sent", stats.CountType).Count(len(clientEvents))
	u.stat.NewStat("processor.transformer_received", stats.CountType).Count(len(outClientEvents))

	return types.Response{
		Events:       outClientEvents,
		FailedEvents: failedEvents,
	}
}

func (u *UserTransformer) request(ctx context.Context, url, stage string, data []types.TransformerEvent) []types.TransformerResponse {
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
	endlessBackoff.MaxInterval = u.config.maxRetryBackoffInterval.Load()

	// endless backoff loop, only nil error or panics inside
	_ = backoff.RetryNotify(
		func() error {
			transformationID := ""
			if len(data[0].Destination.Transformations) > 0 {
				transformationID = data[0].Destination.Transformations[0].ID
			}

			respData, statusCode = u.doPost(ctx, rawJSON, url, stats.Tags{
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
			if statusCode == transformer_utils.StatusCPDown {
				u.stat.NewStat("processor.control_plane_down", stats.GaugeType).Gauge(1)
				return fmt.Errorf("control plane not reachable")
			}
			u.stat.NewStat("processor.control_plane_down", stats.GaugeType).Gauge(0)
			return nil
		},
		endlessBackoff,
		func(err error, t time.Duration) {
			var transformationID, transformationVersionID string
			if len(data[0].Destination.Transformations) > 0 {
				transformationID = data[0].Destination.Transformations[0].ID
				transformationVersionID = data[0].Destination.Transformations[0].VersionID
			}
			u.log.Errorf("JS HTTP connection error: URL: %v Error: %+v. WorkspaceID: %s, sourceID: %s, destinationID: %s, transformationID: %s, transformationVersionID: %s",
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
		u.log.Errorf("Transformer returned status code: %v", statusCode)
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
			u.log.Errorf("Data sent to transformer : %v", string(rawJSON))
			u.log.Errorf("Transformer returned : %v", string(respData))
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

func (u *UserTransformer) doPost(ctx context.Context, rawJSON []byte, url string, tags stats.Tags) ([]byte, int) {
	var (
		retryCount int
		resp       *http.Response
		respData   []byte
	)
	retryStrategy := backoff.NewExponentialBackOff()
	// MaxInterval caps the RetryInterval
	retryStrategy.MaxInterval = u.config.maxRetryBackoffInterval.Load()

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

				resp, reqErr = u.client.Do(req)
			})
			u.stat.NewTaggedStat("processor.transformer_request_time", stats.TimerType, tags).SendTiming(time.Since(requestStartTime))
			if reqErr != nil {
				return reqErr
			}

			defer func() { httputil.CloseResponse(resp) }()

			if !transformer_utils.IsJobTerminated(resp.StatusCode) && resp.StatusCode != transformer_utils.StatusCPDown {
				return fmt.Errorf("transformer returned status code: %v", resp.StatusCode)
			}

			respData, reqErr = io.ReadAll(resp.Body)
			return reqErr
		},
		backoff.WithMaxRetries(retryStrategy, uint64(u.config.maxRetry.Load())),
		func(err error, t time.Duration) {
			retryCount++
			u.log.Warnn(
				"JS HTTP connection error",
				logger.NewErrorField(err),
				logger.NewIntField("attempts", int64(retryCount)),
			)
		},
	)
	if err != nil {
		if u.config.failOnUserTransformTimeout.Load() && os.IsTimeout(err) {
			return []byte(fmt.Sprintf("transformer request timed out: %s", err)), transformer_utils.TransformerRequestTimeout
		} else if u.config.failOnError.Load() {
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
			u.log.Error(unexpectedVersionError)
			panic(unexpectedVersionError)
		}
	}

	return respData, resp.StatusCode
}

func (u *UserTransformer) userTransformURL() string {
	return u.config.userTransformationURL + "/customTransform"
}
