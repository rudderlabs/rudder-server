package sourcehydration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/cenkalti/backoff"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	transformerclient "github.com/rudderlabs/rudder-server/internal/transformer-client"
	transformerutils "github.com/rudderlabs/rudder-server/processor/internal/transformer"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

// HydrationEvent represents a single event in the hydration request/response
type HydrationEvent struct {
	ID         string                 `json:"id"`
	Event      map[string]interface{} `json:"event"`
	StatusCode int                    `json:"statusCode,omitempty"`
}

// Request represents the request format for source hydration API
type Request struct {
	Batch  []HydrationEvent `json:"batch"`
	Source Source           `json:"source"`
}

type Source struct {
	ID               string                          `json:"id"`
	Config           json.RawMessage                 `json:"config"`
	WorkspaceID      string                          `json:"workspaceId"`
	SourceDefinition backendconfig.SourceDefinitionT `json:"sourceDefinition"`
}

// Response represents the response format from source hydration API
type Response struct {
	Batch      []HydrationEvent `json:"batch"`
	StatusCode int
}

type Opt func(*Client)

func WithClient(client transformerclient.Client) Opt {
	return func(s *Client) {
		s.client = client
	}
}

// Client handles source hydration transformations
type Client struct {
	config struct {
		sourceHydrationURL      string
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

// New creates a new source hydration client
func New(conf *config.Config, log logger.Logger, stat stats.Stats, opts ...Opt) *Client {
	handle := &Client{}
	handle.conf = conf
	handle.log = log
	handle.stat = stat
	handle.client = transformerclient.NewClient(transformerutils.TransformerClientConfig(conf, "SourceHydration"))
	handle.config.sourceHydrationURL = handle.conf.GetString("DEST_TRANSFORM_URL", "http://localhost:9090")
	handle.config.timeoutDuration = conf.GetDuration("HttpClient.procTransformer.timeout", 600, time.Second)
	handle.config.maxRetry = conf.GetReloadableIntVar(30, 1, "Processor.SourceHydration.maxRetry", "Processor.maxRetry")
	handle.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.SourceHydration.failOnError", "Processor.Transformer.failOnError")
	handle.config.maxRetryBackoffInterval = conf.GetReloadableDurationVar(30, time.Second, "Processor.SourceHydration.maxRetryBackoffInterval", "Processor.maxRetryBackoffInterval")
	handle.config.batchSize = conf.GetReloadableIntVar(100, 1, "Processor.SourceHydration.batchSize", "Processor.transformBatchSize")

	for _, opt := range opts {
		opt(handle)
	}

	return handle
}

// Hydrate performs source hydration on the provided events
func (c *Client) Hydrate(ctx context.Context, hydrationReq Request) (Response, error) {
	batchSize := c.config.batchSize.Load()
	clientEvents := hydrationReq.Batch
	if len(clientEvents) == 0 {
		return Response{}, nil
	}

	sourceHydrationURL := c.sourceHydrationURL(hydrationReq.Source.SourceDefinition.Name)

	labels := types.TransformerMetricLabels{
		Stage:       "source_hydration",
		SourceID:    hydrationReq.Source.ID,
		WorkspaceID: hydrationReq.Source.WorkspaceID,
		SourceType:  hydrationReq.Source.SourceDefinition.Name,
	}

	var trackWg sync.WaitGroup
	defer trackWg.Wait()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	trackWg.Add(1)
	go func() {
		l := c.log.Withn(labels.ToLoggerFields()...)
		transformerutils.TrackLongRunningTransformation(ctx, "source_hydration", c.config.timeoutDuration, l)
		trackWg.Done()
	}()

	batches := lo.Chunk(clientEvents, batchSize)

	c.stat.NewTaggedStat(
		"processor_transformer_request_batch_count",
		stats.HistogramType,
		labels.ToStatsTag(),
	).Observe(float64(len(batches)))

	hydratedBatches := make([][]HydrationEvent, len(batches))

	g, groupCtx := errgroup.WithContext(ctx)
	var wg sync.WaitGroup
	wg.Add(len(batches))
	lo.ForEach(
		batches,
		func(batch []HydrationEvent, i int) {
			g.Go(func() error {
				resp, err := c.sendBatch(groupCtx, sourceHydrationURL, hydrationReq.Source, labels, batch)
				if err != nil {
					return err
				}
				if resp.StatusCode != http.StatusOK {
					// drop the events and continue
					c.log.Warnn("source hydration failed for a sub-batch, dropping events in the sub-batch", labels.ToLoggerFields()...)
					return nil
				}
				hydratedBatches[i] = resp.Batch
				return nil
			})
		},
	)

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return Response{}, fmt.Errorf("source hydration cancelled: %w", err)
		}
		panic(err)
	}

	var outClientEvents []HydrationEvent

	for _, batch := range hydratedBatches {
		// Add all events from the batch to the result
		outClientEvents = append(outClientEvents, batch...)
	}

	c.stat.NewStat("processor_transformer_sent", stats.CountType).Count(len(clientEvents))
	c.stat.NewStat("processor_transformer_received", stats.CountType).Count(len(outClientEvents))

	return Response{
		Batch: outClientEvents,
	}, nil
}

func (c *Client) sendBatch(ctx context.Context, url string, source Source, labels types.TransformerMetricLabels, data []HydrationEvent) (Response, error) {
	if len(data) == 0 {
		return Response{}, nil
	}

	start := time.Now()

	// Create request in the format expected by the source hydration API
	request := Request{
		Batch:  data,
		Source: source,
	}

	// Marshal request
	rawJSON, err := jsonrs.Marshal(request)
	if err != nil {
		panic(err)
	}

	respData, statusCode, err := c.doPost(ctx, rawJSON, url, labels)
	if err != nil {
		return Response{}, err
	}

	switch statusCode {
	case http.StatusOK:
		var response Response
		err = jsonrs.Unmarshal(respData, &response)
		if err != nil {
			c.log.Errorn("Data sent to transformer", logger.NewStringField("payload", string(rawJSON)))
			c.log.Errorn("Transformer returned", logger.NewStringField("payload", string(respData)))
			panic(err)
		}
		response.StatusCode = statusCode
		c.stat.NewTaggedStat("transformer_client_request_total_events", stats.CountType, labels.ToStatsTag()).Count(len(data))
		c.stat.NewTaggedStat("transformer_client_response_total_events", stats.CountType, labels.ToStatsTag()).Count(len(response.Batch))
		c.stat.NewTaggedStat("transformer_client_total_time", stats.TimerType, labels.ToStatsTag()).SendTiming(time.Since(start))

		return response, nil
	default:
		c.log.Errorn("Source hydration returned status code", logger.NewStringField("statusCode", strconv.Itoa(statusCode)))
		c.stat.NewTaggedStat("transformer_client_request_total_events", stats.CountType, labels.ToStatsTag()).Count(len(data))
		c.stat.NewTaggedStat("transformer_client_total_time", stats.TimerType, labels.ToStatsTag()).SendTiming(time.Since(start))
		return Response{StatusCode: statusCode}, nil
	}
}

func (c *Client) doPost(ctx context.Context, rawJSON []byte, url string, labels types.TransformerMetricLabels) ([]byte, int, error) {
	var (
		retryCount int
		resp       *http.Response
		respData   []byte
		statusCode int
	)

	retryStrategy := backoff.NewExponentialBackOff()
	// MaxInterval caps the RetryInterval
	retryStrategy.MaxInterval = c.config.maxRetryBackoffInterval.Load()

	err := backoff.RetryNotify(
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

			resp, reqErr = c.client.Do(req)
			defer func() { httputil.CloseResponse(resp) }()
			if reqErr != nil {
				if errors.Is(reqErr, context.Canceled) {
					return backoff.Permanent(reqErr)
				}
				return reqErr
			}
			// Record metrics with labels
			tags := labels.ToStatsTag()
			duration := time.Since(requestStartTime)
			c.stat.NewTaggedStat("transformer_client_request_total_bytes", stats.CountType, tags).Count(len(rawJSON))
			c.stat.NewTaggedStat("transformer_client_total_durations_seconds", stats.CountType, tags).Count(int(duration.Seconds()))

			statusCode = resp.StatusCode
			if statusCode != http.StatusOK {
				return fmt.Errorf("source hydration returned status code: %v", statusCode)
			}

			respData, reqErr = io.ReadAll(resp.Body)
			if reqErr == nil {
				c.stat.NewTaggedStat("transformer_client_response_total_bytes", stats.CountType, tags).Count(len(respData))
				// We'll count response events after unmarshaling in the request method
			}
			return reqErr
		}, c.stat, labels),
		backoff.WithMaxRetries(retryStrategy, uint64(c.config.maxRetry.Load())),
		func(err error, t time.Duration) {
			retryCount++
			c.log.Warnn(
				"Source hydration HTTP connection error",
				obskit.Error(err),
				logger.NewIntField("attempts", int64(retryCount)),
			)
		},
	)
	if err != nil {
		if c.config.failOnError.Load() {
			return []byte(fmt.Sprintf("source hydration request failed: %s", err)), transformerutils.TransformerRequestFailure, nil
		} else {
			return nil, statusCode, err
		}
	}

	return respData, statusCode, nil
}

func (c *Client) sourceHydrationURL(sourceType string) string {
	// Based on the OpenAPI spec: /{version}/sources/{source}/hydrate
	return fmt.Sprintf("%s/v2/sources/%s/hydrate", c.config.sourceHydrationURL, strings.ToLower(sourceType))
}
