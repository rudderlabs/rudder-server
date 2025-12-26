package sourcehydration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"golang.org/x/sync/errgroup"

	"github.com/cenkalti/backoff"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	transformerclient "github.com/rudderlabs/rudder-server/internal/transformer-client"
	transformerutils "github.com/rudderlabs/rudder-server/processor/internal/transformer"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

const srcHydrationStage = "source_hydration"

type Opt func(*Client)

func WithClient(client transformerclient.Client) Opt {
	return func(s *Client) {
		s.client = client
	}
}

// Client handles source hydration transformations
type Client struct {
	config struct {
		sourceHydrationURL           string
		maxRetry                     config.ValueLoader[int]
		failOnError                  config.ValueLoader[bool]
		maxRetryBackoffInterval      config.ValueLoader[time.Duration]
		logLongRunningTransformAfter time.Duration
		batchSize                    config.ValueLoader[int]
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
	handle.config.logLongRunningTransformAfter = conf.GetDuration("HttpClient.procTransformer.logLongRunningTransformAfter", 600, time.Second)
	handle.config.maxRetry = conf.GetReloadableIntVar(50, 1, "Processor.SourceHydration.maxRetry", "Processor.maxRetry")
	handle.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.SourceHydration.failOnError", "Processor.Transformer.failOnError")
	handle.config.maxRetryBackoffInterval = conf.GetReloadableDurationVar(60, time.Second, "Processor.SourceHydration.maxRetryBackoffInterval", "Processor.maxRetryBackoffInterval")
	handle.config.batchSize = conf.GetReloadableIntVar(100, 1, "Processor.SourceHydration.batchSize", "Processor.transformBatchSize")

	for _, opt := range opts {
		opt(handle)
	}

	return handle
}

// Hydrate sends a batch of source events to the hydration endpoint and returns the hydrated events.
// It splits the input events into smaller batches (based on configured batch size), sends them
// concurrently for hydration, and aggregates the results.
//
// Error conditions:
//  1. Returns an error if the context is canceled.
//  2. Returns an error if all retry attempts fail and the client is configured to fail on errors.
//  3. Otherwise, if failOnError is disabled, a panic is raised.
func (c *Client) Hydrate(ctx context.Context, hydrationReq types.SrcHydrationRequest) (types.SrcHydrationResponse, error) {
	batchSize := c.config.batchSize.Load()
	clientEvents := hydrationReq.Batch
	if len(clientEvents) == 0 {
		return types.SrcHydrationResponse{}, nil
	}

	sourceHydrationURL := c.sourceHydrationURL(hydrationReq.Source.SourceDefinition.Name)

	labels := types.TransformerMetricLabels{
		Stage:       srcHydrationStage,
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
		transformerutils.TrackLongRunningTransformation(ctx, srcHydrationStage, c.config.logLongRunningTransformAfter, l)
		trackWg.Done()
	}()

	batches := lo.Chunk(clientEvents, batchSize)

	c.stat.NewTaggedStat(
		"processor_transformer_request_batch_count",
		stats.HistogramType,
		labels.ToStatsTag(),
	).Observe(float64(len(batches)))

	hydratedBatches := make([][]types.SrcHydrationEvent, len(batches))

	g, ctx := errgroup.WithContext(ctx)
	for i, batch := range batches {
		g.Go(func() error {
			resp, err := c.sendBatch(ctx, sourceHydrationURL, hydrationReq.Source, labels, batch)
			if err != nil {
				return err
			}
			hydratedBatches[i] = resp.Batch
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return types.SrcHydrationResponse{}, err
		}
		if c.config.failOnError.Load() {
			return types.SrcHydrationResponse{}, err
		}
		panic(err)
	}

	var outClientEvents []types.SrcHydrationEvent

	for _, batch := range hydratedBatches {
		// Add all events from the batch to the result
		outClientEvents = append(outClientEvents, batch...)
	}

	c.stat.NewStat("processor_transformer_sent", stats.CountType).Count(len(clientEvents))
	c.stat.NewStat("processor_transformer_received", stats.CountType).Count(len(outClientEvents))

	return types.SrcHydrationResponse{
		Batch: outClientEvents,
	}, nil
}

func (c *Client) sendBatch(ctx context.Context, url string, source types.SrcHydrationSource, labels types.TransformerMetricLabels, hydrationEvents []types.SrcHydrationEvent) (types.SrcHydrationResponse, error) {
	if len(hydrationEvents) == 0 {
		return types.SrcHydrationResponse{}, nil
	}

	start := time.Now()

	// Create request in the format expected by the source hydration API
	request := types.SrcHydrationRequest{
		Batch:  hydrationEvents,
		Source: source,
	}

	// Marshal request
	rawJSON, err := jsonrs.Marshal(request)
	if err != nil {
		panic(err)
	}

	respData, err := c.doPost(ctx, rawJSON, url, labels)
	if err != nil {
		return types.SrcHydrationResponse{}, err
	}

	var response types.SrcHydrationResponse
	err = jsonrs.Unmarshal(respData, &response)
	if err != nil {
		return response, err
	}
	if len(response.Batch) != len(hydrationEvents) {
		return response, fmt.Errorf("source hydration response length mismatch: got %d, want %d", len(response.Batch), len(hydrationEvents))
	}
	c.stat.NewTaggedStat("transformer_client_request_total_events", stats.CountType, labels.ToStatsTag()).Count(len(hydrationEvents))
	c.stat.NewTaggedStat("transformer_client_response_total_events", stats.CountType, labels.ToStatsTag()).Count(len(response.Batch))
	c.stat.NewTaggedStat("transformer_client_total_time", stats.TimerType, labels.ToStatsTag()).SendTiming(time.Since(start))

	return response, nil
}

func (c *Client) doPost(ctx context.Context, rawJSON []byte, url string, labels types.TransformerMetricLabels) ([]byte, error) {
	var (
		retryCount int
		resp       *http.Response
		respData   []byte
	)

	retryStrategy := backoff.NewExponentialBackOff()
	// MaxInterval caps the RetryInterval
	retryStrategy.MaxInterval = c.config.maxRetryBackoffInterval.Load()

	err := backoff.RetryNotify(
		transformerutils.WithProcTransformReqTimeStat(func() error {
			var err error
			requestStartTime := time.Now()

			var req *http.Request
			req, err = http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(rawJSON))
			if err != nil {
				return err
			}

			req.Header.Set("Content-Type", "application/json; charset=utf-8")
			req.Header.Set("X-Feature-Gzip-Support", "?1")

			resp, err = c.client.Do(req)
			defer func() { httputil.CloseResponse(resp) }()
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return backoff.Permanent(err)
				}
				return fmt.Errorf("client error: %w", err)
			}
			// Record metrics with labels
			tags := labels.ToStatsTag()
			duration := time.Since(requestStartTime)
			c.stat.NewTaggedStat("transformer_client_request_total_bytes", stats.CountType, tags).Count(len(rawJSON))
			c.stat.NewTaggedStat("transformer_client_total_durations_seconds", stats.CountType, tags).Count(int(duration.Seconds()))

			respData, err = io.ReadAll(resp.Body)
			if err != nil {
				return fmt.Errorf("reading response body: %w", err)
			}
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("source hydration returned status code: %v, response: %s", resp.StatusCode, respData)
			}

			c.stat.NewTaggedStat("transformer_client_response_total_bytes", stats.CountType, tags).Count(len(respData))

			return nil
		}, c.stat, labels),
		backoff.WithMaxRetries(retryStrategy, uint64(c.config.maxRetry.Load())),
		func(err error, t time.Duration) {
			retryCount++
			c.log.Warnn(
				"Source hydration HTTP connection error",
				append(
					labels.ToLoggerFields(),
					logger.NewIntField("attempts", int64(retryCount)),
					obskit.Error(err),
				)...,
			)
		},
	)
	if err != nil {
		return nil, err
	}

	return respData, nil
}

func (c *Client) sourceHydrationURL(sourceType string) string {
	// Based on the OpenAPI spec: /{version}/sources/{source}/hydrate
	return fmt.Sprintf("%s/v2/sources/%s/hydrate", c.config.sourceHydrationURL, strings.ToLower(sourceType))
}
