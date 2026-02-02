package user_transformer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
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
	return func(s *Client) { s.client = client }
}

func ForMirroring() Opt {
	return func(s *Client) { s.config.forMirroring = true }
}

func New(conf *config.Config, log logger.Logger, stat stats.Stats, opts ...Opt) *Client {
	handle := &Client{}
	handle.conf = conf
	handle.log = log.Child("user_transformer")
	handle.stat = stat
	handle.client = transformerclient.NewClient(transformerutils.TransformerClientConfig(conf, "UserTransformer"))
	handle.config.userTransformationURL = handle.conf.GetString("USER_TRANSFORM_URL", handle.conf.GetString("DEST_TRANSFORM_URL", "http://localhost:9090"))
	handle.config.pythonTransformationURL = handle.conf.GetString("PYTHON_TRANSFORM_URL", "")
	handle.config.pythonTransformationVersionIDsEnabled = handle.conf.GetBool("PYTHON_TRANSFORM_VERSION_IDS_ENABLE", false)
	if handle.config.pythonTransformationVersionIDsEnabled {
		if versionIDsStr := handle.conf.GetString("PYTHON_TRANSFORM_VERSION_IDS", ""); versionIDsStr != "" {
			allowedVersionIDs := strings.Split(versionIDsStr, ",")
			handle.config.pythonTransformationVersionIDs = make(map[string]struct{}, len(allowedVersionIDs))
			for _, versionID := range allowedVersionIDs {
				handle.config.pythonTransformationVersionIDs[versionID] = struct{}{}
			}
		}
	}
	handle.config.timeoutDuration = conf.GetDuration("HttpClient.procTransformer.timeout", 600, time.Second)
	handle.config.failOnUserTransformTimeout = conf.GetReloadableBoolVar(false, "Processor.UserTransformer.failOnUserTransformTimeout", "Processor.Transformer.failOnUserTransformTimeout")
	handle.config.maxRetry = conf.GetReloadableIntVar(30, 1, "Processor.UserTransformer.maxRetry", "Processor.maxRetry")
	handle.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.UserTransformer.failOnError", "Processor.Transformer.failOnError")
	handle.config.maxRetryBackoffInterval = conf.GetReloadableDurationVar(30, time.Second, "Processor.UserTransformer.maxRetryBackoffInterval", "Processor.maxRetryBackoffInterval")
	handle.config.collectInstanceLevelStats = conf.GetBool("Processor.collectInstanceLevelStats", false)
	handle.config.batchSize = conf.GetReloadableIntVar(200, 1, "Processor.UserTransformer.batchSize", "Processor.userTransformBatchSize")

	for _, opt := range opts {
		opt(handle)
	}

	if handle.config.forMirroring {
		handle.config.userTransformationURL = handle.conf.GetString("USER_TRANSFORM_MIRROR_URL", "")
		handle.config.pythonTransformationURL = handle.conf.GetString("PYTHON_TRANSFORM_MIRROR_URL", "")
		handle.skippedEventsForMirroring = handle.stat.NewStat(
			"processor_transformer_skipped_events_for_mirroring", stats.CountType,
		)
	}

	return handle
}

type Client struct {
	config struct {
		userTransformationURL                 string
		pythonTransformationURL               string
		pythonTransformationVersionIDs        map[string]struct{}
		pythonTransformationVersionIDsEnabled bool
		forMirroring                          bool
		maxRetry                              config.ValueLoader[int]
		failOnUserTransformTimeout            config.ValueLoader[bool]
		failOnError                           config.ValueLoader[bool]
		maxRetryBackoffInterval               config.ValueLoader[time.Duration]
		timeoutDuration                       time.Duration
		collectInstanceLevelStats             bool
		batchSize                             config.ValueLoader[int]
	}
	conf                      *config.Config
	log                       logger.Logger
	stat                      stats.Stats
	client                    transformerclient.Client
	skippedEventsForMirroring stats.Counter
}

func (u *Client) Transform(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
	if len(clientEvents) == 0 {
		return types.Response{}
	}
	batchSize := u.config.batchSize.Load()
	var transformationID string
	if len(clientEvents[0].Destination.Transformations) > 0 {
		transformationID = clientEvents[0].Destination.Transformations[0].ID
	}

	transformationLanguage, transformationVersionID := u.getTransformationInfo(clientEvents)
	userURL, skip := u.userTransformURL(transformationLanguage, transformationVersionID)
	if skip {
		u.skippedEventsForMirroring.Count(len(clientEvents))
		return types.Response{}
	}

	labels := types.TransformerMetricLabels{
		Endpoint:         transformerutils.GetEndpointFromURL(userURL),
		Stage:            "user_transformer",
		DestinationType:  clientEvents[0].Destination.DestinationDefinition.Name,
		SourceType:       clientEvents[0].Metadata.SourceType,
		WorkspaceID:      clientEvents[0].Metadata.WorkspaceID,
		SourceID:         clientEvents[0].Metadata.SourceID,
		DestinationID:    clientEvents[0].Destination.ID,
		TransformationID: transformationID,
		Mirroring:        u.config.forMirroring,
	}

	var trackWg sync.WaitGroup
	defer trackWg.Wait()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	trackWg.Add(1)
	go func() {
		l := u.log.Withn(labels.ToLoggerFields()...)
		transformerutils.TrackLongRunningTransformation(ctx, "user_transformer", u.config.timeoutDuration, l)
		trackWg.Done()
	}()

	batches := lo.Chunk(clientEvents, batchSize)

	u.stat.NewTaggedStat(
		"processor_transformer_request_batch_count",
		stats.HistogramType,
		labels.ToStatsTag(),
	).Observe(float64(len(batches)))

	transformResponse := make([][]types.TransformerResponse, len(batches))

	var wg sync.WaitGroup
	wg.Add(len(batches))

	lo.ForEach(
		batches,
		func(batch []types.TransformerEvent, i int) {
			go func() {
				transformResponse[i] = u.sendBatch(ctx, userURL, labels, batch)
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

	u.stat.NewStat("processor_transformer_sent", stats.CountType).Count(len(clientEvents))
	u.stat.NewStat("processor_transformer_received", stats.CountType).Count(len(outClientEvents))

	return types.Response{
		Events:       outClientEvents,
		FailedEvents: failedEvents,
	}
}

func (u *Client) sendBatch(ctx context.Context, url string, labels types.TransformerMetricLabels, clientEvents []types.TransformerEvent) []types.TransformerResponse {
	if len(clientEvents) == 0 {
		return nil
	}
	start := time.Now()
	// Call remote transformation
	var (
		rawJSON []byte
		err     error
	)

	data := lo.Map(clientEvents, func(clientEvent types.TransformerEvent, index int) types.UserTransformerEvent {
		res := *clientEvent.ToUserTransformerEvent()
		// flip sourceID and originalSourceID if it's a replay source for the purpose of any user transformation
		// flip back afterward
		if res.Metadata.OriginalSourceID != "" {
			res.Metadata.OriginalSourceID, res.Metadata.SourceID = res.Metadata.SourceID, res.Metadata.OriginalSourceID
		}
		return res
	})

	rawJSON, err = jsonrs.Marshal(data)
	if err != nil {
		panic(err)
	}

	var (
		respData   []byte
		statusCode int
	)

	// endless retry if transformer-control plane connection is down
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = u.config.maxRetryBackoffInterval.Load()

	// endless backoff loop, only nil error or panics inside
	_ = backoffvoid.Retry(
		context.Background(),
		func() error {
			respData, statusCode, err = u.doPost(ctx, rawJSON, url, labels)
			if err != nil {
				panic(err)
			}
			if statusCode == transformerutils.StatusCPDown {
				u.stat.NewStat("processor_control_plane_down", stats.GaugeType).Gauge(1)
				return fmt.Errorf("control plane not reachable")
			}
			u.stat.NewStat("processor_control_plane_down", stats.GaugeType).Gauge(0)
			return nil
		},
		backoff.WithBackOff(bo),
		backoff.WithMaxElapsedTime(0), // no max time -> ends only when no error
		backoff.WithNotify(func(err error, t time.Duration) {
			var transformationID, transformationVersionID string
			if len(clientEvents[0].Destination.Transformations) > 0 {
				transformationID = clientEvents[0].Destination.Transformations[0].ID
				transformationVersionID = clientEvents[0].Destination.Transformations[0].VersionID
			}
			u.log.Errorn("User transformation HTTP connection error",
				obskit.Error(err),
				obskit.SourceID(clientEvents[0].Metadata.SourceID),
				obskit.WorkspaceID(clientEvents[0].Metadata.WorkspaceID),
				obskit.DestinationID(clientEvents[0].Metadata.DestinationID),
				logger.NewStringField("url", url),
				obskit.TransformationID(transformationID),
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
		u.log.Errorn("Transformer returned status code", logger.NewStringField("statusCode", strconv.Itoa(statusCode)))
	}

	var transformerResponses []types.TransformerResponse
	switch statusCode {
	case http.StatusOK:
		integrations.CollectIntgTransformErrorStats(respData)
		err = jsonrs.Unmarshal(respData, &transformerResponses)
		// This is returned by our JS engine so should be parseable
		// Panic the processor to avoid replays
		if err != nil {
			u.log.Errorn("Data sent to transformer", logger.NewStringField("payload", string(rawJSON)))
			u.log.Errorn("Transformer returned", logger.NewStringField("payload", string(respData)))
			panic(err)
		}
	default:
		for i := range data {
			transformEvent := &data[i]
			resp := types.TransformerResponse{StatusCode: statusCode, Error: string(respData), Metadata: transformEvent.Metadata}
			transformerResponses = append(transformerResponses, resp)
		}
	}
	u.stat.NewTaggedStat("transformer_client_request_total_events", stats.CountType, labels.ToStatsTag()).Count(len(clientEvents))
	u.stat.NewTaggedStat("transformer_client_response_total_events", stats.CountType, labels.ToStatsTag()).Count(len(transformerResponses))
	u.stat.NewTaggedStat("transformer_client_total_time", stats.TimerType, labels.ToStatsTag()).SendTiming(time.Since(start))
	return transformerResponses
}

func (u *Client) doPost(ctx context.Context, rawJSON []byte, url string, labels types.TransformerMetricLabels) ([]byte, int, error) {
	var (
		retryCount int
		resp       *http.Response
		respData   []byte
	)
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = u.config.maxRetryBackoffInterval.Load()

	err := backoffvoid.Retry(ctx,
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

			resp, reqErr = u.client.Do(req)
			defer func() { httputil.CloseResponse(resp) }()
			// Record metrics with labels
			tags := labels.ToStatsTag()
			duration := time.Since(requestStartTime)
			u.stat.NewTaggedStat("transformer_client_request_total_bytes", stats.CountType, tags).Count(len(rawJSON))
			u.stat.NewTaggedStat("transformer_client_total_durations_seconds", stats.CountType, tags).Count(int(duration.Seconds()))

			if reqErr != nil {
				return reqErr
			}
			headerResponseTime := resp.Header.Get("X-Response-Time")
			instanceWorker := resp.Header.Get("X-Instance-ID")

			if u.config.collectInstanceLevelStats && instanceWorker != "" {
				newTags := lo.Assign(tags)
				newTags["instanceWorker"] = instanceWorker
				dur := duration.Milliseconds()
				headerTime, err := strconv.ParseFloat(strings.TrimSuffix(headerResponseTime, "ms"), 64)
				if err == nil {
					diff := float64(dur) - headerTime
					u.stat.NewTaggedStat("processor_transform_duration_diff_time", stats.TimerType, newTags).SendTiming(time.Duration(diff) * time.Millisecond)
				}
			}

			if !transformerutils.IsJobTerminated(resp.StatusCode) && resp.StatusCode != transformerutils.StatusCPDown {
				return fmt.Errorf("transformer returned status code: %v", resp.StatusCode)
			}

			respData, reqErr = io.ReadAll(resp.Body)
			if reqErr == nil {
				u.stat.NewTaggedStat("transformer_client_response_total_bytes", stats.CountType, tags).Count(len(respData))
				// We'll count response events after unmarshaling in the request method
			}
			return reqErr
		}, u.stat, labels),
		backoff.WithBackOff(bo),
		backoff.WithMaxTries(uint(u.config.maxRetry.Load()+1)),
		backoff.WithNotify(func(err error, t time.Duration) {
			retryCount++
			u.log.Warnn(
				"JS HTTP connection error",
				append(
					labels.ToLoggerFields(),
					obskit.Error(err),
					logger.NewIntField("attempts", int64(retryCount)),
				)...,
			)
		}),
	)
	if err != nil {
		u.log.Errorn("User transformation post error",
			append(labels.ToLoggerFields(), obskit.Error(err))...)
		if u.config.failOnUserTransformTimeout.Load() && os.IsTimeout(err) {
			return []byte(fmt.Sprintf("transformer request timed out: %s", err)), transformerutils.TransformerRequestTimeout, nil
		} else if u.config.failOnError.Load() {
			return []byte(fmt.Sprintf("transformer request failed: %s", err)), transformerutils.TransformerRequestFailure, nil
		}
		return nil, 0, err
	}

	// perform version compatibility check only on success
	if resp.StatusCode == http.StatusOK {
		transformerAPIVersion, _ := strconv.Atoi(resp.Header.Get("apiVersion"))
		if reportingtypes.SupportedTransformerApiVersion != transformerAPIVersion {
			unexpectedVersionError := fmt.Errorf("incompatible transformer version: Expected: %d Received: %s, URL: %v", reportingtypes.SupportedTransformerApiVersion, resp.Header.Get("apiVersion"), url)
			u.log.Errorn("Unexpected version", obskit.Error(unexpectedVersionError))
			return nil, 0, unexpectedVersionError
		}
	}

	return respData, resp.StatusCode, nil
}

func (u *Client) userTransformURL(language, versionID string) (string, bool) {
	isPython := strings.HasPrefix(language, "python")

	if !u.config.forMirroring { // Common production branch
		if isPython && u.isPythonVersionAllowed(versionID) && u.config.pythonTransformationURL != "" {
			return u.config.pythonTransformationURL + "/customTransform", false
		}
		return u.config.userTransformationURL + "/customTransform", false
	}

	// Mirroring
	if isPython {
		if u.config.pythonTransformationURL == "" {
			// mirroring is enabled but without a URL for the PyTransformer, SKIP!
			return "", true
		}
		if !u.isPythonVersionAllowed(versionID) {
			// mirroring is enabled, but this transformation version is not allowed, SKIP!
			return "", true
		}
		return u.config.pythonTransformationURL + "/customTransform", false
	}

	// Mirroring JS
	if u.config.userTransformationURL == "" {
		// mirroring is enabled but without a URL for the JSTransformer, SKIP!
		return "", true
	}

	return u.config.userTransformationURL + "/customTransform", false
}

func (u *Client) isPythonVersionAllowed(versionID string) bool {
	if !u.config.pythonTransformationVersionIDsEnabled {
		return true
	}
	_, ok := u.config.pythonTransformationVersionIDs[versionID]
	return ok
}

func (u *Client) getTransformationInfo(clientEvents []types.TransformerEvent) (language, versionID string) {
	language = "javascript"
	if len(clientEvents) == 0 {
		return language, ""
	}
	if len(clientEvents[0].Destination.Transformations) > 0 {
		transformation := clientEvents[0].Destination.Transformations[0]
		versionID = transformation.VersionID
		if transformation.Language != "" {
			language = transformation.Language
		}
	}
	return language, versionID
}
