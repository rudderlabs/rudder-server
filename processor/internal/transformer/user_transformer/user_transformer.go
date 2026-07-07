package user_transformer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
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
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

const (
	coldStartErrorsMetric = "processor_user_transformer_cold_start_errors_total"
	languagePython        = "python"
)

// ErrPerWorkspacePyTNotEnabled is returned by the test-forwarding methods
// ([Client.Test] et al.) when the per-workspace PyT feature is off (or the
// client is a mirroring one), so callers can reject the request explicitly
// instead of it being forwarded to a URL that isn't a pyt deployment.
var ErrPerWorkspacePyTNotEnabled = errors.New("per-workspace PyT is not enabled")

type Opt func(*Client)

func WithClient(client transformerclient.Client) Opt {
	return func(s *Client) { s.client = client }
}

func ForMirroring() Opt {
	return func(s *Client) { s.config.forMirroring = true }
}

// WithMaxRetryBackoffInterval overrides the max backoff interval for this client
// instance, letting the test-forward path retry a cold pyt with a short backoff
// instead of the event path's large default interval.
func WithMaxRetryBackoffInterval(d config.ValueLoader[time.Duration]) Opt {
	return func(s *Client) { s.config.maxRetryBackoffInterval = d }
}

// WithMaxRetry overrides the retry budget for this client instance, sizing how
// many times the test-forward path retries a cold pyt before giving up (also
// bounded by the caller's deadline).
func WithMaxRetry(maxRetry config.ValueLoader[int]) Opt {
	return func(s *Client) { s.config.maxRetry = maxRetry }
}

func New(conf *config.Config, log logger.Logger, stat stats.Stats, opts ...Opt) *Client {
	handle := &Client{}
	handle.conf = conf
	handle.log = log.Child("user_transformer")
	handle.stat = stat
	handle.client = transformerclient.NewClient("UserTransformer", transformerutils.TransformerClientConfig(conf, "UserTransformer"))
	handle.config.userTransformationURL = handle.conf.GetStringVar(handle.conf.GetStringVar("http://localhost:9090", "DEST_TRANSFORM_URL"), "USER_TRANSFORM_URL")
	handle.config.pythonTransformationURL = handle.conf.GetStringVar("", "PYTHON_TRANSFORM_URL")
	handle.config.perWorkspacePyTEnabled = handle.conf.GetReloadableBoolVar(false, "Processor.UserTransformer.perWorkspacePyTEnabled")
	handle.config.perWorkspacePyTURLTemplate = handle.conf.GetStringVar("http://pyt-{workspaceID}:8080", "Processor.UserTransformer.perWorkspacePyTURLTemplate")
	handle.config.perWorkspacePyTEndlessRetries = handle.conf.GetReloadableBoolVar(true, "Processor.UserTransformer.perWorkspacePyTEndlessRetries")
	handle.config.pythonTransformConfig = transformerutils.LoadPythonTransformConfig(conf)
	handle.config.timeoutDuration = conf.GetDurationVar(600, time.Second, "HttpClient.procTransformer.timeout")
	handle.config.failOnUserTransformTimeout = conf.GetReloadableBoolVar(false, "Processor.UserTransformer.failOnUserTransformTimeout", "Processor.Transformer.failOnUserTransformTimeout")
	handle.config.maxRetry = conf.GetReloadableIntVar(30, 1, "Processor.UserTransformer.maxRetry", "Processor.maxRetry")
	handle.config.cpDownEndlessRetries = conf.GetReloadableBoolVar(true, "Processor.UserTransformer.cpDownEndlessRetries")
	handle.config.failOnError = conf.GetReloadableBoolVar(false, "Processor.UserTransformer.failOnError", "Processor.Transformer.failOnError")
	handle.config.maxRetryBackoffInterval = conf.GetReloadableDurationVar(30, time.Second, "Processor.UserTransformer.maxRetryBackoffInterval", "Processor.maxRetryBackoffInterval")
	handle.config.collectInstanceLevelStats = conf.GetBoolVar(false, "Processor.collectInstanceLevelStats")
	handle.config.batchSize = conf.GetReloadableIntVar(200, 1, "Processor.UserTransformer.batchSize", "Processor.userTransformBatchSize")

	for _, opt := range opts {
		opt(handle)
	}

	if handle.config.forMirroring {
		handle.config.userTransformationURL = handle.conf.GetStringVar("", "USER_TRANSFORM_MIRROR_URL")
		handle.config.pythonTransformationURL = handle.conf.GetStringVar("", "PYTHON_TRANSFORM_MIRROR_URL")
	}

	return handle
}

type Client struct {
	config struct {
		userTransformationURL         string
		pythonTransformationURL       string
		pythonTransformConfig         transformerutils.PythonTransformConfig
		forMirroring                  bool
		maxRetry                      config.ValueLoader[int]
		cpDownEndlessRetries          config.ValueLoader[bool]
		failOnUserTransformTimeout    config.ValueLoader[bool]
		failOnError                   config.ValueLoader[bool]
		maxRetryBackoffInterval       config.ValueLoader[time.Duration]
		timeoutDuration               time.Duration
		collectInstanceLevelStats     bool
		batchSize                     config.ValueLoader[int]
		perWorkspacePyTEnabled        config.ValueLoader[bool]
		perWorkspacePyTURLTemplate    string
		perWorkspacePyTEndlessRetries config.ValueLoader[bool]
	}
	conf   *config.Config
	log    logger.Logger
	stat   stats.Stats
	client transformerclient.Client
}

func (u *Client) Transform(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
	if len(clientEvents) == 0 {
		return types.Response{}
	}
	batchSize := u.config.batchSize.Load()
	transformationLanguage, transformationVersionID, transformationID := transformerutils.GetTransformationInfo(clientEvents)
	workspaceID := ""
	if len(clientEvents) > 0 {
		workspaceID = clientEvents[0].Metadata.WorkspaceID
	}
	userURL := u.userTransformURL(transformationLanguage, transformationVersionID, workspaceID)

	labels := types.TransformerMetricLabels{
		Endpoint:         transformerutils.GetEndpointFromURL(userURL),
		Stage:            "user_transformer",
		Language:         transformationLanguage,
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
	ctx = transformerclient.WithPerpetualRetriesStatsTags(ctx, map[string]string{"language": transformationLanguage})

	trackWg.Go(func() {
		l := u.log.Withn(labels.ToLoggerFields()...)
		transformerutils.TrackLongRunningTransformation(ctx, "user_transformer", u.config.timeoutDuration, l)
	})

	batches := lo.Chunk(clientEvents, batchSize)

	u.stat.NewTaggedStat(
		"processor_transformer_request_batch_count",
		stats.HistogramType,
		labels.ToStatsTag(),
	).Observe(float64(len(batches)))

	type sendBatchResult struct {
		responses      []types.TransformerResponse
		mirrorFiltered bool
	}
	transformResponse := make([]sendBatchResult, len(batches))

	var wg sync.WaitGroup
	lo.ForEach(
		batches,
		func(batch []types.TransformerEvent, i int) {
			wg.Go(func() {
				defer crash.Notify("Core")
				responses, mirrorFiltered := u.sendBatch(ctx, userURL, labels, batch)
				transformResponse[i] = sendBatchResult{responses: responses, mirrorFiltered: mirrorFiltered}
			})
		},
	)
	wg.Wait()

	var outClientEvents []types.TransformerResponse
	var failedEvents []types.TransformerResponse

	for _, br := range transformResponse {
		if br.mirrorFiltered {
			// If any batch was mirror-filtered, the whole response is mirror-filtered.
			// All batches share the same transformation, so this is all-or-nothing.
			return types.Response{MirrorFiltered: true}
		}

		// Transform is one to many mapping so returned
		// response for each is an array. We flatten it out
		for _, transformerResponse := range br.responses {
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

func (u *Client) sendBatch(
	ctx context.Context,
	url string,
	labels types.TransformerMetricLabels,
	clientEvents []types.TransformerEvent,
) (
	[]types.TransformerResponse,
	bool, // is mirror filtered
) {
	if len(clientEvents) == 0 {
		return nil, false
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
	retryOptions := []backoff.RetryOption{
		backoff.WithBackOff(bo),
		backoff.WithMaxElapsedTime(0), // no max time -> ends only when no error
		backoff.WithNotify(func(err error, t time.Duration) {
			var transformationID, transformationVersionID string
			if len(clientEvents[0].Destination.Transformations) > 0 {
				transformationID = clientEvents[0].Destination.Transformations[0].ID
				transformationVersionID = clientEvents[0].Destination.Transformations[0].VersionID
			}
			u.stat.NewStat("processor_user_transformer_cp_down_retries", stats.CountType).Increment()
			u.log.Errorn(
				"User transformation HTTP connection error",
				obskit.Error(err),
				obskit.SourceID(clientEvents[0].Metadata.SourceID),
				obskit.WorkspaceID(clientEvents[0].Metadata.WorkspaceID),
				obskit.DestinationID(clientEvents[0].Metadata.DestinationID),
				logger.NewStringField("url", url),
				obskit.TransformationID(transformationID),
				logger.NewStringField("transformationVersionID", transformationVersionID),
			)
		}),
	}
	_ = backoffvoid.Retry(
		context.Background(),
		func() error {
			respData, statusCode, err = u.doPost(ctx, rawJSON, url, labels)
			if err != nil {
				var transformationID string
				if len(clientEvents[0].Destination.Transformations) > 0 {
					transformationID = clientEvents[0].Destination.Transformations[0].ID
				}
				panic(fmt.Errorf("post to transformer failed for workspace %q source %q destination %q transformation %q url %q: %w",
					clientEvents[0].Metadata.WorkspaceID,
					clientEvents[0].Metadata.SourceID,
					clientEvents[0].Metadata.DestinationID,
					transformationID,
					url,
					err))
			}
			if statusCode == transformerutils.StatusCPDown {
				u.stat.NewStat("processor_control_plane_down", stats.GaugeType).Gauge(1)
				if !u.config.cpDownEndlessRetries.Load() {
					return backoff.Permanent(fmt.Errorf("control plane not reachable"))
				}
				return fmt.Errorf("control plane not reachable")
			}
			u.stat.NewStat("processor_control_plane_down", stats.GaugeType).Gauge(0)
			if statusCode == transformerutils.StatusColdStartWindowFailure {
				if !u.config.perWorkspacePyTEndlessRetries.Load() {
					return backoff.Permanent(fmt.Errorf("cold start error for transformer"))
				}
				return fmt.Errorf("cold start window error for transformer")
			}
			return nil
		},
		retryOptions...,
	)
	// control plane back up

	switch statusCode {
	case http.StatusOK,
		http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusRequestEntityTooLarge,
		transformerutils.StatusMirrorFiltered:
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
	case transformerutils.StatusMirrorFiltered:
		if !u.config.forMirroring {
			panic("received mirror-filtered response (HTTP 297) outside of mirroring mode")
		}
		return nil, true
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
	return transformerResponses, false
}

func (u *Client) doPost(ctx context.Context, rawJSON []byte, url string, labels types.TransformerMetricLabels) ([]byte, int, error) {
	var (
		retryCount int
		resp       *http.Response
		respData   []byte
	)
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = u.config.maxRetryBackoffInterval.Load()

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

			resp, reqErr = u.client.Do(req)
			defer func() { httputil.CloseResponse(resp) }()
			// Record metrics with labels
			tags := labels.ToStatsTag()
			duration := time.Since(requestStartTime)
			u.stat.NewTaggedStat("transformer_client_request_total_bytes", stats.CountType, tags).Count(len(rawJSON))
			u.stat.NewTaggedStat("transformer_client_total_durations_seconds", stats.CountType, tags).Count(int(duration.Seconds()))

			// This metric is to track cold start errors for PyT, which are expected to be higher than usual due to the nature of PyT scaling.
			if u.shouldThrowPythonColdStartErr(labels, reqErr, resp) {
				u.stat.NewTaggedStat(
					coldStartErrorsMetric,
					stats.CountType,
					stats.Tags{
						"workspaceID": labels.WorkspaceID,
						"language":    languagePython,
					},
				).Increment()
				u.log.Warnn("cold start error when connecting to workspace transformer", labels.ToLoggerFields()...)
				return transformerutils.ErrColdStart
			}

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
			u.stat.NewStat("processor_user_transformer_http_retries", stats.CountType).Increment()
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
		// Per-workspace PyT: a persistent transport failure on the workspace-
		// scoped URL is most likely a cold-start window (pod not yet scaled by
		// HPA, EndpointSlice lag, kube-proxy 5xx). Surface a dedicated status
		// code so sendBatch retries the whole call until the pod is up instead of treating it as a failed transformation.
		if errors.Is(err, transformerutils.ErrColdStart) {
			return fmt.Appendf(nil, "workspace transformer not reachable: %s", err), transformerutils.StatusColdStartWindowFailure, nil
		}
		if u.config.failOnUserTransformTimeout.Load() && os.IsTimeout(err) {
			return fmt.Appendf(nil, "transformer request timed out: %s", err), transformerutils.TransformerRequestTimeout, nil
		} else if u.config.failOnError.Load() {
			return fmt.Appendf(nil, "transformer request failed: %s", err), transformerutils.TransformerRequestFailure, nil
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

// Test forwards a control-plane "test" request to the workspace's pyt transformer
// (POST /test) and returns its HTTP status and body unchanged.
//
// NOTE: the four Test/ TestRun/ TestLibrary/ ExtractLibs methods exist only for the Python
// transformation-test flow, which routes through the per-workspace pyt
// deployment. JavaScript tests still run on rudder-transformer and never reach
// here; if JS is ever moved onto this path it will need separate handling for the test endpoints.
// we will have to identify from the request which language is being tested and route accordingly.
func (u *Client) Test(ctx context.Context, workspaceID string, payload []byte) (int, []byte, error) {
	return u.forwardTest(ctx, workspaceID, "/test", payload)
}

// TestRun forwards a control-plane "testRun" request to the workspace's pyt
// transformer (POST /testRun) and returns its HTTP status and body unchanged.
func (u *Client) TestRun(ctx context.Context, workspaceID string, payload []byte) (int, []byte, error) {
	return u.forwardTest(ctx, workspaceID, "/testRun", payload)
}

// TestLibrary forwards a control-plane "testLibrary" request to the workspace's
// pyt transformer (POST /test-library) and returns its HTTP status and body unchanged.
func (u *Client) TestLibrary(ctx context.Context, workspaceID string, payload []byte) (int, []byte, error) {
	return u.forwardTest(ctx, workspaceID, "/test-library", payload)
}

// ExtractLibs forwards a control-plane "extractLibs" request to the workspace's
// pyt transformer (POST /extract-libs) and returns its HTTP status and body unchanged.
func (u *Client) ExtractLibs(ctx context.Context, workspaceID string, payload []byte) (int, []byte, error) {
	return u.forwardTest(ctx, workspaceID, "/extract-libs", payload)
}

// forwardTest is the shared machinery behind the per-endpoint test methods: it
// POSTs payload to the workspace's per-workspace pyt transformer at path and
// returns the pyt HTTP status code and response body unchanged.
//
// It is the cp-router test path's counterpart to the event-processing path
// ([Client.doPost]): both target the pyt-{workspaceID} deployment and share the
// client's maxRetry/maxRetryBackoffInterval settings, but this call is time-boxed
// by ctx (cp-router's ~60s test deadline). Callers that need a quicker cold-start
// recovery build the client with [WithMaxRetryBackoffInterval]/[WithMaxRetry] to
// shrink the backoff and size the retry budget. Like [Client.doPost], it retries
// any transport error plus cold-start 502/503 responses, counting the cold-start
// signals; any other response is returned as-is for the caller to pass through.
func (u *Client) forwardTest(ctx context.Context, workspaceID, path string, payload []byte) (int, []byte, error) {
	// perWorkspacePyTEnabled is reloadable, so check it per call: a request
	// arriving while the feature is off must be rejected rather than silently
	// forwarded to the shared transformer URL the base-URL fallback would yield.
	if !u.config.perWorkspacePyTEnabled.Load() || u.config.forMirroring {
		return 0, nil, ErrPerWorkspacePyTNotEnabled
	}
	url := u.userTransformBaseURL("python", "", workspaceID) + path

	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = u.config.maxRetryBackoffInterval.Load()
	// MaxInterval only caps growth: the first wait would still be the default
	// InitialInterval (500ms), overshooting a smaller configured cap.
	bo.InitialInterval = min(bo.InitialInterval, bo.MaxInterval)

	var (
		statusCode int
		body       []byte
	)
	err := backoffvoid.Retry(
		ctx,
		func() error {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
			if err != nil {
				return backoff.Permanent(err)
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := u.client.Do(req)
			if err != nil {
				// No response at all — retry. Transient network
				// conditions dominate here, and the loop is bounded by maxRetry
				// and the caller's deadline either way. Cold-start signals are
				// additionally counted; retryability doesn't depend on them.
				if isColdStartError(err, nil) {
					u.stat.NewTaggedStat(coldStartErrorsMetric, stats.CountType,
						stats.Tags{"workspaceID": workspaceID, "language": languagePython}).Increment()
				}
				return err
			}
			if isColdStartError(nil, resp) {
				httputil.CloseResponse(resp)
				u.stat.NewTaggedStat(coldStartErrorsMetric, stats.CountType,
					stats.Tags{"workspaceID": workspaceID, "language": languagePython}).Increment()
				return fmt.Errorf("pyt cold start: status %d", resp.StatusCode)
			}
			defer func() { httputil.CloseResponse(resp) }()

			body, err = io.ReadAll(resp.Body)
			if err != nil {
				// Connection broke mid-response — as transient as a failed
				// request; retry.
				return err
			}
			statusCode = resp.StatusCode
			return nil
		},
		backoff.WithBackOff(bo),
		backoff.WithMaxTries(uint(u.config.maxRetry.Load()+1)), //nolint:gosec // maxRetry is a small, bounded value
		// Also bounded by ctx (the caller's ~60s deadline), whichever comes first.
	)
	if err != nil {
		return 0, nil, err
	}
	return statusCode, body, nil
}

// isPerWorkspacePyTPath returns true when the request is targeting the
// per-workspace PyT URL. Single source of truth shared by URL resolution and
// the cold-start error / counter path so the two can't drift.
func (u *Client) isPerWorkspacePyTPath(language, workspaceID string) bool {
	return u.config.perWorkspacePyTEnabled.Load() &&
		!u.config.forMirroring &&
		isPythonTransformation(language) &&
		workspaceID != ""
}

func (u *Client) shouldThrowPythonColdStartErr(labels types.TransformerMetricLabels, err error, resp *http.Response) bool {
	return u.isPerWorkspacePyTPath(labels.Language, labels.WorkspaceID) && isColdStartError(err, resp)
}

// isColdStartError returns true for transient errors that mean the target
// PyT deployment isn't ready yet — zero endpoints, pod not yet ready, or
// kube-proxy returning a no-endpoints status.
func isColdStartError(err error, resp *http.Response) bool {
	if err != nil {
		// ECONNREFUSED: Service has no endpoints (Deployment at 0 replicas).
		// EHOSTUNREACH ("no route to host"): stale iptables / EndpointSlice
		// after a pod replacement or scale-down — same transient signal.
		if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EHOSTUNREACH) {
			return true
		}
		// Dial timeout ("dial tcp ...: i/o timeout"): the SYN went unanswered —
		// the pod behind the headless-service DNS record is either still coming
		// up (IP published before the VM boots) or on its way down (terminating
		// pod whose record hasn't been withdrawn yet). The two are
		// indistinguishable from the dialer, and retrying is right for both:
		// each retry re-resolves DNS, and the pod's replacement (the forward path
		// scales the deployment up before dialing) answers a later attempt.
		var opErr *net.OpError
		if errors.As(err, &opErr) && opErr.Op == "dial" && opErr.Timeout() {
			return true
		}
		var dnsErr *net.DNSError
		return errors.As(err, &dnsErr)
	}
	if resp != nil && (resp.StatusCode == http.StatusServiceUnavailable ||
		resp.StatusCode == http.StatusBadGateway) {
		return true
	}
	return false
}

func isPythonTransformation(language string) bool {
	return strings.HasPrefix(language, "python")
}

func (u *Client) userTransformURL(language, versionID, workspaceID string) string {
	return u.userTransformBaseURL(language, versionID, workspaceID) + "/customTransform"
}

func (u *Client) userTransformBaseURL(language, versionID, workspaceID string) string {
	if !isPythonTransformation(language) {
		return u.config.userTransformationURL
	}
	// Per-workspace PyT: a global version allowlist doesn't apply — each
	// workspace runs its own pod with its own version.
	if u.config.perWorkspacePyTEnabled.Load() && !u.config.forMirroring {
		if workspaceID == "" {
			// Panic so the bug surfaces immediately as this should not happen
			panic("per-workspace PyT enabled but workspaceID is empty")
		}
		base := strings.ReplaceAll(u.config.perWorkspacePyTURLTemplate, "{workspaceID}", strings.ToLower(workspaceID))
		return base
	}
	// Legacy shared-PyT path: the version allowlist is a rollout gate for the shared service.
	if u.config.pythonTransformationURL != "" && u.config.pythonTransformConfig.IsVersionAllowed(versionID) {
		return u.config.pythonTransformationURL
	}
	return u.config.userTransformationURL
}
