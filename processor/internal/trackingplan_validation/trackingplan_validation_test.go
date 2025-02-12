package trackingplan_validation

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer_utils"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	reportingTypes "github.com/rudderlabs/rudder-server/utils/types"
)

type fakeTransformer struct {
	requests [][]types.TransformerEvent
	t        testing.TB
}

func (t *fakeTransformer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var reqBody []types.TransformerEvent
	require.NoError(t.t, json.NewDecoder(r.Body).Decode(&reqBody))

	t.requests = append(t.requests, reqBody)

	responses := make([]types.TransformerResponse, len(reqBody))

	for i := range reqBody {
		statusCode := int(reqBody[i].Message["forceStatusCode"].(float64))
		delete(reqBody[i].Message, "forceStatusCode")
		reqBody[i].Message["echo-key-1"] = reqBody[i].Message["src-key-1"]

		responses[i] = types.TransformerResponse{
			Output:     reqBody[i].Message,
			Metadata:   reqBody[i].Metadata,
			StatusCode: statusCode,
		}
		if statusCode >= http.StatusBadRequest {
			responses[i].Error = "error"
		}
	}

	w.Header().Set("apiVersion", strconv.Itoa(reportingTypes.SupportedTransformerApiVersion))

	require.NoError(t.t, json.NewEncoder(w).Encode(responses))
}

type endlessLoopTransformer struct {
	retryCount    int
	maxRetryCount int

	skipApiVersion bool
	apiVersion     int

	statusCode  int
	statusError string

	t testing.TB
}

func (elt *endlessLoopTransformer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	elt.retryCount++

	var reqBody []types.TransformerEvent
	require.NoError(elt.t, json.NewDecoder(r.Body).Decode(&reqBody))

	responses := make([]types.TransformerResponse, len(reqBody))

	if elt.retryCount < elt.maxRetryCount {
		http.Error(w, response.MakeResponse(elt.statusError), elt.statusCode)
		return
	}

	for i := range reqBody {
		responses[i] = types.TransformerResponse{
			Output:     reqBody[i].Message,
			Metadata:   reqBody[i].Metadata,
			StatusCode: http.StatusOK,
			Error:      "",
		}
	}

	if !elt.skipApiVersion {
		w.Header().Set("apiVersion", strconv.Itoa(elt.apiVersion))
	}

	require.NoError(elt.t, json.NewEncoder(w).Encode(responses))
}

type endpointTransformer struct {
	t              testing.TB
	supportedPaths []string
}

func (et *endpointTransformer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	if !slices.Contains(et.supportedPaths, path) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	var reqBody []types.TransformerEvent
	require.NoError(et.t, json.NewDecoder(r.Body).Decode(&reqBody))

	responses := make([]types.TransformerResponse, len(reqBody))

	for i := range reqBody {
		responses[i] = types.TransformerResponse{
			Output:     reqBody[i].Message,
			Metadata:   reqBody[i].Metadata,
			StatusCode: http.StatusOK,
			Error:      "",
		}
	}

	w.Header().Set("apiVersion", strconv.Itoa(reportingTypes.SupportedTransformerApiVersion))

	require.NoError(et.t, json.NewEncoder(w).Encode(responses))
}

func TestTrackingPlanValidator(t *testing.T) {
	clientTypes := []string{"stdlib", "recycled", "httplb"}
	for _, clientType := range clientTypes {
		t.Run(fmt.Sprintf("with %s client", clientType), func(t *testing.T) {
			conf := config.New()
			conf.Set("Transformer.Client.type", clientType)

			t.Run("success", func(t *testing.T) {
				ft := &fakeTransformer{
					t: t,
				}

				srv := httptest.NewServer(ft)
				defer srv.Close()

				tc := []struct {
					batchSize   int
					eventsCount int
					failEvery   int
				}{
					{batchSize: 0, eventsCount: 0},
					{batchSize: 10, eventsCount: 100},
					{batchSize: 10, eventsCount: 9},
					{batchSize: 10, eventsCount: 91},
					{batchSize: 10, eventsCount: 99},
					{batchSize: 10, eventsCount: 1},
					{batchSize: 10, eventsCount: 80, failEvery: 4},
					{batchSize: 10, eventsCount: 80, failEvery: 1},
				}

				for _, tt := range tc {
					statsStore, err := memstats.New()
					require.NoError(t, err)

					tr := TPValidator{}
					tr.stat = statsStore
					tr.log = logger.NOP
					tr.conf = conf
					tr.client = srv.Client()
					tr.guardConcurrency = make(chan struct{}, 200)
					tr.config.timeoutDuration = 1 * time.Second
					tr.config.failOnError = config.SingleValueLoader(true)
					tr.config.maxRetryBackoffInterval = config.SingleValueLoader(1 * time.Second)
					tr.config.maxRetry = config.SingleValueLoader(1)

					batchSize := tt.batchSize
					eventsCount := tt.eventsCount
					failEvery := tt.failEvery

					events := make([]types.TransformerEvent, eventsCount)
					expectedResponse := types.Response{}

					transformationID := rand.String(10)

					destinationConfig := backendconfigtest.NewDestinationBuilder("WEBHOOK").
						WithUserTransformation(transformationID, rand.String(10)).Build()

					Metadata := types.Metadata{
						DestinationType:  destinationConfig.DestinationDefinition.Name,
						SourceID:         rand.String(10),
						DestinationID:    destinationConfig.ID,
						TransformationID: destinationConfig.Transformations[0].ID,
					}

					for i := range events {
						msgID := fmt.Sprintf("messageID-%d", i)
						statusCode := http.StatusOK

						if failEvery != 0 && i%failEvery == 0 {
							statusCode = http.StatusBadRequest
						}

						Metadata := Metadata
						Metadata.MessageID = msgID

						events[i] = types.TransformerEvent{
							Metadata: Metadata,
							Message: map[string]interface{}{
								"src-key-1":       msgID,
								"forceStatusCode": statusCode,
							},
							Destination: destinationConfig,
							Credentials: []types.Credential{
								{
									ID:       "test-credential",
									Key:      "test-key",
									Value:    "test-value",
									IsSecret: false,
								},
							},
						}

						tResp := types.TransformerResponse{
							Metadata:   Metadata,
							StatusCode: statusCode,
							Output: map[string]interface{}{
								"src-key-1":  msgID,
								"echo-key-1": msgID,
							},
						}

						if statusCode < http.StatusBadRequest {
							expectedResponse.Events = append(expectedResponse.Events, tResp)
						} else {
							tResp.Error = "error"
							expectedResponse.FailedEvents = append(expectedResponse.FailedEvents, tResp)
						}
					}

					rsp := tr.transform(context.TODO(), events, srv.URL, batchSize)
					require.Equal(t, expectedResponse, rsp)

					metrics := statsStore.GetByName("processor.transformer_request_time")
					if tt.eventsCount > 0 {
						require.NotEmpty(t, metrics)
						for _, m := range metrics {
							require.Equal(t, stats.Tags{
								"stage":            "trackingPlan_validation",
								"sourceId":         Metadata.SourceID,
								"destinationType":  destinationConfig.DestinationDefinition.Name,
								"destinationId":    destinationConfig.ID,
								"transformationId": destinationConfig.Transformations[0].ID,
							}, m.Tags)
						}
					}
				}
			})
			t.Run("endless retries in case of control plane down", func(t *testing.T) {
				msgID := "messageID-0"
				events := append([]types.TransformerEvent{}, types.TransformerEvent{
					Metadata: types.Metadata{
						MessageID: msgID,
					},
					Message: map[string]interface{}{
						"src-key-1": msgID,
					},
					Credentials: []types.Credential{
						{
							ID:       "test-credential",
							Key:      "test-key",
							Value:    "test-value",
							IsSecret: false,
						},
					},
				})

				elt := &endlessLoopTransformer{
					maxRetryCount: 3,
					statusCode:    transformer_utils.StatusCPDown,
					statusError:   "control plane not reachable",
					apiVersion:    reportingTypes.SupportedTransformerApiVersion,
					t:             t,
				}

				srv := httptest.NewServer(elt)
				defer srv.Close()

				tr := TPValidator{}
				tr.stat = stats.Default
				tr.log = logger.NOP
				tr.conf = conf
				tr.client = srv.Client()
				tr.config.maxRetry = config.SingleValueLoader(1)
				tr.config.maxRetryBackoffInterval = config.SingleValueLoader(1 * time.Second)
				tr.config.timeoutDuration = 1 * time.Second

				rsp := tr.request(context.TODO(), srv.URL, events)
				require.Equal(t, rsp, []types.TransformerResponse{
					{
						Metadata: types.Metadata{
							MessageID: msgID,
						},
						StatusCode: http.StatusOK,
						Output: map[string]interface{}{
							"src-key-1": msgID,
						},
					},
				})
				require.Equal(t, elt.retryCount, 3)
			})

			t.Run("retries", func(t *testing.T) {
				msgID := "messageID-0"
				events := append([]types.TransformerEvent{}, types.TransformerEvent{
					Metadata: types.Metadata{
						MessageID: msgID,
					},
					Message: map[string]interface{}{
						"src-key-1": msgID,
					},
					Destination: backendconfig.DestinationT{
						Transformations: []backendconfig.TransformationT{
							{
								ID:        "test-transformation",
								VersionID: "test-version",
							},
						},
					},
					Credentials: []types.Credential{
						{
							ID:       "test-credential",
							Key:      "test-key",
							Value:    "test-value",
							IsSecret: false,
						},
					},
				})

				testCases := []struct {
					name             string
					retries          int
					maxRetryCount    int
					statusCode       int
					statusError      string
					expectedRetries  int
					expectPanic      bool
					expectedResponse []types.TransformerResponse
					failOnError      bool
				}{
					{
						name:            "too many requests",
						retries:         3,
						maxRetryCount:   10,
						statusCode:      http.StatusTooManyRequests,
						statusError:     "too many requests",
						expectedRetries: 4,
						expectPanic:     true,
						failOnError:     false,
					},
					{
						name:            "too many requests with fail on error",
						retries:         3,
						maxRetryCount:   10,
						statusCode:      http.StatusTooManyRequests,
						statusError:     "too many requests",
						expectedRetries: 4,
						expectPanic:     false,
						expectedResponse: []types.TransformerResponse{
							{
								Metadata: types.Metadata{
									MessageID: msgID,
								},
								StatusCode: transformer_utils.TransformerRequestFailure,
								Error:      "transformer request failed: transformer returned status code: 429",
							},
						},
						failOnError: true,
					},
					{
						name:            "transient control plane error",
						retries:         30,
						maxRetryCount:   3,
						statusCode:      transformer_utils.StatusCPDown,
						statusError:     "control plane not reachable",
						expectedRetries: 3,
						expectPanic:     false,
						expectedResponse: []types.TransformerResponse{
							{
								Metadata: types.Metadata{
									MessageID: msgID,
								},
								StatusCode: http.StatusOK,
								Output: map[string]interface{}{
									"src-key-1": msgID,
								},
							},
						},
						failOnError: false,
					},
				}

				for _, tc := range testCases {
					tc := tc

					t.Run(tc.name, func(t *testing.T) {
						elt := &endlessLoopTransformer{
							maxRetryCount: tc.maxRetryCount,
							statusCode:    tc.statusCode,
							statusError:   tc.statusError,
							apiVersion:    reportingTypes.SupportedTransformerApiVersion,
							t:             t,
						}

						srv := httptest.NewServer(elt)
						defer srv.Close()

						tr := TPValidator{}
						tr.stat = stats.Default
						tr.log = logger.NOP
						tr.conf = conf
						tr.client = srv.Client()
						tr.config.maxRetry = config.SingleValueLoader(tc.retries)
						tr.config.failOnError = config.SingleValueLoader(tc.failOnError)
						tr.config.timeoutDuration = 1 * time.Second
						tr.config.maxRetryBackoffInterval = config.SingleValueLoader(1 * time.Second)

						if tc.expectPanic {
							require.Panics(t, func() {
								_ = tr.request(context.TODO(), srv.URL, events)
							})
							require.Equal(t, elt.retryCount, tc.expectedRetries)
							return
						}

						rsp := tr.request(context.TODO(), srv.URL, events)
						require.Equal(t, tc.expectedResponse, rsp)
						require.Equal(t, tc.expectedRetries, elt.retryCount)
					})
				}
			})

			t.Run("version compatibility", func(t *testing.T) {
				msgID := "messageID-0"
				events := append([]types.TransformerEvent{}, types.TransformerEvent{
					Metadata: types.Metadata{
						MessageID: msgID,
					},
					Message: map[string]interface{}{
						"src-key-1": msgID,
					},
					Destination: backendconfig.DestinationT{
						Transformations: []backendconfig.TransformationT{
							{
								ID:        "test-transformation",
								VersionID: "test-version",
							},
						},
					},
					Credentials: []types.Credential{
						{
							ID:       "test-credential",
							Key:      "test-key",
							Value:    "test-value",
							IsSecret: false,
						},
					},
				})

				testCases := []struct {
					name             string
					apiVersion       int
					skipApiVersion   bool
					expectPanic      bool
					expectedResponse []types.TransformerResponse
				}{
					{
						name:        "compatible api version",
						apiVersion:  reportingTypes.SupportedTransformerApiVersion,
						expectPanic: false,
						expectedResponse: []types.TransformerResponse{
							{
								Metadata: types.Metadata{
									MessageID: msgID,
								},
								StatusCode: http.StatusOK,
								Output: map[string]interface{}{
									"src-key-1": msgID,
								},
							},
						},
					},
					{
						name:        "incompatible api version",
						apiVersion:  1,
						expectPanic: true,
					},
					{
						name:           "unexpected api version",
						skipApiVersion: true,
						expectPanic:    true,
					},
				}

				for _, tc := range testCases {
					tc := tc

					t.Run(tc.name, func(t *testing.T) {
						elt := &endlessLoopTransformer{
							maxRetryCount:  0,
							skipApiVersion: tc.skipApiVersion,
							apiVersion:     tc.apiVersion,
							t:              t,
						}

						srv := httptest.NewServer(elt)
						defer srv.Close()

						tr := TPValidator{}
						tr.client = srv.Client()
						tr.stat = stats.Default
						tr.conf = conf
						tr.log = logger.NOP
						tr.config.maxRetry = config.SingleValueLoader(1)
						tr.config.timeoutDuration = 1 * time.Second
						tr.config.maxRetryBackoffInterval = config.SingleValueLoader(1 * time.Second)

						if tc.expectPanic {
							require.Panics(t, func() {
								_ = tr.request(context.TODO(), srv.URL, events)
							})
							return
						}

						rsp := tr.request(context.TODO(), srv.URL, events)
						require.Equal(t, tc.expectedResponse, rsp)
					})
				}
			})

			t.Run("endpoints", func(t *testing.T) {
				msgID := "messageID-0"
				expectedResponse := types.Response{
					Events: []types.TransformerResponse{
						{
							Output: map[string]interface{}{
								"src-key-1": msgID,
							},
							Metadata: types.Metadata{
								MessageID: msgID,
							},
							StatusCode: http.StatusOK,
						},
					},
				}
				events := append([]types.TransformerEvent{}, types.TransformerEvent{
					Metadata: types.Metadata{
						MessageID: msgID,
					},
					Message: map[string]interface{}{
						"src-key-1": msgID,
					},
					Destination: backendconfig.DestinationT{
						DestinationDefinition: backendconfig.DestinationDefinitionT{
							Name: "test-destination",
						},
						Transformations: []backendconfig.TransformationT{
							{
								ID:        "test-transformation",
								VersionID: "test-version",
							},
						},
					},
					Credentials: []types.Credential{
						{
							ID:       "test-credential",
							Key:      "test-key",
							Value:    "test-value",
							IsSecret: false,
						},
					},
				})

				t.Run("Tracking Plan Validations", func(t *testing.T) {
					et := &endpointTransformer{
						supportedPaths: []string{"/v0/validate"},
						t:              t,
					}

					srv := httptest.NewServer(et)
					defer srv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("DEST_TRANSFORM_URL", srv.URL)

					tr := NewTPValidator(c, logger.NOP, stats.Default, WithClient(srv.Client()))
					url, err := url.JoinPath(srv.URL, "/v0/validate")
					require.NoError(t, err)
					rsp := tr.transform(context.TODO(), events, url, 10)
					require.Equal(t, rsp, expectedResponse)
				})
			})
		})
	}
}

func TestLongRunningTransformation(t *testing.T) {
	fileName := t.TempDir() + "out.log"
	f, err := os.Create(fileName)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	ctrl := gomock.NewController(t)

	t.Run("context cancels before timeout", func(t *testing.T) {
		mockLogger := mock_logger.NewMockLogger(ctrl)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		transformer_utils.TrackLongRunningTransformation(ctx, "stage", time.Hour, mockLogger)
	})

	t.Run("log stmt", func(t *testing.T) {
		mockLogger := mock_logger.NewMockLogger(ctrl)
		var fired atomic.Bool
		mockLogger.EXPECT().Errorw(gomock.Any(), gomock.Any()).Do(func(msg string, keysAndValues ...interface{}) {
			require.Equal(t, "Long running transformation detected", msg)
			require.Len(t, keysAndValues, 4)
			require.Equal(t, "stage", keysAndValues[0])
			require.Equal(t, "stage", keysAndValues[1])
			require.Equal(t, "duration", keysAndValues[2])
			_, err := time.ParseDuration(keysAndValues[3].(string))
			require.NoError(t, err)
			fired.Store(true)
		}).MinTimes(1)
		ctx, cancel := context.WithCancel(context.Background())
		go transformer_utils.TrackLongRunningTransformation(ctx, "stage", time.Millisecond, mockLogger)
		for !fired.Load() {
			time.Sleep(time.Millisecond)
		}
		cancel()
	})
}
