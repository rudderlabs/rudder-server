package user_transformer_test

import (
	"context"
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

	"github.com/rudderlabs/rudder-server/jsonrs"

	"github.com/stretchr/testify/assert"
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
	transformerutils "github.com/rudderlabs/rudder-server/processor/internal/transformer"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/user_transformer"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

type fakeTransformer struct {
	requests [][]types.TransformerEvent
	t        testing.TB
}

func (t *fakeTransformer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var reqBody []types.TransformerEvent
	require.NoError(t.t, jsonrs.NewDecoder(r.Body).Decode(&reqBody))

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

	w.Header().Set("apiVersion", strconv.Itoa(reportingtypes.SupportedTransformerApiVersion))

	require.NoError(t.t, jsonrs.NewEncoder(w).Encode(responses))
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
	require.NoError(elt.t, jsonrs.NewDecoder(r.Body).Decode(&reqBody))

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

	require.NoError(elt.t, jsonrs.NewEncoder(w).Encode(responses))
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
	require.NoError(et.t, jsonrs.NewDecoder(r.Body).Decode(&reqBody))

	responses := make([]types.TransformerResponse, len(reqBody))

	for i := range reqBody {
		responses[i] = types.TransformerResponse{
			Output:     reqBody[i].Message,
			Metadata:   reqBody[i].Metadata,
			StatusCode: http.StatusOK,
			Error:      "",
		}
	}

	w.Header().Set("apiVersion", strconv.Itoa(reportingtypes.SupportedTransformerApiVersion))

	require.NoError(et.t, jsonrs.NewEncoder(w).Encode(responses))
}

func TestUserTransformer(t *testing.T) {
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
					conf.Set("Processor.maxConcurrency", 200)
					conf.Set("Processor.Transformer.failOnUserTransformTimeout", true)
					conf.Set("Processor.Transformer.failOnError", true)
					conf.Set("Processor.Transformer.maxRetryBackoffInterval", 1*time.Second)
					conf.Set("Processor.Transformer.maxRetry", 1)
					conf.Set("Processor.Transformer.timeoutDuration", 1*time.Second)
					conf.Set("USER_TRANSFORM_URL", srv.URL)
					conf.Set("Processor.userTransformBatchSize", tt.batchSize)
					tr := user_transformer.New(conf, logger.NOP, statsStore, user_transformer.WithClient(srv.Client()))
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

					labels := types.TransformerMetricLabels{
						Endpoint:         transformerutils.GetEndpointFromURL(srv.URL),
						Stage:            "user_transformer",
						SourceID:         Metadata.SourceID,
						SourceType:       Metadata.SourceType,
						DestinationType:  destinationConfig.DestinationDefinition.Name,
						DestinationID:    destinationConfig.ID,
						WorkspaceID:      Metadata.WorkspaceID,
						TransformationID: destinationConfig.Transformations[0].ID,
					}
					rsp := tr.Transform(context.TODO(), events)
					require.Equal(t, expectedResponse, rsp)

					metrics := statsStore.GetByName("processor.transformer_request_time")
					if tt.eventsCount > 0 {
						require.NotEmpty(t, metrics)
						for _, m := range metrics {
							require.Equal(t, stats.Tags{
								"endpoint":         transformerutils.GetEndpointFromURL(srv.URL),
								"stage":            "user_transformer",
								"sourceId":         Metadata.SourceID,
								"sourceType":       Metadata.SourceType,
								"destinationType":  destinationConfig.DestinationDefinition.Name,
								"destinationId":    destinationConfig.ID,
								"transformationId": destinationConfig.Transformations[0].ID,
								"workspaceId":      Metadata.WorkspaceID,
								"language":         "",

								// Legacy tags: to be removed
								"dest_type": destinationConfig.DestinationDefinition.Name,
								"dest_id":   destinationConfig.ID,
								"src_id":    Metadata.SourceID,
							}, m.Tags)
						}
						metricsToCheck := []string{
							"transformer_client_request_total_bytes",
							"transformer_client_response_total_bytes",
							"transformer_client_request_total_events",
							"transformer_client_response_total_events",
							"transformer_client_total_durations_seconds",
						}

						expectedTags := labels.ToStatsTag()
						for _, metricName := range metricsToCheck {
							measurements := statsStore.GetByName(metricName)
							require.NotEmpty(t, measurements, "metric %s should not be empty", metricName)
							require.Equal(t, expectedTags, measurements[0].Tags, "metric %s tags mismatch", metricName)
						}
					}
				}
			})

			t.Run("timeout", func(t *testing.T) {
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

				testCases := []struct {
					name                       string
					retries                    int
					expectedRetries            int
					expectPanic                bool
					stage                      string
					expectedResponse           []types.TransformerResponse
					failOnUserTransformTimeout bool
				}{
					{
						name:        "user transformation timeout with fail on timeout",
						retries:     3,
						expectPanic: false,
						expectedResponse: []types.TransformerResponse{
							{
								Metadata: types.Metadata{
									MessageID: msgID,
								},
								StatusCode: transformerutils.TransformerRequestTimeout,
								Output: map[string]interface{}{
									"src-key-1": msgID,
								},
							},
						},
						failOnUserTransformTimeout: true,
					},
				}

				for _, tc := range testCases {
					tc := tc

					t.Run(tc.name, func(t *testing.T) {
						conf := config.New()
						ch := make(chan struct{})
						srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							<-ch
						}))
						defer srv.Close()

						client := srv.Client()
						client.Timeout = 1 * time.Millisecond
						conf.Set("USER_TRANSFORM_URL", srv.URL)
						conf.Set("Processor.maxRetry", tc.retries)
						conf.Set("Processor.Transformer.failOnUserTransformTimeout", tc.failOnUserTransformTimeout)
						conf.Set("Processor.maxRetryBackoffInterval", 1*time.Second)
						tr := user_transformer.New(conf, logger.NOP, stats.NOP, user_transformer.WithClient(client))
						_, err := url.Parse(srv.URL)
						require.NoError(t, err)

						rsp := tr.Transform(context.TODO(), events)
						require.Len(t, rsp.FailedEvents, 1)
						require.Equal(t, rsp.FailedEvents[0].StatusCode, transformerutils.TransformerRequestTimeout)
						require.Equal(t, rsp.FailedEvents[0].Metadata, types.Metadata{
							MessageID: msgID,
						})
						require.Contains(t, rsp.FailedEvents[0].Error, "transformer request timed out:")
						close(ch)
					})
				}
			})

			t.Run("endless retries in case of control plane down", func(t *testing.T) {
				conf := config.New()
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
					statusCode:    transformerutils.StatusCPDown,
					statusError:   "control plane not reachable",
					apiVersion:    reportingtypes.SupportedTransformerApiVersion,
					t:             t,
				}

				srv := httptest.NewServer(elt)
				defer srv.Close()

				conf.Set("USER_TRANSFORM_URL", srv.URL)
				conf.Set("Processor.maxRetry", 1)
				conf.Set("Processor.maxRetryBackoffInterval", 1*time.Second)
				conf.Set("Processor.timeoutDuration", 1*time.Second)
				conf.Set("Processor.Transformer.failOnUserTransformTimeout", false)
				tr := user_transformer.New(conf, logger.NOP, stats.Default, user_transformer.WithClient(srv.Client()))

				rsp := tr.Transform(context.TODO(), events)

				require.Equal(t, rsp, types.Response{
					FailedEvents: nil,
					Events: []types.TransformerResponse{
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
					expectedResponse types.Response
					failOnError      bool
				}{
					{
						name:            "too many requests with fail on error",
						retries:         3,
						maxRetryCount:   10,
						statusCode:      http.StatusTooManyRequests,
						statusError:     "too many requests",
						expectedRetries: 4,
						expectPanic:     false,
						expectedResponse: types.Response{
							Events: nil,
							FailedEvents: []types.TransformerResponse{
								{
									Metadata: types.Metadata{
										MessageID: msgID,
									},
									StatusCode: transformerutils.TransformerRequestFailure,
									Error:      "transformer request failed: transformer returned status code: 429",
								},
							},
						},
						failOnError: true,
					},
					{
						name:            "transient control plane error",
						retries:         30,
						maxRetryCount:   3,
						statusCode:      transformerutils.StatusCPDown,
						statusError:     "control plane not reachable",
						expectedRetries: 3,
						expectPanic:     false,
						expectedResponse: types.Response{
							FailedEvents: nil,
							Events: []types.TransformerResponse{
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
						failOnError: false,
					},
				}

				for _, tc := range testCases {
					tc := tc
					conf := config.New()
					t.Run(tc.name, func(t *testing.T) {
						elt := &endlessLoopTransformer{
							maxRetryCount: tc.maxRetryCount,
							statusCode:    tc.statusCode,
							statusError:   tc.statusError,
							apiVersion:    reportingtypes.SupportedTransformerApiVersion,
							t:             t,
						}

						srv := httptest.NewServer(elt)
						defer srv.Close()
						conf.Set("USER_TRANSFORM_URL", srv.URL)
						conf.Set("Processor.maxRetry", tc.retries)
						conf.Set("Processor.maxRetryBackoffInterval", 1*time.Second)
						conf.Set("Processor.timeoutDuration", 1*time.Second)
						conf.Set("Processor.Transformer.failOnUserTransformTimeout", false)
						conf.Set("Processor.Transformer.failOnError", tc.failOnError)
						tr := user_transformer.New(conf, logger.NOP, stats.Default, user_transformer.WithClient(srv.Client()))
						rsp := tr.Transform(context.TODO(), events)
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
					expectedResponse types.Response
				}{
					{
						name:        "compatible api version",
						apiVersion:  reportingtypes.SupportedTransformerApiVersion,
						expectPanic: false,
						expectedResponse: types.Response{
							Events: []types.TransformerResponse{
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
							FailedEvents: nil,
						},
					},
				}

				for _, tc := range testCases {
					tc := tc
					conf := config.New()
					t.Run(tc.name, func(t *testing.T) {
						elt := &endlessLoopTransformer{
							maxRetryCount:  0,
							skipApiVersion: tc.skipApiVersion,
							apiVersion:     tc.apiVersion,
							t:              t,
						}

						srv := httptest.NewServer(elt)
						defer srv.Close()
						conf.Set("USER_TRANSFORM_URL", srv.URL)
						conf.Set("Processor.maxRetry", 1)
						conf.Set("Processor.maxRetryBackoffInterval", 1*time.Second)
						conf.Set("Processor.timeoutDuration", 1*time.Second)
						tr := user_transformer.New(conf, logger.NOP, stats.Default, user_transformer.WithClient(srv.Client()))
						rsp := tr.Transform(context.TODO(), events)
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

				t.Run("User transformations", func(t *testing.T) {
					et := &endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					}

					srv := httptest.NewServer(et)
					defer srv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_URL", srv.URL)

					tr := user_transformer.New(c, logger.NOP, stats.Default, user_transformer.WithClient(srv.Client()))
					rsp := tr.Transform(context.TODO(), events)
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
		transformerutils.TrackLongRunningTransformation(ctx, "stage", time.Hour, mockLogger)
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
		go transformerutils.TrackLongRunningTransformation(ctx, "stage", time.Millisecond, mockLogger)
		for !fired.Load() {
			time.Sleep(time.Millisecond)
		}
		cancel()
	})
}

func TestTransformerEvent_GetVersionsOnly(t *testing.T) {
	testCases := []struct {
		name     string
		event    *types.TransformerEvent
		expected *types.TransformerEvent
	}{
		{
			name: "remove connections",
			event: &types.TransformerEvent{
				Metadata: types.Metadata{},
				Message:  map[string]interface{}{},
				Connection: backendconfig.Connection{
					SourceID:      "source-id",
					DestinationID: "destination-id",
				},
				Destination: backendconfig.DestinationT{
					Transformations: make([]backendconfig.TransformationT, 0),
				},
			},
			expected: &types.TransformerEvent{
				Metadata:   types.Metadata{},
				Message:    map[string]interface{}{},
				Connection: backendconfig.Connection{},
				Destination: backendconfig.DestinationT{
					Transformations: make([]backendconfig.TransformationT, 0),
				},
			},
		},
		{
			name: "remove everything except transformations in destination",
			event: &types.TransformerEvent{
				Metadata: types.Metadata{},
				Message:  map[string]interface{}{},
				Connection: backendconfig.Connection{
					SourceID:      "source-id",
					DestinationID: "destination-id",
				},
				Destination: backendconfig.DestinationT{
					ID:              "destination-id",
					Transformations: make([]backendconfig.TransformationT, 0),
					Name:            "destination-name",
					Config: map[string]interface{}{
						"key": "value",
						"key2": map[string]interface{}{
							"key": "value",
						},
						"key3": []interface{}{"value"},
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name:          "destination-definition-name",
						Config:        map[string]interface{}{},
						ResponseRules: map[string]interface{}{},
					},
				},
			},
			expected: &types.TransformerEvent{
				Metadata:   types.Metadata{},
				Message:    map[string]interface{}{},
				Connection: backendconfig.Connection{},
				Destination: backendconfig.DestinationT{
					Transformations: make([]backendconfig.TransformationT, 0),
				},
			},
		},
		{
			name: "remove everything except transformations in destination with multiple transformer versions",
			event: &types.TransformerEvent{
				Metadata: types.Metadata{},
				Message:  map[string]interface{}{},
				Connection: backendconfig.Connection{
					SourceID:      "source-id",
					DestinationID: "destination-id",
				},
				Destination: backendconfig.DestinationT{
					ID: "destination-id",
					Transformations: []backendconfig.TransformationT{
						{
							ID:        "transformation-id",
							VersionID: "version-id",
							Config:    map[string]interface{}{},
						},
						{
							ID:        "transformation-id-2",
							VersionID: "version-id-2",
							Config:    map[string]interface{}{},
						},
					},
					Name: "destination-name",
					Config: map[string]interface{}{
						"key": "value",
						"key2": map[string]interface{}{
							"key": "value",
						},
						"key3": []interface{}{"value"},
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name:          "destination-definition-name",
						Config:        map[string]interface{}{},
						ResponseRules: map[string]interface{}{},
					},
				},
			},
			expected: &types.TransformerEvent{
				Metadata:   types.Metadata{},
				Message:    map[string]interface{}{},
				Connection: backendconfig.Connection{},
				Destination: backendconfig.DestinationT{
					Transformations: []backendconfig.TransformationT{
						{
							VersionID: "version-id",
						},
						{
							VersionID: "version-id-2",
						},
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.event.GetVersionsOnly())
		})
	}
}
