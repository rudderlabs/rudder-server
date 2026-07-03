package user_transformer_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
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
	if r.Header.Get("X-Content-Format") == "json+compactedv1" {
		var ctr types.CompactedTransformRequest
		require.NoError(t.t, jsonrs.NewDecoder(r.Body).Decode(&ctr))
		reqBody = ctr.ToTransformerEvents()
	} else {
		require.NoError(t.t, jsonrs.NewDecoder(r.Body).Decode(&reqBody))
	}

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
	if !slices.Contains(et.supportedPaths, r.URL.Path) {
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
							Message: map[string]any{
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
							Output: map[string]any{
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
						Language:         "javascript",
						SourceID:         Metadata.SourceID,
						SourceType:       Metadata.SourceType,
						DestinationType:  destinationConfig.DestinationDefinition.Name,
						DestinationID:    destinationConfig.ID,
						WorkspaceID:      Metadata.WorkspaceID,
						TransformationID: destinationConfig.Transformations[0].ID,
					}
					rsp := tr.Transform(context.TODO(), events)
					require.Equal(t, expectedResponse, rsp)

					metrics := statsStore.GetByName("processor_transformer_request_time")
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
								"language":         "javascript",
								"mirroring":        "false",
								"success":          "true",

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
					Message: map[string]any{
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
								Output: map[string]any{
									"src-key-1": msgID,
								},
							},
						},
						failOnUserTransformTimeout: true,
					},
				}

				for _, tc := range testCases {
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
					Message: map[string]any{
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
							Output: map[string]any{
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
					Message: map[string]any{
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
									Output: map[string]any{
										"src-key-1": msgID,
									},
								},
							},
						},
						failOnError: false,
					},
				}

				for _, tc := range testCases {
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
					Message: map[string]any{
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
									Output: map[string]any{
										"src-key-1": msgID,
									},
								},
							},
							FailedEvents: nil,
						},
					},
				}

				for _, tc := range testCases {
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
							Output: map[string]any{
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
					Message: map[string]any{
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

			t.Run("python transformation routing", func(t *testing.T) {
				msgID := "messageID-0"
				expectedResponse := types.Response{
					Events: []types.TransformerResponse{
						{
							Output: map[string]any{
								"src-key-1": msgID,
							},
							Metadata: types.Metadata{
								MessageID: msgID,
							},
							StatusCode: http.StatusOK,
						},
					},
				}

				makeEvents := func(language string) []types.TransformerEvent {
					return []types.TransformerEvent{{
						Metadata: types.Metadata{
							MessageID: msgID,
						},
						Message: map[string]any{
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
									Language:  language,
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
					}}
				}

				t.Run("python transformation routes to python URL when configured", func(t *testing.T) {
					var jsHits, pythonHits atomic.Int32

					jsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						jsHits.Add(1)
						t.Error("request should not hit JS transformer")
					}))
					defer jsSrv.Close()

					pythonSrv := httptest.NewServer(&endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					})
					defer pythonSrv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_URL", jsSrv.URL)
					c.Set("PYTHON_TRANSFORM_URL", pythonSrv.URL)

					tr := user_transformer.New(c, logger.NOP, stats.Default)
					rsp := tr.Transform(context.TODO(), makeEvents("pythonfaas"))

					require.Equal(t, expectedResponse, rsp)
					require.Equal(t, int32(0), jsHits.Load(), "JS transformer should not be hit")

					pythonHits.Add(1)
					require.Equal(t, int32(1), pythonHits.Load(), "Python transformer should be hit")
				})

				t.Run("pythonwithlibs transformation routes to python URL", func(t *testing.T) {
					jsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						t.Error("request should not hit JS transformer")
					}))
					defer jsSrv.Close()

					pythonSrv := httptest.NewServer(&endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					})
					defer pythonSrv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_URL", jsSrv.URL)
					c.Set("PYTHON_TRANSFORM_URL", pythonSrv.URL)

					tr := user_transformer.New(c, logger.NOP, stats.Default)
					rsp := tr.Transform(context.TODO(), makeEvents("pythonwithlibs"))

					require.Equal(t, expectedResponse, rsp)
				})

				t.Run("python transformation falls back to JS URL when python URL not configured", func(t *testing.T) {
					jsSrv := httptest.NewServer(&endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					})
					defer jsSrv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_URL", jsSrv.URL)
					// PYTHON_TRANSFORM_URL not set

					tr := user_transformer.New(c, logger.NOP, stats.Default)
					rsp := tr.Transform(context.TODO(), makeEvents("pythonfaas"))

					require.Equal(t, expectedResponse, rsp)
				})

				t.Run("javascript transformation routes to JS URL", func(t *testing.T) {
					jsSrv := httptest.NewServer(&endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					})
					defer jsSrv.Close()

					pythonSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						t.Error("request should not hit Python transformer")
					}))
					defer pythonSrv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_URL", jsSrv.URL)
					c.Set("PYTHON_TRANSFORM_URL", pythonSrv.URL)

					tr := user_transformer.New(c, logger.NOP, stats.Default)
					rsp := tr.Transform(context.TODO(), makeEvents("javascript"))

					require.Equal(t, expectedResponse, rsp)
				})

				t.Run("empty language defaults to JS URL", func(t *testing.T) {
					jsSrv := httptest.NewServer(&endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					})
					defer jsSrv.Close()

					pythonSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						t.Error("request should not hit Python transformer")
					}))
					defer pythonSrv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_URL", jsSrv.URL)
					c.Set("PYTHON_TRANSFORM_URL", pythonSrv.URL)

					tr := user_transformer.New(c, logger.NOP, stats.Default)
					rsp := tr.Transform(context.TODO(), makeEvents(""))

					require.Equal(t, expectedResponse, rsp)
				})
			})

			t.Run("python mirroring routing", func(t *testing.T) {
				msgID := "messageID-0"
				expectedResponse := types.Response{
					Events: []types.TransformerResponse{
						{
							Output: map[string]any{
								"src-key-1": msgID,
							},
							Metadata: types.Metadata{
								MessageID: msgID,
							},
							StatusCode: http.StatusOK,
						},
					},
				}

				makeEvents := func(language string) []types.TransformerEvent {
					return []types.TransformerEvent{{
						Metadata: types.Metadata{
							MessageID: msgID,
						},
						Message: map[string]any{
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
									Language:  language,
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
					}}
				}

				t.Run("python mirroring routes to python mirror URL when configured", func(t *testing.T) {
					jsMirrorSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						t.Error("request should not hit JS mirror transformer")
					}))
					defer jsMirrorSrv.Close()

					pythonMirrorSrv := httptest.NewServer(&endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					})
					defer pythonMirrorSrv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_MIRROR_URL", jsMirrorSrv.URL)
					c.Set("PYTHON_TRANSFORM_MIRROR_URL", pythonMirrorSrv.URL)

					tr := user_transformer.New(c, logger.NOP, stats.Default, user_transformer.ForMirroring())
					rsp := tr.Transform(context.TODO(), makeEvents("pythonfaas"))

					require.Equal(t, expectedResponse, rsp)
				})

				t.Run("pythonwithlibs mirroring routes to python mirror URL", func(t *testing.T) {
					jsMirrorSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						t.Error("request should not hit JS mirror transformer")
					}))
					defer jsMirrorSrv.Close()

					pythonMirrorSrv := httptest.NewServer(&endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					})
					defer pythonMirrorSrv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_MIRROR_URL", jsMirrorSrv.URL)
					c.Set("PYTHON_TRANSFORM_MIRROR_URL", pythonMirrorSrv.URL)

					tr := user_transformer.New(c, logger.NOP, stats.Default, user_transformer.ForMirroring())
					rsp := tr.Transform(context.TODO(), makeEvents("pythonwithlibs"))

					require.Equal(t, expectedResponse, rsp)
				})

				t.Run("javascript mirroring routes to JS mirror URL when configured", func(t *testing.T) {
					jsMirrorSrv := httptest.NewServer(&endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					})
					defer jsMirrorSrv.Close()

					pythonMirrorSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						t.Error("request should not hit Python mirror transformer")
					}))
					defer pythonMirrorSrv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_MIRROR_URL", jsMirrorSrv.URL)
					c.Set("PYTHON_TRANSFORM_MIRROR_URL", pythonMirrorSrv.URL)

					tr := user_transformer.New(c, logger.NOP, stats.Default, user_transformer.ForMirroring())
					rsp := tr.Transform(context.TODO(), makeEvents("javascript"))

					require.Equal(t, expectedResponse, rsp)
				})
			})

			t.Run("python version ID filtering", func(t *testing.T) {
				msgID := "messageID-0"
				expectedResponse := types.Response{
					Events: []types.TransformerResponse{
						{
							Output: map[string]any{
								"src-key-1": msgID,
							},
							Metadata: types.Metadata{
								MessageID: msgID,
							},
							StatusCode: http.StatusOK,
						},
					},
				}

				makeEventsWithVersion := func(language, versionID string) []types.TransformerEvent {
					return []types.TransformerEvent{{
						Metadata: types.Metadata{
							MessageID: msgID,
						},
						Message: map[string]any{
							"src-key-1": msgID,
						},
						Destination: backendconfig.DestinationT{
							DestinationDefinition: backendconfig.DestinationDefinitionT{
								Name: "test-destination",
							},
							Transformations: []backendconfig.TransformationT{
								{
									ID:        "test-transformation",
									VersionID: versionID,
									Language:  language,
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
					}}
				}

				t.Run("empty version list falls back to JS transformer", func(t *testing.T) {
					jsSrv := httptest.NewServer(&endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					})
					defer jsSrv.Close()

					pythonSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						t.Error("request should not hit Python transformer")
					}))
					defer pythonSrv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_URL", jsSrv.URL)
					c.Set("PYTHON_TRANSFORM_URL", pythonSrv.URL)
					c.Set("PYTHON_TRANSFORM_VERSION_IDS_ENABLE", true)
					// PYTHON_TRANSFORM_VERSION_IDS not set (empty)

					tr := user_transformer.New(c, logger.NOP, stats.Default)
					rsp := tr.Transform(context.TODO(), makeEventsWithVersion("pythonfaas", "any-version-id"))

					require.Equal(t, expectedResponse, rsp)
				})

				t.Run("version in allowlist routes to python transformer", func(t *testing.T) {
					jsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						t.Error("request should not hit JS transformer")
					}))
					defer jsSrv.Close()

					pythonSrv := httptest.NewServer(&endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					})
					defer pythonSrv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_URL", jsSrv.URL)
					c.Set("PYTHON_TRANSFORM_URL", pythonSrv.URL)
					c.Set("PYTHON_TRANSFORM_VERSION_IDS_ENABLE", true)
					c.Set("PYTHON_TRANSFORM_VERSION_IDS", "allowed-version-1,allowed-version-2")

					tr := user_transformer.New(c, logger.NOP, stats.Default)
					rsp := tr.Transform(context.TODO(), makeEventsWithVersion("pythonfaas", "allowed-version-1"))

					require.Equal(t, expectedResponse, rsp)
				})

				t.Run("version not in allowlist falls back to JS transformer", func(t *testing.T) {
					jsSrv := httptest.NewServer(&endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					})
					defer jsSrv.Close()

					pythonSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						t.Error("request should not hit Python transformer")
					}))
					defer pythonSrv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_URL", jsSrv.URL)
					c.Set("PYTHON_TRANSFORM_URL", pythonSrv.URL)
					c.Set("PYTHON_TRANSFORM_VERSION_IDS_ENABLE", true)
					c.Set("PYTHON_TRANSFORM_VERSION_IDS", "allowed-version-1,allowed-version-2")

					tr := user_transformer.New(c, logger.NOP, stats.Default)
					rsp := tr.Transform(context.TODO(), makeEventsWithVersion("pythonfaas", "not-allowed-version"))

					require.Equal(t, expectedResponse, rsp)
				})

				t.Run("mirroring with version in allowlist routes to python mirror", func(t *testing.T) {
					jsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						t.Error("request should not hit JS transformer")
					}))
					defer jsSrv.Close()

					pythonSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						t.Error("request should not hit Python transformer")
					}))
					defer pythonSrv.Close()

					pythonMirrorSrv := httptest.NewServer(&endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					})
					defer pythonMirrorSrv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_URL", jsSrv.URL)
					c.Set("PYTHON_TRANSFORM_URL", pythonSrv.URL)
					c.Set("PYTHON_TRANSFORM_MIRROR_URL", pythonMirrorSrv.URL)
					c.Set("PYTHON_TRANSFORM_VERSION_IDS_ENABLE", true)
					c.Set("PYTHON_TRANSFORM_VERSION_IDS", "allowed-version-1")

					tr := user_transformer.New(c, logger.NOP, stats.Default, user_transformer.ForMirroring())
					rsp := tr.Transform(context.TODO(), makeEventsWithVersion("pythonfaas", "allowed-version-1"))

					require.Equal(t, expectedResponse, rsp)
				})

				t.Run("filtering disabled allows all python transformations", func(t *testing.T) {
					jsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						t.Error("request should not hit JS transformer")
					}))
					defer jsSrv.Close()

					pythonSrv := httptest.NewServer(&endpointTransformer{
						supportedPaths: []string{"/customTransform"},
						t:              t,
					})
					defer pythonSrv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("USER_TRANSFORM_URL", jsSrv.URL)
					c.Set("PYTHON_TRANSFORM_URL", pythonSrv.URL)
					// PYTHON_TRANSFORM_VERSION_IDS_ENABLE defaults to false
					c.Set("PYTHON_TRANSFORM_VERSION_IDS", "some-other-version")

					tr := user_transformer.New(c, logger.NOP, stats.Default)
					rsp := tr.Transform(context.TODO(), makeEventsWithVersion("pythonfaas", "any-version-id"))

					require.Equal(t, expectedResponse, rsp)
				})
			})

			t.Run("language label in metrics", func(t *testing.T) {
				testCases := []struct {
					name             string
					language         string
					expectedLanguage string
				}{
					{
						name:             "javascript transformation emits javascript language label",
						language:         "javascript",
						expectedLanguage: "javascript",
					},
					{
						name:             "pythonfaas transformation emits pythonfaas language label",
						language:         "pythonfaas",
						expectedLanguage: "pythonfaas",
					},
					{
						name:             "pythonwithlibs transformation emits pythonwithlibs language label",
						language:         "pythonwithlibs",
						expectedLanguage: "pythonwithlibs",
					},
					{
						name:             "empty language defaults to javascript",
						language:         "",
						expectedLanguage: "javascript",
					},
				}

				for _, tc := range testCases {
					t.Run(tc.name, func(t *testing.T) {
						statsStore, err := memstats.New()
						require.NoError(t, err)

						srv := httptest.NewServer(&endpointTransformer{
							t:              t,
							supportedPaths: []string{"/customTransform"},
						})
						defer srv.Close()

						c := config.New()
						c.Set("Processor.maxRetry", 1)
						c.Set("USER_TRANSFORM_URL", srv.URL)
						c.Set("PYTHON_TRANSFORM_URL", srv.URL)
						c.Set("Processor.userTransformBatchSize", 10)

						tr := user_transformer.New(c, logger.NOP, statsStore, user_transformer.WithClient(srv.Client()))

						msgID := "messageID-0"
						events := []types.TransformerEvent{{
							Metadata: types.Metadata{
								MessageID:   msgID,
								SourceID:    "source-1",
								SourceType:  "webhook",
								WorkspaceID: "workspace-1",
							},
							Message: map[string]any{"src-key-1": msgID},
							Destination: backendconfig.DestinationT{
								ID: "dest-1",
								DestinationDefinition: backendconfig.DestinationDefinitionT{
									Name: "WEBHOOK",
								},
								Transformations: []backendconfig.TransformationT{{
									ID:        "transform-1",
									VersionID: "version-1",
									Language:  tc.language,
								}},
							},
							Credentials: []types.Credential{{
								ID:       "test-credential",
								Key:      "test-key",
								Value:    "test-value",
								IsSecret: false,
							}},
						}}

						rsp := tr.Transform(context.TODO(), events)
						require.Len(t, rsp.Events, 1)

						metricsToCheck := []string{
							"transformer_client_request_total_events",
							"transformer_client_response_total_events",
							"transformer_client_total_time",
							"processor_transformer_request_batch_count",
						}
						for _, metricName := range metricsToCheck {
							measurements := statsStore.GetByName(metricName)
							require.NotEmpty(t, measurements, "metric %s should have measurements", metricName)
							for _, m := range measurements {
								assert.Equal(t, tc.expectedLanguage, m.Tags["language"],
									"metric %s should have language=%s, got language=%s", metricName, tc.expectedLanguage, m.Tags["language"])
							}
						}
					})
				}
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
		mockLogger.EXPECT().Errorn(gomock.Any(), gomock.Any(), gomock.Any()).Do(
			func(msg string, fields ...logger.Field) {
				require.Equal(t, "Long running transformation detected", msg)
				require.Len(t, fields, 2)
				require.Equal(t, "stage", fields[0].Name())
				require.Equal(t, "stage", fields[0].Value())
				require.Equal(t, "duration", fields[1].Name())
				_, ok := fields[1].Value().(time.Duration)
				require.True(t, ok)
				fired.Store(true)
			},
		).MinTimes(1)
		ctx, cancel := context.WithCancel(context.Background())
		go transformerutils.TrackLongRunningTransformation(ctx, "stage", time.Millisecond, mockLogger)
		for !fired.Load() {
			time.Sleep(time.Millisecond)
		}
		cancel()
	})
}

func TestTransformerEvent_ToUserTransformerEvent(t *testing.T) {
	testCases := []struct {
		name     string
		event    *types.TransformerEvent
		expected *types.UserTransformerEvent
	}{
		{
			name: "remove connections",
			event: &types.TransformerEvent{
				Metadata: types.Metadata{},
				Message:  map[string]any{},
				Connection: backendconfig.Connection{
					SourceID:      "source-id",
					DestinationID: "destination-id",
				},
				Destination: backendconfig.DestinationT{
					Transformations: make([]backendconfig.TransformationT, 0),
				},
			},
			expected: &types.UserTransformerEvent{
				Metadata: types.Metadata{},
				Message:  map[string]any{},
			},
		},
		{
			name: "remove everything except transformations in destination",
			event: &types.TransformerEvent{
				Metadata: types.Metadata{},
				Message:  map[string]any{},
				Connection: backendconfig.Connection{
					SourceID:      "source-id",
					DestinationID: "destination-id",
				},
				Destination: backendconfig.DestinationT{
					ID:              "destination-id",
					Transformations: make([]backendconfig.TransformationT, 0),
					Name:            "destination-name",
					Config: map[string]any{
						"key": "value",
						"key2": map[string]any{
							"key": "value",
						},
						"key3": []any{"value"},
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name:   "destination-definition-name",
						Config: map[string]any{},
					},
				},
			},
			expected: &types.UserTransformerEvent{
				Metadata: types.Metadata{},
				Message:  map[string]any{},
			},
		},
		{
			name: "remove everything except transformations in destination with multiple transformer versions",
			event: &types.TransformerEvent{
				Metadata: types.Metadata{},
				Message:  map[string]any{},
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
							Config:    map[string]any{},
						},
						{
							ID:        "transformation-id-2",
							VersionID: "version-id-2",
							Config:    map[string]any{},
						},
					},
					Name: "destination-name",
					Config: map[string]any{
						"key": "value",
						"key2": map[string]any{
							"key": "value",
						},
						"key3": []any{"value"},
					},
					DestinationDefinition: backendconfig.DestinationDefinitionT{
						Name:   "destination-definition-name",
						Config: map[string]any{},
					},
				},
			},
			expected: &types.UserTransformerEvent{
				Metadata: types.Metadata{},
				Message:  map[string]any{},
				Destination: struct {
					Transformations []struct{ VersionID string }
				}{
					Transformations: []struct{ VersionID string }{
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
			assert.Equal(t, tc.expected, tc.event.ToUserTransformerEvent())
		})
	}
}

// TestUserTransformURLRouting verifies the URL-resolution branches added by
// the per-workspace PyT routing feature. Each case spins up real httptest
// servers for the JS, legacy-Python, mirror-Python, and per-workspace endpoints
// and asserts which one is hit by Transform.
func TestUserTransformURLRouting(t *testing.T) {
	const (
		workspaceID = "ws-A1"
		messageID   = "messageID-0"
		allowedV    = "v-allowed"
		blockedV    = "v-blocked"
	)

	type recordingSrv struct {
		srv   *httptest.Server
		hits  *atomic.Int32
		paths *atomic.Value // last hit path
	}

	newRecordingSrv := func(t *testing.T) recordingSrv {
		var hits atomic.Int32
		var path atomic.Value
		path.Store("")
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			path.Store(r.URL.Path)

			var reqBody []types.TransformerEvent
			require.NoError(t, jsonrs.NewDecoder(r.Body).Decode(&reqBody))

			responses := make([]types.TransformerResponse, len(reqBody))
			for i := range reqBody {
				responses[i] = types.TransformerResponse{
					Output:     reqBody[i].Message,
					Metadata:   reqBody[i].Metadata,
					StatusCode: http.StatusOK,
				}
			}
			w.Header().Set("apiVersion", strconv.Itoa(reportingtypes.SupportedTransformerApiVersion))
			require.NoError(t, jsonrs.NewEncoder(w).Encode(responses))
		}))
		t.Cleanup(srv.Close)
		return recordingSrv{srv: srv, hits: &hits, paths: &path}
	}

	makeEvents := func(language, versionID, wsID string) []types.TransformerEvent {
		return []types.TransformerEvent{{
			Metadata: types.Metadata{
				MessageID:   messageID,
				WorkspaceID: wsID,
			},
			Message: map[string]any{"src-key-1": messageID},
			Destination: backendconfig.DestinationT{
				DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "test-destination"},
				Transformations: []backendconfig.TransformationT{{
					ID:        "test-transformation",
					VersionID: versionID,
					Language:  language,
				}},
			},
		}}
	}

	// The recording server echoes back the request's Message + Metadata with
	// StatusCode 200, so Transform's response is deterministic given the input.
	expectedResponseFor := func(wsID string) types.Response {
		return types.Response{
			Events: []types.TransformerResponse{{
				Output:     map[string]any{"src-key-1": messageID},
				Metadata:   types.Metadata{MessageID: messageID, WorkspaceID: wsID},
				StatusCode: http.StatusOK,
			}},
		}
	}

	type fixture struct {
		conf            *config.Config
		js, py, pyMir   recordingSrv
		perWS           recordingSrv
		perWSExpectPath string
	}

	newFixture := func(t *testing.T) *fixture {
		f := &fixture{
			js:    newRecordingSrv(t),
			py:    newRecordingSrv(t),
			pyMir: newRecordingSrv(t),
			perWS: newRecordingSrv(t),
		}
		f.perWSExpectPath = "/pyt-" + strings.ToLower(workspaceID) + "/customTransform"
		f.conf = config.New()
		f.conf.Set("Processor.UserTransformer.maxRetry", 1)
		f.conf.Set("USER_TRANSFORM_URL", f.js.srv.URL)
		f.conf.Set("PYTHON_TRANSFORM_URL", f.py.srv.URL)
		f.conf.Set("USER_TRANSFORM_MIRROR_URL", f.js.srv.URL)
		f.conf.Set("PYTHON_TRANSFORM_MIRROR_URL", f.pyMir.srv.URL)
		// Pin the per-workspace template to the recording server so the request
		// resolves; embed {workspaceID} in the path so we can assert it.
		f.conf.Set("Processor.UserTransformer.perWorkspacePyTURLTemplate", f.perWS.srv.URL+"/pyt-{workspaceID}")
		// Enable version allowlist with one allowed version.
		f.conf.Set("PYTHON_TRANSFORM_VERSION_IDS_ENABLE", true)
		f.conf.Set("PYTHON_TRANSFORM_VERSION_IDS", allowedV)
		return f
	}

	assertOnly := func(t *testing.T, f *fixture, hit *recordingSrv, expectPath string) {
		t.Helper()
		require.Equal(t, int32(1), hit.hits.Load(), "expected target server to be hit once")
		if expectPath != "" {
			require.Equal(t, expectPath, hit.paths.Load().(string))
		}
		for name, s := range map[string]*recordingSrv{
			"js": &f.js, "py": &f.py, "pyMir": &f.pyMir, "perWS": &f.perWS,
		} {
			if s == hit {
				continue
			}
			require.Equal(t, int32(0), s.hits.Load(), "server %s should not be hit", name)
		}
	}

	t.Run("JS — flag off → JS URL", func(t *testing.T) {
		f := newFixture(t)
		tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
		rsp := tr.Transform(context.Background(), makeEvents("javascript", allowedV, workspaceID))
		require.Equal(t, expectedResponseFor(workspaceID), rsp)
		assertOnly(t, f, &f.js, "/customTransform")
	})

	t.Run("JS — flag on → JS URL (unaffected)", func(t *testing.T) {
		f := newFixture(t)
		f.conf.Set("Processor.UserTransformer.perWorkspacePyTEnabled", true)
		tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
		rsp := tr.Transform(context.Background(), makeEvents("javascript", allowedV, workspaceID))
		require.Equal(t, expectedResponseFor(workspaceID), rsp)
		assertOnly(t, f, &f.js, "/customTransform")
	})

	t.Run("Python — flag off → global Python URL", func(t *testing.T) {
		f := newFixture(t)
		tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
		rsp := tr.Transform(context.Background(), makeEvents("pythonfaas", allowedV, workspaceID))
		require.Equal(t, expectedResponseFor(workspaceID), rsp)
		assertOnly(t, f, &f.py, "/customTransform")
	})

	t.Run("Python — flag on, version allowed → per-workspace URL", func(t *testing.T) {
		f := newFixture(t)
		f.conf.Set("Processor.UserTransformer.perWorkspacePyTEnabled", true)
		tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
		rsp := tr.Transform(context.Background(), makeEvents("pythonfaas", allowedV, workspaceID))
		require.Equal(t, expectedResponseFor(workspaceID), rsp)
		assertOnly(t, f, &f.perWS, f.perWSExpectPath)
	})

	t.Run("Python — flag on, version disallowed → still routes to per-workspace URL (allowlist doesn't apply in per-ws mode)", func(t *testing.T) {
		f := newFixture(t)
		f.conf.Set("Processor.UserTransformer.perWorkspacePyTEnabled", true)
		tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
		rsp := tr.Transform(context.Background(), makeEvents("pythonfaas", blockedV, workspaceID))
		require.Equal(t, expectedResponseFor(workspaceID), rsp)
		assertOnly(t, f, &f.perWS, f.perWSExpectPath)
	})

	t.Run("Python — flag off, version disallowed → JS URL fallback (legacy allowlist still applies)", func(t *testing.T) {
		f := newFixture(t)
		// per-ws flag off, legacy path active; disallowed version → JS fallback.
		tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
		rsp := tr.Transform(context.Background(), makeEvents("pythonfaas", blockedV, workspaceID))
		require.Equal(t, expectedResponseFor(workspaceID), rsp)
		assertOnly(t, f, &f.js, "/customTransform")
	})

	t.Run("Python — flag on, mirroring → mirror Python URL", func(t *testing.T) {
		f := newFixture(t)
		f.conf.Set("Processor.UserTransformer.perWorkspacePyTEnabled", true)
		tr := user_transformer.New(f.conf, logger.NOP, stats.NOP, user_transformer.ForMirroring())
		rsp := tr.Transform(context.Background(), makeEvents("pythonfaas", allowedV, workspaceID))
		require.Equal(t, expectedResponseFor(workspaceID), rsp)
		assertOnly(t, f, &f.pyMir, "/customTransform")
	})

	t.Run("Python — flag on, empty workspaceID → panics (invariant violation)", func(t *testing.T) {
		f := newFixture(t)
		f.conf.Set("Processor.UserTransformer.perWorkspacePyTEnabled", true)
		tr := user_transformer.New(f.conf, logger.NOP, stats.NOP)
		require.PanicsWithValue(
			t,
			"per-workspace PyT enabled but workspaceID is empty",
			func() {
				tr.Transform(context.Background(), makeEvents("pythonfaas", allowedV, ""))
			},
		)
	})
}

// fakeColdStartTransport is a transformerclient.Client that returns a
// configured failure (failStatus or failErr) for the first `failures` calls and
// then a 200 success (carrying successBody) for every subsequent call. Lets
// tests simulate a pod that's cold for N attempts and then warms up.
type fakeColdStartTransport struct {
	// At most one of failStatus / failErr is set. failStatus > 0 returns an
	// HTTP response with that status code and an empty body; failErr returns
	// a transport-level error (ECONNREFUSED / DNS / ...).
	failStatus int
	failErr    error
	// failures: number of failing calls before switching to successBody. Use
	// math.MaxInt for "always fail".
	failures int
	// successBody: marshaled []TransformerResponse returned on the (failures+1)th
	// and later calls. Empty body is fine when the test doesn't reach success.
	successBody []byte

	calls atomic.Int32
}

func (f *fakeColdStartTransport) Do(_ *http.Request) (*http.Response, error) {
	hdr := http.Header{}
	hdr.Set("apiVersion", strconv.Itoa(reportingtypes.SupportedTransformerApiVersion))
	n := int(f.calls.Add(1))
	if n <= f.failures {
		if f.failStatus > 0 {
			return &http.Response{
				StatusCode: f.failStatus,
				Header:     hdr,
				Body:       io.NopCloser(bytes.NewReader(nil)),
			}, nil
		}
		return nil, f.failErr
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     hdr,
		Body:       io.NopCloser(bytes.NewReader(f.successBody)),
	}, nil
}

func TestColdStartCounter(t *testing.T) {
	const (
		coldStartMetric = "processor_user_transformer_cold_start_errors_total"
		workspaceID     = "ws-A1"
		messageID       = "messageID-0"
		allowedV        = "v-allowed"
	)

	connRefused := &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED},
	}
	noRouteToHost := &net.OpError{
		Op:  "dial",
		Net: "tcp",
		Err: &os.SyscallError{Syscall: "connect", Err: syscall.EHOSTUNREACH},
	}
	dnsErr := &net.DNSError{Err: "no such host", Name: "pyt-ws-A1", IsNotFound: true}

	// Marshaled success response — what a warm PyT pod would return.
	successBody, err := jsonrs.Marshal([]types.TransformerResponse{{
		Output:     map[string]any{"src-key-1": messageID},
		Metadata:   types.Metadata{MessageID: messageID, WorkspaceID: workspaceID},
		StatusCode: http.StatusOK,
	}})
	require.NoError(t, err)

	cases := []struct {
		name string
		// failStatus / failErr: what the fake transport returns for the first
		// `failures` calls. After that it returns a 200 success.
		failStatus int
		failErr    error
		failures   int

		// Optional per-case overrides on top of the default fixture
		// (perWorkspacePyTEnabled=true, endless retries=true, language=python,
		// non-mirroring). Each toggles one of the cold-start gating predicates.
		disableFlag    bool   // sets perWorkspacePyTEnabled=false
		mirroring      bool   // applies the ForMirroring() opt
		language       string // overrides "pythonfaas"
		disableEndless bool   // sets perWorkspacePyTEndlessRetries=false

		expectCounter    int
		expectEventCount int // events on the success path
		expectFailedCode int // StatusCode on FailedEvent (0 = no failed event expected)
	}{
		{
			// 2 ECONNREFUSED then warm. Fits within doPost's inner retry budget
			// (maxRetry+1 = 3), so the outer sendBatch loop runs exactly once.
			name:             "ECONNREFUSED twice → counted twice, then warm",
			failErr:          connRefused,
			failures:         2,
			expectCounter:    2,
			expectEventCount: 1,
		},
		{
			name:             "DNS not found twice → counted twice, then warm",
			failErr:          dnsErr,
			failures:         2,
			expectCounter:    2,
			expectEventCount: 1,
		},
		{
			// "no route to host" — stale iptables / EndpointSlice during pod
			// replacement or scale-down. Same transient signal as ECONNREFUSED.
			name:             "EHOSTUNREACH twice → counted twice, then warm",
			failErr:          noRouteToHost,
			failures:         2,
			expectCounter:    2,
			expectEventCount: 1,
		},
		{
			name:             "503 twice → counted twice, then warm",
			failStatus:       http.StatusServiceUnavailable,
			failures:         2,
			expectCounter:    2,
			expectEventCount: 1,
		},
		{
			name:             "502 twice → counted twice, then warm",
			failStatus:       http.StatusBadGateway,
			failures:         2,
			expectCounter:    2,
			expectEventCount: 1,
		},
		{
			// 4 failures > maxRetry+1, forces sendBatch's outer retry to kick
			// in: doPost exhausts inner retries on cycle 1, returns the
			// cold-start status, sendBatch retries doPost, second cycle warms.
			name:             "cold-start spans multiple outer cycles → still recovers",
			failErr:          connRefused,
			failures:         4,
			expectCounter:    4,
			expectEventCount: 1,
		},
		{
			// 4xx is job-terminated — no retry, no cold-start counter, just a
			// straight FailedEvent with the upstream status code.
			name:             "non-transient 400 → no retry, failed event with 400",
			failStatus:       http.StatusBadRequest,
			failures:         math.MaxInt,
			expectCounter:    0,
			expectFailedCode: http.StatusBadRequest,
		},
		{
			// With cpDownEndlessRetries=false, CPDown bails after one attempt
			// via backoff.Permanent. Not a cold-start signal — counter stays 0.
			name:             "StatusCPDown → bails, not counted",
			failStatus:       transformerutils.StatusCPDown,
			failures:         math.MaxInt,
			expectCounter:    0,
			expectFailedCode: transformerutils.StatusCPDown,
		},
		// --- Guard negatives: cold-start counter must stay 0 when any of the
		// gating predicates is false, even if the transport returns
		// cold-start-looking errors. The transport warms after 2 failures so
		// doPost recovers via its inner retry budget (3 inner attempts at
		// maxRetry=2) — no panic, just a clean success. Empty-workspaceID is
		// covered by TestUserTransformURLRouting's panic-on-invariant case.
		{
			name:             "guard: flag off → cold-start error not counted",
			disableFlag:      true,
			failErr:          connRefused,
			failures:         2,
			expectCounter:    0,
			expectEventCount: 1,
		},
		{
			name:             "guard: mirroring on → cold-start error not counted",
			mirroring:        true,
			failErr:          connRefused,
			failures:         2,
			expectCounter:    0,
			expectEventCount: 1,
		},
		{
			name:             "guard: JS language → cold-start error not counted",
			language:         "javascript",
			failErr:          connRefused,
			failures:         2,
			expectCounter:    0,
			expectEventCount: 1,
		},
		{
			// perWorkspacePyTEndlessRetries=false → after doPost exhausts its
			// inner retries and returns StatusColdStartWindowFailure, the
			// outer sendBatch loop bails via backoff.Permanent on the very
			// next cycle (no further doPost call). Inner attempts still emit
			// the counter, so counter = maxRetry+1 = 3.
			name:             "endless retries disabled → bails with cold-start failed event",
			disableEndless:   true,
			failErr:          connRefused,
			failures:         math.MaxInt,
			expectCounter:    3,
			expectFailedCode: transformerutils.StatusColdStartWindowFailure,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			statsStore, err := memstats.New()
			require.NoError(t, err)

			c := config.New()
			c.Set("Processor.UserTransformer.maxRetry", 2)
			c.Set("Processor.UserTransformer.maxRetryBackoffInterval", "1ms")
			// Bound CPDown retries so the CPDown case terminates. Per-workspace
			// PyT retries stay at the production default (endless) — the
			// transport itself warms up to end the loop.
			c.Set("Processor.UserTransformer.cpDownEndlessRetries", false)
			c.Set("USER_TRANSFORM_URL", "http://js-stub:9090")
			c.Set("PYTHON_TRANSFORM_URL", "http://py-stub:9090")
			c.Set("Processor.UserTransformer.perWorkspacePyTEnabled", !tc.disableFlag)
			c.Set("Processor.UserTransformer.perWorkspacePyTEndlessRetries", !tc.disableEndless)
			c.Set("Processor.UserTransformer.perWorkspacePyTURLTemplate", "http://pyt-{workspaceID}:9090")
			c.Set("PYTHON_TRANSFORM_VERSION_IDS_ENABLE", true)
			c.Set("PYTHON_TRANSFORM_VERSION_IDS", allowedV)

			transport := &fakeColdStartTransport{
				failStatus:  tc.failStatus,
				failErr:     tc.failErr,
				failures:    tc.failures,
				successBody: successBody,
			}
			opts := []user_transformer.Opt{user_transformer.WithClient(transport)}
			if tc.mirroring {
				opts = append(opts, user_transformer.ForMirroring())
			}
			tr := user_transformer.New(c, logger.NOP, statsStore, opts...)

			language := tc.language
			if language == "" {
				language = "pythonfaas"
			}
			events := []types.TransformerEvent{{
				Metadata: types.Metadata{
					MessageID:   messageID,
					WorkspaceID: workspaceID,
				},
				Message: map[string]any{"src-key-1": messageID},
				Destination: backendconfig.DestinationT{
					DestinationDefinition: backendconfig.DestinationDefinitionT{Name: "test-destination"},
					Transformations: []backendconfig.TransformationT{{
						ID:        "transform-1",
						VersionID: allowedV,
						Language:  language,
					}},
				},
			}}

			rsp := tr.Transform(context.Background(), events)

			require.Len(t, rsp.Events, tc.expectEventCount)
			if tc.expectFailedCode != 0 {
				require.Len(t, rsp.FailedEvents, 1)
				require.Equal(t, tc.expectFailedCode, rsp.FailedEvents[0].StatusCode)
				require.Equal(t, messageID, rsp.FailedEvents[0].Metadata.MessageID)
				require.Equal(t, workspaceID, rsp.FailedEvents[0].Metadata.WorkspaceID)
			} else {
				require.Empty(t, rsp.FailedEvents)
				require.Equal(t, messageID, rsp.Events[0].Metadata.MessageID)
				require.Equal(t, workspaceID, rsp.Events[0].Metadata.WorkspaceID)
				require.Equal(t, http.StatusOK, rsp.Events[0].StatusCode)
			}

			measurements := statsStore.GetByName(coldStartMetric)
			total := 0
			for _, m := range measurements {
				total += int(m.Value)
			}
			require.Equal(t, tc.expectCounter, total)

			if tc.expectCounter > 0 {
				// All increments must land on the same tag set: workspaceID +
				// hardcoded language=python (not the raw label).
				require.Len(t, measurements, 1)
				require.Equal(t, workspaceID, measurements[0].Tags["workspaceID"])
				require.Equal(t, "python", measurements[0].Tags["language"])
			}
		})
	}
}

func TestForwardTest(t *testing.T) {
	t.Run("each endpoint method posts to its pyt path and returns status/body", func(t *testing.T) {
		var gotPath, gotContentType string
		var gotBody []byte
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotPath = r.URL.Path
			gotContentType = r.Header.Get("Content-Type")
			gotBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer srv.Close()

		conf := config.New()
		conf.Set("Processor.UserTransformer.perWorkspacePyTEnabled", true)
		conf.Set("Processor.UserTransformer.perWorkspacePyTURLTemplate", srv.URL)
		client := user_transformer.New(conf, logger.NOP, stats.NOP)

		cases := map[string]struct {
			call     func(context.Context, string, []byte) (int, []byte, error)
			wantPath string
		}{
			"Test":        {client.Test, "/test"},
			"TestRun":     {client.TestRun, "/testRun"},
			"TestLibrary": {client.TestLibrary, "/test-library"},
			"ExtractLibs": {client.ExtractLibs, "/extract-libs"},
		}
		for name, tc := range cases {
			t.Run(name, func(t *testing.T) {
				status, body, err := tc.call(context.Background(), "WS-1", []byte(`{"code":"x"}`))
				require.NoError(t, err)
				require.Equal(t, http.StatusCreated, status)
				require.JSONEq(t, `{"ok":true}`, string(body))
				require.Equal(t, tc.wantPath, gotPath)
				require.Equal(t, "application/json", gotContentType)
				require.JSONEq(t, `{"code":"x"}`, string(gotBody))
			})
		}
	})

	t.Run("retries a cold-start 503 with a small backoff then succeeds", func(t *testing.T) {
		var attempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if attempts.Add(1) < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`done`))
		}))
		defer srv.Close()

		statsStore, err := memstats.New()
		require.NoError(t, err)
		conf := config.New()
		conf.Set("Processor.UserTransformer.perWorkspacePyTEnabled", true)
		conf.Set("Processor.UserTransformer.perWorkspacePyTURLTemplate", srv.URL)
		client := user_transformer.New(conf, logger.NOP, statsStore,
			user_transformer.WithMaxRetryBackoffInterval(config.NewMockValueLoader(5*time.Millisecond)))

		status, body, err := client.Test(context.Background(), "ws-1", []byte(`{}`))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "done", string(body))
		require.EqualValues(t, 3, attempts.Load())

		total := 0
		for _, m := range statsStore.GetByName("processor_user_transformer_cold_start_errors_total") {
			total += int(m.Value)
		}
		require.Equal(t, 2, total, "each cold-start retry should increment the counter")
	})

	t.Run("stops retrying when the deadline is exceeded", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer srv.Close()

		conf := config.New()
		conf.Set("Processor.UserTransformer.perWorkspacePyTEnabled", true)
		conf.Set("Processor.UserTransformer.perWorkspacePyTURLTemplate", srv.URL)
		client := user_transformer.New(conf, logger.NOP, stats.NOP,
			user_transformer.WithMaxRetryBackoffInterval(config.NewMockValueLoader(5*time.Millisecond)))

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, _, err := client.Test(ctx, "ws-1", []byte(`{}`))
		require.Error(t, err)
	})
}
