package transformer

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
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/utils/types"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type fakeTransformer struct {
	requests [][]TransformerEvent
	t        testing.TB
}

func (t *fakeTransformer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var reqBody []TransformerEvent
	require.NoError(t.t, json.NewDecoder(r.Body).Decode(&reqBody))

	t.requests = append(t.requests, reqBody)

	responses := make([]TransformerResponse, len(reqBody))

	for i := range reqBody {
		statusCode := int(reqBody[i].Message["forceStatusCode"].(float64))
		delete(reqBody[i].Message, "forceStatusCode")
		reqBody[i].Message["echo-key-1"] = reqBody[i].Message["src-key-1"]

		responses[i] = TransformerResponse{
			Output:     reqBody[i].Message,
			Metadata:   reqBody[i].Metadata,
			StatusCode: statusCode,
		}
		if statusCode >= http.StatusBadRequest {
			responses[i].Error = "error"
		}
	}

	w.Header().Set("apiVersion", strconv.Itoa(types.SupportedTransformerApiVersion))

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

	var reqBody []TransformerEvent
	require.NoError(elt.t, json.NewDecoder(r.Body).Decode(&reqBody))

	responses := make([]TransformerResponse, len(reqBody))

	if elt.retryCount < elt.maxRetryCount {
		http.Error(w, response.MakeResponse(elt.statusError), elt.statusCode)
		return
	}

	for i := range reqBody {
		responses[i] = TransformerResponse{
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

	var reqBody []TransformerEvent
	require.NoError(et.t, json.NewDecoder(r.Body).Decode(&reqBody))

	responses := make([]TransformerResponse, len(reqBody))

	for i := range reqBody {
		responses[i] = TransformerResponse{
			Output:     reqBody[i].Message,
			Metadata:   reqBody[i].Metadata,
			StatusCode: http.StatusOK,
			Error:      "",
		}
	}

	w.Header().Set("apiVersion", strconv.Itoa(types.SupportedTransformerApiVersion))

	require.NoError(et.t, json.NewEncoder(w).Encode(responses))
}

func TestTransformer(t *testing.T) {
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

					tr := handle{}
					tr.stat = statsStore
					tr.logger = logger.NOP
					tr.conf = conf
					tr.httpClient = srv.Client()
					tr.guardConcurrency = make(chan struct{}, 200)
					tr.sentStat = tr.stat.NewStat("transformer_sent", stats.CountType)
					tr.receivedStat = tr.stat.NewStat("transformer_received", stats.CountType)
					tr.cpDownGauge = tr.stat.NewStat("control_plane_down", stats.GaugeType)
					tr.config.timeoutDuration = 1 * time.Second
					tr.config.failOnUserTransformTimeout = config.SingleValueLoader(true)
					tr.config.failOnError = config.SingleValueLoader(true)
					tr.config.maxRetryBackoffInterval = config.SingleValueLoader(1 * time.Second)
					tr.config.maxRetry = config.SingleValueLoader(1)

					batchSize := tt.batchSize
					eventsCount := tt.eventsCount
					failEvery := tt.failEvery

					events := make([]TransformerEvent, eventsCount)
					expectedResponse := Response{}

					transformationID := rand.String(10)

					destinationConfig := backendconfigtest.NewDestinationBuilder("WEBHOOK").
						WithUserTransformation(transformationID, rand.String(10)).Build()

					metadata := Metadata{
						DestinationType:  destinationConfig.DestinationDefinition.Name,
						SourceID:         rand.String(10),
						DestinationID:    destinationConfig.ID,
						TransformationID: destinationConfig.Transformations[0].ID,
						SourceType:       "webhook",
					}

					for i := range events {
						msgID := fmt.Sprintf("messageID-%d", i)
						statusCode := http.StatusOK

						if failEvery != 0 && i%failEvery == 0 {
							statusCode = http.StatusBadRequest
						}

						metadata := metadata
						metadata.MessageID = msgID

						events[i] = TransformerEvent{
							Metadata: metadata,
							Message: map[string]interface{}{
								"src-key-1":       msgID,
								"forceStatusCode": statusCode,
							},
							Destination: destinationConfig,
							Credentials: []Credential{
								{
									ID:       "test-credential",
									Key:      "test-key",
									Value:    "test-value",
									IsSecret: false,
								},
							},
						}

						tResp := TransformerResponse{
							Metadata:   metadata,
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

					labels := transformerMetricLabels{
						Endpoint:         getEndpointFromURL(srv.URL),
						Stage:            "test-stage",
						SourceID:         metadata.SourceID,
						SourceType:       metadata.SourceType,
						DestinationType:  destinationConfig.DestinationDefinition.Name,
						DestinationID:    destinationConfig.ID,
						WorkspaceID:      metadata.WorkspaceID,
						TransformationID: destinationConfig.Transformations[0].ID,
					}
					rsp := tr.transform(context.TODO(), events, srv.URL, batchSize, labels)
					require.Equal(t, expectedResponse, rsp)

					metrics := statsStore.GetByName("processor.transformer_request_time")
					if tt.eventsCount > 0 {
						require.NotEmpty(t, metrics)
						for _, m := range metrics {
							require.Equal(t, stats.Tags{
								"endpoint":         getEndpointFromURL(srv.URL),
								"stage":            "test-stage",
								"sourceId":         metadata.SourceID,
								"sourceType":       metadata.SourceType,
								"destinationType":  destinationConfig.DestinationDefinition.Name,
								"destinationId":    destinationConfig.ID,
								"transformationId": destinationConfig.Transformations[0].ID,
								"workspaceId":      metadata.WorkspaceID,
								"language":         "",

								// Legacy tags: to be removed
								"dest_type": destinationConfig.DestinationDefinition.Name,
								"dest_id":   destinationConfig.ID,
								"src_id":    metadata.SourceID,
							}, m.Tags)

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
				}
			})

			t.Run("timeout", func(t *testing.T) {
				msgID := "messageID-0"
				events := append([]TransformerEvent{}, TransformerEvent{
					Metadata: Metadata{
						MessageID: msgID,
					},
					Message: map[string]interface{}{
						"src-key-1": msgID,
					},
					Credentials: []Credential{
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
					expectedResponse           []TransformerResponse
					failOnUserTransformTimeout bool
				}{
					{
						name:                       "user transformation timeout",
						retries:                    3,
						stage:                      userTransformerStage,
						expectPanic:                true,
						failOnUserTransformTimeout: false,
					},
					{
						name:        "user transformation timeout with fail on timeout",
						retries:     3,
						stage:       userTransformerStage,
						expectPanic: false,
						expectedResponse: []TransformerResponse{
							{
								Metadata: Metadata{
									MessageID: msgID,
								},
								StatusCode: TransformerRequestTimeout,
								Output: map[string]interface{}{
									"src-key-1": msgID,
								},
							},
						},
						failOnUserTransformTimeout: true,
					},
					{
						name:                       "destination transformation timeout",
						retries:                    3,
						stage:                      destTransformerStage,
						expectPanic:                true,
						failOnUserTransformTimeout: false,
					},
					{
						name:                       "destination transformation timeout with fail on timeout",
						retries:                    3,
						stage:                      destTransformerStage,
						expectPanic:                true,
						failOnUserTransformTimeout: true,
					},
				}

				for _, tc := range testCases {
					tc := tc

					t.Run(tc.name, func(t *testing.T) {
						ch := make(chan struct{})
						srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							<-ch
						}))
						defer srv.Close()

						client := srv.Client()
						client.Timeout = 1 * time.Millisecond

						tr := handle{}
						tr.config.timeoutDuration = 1 * time.Millisecond
						tr.stat = stats.Default
						tr.logger = logger.NOP
						tr.conf = conf
						tr.httpClient = client
						tr.config.maxRetry = config.SingleValueLoader(tc.retries)
						tr.config.failOnUserTransformTimeout = config.SingleValueLoader(tc.failOnUserTransformTimeout)
						tr.cpDownGauge = tr.stat.NewStat("control_plane_down", stats.GaugeType)
						tr.config.maxRetryBackoffInterval = config.SingleValueLoader(1 * time.Second)

						if tc.expectPanic {
							require.Panics(t, func() {
								_ = tr.request(context.TODO(), srv.URL, transformerMetricLabels{
									Endpoint: getEndpointFromURL(srv.URL),
									Stage:    tc.stage,
								}, events)
							})
							close(ch)
							return
						}

						_, err := url.Parse(srv.URL)
						require.NoError(t, err)

						rsp := tr.request(context.TODO(), srv.URL, transformerMetricLabels{
							Endpoint: getEndpointFromURL(srv.URL),
							Stage:    tc.stage,
						}, events)
						require.Len(t, rsp, 1)
						require.Equal(t, rsp[0].StatusCode, TransformerRequestTimeout)
						require.Equal(t, rsp[0].Metadata, Metadata{
							MessageID: msgID,
						})
						require.Contains(t, rsp[0].Error, "transformer request timed out:")
						close(ch)
					})
				}
			})

			t.Run("endless retries in case of control plane down", func(t *testing.T) {
				msgID := "messageID-0"
				events := append([]TransformerEvent{}, TransformerEvent{
					Metadata: Metadata{
						MessageID: msgID,
					},
					Message: map[string]interface{}{
						"src-key-1": msgID,
					},
					Credentials: []Credential{
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
					statusCode:    StatusCPDown,
					statusError:   "control plane not reachable",
					apiVersion:    types.SupportedTransformerApiVersion,
					t:             t,
				}

				srv := httptest.NewServer(elt)
				defer srv.Close()

				tr := handle{}
				tr.stat = stats.Default
				tr.logger = logger.NOP
				tr.conf = conf
				tr.httpClient = srv.Client()
				tr.config.maxRetry = config.SingleValueLoader(1)
				tr.config.maxRetryBackoffInterval = config.SingleValueLoader(1 * time.Second)
				tr.config.timeoutDuration = 1 * time.Second
				tr.config.failOnUserTransformTimeout = config.SingleValueLoader(false)
				tr.cpDownGauge = tr.stat.NewStat("control_plane_down", stats.GaugeType)

				rsp := tr.request(context.TODO(), srv.URL, transformerMetricLabels{
					Endpoint: getEndpointFromURL(srv.URL),
					Stage:    "test-stage",
				}, events)
				require.Equal(t, rsp, []TransformerResponse{
					{
						Metadata: Metadata{
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
				events := append([]TransformerEvent{}, TransformerEvent{
					Metadata: Metadata{
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
					Credentials: []Credential{
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
					expectedResponse []TransformerResponse
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
						expectedResponse: []TransformerResponse{
							{
								Metadata: Metadata{
									MessageID: msgID,
								},
								StatusCode: TransformerRequestFailure,
								Error:      "transformer request failed: transformer returned status code: 429",
							},
						},
						failOnError: true,
					},
					{
						name:            "transient control plane error",
						retries:         30,
						maxRetryCount:   3,
						statusCode:      StatusCPDown,
						statusError:     "control plane not reachable",
						expectedRetries: 3,
						expectPanic:     false,
						expectedResponse: []TransformerResponse{
							{
								Metadata: Metadata{
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
							apiVersion:    types.SupportedTransformerApiVersion,
							t:             t,
						}

						srv := httptest.NewServer(elt)
						defer srv.Close()

						tr := handle{}
						tr.stat = stats.Default
						tr.logger = logger.NOP
						tr.conf = conf
						tr.httpClient = srv.Client()
						tr.config.failOnUserTransformTimeout = config.SingleValueLoader(false)
						tr.config.maxRetry = config.SingleValueLoader(tc.retries)
						tr.config.failOnError = config.SingleValueLoader(tc.failOnError)
						tr.cpDownGauge = tr.stat.NewStat("control_plane_down", stats.GaugeType)
						tr.config.timeoutDuration = 1 * time.Second
						tr.config.maxRetryBackoffInterval = config.SingleValueLoader(1 * time.Second)

						if tc.expectPanic {
							require.Panics(t, func() {
								_ = tr.request(context.TODO(), srv.URL, transformerMetricLabels{
									Endpoint: getEndpointFromURL(srv.URL),
									Stage:    "test-stage",
								}, events)
							})
							require.Equal(t, elt.retryCount, tc.expectedRetries)
							return
						}

						rsp := tr.request(context.TODO(), srv.URL, transformerMetricLabels{
							Endpoint: getEndpointFromURL(srv.URL),
							Stage:    "test-stage",
						}, events)
						require.Equal(t, tc.expectedResponse, rsp)
						require.Equal(t, tc.expectedRetries, elt.retryCount)
					})
				}
			})

			t.Run("version compatibility", func(t *testing.T) {
				msgID := "messageID-0"
				events := append([]TransformerEvent{}, TransformerEvent{
					Metadata: Metadata{
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
					Credentials: []Credential{
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
					expectedResponse []TransformerResponse
				}{
					{
						name:        "compatible api version",
						apiVersion:  types.SupportedTransformerApiVersion,
						expectPanic: false,
						expectedResponse: []TransformerResponse{
							{
								Metadata: Metadata{
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

						tr := handle{}
						tr.httpClient = srv.Client()
						tr.stat = stats.Default
						tr.conf = conf
						tr.logger = logger.NOP
						tr.cpDownGauge = tr.stat.NewStat("control_plane_down", stats.GaugeType)
						tr.config.maxRetry = config.SingleValueLoader(1)
						tr.config.timeoutDuration = 1 * time.Second
						tr.config.maxRetryBackoffInterval = config.SingleValueLoader(1 * time.Second)

						if tc.expectPanic {
							require.Panics(t, func() {
								_ = tr.request(context.TODO(), srv.URL, transformerMetricLabels{
									Endpoint: getEndpointFromURL(srv.URL),
									Stage:    "test-stage",
								}, events)
							})
							return
						}

						rsp := tr.request(context.TODO(), srv.URL, transformerMetricLabels{
							Endpoint: getEndpointFromURL(srv.URL),
							Stage:    "test-stage",
						}, events)
						require.Equal(t, tc.expectedResponse, rsp)
					})
				}
			})

			t.Run("endpoints", func(t *testing.T) {
				msgID := "messageID-0"
				expectedResponse := Response{
					Events: []TransformerResponse{
						{
							Output: map[string]interface{}{
								"src-key-1": msgID,
							},
							Metadata: Metadata{
								MessageID: msgID,
							},
							StatusCode: http.StatusOK,
						},
					},
				}
				events := append([]TransformerEvent{}, TransformerEvent{
					Metadata: Metadata{
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
					Credentials: []Credential{
						{
							ID:       "test-credential",
							Key:      "test-key",
							Value:    "test-value",
							IsSecret: false,
						},
					},
				})

				t.Run("Destination transformations", func(t *testing.T) {
					et := &endpointTransformer{
						supportedPaths: []string{"/v0/destinations/test-destination"},
						t:              t,
					}

					srv := httptest.NewServer(et)
					defer srv.Close()

					c := config.New()
					c.Set("Processor.maxRetry", 1)
					c.Set("DEST_TRANSFORM_URL", srv.URL)

					tr := NewTransformer(c, logger.NOP, stats.Default, WithClient(srv.Client()))
					rsp := tr.Transform(context.TODO(), events, 10)
					require.Equal(t, rsp, expectedResponse)
				})

				t.Run("Destination warehouse transformations", func(t *testing.T) {
					testCases := []struct {
						name            string
						destinationType string
					}{
						{
							name:            "rs",
							destinationType: warehouseutils.RS,
						},
						{
							name:            "clickhouse",
							destinationType: warehouseutils.CLICKHOUSE,
						},
						{
							name:            "snowflake",
							destinationType: warehouseutils.SNOWFLAKE,
						},
					}

					for _, tc := range testCases {
						tc := tc

						t.Run(tc.name, func(t *testing.T) {
							et := &endpointTransformer{
								supportedPaths: []string{`/v0/destinations/` + tc.name},
								t:              t,
							}

							srv := httptest.NewServer(et)
							defer srv.Close()

							c := config.New()
							c.Set("Processor.maxRetry", 1)
							c.Set("DEST_TRANSFORM_URL", srv.URL)

							tr := NewTransformer(c, logger.NOP, stats.Default, WithClient(srv.Client()))

							events := append([]TransformerEvent{}, TransformerEvent{
								Metadata: Metadata{
									MessageID: msgID,
								},
								Message: map[string]interface{}{
									"src-key-1": msgID,
								},
								Destination: backendconfig.DestinationT{
									DestinationDefinition: backendconfig.DestinationDefinitionT{
										Name: tc.destinationType,
									},
									Transformations: []backendconfig.TransformationT{
										{
											ID:        "test-transformation",
											VersionID: "test-version",
										},
									},
								},
								Credentials: []Credential{
									{
										ID:       "test-credential",
										Key:      "test-key",
										Value:    "test-value",
										IsSecret: false,
									},
								},
							})

							rsp := tr.Transform(context.TODO(), events, 10)
							require.Equal(t, rsp, expectedResponse)
						})
					}
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

					tr := NewTransformer(c, logger.NOP, stats.Default, WithClient(srv.Client()))
					rsp := tr.UserTransform(context.TODO(), events, 10)
					require.Equal(t, rsp, expectedResponse)
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

					tr := NewTransformer(c, logger.NOP, stats.Default, WithClient(srv.Client()))
					rsp := tr.Validate(context.TODO(), events, 10)
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
		trackLongRunningTransformation(ctx, "stage", time.Hour, mockLogger)
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
		go trackLongRunningTransformation(ctx, "stage", time.Millisecond, mockLogger)
		for !fired.Load() {
			time.Sleep(time.Millisecond)
		}
		cancel()
	})
}

func TestTransformerEvent_Dehydrate(t *testing.T) {
	testCases := []struct {
		name     string
		event    *TransformerEvent
		expected *TransformerEvent
	}{
		{
			name: "remove connections",
			event: &TransformerEvent{
				Metadata: Metadata{},
				Message:  map[string]interface{}{},
				Connection: backendconfig.Connection{
					SourceID:      "source-id",
					DestinationID: "destination-id",
				},
				Destination: backendconfig.DestinationT{
					Transformations: make([]backendconfig.TransformationT, 0),
				},
			},
			expected: &TransformerEvent{
				Metadata:   Metadata{},
				Message:    map[string]interface{}{},
				Connection: backendconfig.Connection{},
				Destination: backendconfig.DestinationT{
					Transformations: make([]backendconfig.TransformationT, 0),
				},
			},
		},
		{
			name: "remove everything except transformations in destination",
			event: &TransformerEvent{
				Metadata: Metadata{},
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
			expected: &TransformerEvent{
				Metadata:   Metadata{},
				Message:    map[string]interface{}{},
				Connection: backendconfig.Connection{},
				Destination: backendconfig.DestinationT{
					Transformations: make([]backendconfig.TransformationT, 0),
				},
			},
		},
		{
			name: "remove everything except transformations in destination with multiple transformer versions",
			event: &TransformerEvent{
				Metadata: Metadata{},
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
			expected: &TransformerEvent{
				Metadata:   Metadata{},
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
