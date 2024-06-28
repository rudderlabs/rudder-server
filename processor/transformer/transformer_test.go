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

	"github.com/golang/mock/gomock"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/utils/types"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway/response"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
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
	t.Run("success", func(t *testing.T) {
		ft := &fakeTransformer{
			t: t,
		}

		srv := httptest.NewServer(ft)
		defer srv.Close()

		tr := handle{}
		tr.stat = stats.Default
		tr.logger = logger.NOP
		tr.conf = config.Default
		tr.client = srv.Client()
		tr.guardConcurrency = make(chan struct{}, 200)
		tr.receivedStat = tr.stat.NewStat("transformer_received", stats.CountType)
		tr.cpDownGauge = tr.stat.NewStat("control_plane_down", stats.GaugeType)
		tr.config.timeoutDuration = 1 * time.Second
		tr.config.failOnUserTransformTimeout = config.SingleValueLoader(true)
		tr.config.failOnError = config.SingleValueLoader(true)

		tr.config.maxRetry = config.SingleValueLoader(1)

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
			batchSize := tt.batchSize
			eventsCount := tt.eventsCount
			failEvery := tt.failEvery

			events := make([]TransformerEvent, eventsCount)
			expectedResponse := Response{}

			for i := range events {
				msgID := fmt.Sprintf("messageID-%d", i)
				statusCode := http.StatusOK

				if failEvery != 0 && i%failEvery == 0 {
					statusCode = http.StatusBadRequest
				}

				events[i] = TransformerEvent{
					Metadata: Metadata{
						MessageID: msgID,
					},
					Message: map[string]interface{}{
						"src-key-1":       msgID,
						"forceStatusCode": statusCode,
					},
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
					Metadata: Metadata{
						MessageID: msgID,
					},
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

			rsp := tr.transform(context.TODO(), events, srv.URL, batchSize, "test-stage")
			require.Equal(t, expectedResponse, rsp)
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
				tr.conf = config.Default
				tr.client = client
				tr.config.maxRetry = config.SingleValueLoader(tc.retries)
				tr.config.failOnUserTransformTimeout = config.SingleValueLoader(tc.failOnUserTransformTimeout)
				tr.cpDownGauge = tr.stat.NewStat("control_plane_down", stats.GaugeType)

				if tc.expectPanic {
					require.Panics(t, func() {
						_ = tr.request(context.TODO(), srv.URL, tc.stage, events)
					})
					close(ch)
					return
				}

				_, err := url.Parse(srv.URL)
				require.NoError(t, err)

				rsp := tr.request(context.TODO(), srv.URL, tc.stage, events)
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
		tr.conf = config.Default
		tr.client = srv.Client()
		tr.config.maxRetry = config.SingleValueLoader(1)
		tr.config.timeoutDuration = 1 * time.Second
		tr.config.failOnUserTransformTimeout = config.SingleValueLoader(false)
		tr.cpDownGauge = tr.stat.NewStat("control_plane_down", stats.GaugeType)

		rsp := tr.request(context.TODO(), srv.URL, "test-stage", events)
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
				tr.conf = config.Default
				tr.client = srv.Client()
				tr.config.failOnUserTransformTimeout = config.SingleValueLoader(false)
				tr.config.maxRetry = config.SingleValueLoader(tc.retries)
				tr.config.failOnError = config.SingleValueLoader(tc.failOnError)
				tr.cpDownGauge = tr.stat.NewStat("control_plane_down", stats.GaugeType)
				tr.config.timeoutDuration = 1 * time.Second

				if tc.expectPanic {
					require.Panics(t, func() {
						_ = tr.request(context.TODO(), srv.URL, "test-stage", events)
					})
					require.Equal(t, elt.retryCount, tc.expectedRetries)
					return
				}

				rsp := tr.request(context.TODO(), srv.URL, "test-stage", events)
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
				tr.client = srv.Client()
				tr.stat = stats.Default
				tr.conf = config.Default
				tr.logger = logger.NOP
				tr.cpDownGauge = tr.stat.NewStat("control_plane_down", stats.GaugeType)
				tr.config.maxRetry = config.SingleValueLoader(1)
				tr.config.timeoutDuration = 1 * time.Second

				if tc.expectPanic {
					require.Panics(t, func() {
						_ = tr.request(context.TODO(), srv.URL, "test-stage", events)
					})
					return
				}

				rsp := tr.request(context.TODO(), srv.URL, "test-stage", events)
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

		t.Run("version", func(t *testing.T) {
			config.Reset()
			defer config.Reset()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, r.URL.Path, "/transformerBuildVersion")

				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("1.34.0"))
			}))
			defer srv.Close()

			config.Set("DEST_TRANSFORM_URL", srv.URL)

			require.Equal(t, GetVersion(), "1.34.0")
		})
	})
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
