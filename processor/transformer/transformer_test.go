package transformer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"golang.org/x/exp/slices"

	"github.com/rudderlabs/rudder-server/utils/types"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/gateway/response"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
)

type fakeTransformer struct {
	requests [][]TransformerEventT
	t        testing.TB
}

func (t *fakeTransformer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var reqBody []TransformerEventT
	require.NoError(t.t, json.NewDecoder(r.Body).Decode(&reqBody))

	t.requests = append(t.requests, reqBody)

	responses := make([]TransformerResponseT, len(reqBody))

	for i := range reqBody {
		statusCode := int(reqBody[i].Message["forceStatusCode"].(float64))
		delete(reqBody[i].Message, "forceStatusCode")
		reqBody[i].Message["echo-key-1"] = reqBody[i].Message["src-key-1"]

		responses[i] = TransformerResponseT{
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

	var reqBody []TransformerEventT
	require.NoError(elt.t, json.NewDecoder(r.Body).Decode(&reqBody))

	responses := make([]TransformerResponseT, len(reqBody))

	if elt.retryCount < elt.maxRetryCount {
		http.Error(w, response.MakeResponse(elt.statusError), elt.statusCode)
		return
	}

	for i := range reqBody {
		responses[i] = TransformerResponseT{
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

	var reqBody []TransformerEventT
	require.NoError(et.t, json.NewDecoder(r.Body).Decode(&reqBody))

	responses := make([]TransformerResponseT, len(reqBody))

	for i := range reqBody {
		responses[i] = TransformerResponseT{
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
	Init()

	t.Run("success", func(t *testing.T) {
		ft := &fakeTransformer{
			t: t,
		}

		srv := httptest.NewServer(ft)
		defer srv.Close()

		tr := NewTransformer()
		tr.Client = srv.Client()

		tr.Setup(config.Default, logger.NOP, stats.Default)

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

			events := make([]TransformerEventT, eventsCount)
			expectedResponse := ResponseT{}

			for i := range events {
				msgID := fmt.Sprintf("messageID-%d", i)
				statusCode := http.StatusOK

				if failEvery != 0 && i%failEvery == 0 {
					statusCode = http.StatusBadRequest
				}

				events[i] = TransformerEventT{
					Metadata: MetadataT{
						MessageID: msgID,
					},
					Message: map[string]interface{}{
						"src-key-1":       msgID,
						"forceStatusCode": statusCode,
					},
				}

				tResp := TransformerResponseT{
					Metadata: MetadataT{
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
		events := append([]TransformerEventT{}, TransformerEventT{
			Metadata: MetadataT{
				MessageID: msgID,
			},
			Message: map[string]interface{}{
				"src-key-1": msgID,
			},
		})

		testCases := []struct {
			name             string
			retries          int
			expectedRetries  int
			expectPanic      bool
			stage            string
			expectedResponse []TransformerResponseT
			failOnTimeout    bool
		}{
			{
				name:          "user transformation timeout",
				retries:       3,
				stage:         UserTransformerStage,
				expectPanic:   true,
				failOnTimeout: false,
			},
			{
				name:        "user transformation timeout with fail on timeout",
				retries:     3,
				stage:       UserTransformerStage,
				expectPanic: false,
				expectedResponse: []TransformerResponseT{
					{
						Metadata: MetadataT{
							MessageID: msgID,
						},
						StatusCode: TransformerRequestTimeout,
						Output: map[string]interface{}{
							"src-key-1": msgID,
						},
					},
				},
				failOnTimeout: true,
			},
			{
				name:          "destination transformation timeout",
				retries:       3,
				stage:         DestTransformerStage,
				expectPanic:   true,
				failOnTimeout: false,
			},
			{
				name:          "destination transformation timeout with fail on timeout",
				retries:       3,
				stage:         DestTransformerStage,
				expectPanic:   true,
				failOnTimeout: true,
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

				c := config.New()
				c.Set("Processor.maxRetry", tc.retries)
				c.Set("Processor.Transformer.failOnTimeout", tc.failOnTimeout)

				tr := NewTransformer()
				tr.Client = srv.Client()
				tr.Client.Timeout = 1 * time.Millisecond
				tr.Setup(c, logger.NOP, stats.Default)

				if tc.expectPanic {
					require.Panics(t, func() {
						_ = tr.request(context.TODO(), srv.URL, tc.stage, events)
					})
					close(ch)
					return
				}

				u, err := url.Parse(srv.URL)
				require.NoError(t, err)

				rsp := tr.request(context.TODO(), srv.URL, tc.stage, events)
				require.Len(t, rsp, 1)
				require.Equal(t, rsp[0].StatusCode, TransformerRequestTimeout)
				require.Equal(t, rsp[0].Metadata, MetadataT{
					MessageID: msgID,
				})
				require.Equal(t, rsp[0].Error, errors.New(`transformer request timed out: Post "http://127.0.0.1:`+u.Port()+`": context deadline exceeded (Client.Timeout exceeded while awaiting headers)`).Error())
				close(ch)
			})
		}
	})

	t.Run("endless retries in case of control plane down", func(t *testing.T) {
		msgID := "messageID-0"
		events := append([]TransformerEventT{}, TransformerEventT{
			Metadata: MetadataT{
				MessageID: msgID,
			},
			Message: map[string]interface{}{
				"src-key-1": msgID,
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

		c := config.New()
		c.Set("Processor.maxRetry", 1)

		tr := NewTransformer()
		tr.Client = srv.Client()
		tr.Setup(c, logger.NOP, stats.Default)

		rsp := tr.request(context.TODO(), srv.URL, "test-stage", events)
		require.Equal(t, rsp, []TransformerResponseT{
			{
				Metadata: MetadataT{
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
		events := append([]TransformerEventT{}, TransformerEventT{
			Metadata: MetadataT{
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
		})

		testCases := []struct {
			name             string
			retries          int
			maxRetryCount    int
			statusCode       int
			statusError      string
			expectedRetries  int
			expectPanic      bool
			expectedResponse []TransformerResponseT
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
				expectedResponse: []TransformerResponseT{
					{
						Metadata: MetadataT{
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
				expectedResponse: []TransformerResponseT{
					{
						Metadata: MetadataT{
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

				c := config.New()
				c.Set("Processor.maxRetry", tc.retries)
				c.Set("Processor.Transformer.failOnError", tc.failOnError)

				tr := NewTransformer()
				tr.Client = srv.Client()
				tr.Setup(c, logger.NOP, stats.Default)

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
		events := append([]TransformerEventT{}, TransformerEventT{
			Metadata: MetadataT{
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
		})

		testCases := []struct {
			name             string
			apiVersion       int
			skipApiVersion   bool
			expectPanic      bool
			expectedResponse []TransformerResponseT
		}{
			{
				name:        "compatible api version",
				apiVersion:  types.SupportedTransformerApiVersion,
				expectPanic: false,
				expectedResponse: []TransformerResponseT{
					{
						Metadata: MetadataT{
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

				tr := NewTransformer()
				tr.Client = srv.Client()
				tr.Setup(config.Default, logger.NOP, stats.Default)

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
		expectedResponse := ResponseT{
			Events: []TransformerResponseT{
				{
					Output: map[string]interface{}{
						"src-key-1": msgID,
					},
					Metadata: MetadataT{
						MessageID: msgID,
					},
					StatusCode: http.StatusOK,
				},
			},
		}
		events := append([]TransformerEventT{}, TransformerEventT{
			Metadata: MetadataT{
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

			tr := NewTransformer()
			tr.Client = srv.Client()
			tr.Setup(c, logger.NOP, stats.Default)

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

					tr := NewTransformer()
					tr.Client = srv.Client()
					tr.Setup(c, logger.NOP, stats.Default)

					events := append([]TransformerEventT{}, TransformerEventT{
						Metadata: MetadataT{
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

			tr := NewTransformer()
			tr.Client = srv.Client()
			tr.Setup(c, logger.NOP, stats.Default)

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

			tr := NewTransformer()
			tr.Client = srv.Client()
			tr.Setup(c, logger.NOP, stats.Default)

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
