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
			Error:      "",
		}
		if statusCode >= 400 {
			responses[i].Error = "error"
		}
	}

	w.Header().Set("apiVersion", "2")

	require.NoError(t.t, json.NewEncoder(w).Encode(responses))
}

type endlessLoopTransformer struct {
	retryCount    int
	maxRetryCount int

	apiVersion int

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

	w.Header().Set("apiVersion", strconv.Itoa(elt.apiVersion))

	require.NoError(elt.t, json.NewEncoder(w).Encode(responses))
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

			rsp := tr.Transform(context.TODO(), events, srv.URL, batchSize, "test-stage")
			require.Equal(t, expectedResponse, rsp)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		msgID := "messageID-0"
		events := make([]TransformerEventT, 1)
		events[0] = TransformerEventT{
			Metadata: MetadataT{
				MessageID: msgID,
			},
			Message: map[string]interface{}{
				"src-key-1": msgID,
			},
		}

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

	t.Run("endless in case of control plane down", func(t *testing.T) {
		msgID := "messageID-0"
		events := make([]TransformerEventT, 1)
		events[0] = TransformerEventT{
			Metadata: MetadataT{
				MessageID: msgID,
			},
			Message: map[string]interface{}{
				"src-key-1": msgID,
			},
		}

		elt := &endlessLoopTransformer{
			maxRetryCount: 3,
			statusCode:    StatusCPDown,
			statusError:   "control plane not reachable",
			apiVersion:    2,
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
		events := make([]TransformerEventT, 1)
		events[0] = TransformerEventT{
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
		}

		testCases := []struct {
			name             string
			retries          int
			maxRetryCount    int
			statusCode       int
			apiVersion       int
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
				apiVersion:      2,
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
				apiVersion:      2,
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
				apiVersion:      2,
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
			{
				name:            "incompatible api version",
				retries:         30,
				maxRetryCount:   0,
				apiVersion:      1,
				expectedRetries: 1,
				expectPanic:     true,
			},
		}

		for _, tc := range testCases {
			tc := tc

			t.Run(tc.name, func(t *testing.T) {
				elt := &endlessLoopTransformer{
					maxRetryCount: tc.maxRetryCount,
					statusCode:    tc.statusCode,
					statusError:   tc.statusError,
					apiVersion:    tc.apiVersion,
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
}
