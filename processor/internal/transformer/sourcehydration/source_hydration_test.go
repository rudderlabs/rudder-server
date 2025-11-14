package sourcehydration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/processor/types"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/sourcehydration"
)

func TestSourceHydration_Hydrate(t *testing.T) {
	ctx := context.Background()
	t.Run("successful hydration", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Send back the same events with status code 200
			var req types.SrcHydrationRequest
			err := jsonrs.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			response := types.SrcHydrationResponse{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = jsonrs.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		events := []types.SrcHydrationEvent{
			{
				ID: "1",
				Event: map[string]interface{}{
					"test": "event1",
				},
			},
			{
				ID: "2",
				Event: map[string]interface{}{
					"test": "event2",
				},
			},
		}

		source := types.SrcHydrationSource{
			ID:             "source-id",
			WorkspaceID:    "workspace-id",
			Config:         []byte("{}"),
			InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := types.SrcHydrationRequest{
			Batch:  events,
			Source: source,
		}

		resp, err := client.Hydrate(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.Batch, 2)
		require.Equal(t, events[0].ID, resp.Batch[0].ID)
		require.Equal(t, events[0].Event, resp.Batch[0].Event)
		require.Equal(t, events[1].ID, resp.Batch[1].ID)
		require.Equal(t, events[1].Event, resp.Batch[1].Event)
	})

	t.Run("empty batch", func(t *testing.T) {
		conf := config.New()
		client := sourcehydration.New(conf, logger.NOP, stats.NOP)
		resp, err := client.Hydrate(ctx, types.SrcHydrationRequest{})
		require.NoError(t, err)
		require.Len(t, resp.Batch, 0)
	})

	t.Run("batching", func(t *testing.T) {
		var receivedBatches [][]types.SrcHydrationEvent

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req types.SrcHydrationRequest
			err := jsonrs.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			receivedBatches = append(receivedBatches, req.Batch)

			response := types.SrcHydrationResponse{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = jsonrs.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		conf.Set("Processor.SourceHydration.batchSize", 2)
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		// Create 5 events, expect them to be split into batches of 2
		var events []types.SrcHydrationEvent
		for i := 0; i < 5; i++ {
			events = append(events, types.SrcHydrationEvent{
				ID: fmt.Sprintf("%d", i),
				Event: map[string]interface{}{
					"test": fmt.Sprintf("event%d", i),
				},
			})
		}

		source := types.SrcHydrationSource{
			ID:             "source-id",
			WorkspaceID:    "workspace-id",
			Config:         []byte("{}"),
			InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := types.SrcHydrationRequest{
			Batch:  events,
			Source: source,
		}

		resp, err := client.Hydrate(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.Batch, 5)

		// Check batching worked correctly (3 batches: 2, 2, 1)
		require.Len(t, receivedBatches, 3)

		for _, batch := range receivedBatches {
			if len(batch) == 1 {
				require.Equal(t, "4", batch[0].ID)
			} else {
				require.Len(t, batch, 2)
			}
		}
	})

	t.Run("transformer error response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("Internal Server Error"))
		}))
		defer server.Close()

		testFn := func(failOnError bool) {
			conf := config.New()
			conf.Set("DEST_TRANSFORM_URL", server.URL)
			conf.Set("Processor.SourceHydration.maxRetry", 2)
			conf.Set("Processor.SourceHydration.maxRetryBackoffInterval", time.Millisecond*10)
			conf.Set("Processor.SourceHydration.failOnError", failOnError)
			client := sourcehydration.New(conf, logger.NOP, stats.NOP)

			events := []types.SrcHydrationEvent{
				{
					ID: "1",
					Event: map[string]interface{}{
						"test": "event1",
					},
				},
			}

			source := types.SrcHydrationSource{
				ID:             "source-id",
				WorkspaceID:    "workspace-id",
				Config:         []byte("{}"),
				InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
				SourceDefinition: backendconfig.SourceDefinitionT{
					Name: "TestSource",
				},
			}

			req := types.SrcHydrationRequest{
				Batch:  events,
				Source: source,
			}

			if failOnError {
				resp, err := client.Hydrate(ctx, req)
				require.Error(t, err)
				require.Len(t, resp.Batch, 0)
			} else {
				require.Panics(t, func() {
					_, _ = client.Hydrate(ctx, req)
				})
			}
		}
		t.Run("failOnError", func(t *testing.T) {
			testFn(true)
		})
		t.Run("no failOnError - panic", func(t *testing.T) {
			testFn(false)
		})
	})

	t.Run("network error with retry", func(t *testing.T) {
		attempts := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts++
			if attempts < 3 {
				// Fail first two attempts
				http.Error(w, "Temporary error", http.StatusServiceUnavailable)
				return
			}
			// Succeed on third attempt
			var req types.SrcHydrationRequest
			err := jsonrs.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			response := types.SrcHydrationResponse{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = jsonrs.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		conf.Set("Processor.SourceHydration.maxRetry", 5)
		conf.Set("Processor.SourceHydration.maxRetryBackoffInterval", time.Millisecond*10)
		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		events := []types.SrcHydrationEvent{
			{
				ID: "1",
				Event: map[string]interface{}{
					"test": "event1",
				},
			},
		}

		source := types.SrcHydrationSource{
			ID:             "source-id",
			WorkspaceID:    "workspace-id",
			Config:         []byte("{}"),
			InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := types.SrcHydrationRequest{
			Batch:  events,
			Source: source,
		}

		resp, err := client.Hydrate(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.Batch, 1)
		require.Equal(t, 3, attempts) // Should have retried twice before succeeding
	})

	t.Run("context cancellation", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Simulate slow response
			time.Sleep(100 * time.Millisecond)

			var req types.SrcHydrationRequest
			err := jsonrs.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			response := types.SrcHydrationResponse{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = jsonrs.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)

		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		events := []types.SrcHydrationEvent{
			{
				ID: "1",
				Event: map[string]interface{}{
					"test": "event1",
				},
			},
		}

		source := types.SrcHydrationSource{
			ID:             "source-id",
			WorkspaceID:    "workspace-id",
			Config:         []byte("{}"),
			InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := types.SrcHydrationRequest{
			Batch:  events,
			Source: source,
		}

		// Create a context that times out quickly
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		resp, err := client.Hydrate(canceledCtx, req)
		require.Error(t, err)
		require.Len(t, resp.Batch, 0)
	})

	t.Run("url construction", func(t *testing.T) {
		var receivedURL string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedURL = r.URL.Path

			var req types.SrcHydrationRequest
			err := jsonrs.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			response := types.SrcHydrationResponse{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = jsonrs.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)

		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		events := []types.SrcHydrationEvent{
			{
				ID: "1",
				Event: map[string]interface{}{
					"test": "event1",
				},
			},
		}

		source := types.SrcHydrationSource{
			ID:             "source-id",
			WorkspaceID:    "workspace-id",
			Config:         []byte("{}"),
			InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := types.SrcHydrationRequest{
			Batch:  events,
			Source: source,
		}

		_, err := client.Hydrate(ctx, req)
		require.NoError(t, err)
		require.Equal(t, "/v2/sources/testsource/hydrate", receivedURL)
	})
}

func TestSourceHydration_ErrorResponses(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		statusCode     int
		responseBody   string
		expectPanic    bool
		expectedLength int
	}{
		{
			name:           "bad request error",
			statusCode:     http.StatusBadRequest,
			responseBody:   "Bad Request",
			expectPanic:    false,
			expectedLength: 0,
		},
		{
			name:           "not found error",
			statusCode:     http.StatusNotFound,
			responseBody:   "Not Found",
			expectPanic:    false,
			expectedLength: 0,
		},
		{
			name:           "internal server error",
			statusCode:     http.StatusInternalServerError,
			responseBody:   "Internal Server Error",
			expectPanic:    true,
			expectedLength: 0,
		},
		{
			name:           "service unavailable",
			statusCode:     http.StatusServiceUnavailable,
			responseBody:   "Service Unavailable",
			expectPanic:    true,
			expectedLength: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(tt.responseBody))
			}))
			defer server.Close()

			conf := config.New()
			conf.Set("DEST_TRANSFORM_URL", server.URL)
			conf.Set("Processor.SourceHydration.maxRetry", 1)
			conf.Set("Processor.SourceHydration.maxRetryBackoffInterval", time.Millisecond*10)

			if tt.expectPanic {
				conf.Set("Processor.SourceHydration.failOnError", false)
			} else {
				conf.Set("Processor.SourceHydration.failOnError", true)
			}

			client := sourcehydration.New(conf, logger.NOP, stats.NOP)

			events := []types.SrcHydrationEvent{
				{
					ID: "1",
					Event: map[string]interface{}{
						"test": "event1",
					},
				},
			}

			source := types.SrcHydrationSource{
				ID:             "source-id",
				WorkspaceID:    "workspace-id",
				Config:         []byte("{}"),
				InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
				SourceDefinition: backendconfig.SourceDefinitionT{
					Name: "TestSource",
				},
			}

			req := types.SrcHydrationRequest{
				Batch:  events,
				Source: source,
			}

			if tt.expectPanic {
				require.Panics(t, func() {
					_, _ = client.Hydrate(ctx, req)
				})
			} else {
				resp, err := client.Hydrate(ctx, req)
				require.Error(t, err)
				require.Len(t, resp.Batch, tt.expectedLength)
			}
		})
	}
}

func TestSourceHydration_MalformedResponse(t *testing.T) {
	ctx := context.Background()

	t.Run("invalid JSON response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("{ invalid json }"))
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		conf.Set("Processor.SourceHydration.maxRetry", 1)
		conf.Set("Processor.SourceHydration.maxRetryBackoffInterval", time.Millisecond*10)
		conf.Set("Processor.SourceHydration.failOnError", false)

		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		events := []types.SrcHydrationEvent{
			{
				ID: "1",
				Event: map[string]interface{}{
					"test": "event1",
				},
			},
		}

		source := types.SrcHydrationSource{
			ID:             "source-id",
			WorkspaceID:    "workspace-id",
			Config:         []byte("{}"),
			InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := types.SrcHydrationRequest{
			Batch:  events,
			Source: source,
		}

		require.Panics(t, func() {
			_, _ = client.Hydrate(ctx, req)
		})
	})

	t.Run("unexpected response format", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			// Return unexpected format (missing batch field)
			_, _ = w.Write([]byte(`{"message": "success"}`))
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		conf.Set("Processor.SourceHydration.maxRetry", 1)
		conf.Set("Processor.SourceHydration.maxRetryBackoffInterval", time.Millisecond*10)
		conf.Set("Processor.SourceHydration.failOnError", false)

		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		events := []types.SrcHydrationEvent{
			{
				ID: "1",
				Event: map[string]interface{}{
					"test": "event1",
				},
			},
		}

		source := types.SrcHydrationSource{
			ID:             "source-id",
			WorkspaceID:    "workspace-id",
			Config:         []byte("{}"),
			InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := types.SrcHydrationRequest{
			Batch:  events,
			Source: source,
		}

		require.Panics(t, func() {
			_, _ = client.Hydrate(ctx, req)
		})
	})
}

func TestSourceHydration_Timeout(t *testing.T) {
	ctx := context.Background()

	t.Run("timeout behavior", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Sleep longer than the configured timeout
			time.Sleep(200 * time.Millisecond)

			var req types.SrcHydrationRequest
			err := jsonrs.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			response := types.SrcHydrationResponse{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = jsonrs.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		conf.Set("HttpClient.procTransformer.timeout", 100*time.Millisecond)
		conf.Set("Processor.SourceHydration.maxRetry", 1)
		conf.Set("Processor.SourceHydration.maxRetryBackoffInterval", time.Millisecond*10)

		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		events := []types.SrcHydrationEvent{
			{
				ID: "1",
				Event: map[string]interface{}{
					"test": "event1",
				},
			},
		}

		source := types.SrcHydrationSource{
			ID:             "source-id",
			WorkspaceID:    "workspace-id",
			Config:         []byte("{}"),
			InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := types.SrcHydrationRequest{
			Batch:  events,
			Source: source,
		}

		require.Panics(t, func() {
			_, _ = client.Hydrate(ctx, req)
		})
	})
}

func TestSourceHydration_BatchEdgeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("very large batch", func(t *testing.T) {
		var receivedBatches [][]types.SrcHydrationEvent

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req types.SrcHydrationRequest
			err := jsonrs.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			receivedBatches = append(receivedBatches, req.Batch)

			response := types.SrcHydrationResponse{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = jsonrs.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		conf.Set("Processor.SourceHydration.batchSize", 100)

		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		// Create 500 events to test large batch splitting
		var events []types.SrcHydrationEvent
		for i := 0; i < 500; i++ {
			events = append(events, types.SrcHydrationEvent{
				ID: fmt.Sprintf("%d", i),
				Event: map[string]interface{}{
					"test": fmt.Sprintf("event%d", i),
				},
			})
		}

		source := types.SrcHydrationSource{
			ID:             "source-id",
			WorkspaceID:    "workspace-id",
			Config:         []byte("{}"),
			InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := types.SrcHydrationRequest{
			Batch:  events,
			Source: source,
		}

		resp, err := client.Hydrate(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.Batch, 500)

		// Check batching worked correctly (5 batches of 100 each)
		require.Len(t, receivedBatches, 5)
		for _, batch := range receivedBatches {
			require.Len(t, batch, 100)
		}
	})

	t.Run("exact batch size boundaries", func(t *testing.T) {
		var receivedBatches [][]types.SrcHydrationEvent

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req types.SrcHydrationRequest
			err := jsonrs.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			receivedBatches = append(receivedBatches, req.Batch)

			response := types.SrcHydrationResponse{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = jsonrs.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		conf.Set("Processor.SourceHydration.batchSize", 5)

		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		// Create exactly 10 events - exactly 2 batches
		var events []types.SrcHydrationEvent
		for i := 0; i < 10; i++ {
			events = append(events, types.SrcHydrationEvent{
				ID: fmt.Sprintf("%d", i),
				Event: map[string]interface{}{
					"test": fmt.Sprintf("event%d", i),
				},
			})
		}

		source := types.SrcHydrationSource{
			ID:             "source-id",
			WorkspaceID:    "workspace-id",
			Config:         []byte("{}"),
			InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := types.SrcHydrationRequest{
			Batch:  events,
			Source: source,
		}

		resp, err := client.Hydrate(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.Batch, 10)

		// Check batching worked correctly (exactly 2 batches of 5 each)
		require.Len(t, receivedBatches, 2)
		for _, batch := range receivedBatches {
			require.Len(t, batch, 5)
		}
	})
}

func TestSourceHydration_SourceDefinitions(t *testing.T) {
	ctx := context.Background()

	t.Run("url construction with various source names", func(t *testing.T) {
		testCases := []struct {
			sourceName   string
			expectedPath string
		}{
			{"Webhook", "/v2/sources/webhook/hydrate"},
			{"WebHook", "/v2/sources/webhook/hydrate"},
			{"WEBHOOK", "/v2/sources/webhook/hydrate"},
			{"webhook-source", "/v2/sources/webhook-source/hydrate"},
			{"Webhook_Source", "/v2/sources/webhook_source/hydrate"},
			{"Webhook123", "/v2/sources/webhook123/hydrate"},
		}

		for _, tc := range testCases {
			t.Run(tc.sourceName, func(t *testing.T) {
				var receivedURL string

				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					receivedURL = r.URL.Path

					var req types.SrcHydrationRequest
					err := jsonrs.NewDecoder(r.Body).Decode(&req)
					require.NoError(t, err)

					response := types.SrcHydrationResponse{
						Batch: req.Batch,
					}

					w.WriteHeader(http.StatusOK)
					err = jsonrs.NewEncoder(w).Encode(response)
					require.NoError(t, err)
				}))
				defer server.Close()

				conf := config.New()
				conf.Set("DEST_TRANSFORM_URL", server.URL)

				client := sourcehydration.New(conf, logger.NOP, stats.NOP)

				events := []types.SrcHydrationEvent{
					{
						ID: "1",
						Event: map[string]interface{}{
							"test": "event1",
						},
					},
				}

				source := types.SrcHydrationSource{
					ID:             "source-id",
					WorkspaceID:    "workspace-id",
					Config:         []byte("{}"),
					InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
					SourceDefinition: backendconfig.SourceDefinitionT{
						Name: tc.sourceName,
					},
				}

				req := types.SrcHydrationRequest{
					Batch:  events,
					Source: source,
				}

				_, err := client.Hydrate(ctx, req)
				require.NoError(t, err)
				require.Equal(t, tc.expectedPath, receivedURL)
			})
		}
	})
}

func TestSourceHydration_ConcurrentRequests(t *testing.T) {
	ctx := context.Background()

	t.Run("multiple concurrent hydrate calls", func(t *testing.T) {
		var mu sync.Mutex
		requestCounts := make(map[string]int)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requestCounts[r.URL.Path]++
			mu.Unlock()

			var req types.SrcHydrationRequest
			err := jsonrs.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			response := types.SrcHydrationResponse{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = jsonrs.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		conf.Set("Processor.SourceHydration.batchSize", 2)

		client := sourcehydration.New(conf, logger.NOP, stats.NOP, sourcehydration.WithClient(server.Client()))

		// Create multiple sources
		sources := []types.SrcHydrationSource{
			{
				ID:             "source-1",
				WorkspaceID:    "workspace-1",
				Config:         []byte("{}"),
				InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
				SourceDefinition: backendconfig.SourceDefinitionT{
					Name: "TestSource1",
				},
			},
			{
				ID:          "source-2",
				WorkspaceID: "workspace-2",
				Config:      []byte("{}"),
				SourceDefinition: backendconfig.SourceDefinitionT{
					Name: "TestSource2",
				},
			},
			{
				ID:          "source-3",
				WorkspaceID: "workspace-3",
				Config:      []byte("{}"),
				SourceDefinition: backendconfig.SourceDefinitionT{
					Name: "TestSource3",
				},
			},
		}

		// Create events for each source
		var requests []types.SrcHydrationRequest
		for i, source := range sources {
			var events []types.SrcHydrationEvent
			for j := 0; j < 3; j++ {
				events = append(events, types.SrcHydrationEvent{
					ID: fmt.Sprintf("%d-%d", i, j),
					Event: map[string]interface{}{
						"source": source.ID,
						"event":  fmt.Sprintf("event%d-%d", i, j),
					},
				})
			}
			requests = append(requests, types.SrcHydrationRequest{
				Batch:  events,
				Source: source,
			})
		}

		// Execute concurrent requests
		var wg sync.WaitGroup
		results := make([]types.SrcHydrationResponse, len(requests))
		errors := make([]error, len(requests))

		for i, req := range requests {
			wg.Add(1)
			go func(index int, request types.SrcHydrationRequest) {
				defer wg.Done()
				results[index], errors[index] = client.Hydrate(ctx, request)
			}(i, req)
		}

		wg.Wait()

		// Check results
		for i, err := range errors {
			require.NoError(t, err, "request %d failed", i)
			require.Len(t, results[i].Batch, 3, "request %d should have 3 events", i)
		}

		// Check that all requests were processed
		mu.Lock()
		require.Equal(t, 3, len(requestCounts), "should have received requests for 3 different sources")
		for sourceID := range requestCounts {
			// since batch size is 2 and we have 3 events per source, each source should have received 2 requests
			require.Equal(t, 2, requestCounts[sourceID], "should have received 2 request for source %s", sourceID)
		}
		mu.Unlock()
	})
}

func TestSourceHydration_EmptyNilEvents(t *testing.T) {
	ctx := context.Background()

	t.Run("events with empty event data", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req types.SrcHydrationRequest
			err := jsonrs.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			response := types.SrcHydrationResponse{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = jsonrs.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		events := []types.SrcHydrationEvent{
			{
				ID:    "1",
				Event: nil, // Nil event
			},
			{
				ID:    "2",
				Event: map[string]interface{}{}, // Empty event
			},
			{
				ID: "3",
				Event: map[string]interface{}{
					"test": "valid-event",
				},
			},
		}

		source := types.SrcHydrationSource{
			ID:             "source-id",
			WorkspaceID:    "workspace-id",
			Config:         []byte("{}"),
			InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := types.SrcHydrationRequest{
			Batch:  events,
			Source: source,
		}

		resp, err := client.Hydrate(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.Batch, 3)
		require.Equal(t, "1", resp.Batch[0].ID)
		require.Nil(t, resp.Batch[0].Event)
		require.Equal(t, "2", resp.Batch[1].ID)
		require.Empty(t, resp.Batch[1].Event)
		require.Equal(t, "3", resp.Batch[2].ID)
		require.Equal(t, map[string]interface{}{"test": "valid-event"}, resp.Batch[2].Event)
	})
}

func TestSourceHydration_MetricsTracking(t *testing.T) {
	ctx := context.Background()

	t.Run("stats are properly recorded", func(t *testing.T) {
		var mu sync.Mutex
		requestCounts := make(map[string]int)
		requestBytes := make(map[string]int)
		responseBytes := make(map[string]int)
		requestDurations := make(map[string][]time.Duration)

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requestCounts[r.URL.Path]++
			// Read request body to get size
			body, _ := io.ReadAll(r.Body)
			requestBytes[r.URL.Path] += len(body)
			mu.Unlock()

			var req types.SrcHydrationRequest
			err := jsonrs.NewDecoder(bytes.NewReader(body)).Decode(&req)
			require.NoError(t, err)

			response := types.SrcHydrationResponse{
				Batch: req.Batch,
			}

			// Record response size
			responseData, _ := jsonrs.Marshal(response)
			mu.Lock()
			responseBytes[r.URL.Path] += len(responseData)
			mu.Unlock()

			start := time.Now()
			w.WriteHeader(http.StatusOK)
			err = jsonrs.NewEncoder(w).Encode(response)
			require.NoError(t, err)

			mu.Lock()
			requestDurations[r.URL.Path] = append(requestDurations[r.URL.Path], time.Since(start))
			mu.Unlock()
		}))
		defer server.Close()

		// Create a stats store to capture metrics
		statsStore, err := memstats.New()
		require.NoError(t, err)

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		conf.Set("Processor.SourceHydration.batchSize", 2)
		conf.Set("Processor.SourceHydration.maxRetry", 1)

		client := sourcehydration.New(conf, logger.NOP, statsStore)

		// Create 5 events, expect them to be split into batches of 2
		var events []types.SrcHydrationEvent
		for i := 0; i < 5; i++ {
			events = append(events, types.SrcHydrationEvent{
				ID: fmt.Sprintf("%d", i),
				Event: map[string]interface{}{
					"test": fmt.Sprintf("event%d", i),
				},
			})
		}

		source := types.SrcHydrationSource{
			ID:             "source-id",
			WorkspaceID:    "workspace-id",
			Config:         []byte("{}"),
			InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := types.SrcHydrationRequest{
			Batch:  events,
			Source: source,
		}

		resp, err := client.Hydrate(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.Batch, 5)

		labels := types.TransformerMetricLabels{
			Stage:       "source_hydration",
			SourceID:    source.ID,
			WorkspaceID: source.WorkspaceID,
			SourceType:  source.SourceDefinition.Name,
		}

		// Check that stats were recorded
		// Check request batch count stat
		batchCountStat := statsStore.Get(
			"processor_transformer_request_batch_count",
			labels.ToStatsTag(),
		)
		require.NotNil(t, batchCountStat)
		require.EqualValues(t, 3, batchCountStat.LastValue()) // 3 batches of 2, 2, 1

		// Check sent events stat
		sentStat := statsStore.Get("processor_transformer_sent", stats.Tags{})
		require.NotNil(t, sentStat)
		require.EqualValues(t, 5, sentStat.LastValue()) // 5 events sent

		// Check received events stat
		receivedStat := statsStore.Get("processor_transformer_received", stats.Tags{})
		require.NotNil(t, receivedStat)
		require.EqualValues(t, 5, receivedStat.LastValue()) // 5 events received

		// Check per-request stats
		totalEventsStat := statsStore.Get(
			"transformer_client_request_total_events",
			labels.ToStatsTag(),
		)
		require.NotNil(t, totalEventsStat)
		require.EqualValues(t, 5, totalEventsStat.LastValue()) // Total of 5 events across all requests

		responseEventsStat := statsStore.Get(
			"transformer_client_response_total_events",
			labels.ToStatsTag(),
		)
		require.NotNil(t, responseEventsStat)
		require.EqualValues(t, 5, responseEventsStat.LastValue()) // Total of 5 events in responses
	})
}

func TestSourceHydration_PartialBatchFailures(t *testing.T) {
	ctx := context.Background()

	t.Run("some batches fail while others succeed", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req types.SrcHydrationRequest
			err := jsonrs.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			// Fail the batch with 1 event, succeed others
			if len(req.Batch) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("Internal Server Error"))
				return
			}

			response := types.SrcHydrationResponse{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = jsonrs.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		conf.Set("Processor.SourceHydration.batchSize", 2)
		conf.Set("Processor.SourceHydration.maxRetry", 2)
		conf.Set("Processor.SourceHydration.failOnError", true)

		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		// Create 5 events, split into 3 batches: [0,1], [2,3], [4]
		var events []types.SrcHydrationEvent
		for i := 0; i < 5; i++ {
			events = append(events, types.SrcHydrationEvent{
				ID: fmt.Sprintf("%d", i),
				Event: map[string]interface{}{
					"test": fmt.Sprintf("event%d", i),
				},
			})
		}

		source := types.SrcHydrationSource{
			ID:             "source-id",
			WorkspaceID:    "workspace-id",
			Config:         []byte("{}"),
			InternalSecret: []byte(`{"pageAccessToken": "some-page-access-token"}`),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := types.SrcHydrationRequest{
			Batch:  events,
			Source: source,
		}

		resp, err := client.Hydrate(ctx, req)
		require.Error(t, err)

		require.Len(t, resp.Batch, 0)
	})
}
