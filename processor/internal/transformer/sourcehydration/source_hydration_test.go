package sourcehydration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/sourcehydration"
)

func TestSourceHydration_Hydrate(t *testing.T) {
	ctx := context.Background()
	t.Run("successful hydration", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Send back the same events with status code 200
			var req sourcehydration.Request
			err := json.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			response := sourcehydration.Response{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = json.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		events := []sourcehydration.HydrationEvent{
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

		source := sourcehydration.Source{
			ID:          "source-id",
			WorkspaceID: "workspace-id",
			Config:      []byte("{}"),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := sourcehydration.Request{
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
		resp, err := client.Hydrate(ctx, sourcehydration.Request{})
		require.NoError(t, err)
		require.Len(t, resp.Batch, 0)
	})

	t.Run("batching", func(t *testing.T) {
		var receivedBatches [][]sourcehydration.HydrationEvent

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req sourcehydration.Request
			err := json.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			receivedBatches = append(receivedBatches, req.Batch)

			response := sourcehydration.Response{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = json.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		conf.Set("Processor.SourceHydration.batchSize", 2)
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		// Create 5 events, expect them to be split into batches of 2
		var events []sourcehydration.HydrationEvent
		for i := 0; i < 5; i++ {
			events = append(events, sourcehydration.HydrationEvent{
				ID: fmt.Sprintf("%d", i),
				Event: map[string]interface{}{
					"test": fmt.Sprintf("event%d", i),
				},
			})
		}

		source := sourcehydration.Source{
			ID:          "source-id",
			WorkspaceID: "workspace-id",
			Config:      []byte("{}"),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := sourcehydration.Request{
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

			events := []sourcehydration.HydrationEvent{
				{
					ID: "1",
					Event: map[string]interface{}{
						"test": "event1",
					},
				},
			}

			source := sourcehydration.Source{
				ID:          "source-id",
				WorkspaceID: "workspace-id",
				Config:      []byte("{}"),
				SourceDefinition: backendconfig.SourceDefinitionT{
					Name: "TestSource",
				},
			}

			req := sourcehydration.Request{
				Batch:  events,
				Source: source,
			}

			if failOnError {
				resp, err := client.Hydrate(ctx, req)
				require.NoError(t, err)
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
			var req sourcehydration.Request
			err := json.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			response := sourcehydration.Response{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = json.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)
		conf.Set("Processor.SourceHydration.maxRetry", 5)
		conf.Set("Processor.SourceHydration.maxRetryBackoffInterval", time.Millisecond*10)
		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		events := []sourcehydration.HydrationEvent{
			{
				ID: "1",
				Event: map[string]interface{}{
					"test": "event1",
				},
			},
		}

		source := sourcehydration.Source{
			ID:          "source-id",
			WorkspaceID: "workspace-id",
			Config:      []byte("{}"),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := sourcehydration.Request{
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

			var req sourcehydration.Request
			err := json.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			response := sourcehydration.Response{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = json.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)

		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		events := []sourcehydration.HydrationEvent{
			{
				ID: "1",
				Event: map[string]interface{}{
					"test": "event1",
				},
			},
		}

		source := sourcehydration.Source{
			ID:          "source-id",
			WorkspaceID: "workspace-id",
			Config:      []byte("{}"),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := sourcehydration.Request{
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

			var req sourcehydration.Request
			err := json.NewDecoder(r.Body).Decode(&req)
			require.NoError(t, err)

			response := sourcehydration.Response{
				Batch: req.Batch,
			}

			w.WriteHeader(http.StatusOK)
			err = json.NewEncoder(w).Encode(response)
			require.NoError(t, err)
		}))
		defer server.Close()

		conf := config.New()
		conf.Set("DEST_TRANSFORM_URL", server.URL)

		client := sourcehydration.New(conf, logger.NOP, stats.NOP)

		events := []sourcehydration.HydrationEvent{
			{
				ID: "1",
				Event: map[string]interface{}{
					"test": "event1",
				},
			},
		}

		source := sourcehydration.Source{
			ID:          "source-id",
			WorkspaceID: "workspace-id",
			Config:      []byte("{}"),
			SourceDefinition: backendconfig.SourceDefinitionT{
				Name: "TestSource",
			},
		}

		req := sourcehydration.Request{
			Batch:  events,
			Source: source,
		}

		_, err := client.Hydrate(ctx, req)
		require.NoError(t, err)
		require.Equal(t, "/v2/sources/testsource/hydrate", receivedURL)
	})
}
