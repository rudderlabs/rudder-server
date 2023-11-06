package client_test

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/client"
)

func loadFile(t *testing.T, path string) string {
	t.Helper()

	b, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(b)
}

func TestWarehouse(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		t.Parallel()

		t.Log("using the same request file as API testing")
		body := loadFile(t, "../internal/api/testdata/process_request.json")
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "/v1/process", r.URL.Path)
			require.Equal(t, http.MethodPost, r.Method)

			b, err := io.ReadAll(r.Body)
			require.NoError(t, err)

			require.JSONEq(t, body, string(b))

			w.WriteHeader(http.StatusOK)
		}))

		t.Cleanup(ts.Close)

		c := client.NewWarehouse(ts.URL, client.WithTimeout(10*time.Millisecond))
		err := c.Process(context.Background(), client.StagingFile{
			WorkspaceID:   "279L3V7FSpx43LaNJ0nIs9KRaNC",
			SourceID:      "279L3gEKqwruBoKGsXZtSVX7vIy",
			DestinationID: "27CHciD6leAhurSyFAeN4dp14qZ",

			Location:              "rudder-warehouse-staging-logs/279L3gEKqwruBoKGsXZtSVX7vIy/2022-11-08/1667913810.279L3gEKqwruBoKGsXZtSVX7vIy.7a6e7785-7a75-4345-8d3c-d7a1ce49a43f.json.gz",
			TotalEvents:           2,
			TotalBytes:            2000,
			FirstEventAt:          "2022-11-08T13:23:07Z",
			LastEventAt:           "2022-11-08T13:23:07Z",
			UseRudderStorage:      false,
			DestinationRevisionID: "2H1cLBvL3v0prRBNzpe8D34XTzU",
			SourceTaskRunID:       "<source-task-run-id>",
			SourceJobID:           "<source-job-id>",
			SourceJobRunID:        "<source-job-run-id>",
			TimeWindow:            time.Date(1, 1, 1, 0, 40, 0, 0, time.UTC),
			Schema: map[string]map[string]interface{}{
				"product_track": {
					"context_destination_id":   "string",
					"context_destination_type": "string",
					"context_ip":               "string",
					"context_library_name":     "string",
					"context_passed_ip":        "string",
					"context_request_ip":       "string",
					"context_source_id":        "string",
					"context_source_type":      "string",
					"event":                    "string",
					"event_text":               "string",
					"id":                       "string",
					"original_timestamp":       "datetime",
					"product_id":               "string",
					"rating":                   "int",
					"received_at":              "datetime",
					"revenue":                  "float",
					"review_body":              "string",
					"review_id":                "string",
					"sent_at":                  "datetime",
					"timestamp":                "datetime",
					"user_id":                  "string",
					"uuid_ts":                  "datetime",
				},
				"tracks": {
					"context_destination_id":   "string",
					"context_destination_type": "string",
					"context_ip":               "string",
					"context_library_name":     "string",
					"context_passed_ip":        "string",
					"context_request_ip":       "string",
					"context_source_id":        "string",
					"context_source_type":      "string",
					"event":                    "string",
					"event_text":               "string",
					"id":                       "string",
					"original_timestamp":       "datetime",
					"received_at":              "datetime",
					"sent_at":                  "datetime",
					"timestamp":                "datetime",
					"user_id":                  "string",
					"uuid_ts":                  "datetime",
				},
			},
		})
		require.NoError(t, err)
	})

	t.Run("failure: unexpected status code", func(t *testing.T) {
		t.Parallel()

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprintf(w, "bad request")
		}))
		t.Cleanup(ts.Close)

		c := client.NewWarehouse(ts.URL)
		err := c.Process(context.Background(), client.StagingFile{})

		require.EqualError(t, err, "unexpected status code \"400 Bad Request\" on "+ts.URL+": bad request")
	})

	t.Run("failure: nil context", func(t *testing.T) {
		t.Parallel()
		c := client.NewWarehouse("")
		err := c.Process(nil, client.StagingFile{}) //nolint:staticcheck // SA1012: using nil context to trigger failure

		require.EqualError(t, err, "creating request: net/http: nil Context")
	})

	t.Run("failure: json marshal", func(t *testing.T) {
		t.Parallel()
		c := client.NewWarehouse("")
		err := c.Process(context.Background(), client.StagingFile{
			Schema: map[string]map[string]interface{}{
				"foo": {"bar": math.Inf(1)},
			},
		}) //nolint:staticcheck // SA1012: using nil context to trigger failure

		require.ErrorContains(t, err, "unsupported value: +Inf")
	})

	t.Run("timeout", func(t *testing.T) {
		t.Parallel()

		block := make(chan struct{})
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-block
		}))
		t.Cleanup(ts.Close)
		t.Cleanup(func() {
			close(block)
		})

		c := client.NewWarehouse(ts.URL, client.WithTimeout(10*time.Millisecond))

		err := c.Process(context.Background(), client.StagingFile{})
		require.EqualError(t, err, fmt.Sprintf("http request to \"%[1]s\": Post \"%[1]s/v1/process\": context deadline exceeded (Client.Timeout exceeded while awaiting headers)", ts.URL))
	})
}
