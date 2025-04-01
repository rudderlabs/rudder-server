package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	"github.com/rudderlabs/rudder-server/enterprise/reporting/client"
	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/rudderlabs/rudder-server/utils/types"
)

var update = flag.Bool("update", false, "update golden files")

func TestClientSendMetric(t *testing.T) {
	// Create a channel to capture the request payload
	var receivedPayload []byte
	payloadChan := make(chan []byte, 1)

	// Create a test server to mock the reporting service
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		receivedPayload, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		payloadChan <- receivedPayload
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create stats store
	statsStore, err := memstats.New()
	require.NoError(t, err)

	// Create a test config
	conf := config.New()
	conf.Set("INSTANCE_ID", "test-instance")
	conf.Set("clientName", "test-client")

	// Create the client
	c := client.New(server.URL, client.PathMetrics, conf, logger.NOP, statsStore)

	bucket, _ := reporting.GetAggregationBucketMinute(28017690, 10)

	workspaceID := "test-workspace"
	instanceID := "test-instance"

	metric := types.Metric{
		InstanceDetails: types.InstanceDetails{
			WorkspaceID: workspaceID,
			InstanceID:  instanceID,
		},
		ConnectionDetails: types.ConnectionDetails{
			SourceID:         "some-source-id",
			DestinationID:    "some-destination-id",
			TransformationID: "some-transformation-id",
			TrackingPlanID:   "some-tracking-plan-id",
		},
		PUDetails: types.PUDetails{
			InPU: "some-in-pu",
			PU:   "some-pu",
		},
		ReportMetadata: types.ReportMetadata{
			ReportedAt:        28017690 * 60 * 1000,
			SampleEventBucket: bucket * 60 * 1000,
		},
		StatusDetails: []*types.StatusDetail{
			{
				Status:         "some-status",
				Count:          3,
				ViolationCount: 5,
				StatusCode:     200,
				SampleResponse: "",
				SampleEvent:    []byte(`{}`),
				ErrorType:      "",
			},
			{
				Status:         "some-status",
				Count:          2,
				ViolationCount: 10,
				StatusCode:     200,
				SampleResponse: "",
				SampleEvent:    []byte(`{}`),
				ErrorType:      "some-error-type",
			},
		},
	}

	err = c.Send(context.Background(), &metric)
	require.NoError(t, err)

	// Get server hostname
	serverURL, _ := url.Parse(server.URL)

	// Wait for the request payload
	receivedPayload = <-payloadChan

	// Load golden file
	golden := filepath.Join("testdata", "send_metric.json")
	if *update {
		var prettyJSON bytes.Buffer
		err = json.Indent(&prettyJSON, receivedPayload, "", "  ")
		require.NoError(t, err, "failed to format JSON")
		err = os.WriteFile(golden, prettyJSON.Bytes(), 0o644)
		require.NoError(t, err, "failed to update golden file")
	}

	// Read golden file
	expected, err := os.ReadFile(golden)
	require.NoError(t, err, "failed to read golden file")

	// Compare JSON payloads
	var expectedJSON, actualJSON interface{}
	err = jsonrs.Unmarshal(expected, &expectedJSON)
	require.NoError(t, err, "failed to unmarshal expected JSON")
	err = jsonrs.Unmarshal(receivedPayload, &actualJSON)
	require.NoError(t, err, "failed to unmarshal actual JSON")

	require.Equal(t, expectedJSON, actualJSON, "payload does not match golden file")

	t.Run("ensure metrics are recorded", func(t *testing.T) {
		// Expected tags for all metrics
		expectedTags := stats.Tags{
			"path":       string(client.PathMetrics),
			"module":     "test-client",
			"instanceId": instanceID,
			"endpoint":   serverURL.Host,
		}

		// Expected tags for HTTP metrics
		expectedHttpTags := stats.Tags{
			"path":       string(client.PathMetrics),
			"module":     "test-client",
			"instanceId": instanceID,
			"endpoint":   serverURL.Host,
			"status":     "200",
		}

		// Verify total bytes metric
		metrics := statsStore.GetByName(client.StatRequestTotalBytes)
		require.Len(t, metrics, 1, "should have exactly one total bytes metric")
		require.Equal(t, expectedTags, metrics[0].Tags, "total bytes metric should have correct tags")

		// Verify duration metric
		metrics = statsStore.GetByName(client.StatTotalDurationsSeconds)
		require.Len(t, metrics, 1, "should have exactly one duration metric")
		require.Equal(t, expectedTags, metrics[0].Tags, "duration metric should have correct tags")

		// Verify HTTP request metric
		metrics = statsStore.GetByName(client.StatHttpRequest)
		require.Len(t, metrics, 1, "should have exactly one http request metric")
		require.Equal(t, expectedHttpTags, metrics[0].Tags, "http request metric should have correct tags")
	})
}

func TestClientSendErrorMetric(t *testing.T) {
	// Create a channel to capture the request payload
	var receivedPayload []byte
	payloadChan := make(chan []byte, 1)

	// Create a test server to mock the reporting service
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		receivedPayload, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		payloadChan <- receivedPayload
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create stats store
	statsStore, err := memstats.New()
	require.NoError(t, err)

	// Create a test config
	conf := config.New()
	conf.Set("INSTANCE_ID", "test-instance")
	conf.Set("clientName", "test-client")

	// Create the client
	c := client.New(server.URL, client.PathMetrics, conf, logger.NOP, statsStore)

	// Create sample event as json.RawMessage
	sampleEvent := json.RawMessage(`{"event": "test_event", "properties": {"test": "value"}}`)

	// Create a test error metric
	metric := &types.EDMetric{
		EDInstanceDetails: types.EDInstanceDetails{
			WorkspaceID: "test-workspace",
			InstanceID:  "test-instance",
		},
		Errors: []types.EDErrorDetails{
			{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					StatusCode:   400,
					ErrorCode:    "ERR_001",
					ErrorMessage: "Test error",
					EventType:    "track",
					EventName:    "test_event",
				},
				SampleResponse: "error response",
				SampleEvent:    sampleEvent,
				ErrorCount:     5,
			},
		},
	}

	// Send the error metric
	err = c.Send(context.Background(), metric)
	require.NoError(t, err)

	// Get server hostname
	serverURL, _ := url.Parse(server.URL)

	// Wait for the request payload
	receivedPayload = <-payloadChan

	// Load golden file
	golden := filepath.Join("testdata", "send_error_metric.json")
	if *update {
		var prettyJSON bytes.Buffer
		err = json.Indent(&prettyJSON, receivedPayload, "", "  ")
		require.NoError(t, err, "failed to format JSON")
		err = os.WriteFile(golden, prettyJSON.Bytes(), 0o644)
		require.NoError(t, err, "failed to update golden file")
	}

	// Read golden file
	expected, err := os.ReadFile(golden)
	require.NoError(t, err, "failed to read golden file")

	// Compare JSON payloads
	var expectedJSON, actualJSON interface{}
	err = jsonrs.Unmarshal(expected, &expectedJSON)
	require.NoError(t, err, "failed to unmarshal expected JSON")
	err = jsonrs.Unmarshal(receivedPayload, &actualJSON)
	require.NoError(t, err, "failed to unmarshal actual JSON")

	require.Equal(t, expectedJSON, actualJSON, "payload does not match golden file")

	// Expected tags for all metrics
	expectedTags := stats.Tags{
		"path":       string(client.PathMetrics),
		"module":     "test-client",
		"instanceId": "test-instance",
		"endpoint":   serverURL.Host,
	}

	// Expected tags for HTTP metrics
	expectedHttpTags := stats.Tags{
		"path":       string(client.PathMetrics),
		"module":     "test-client",
		"instanceId": "test-instance",
		"endpoint":   serverURL.Host,
		"status":     "200",
	}

	// Verify total bytes metric
	metrics := statsStore.GetByName(client.StatRequestTotalBytes)
	require.Len(t, metrics, 1, "should have exactly one total bytes metric")
	require.Equal(t, expectedTags, metrics[0].Tags, "total bytes metric should have correct tags")

	// Verify duration metric
	metrics = statsStore.GetByName(client.StatTotalDurationsSeconds)
	require.Len(t, metrics, 1, "should have exactly one duration metric")
	require.Equal(t, expectedTags, metrics[0].Tags, "duration metric should have correct tags")

	// Verify HTTP request metric
	metrics = statsStore.GetByName(client.StatHttpRequest)
	require.Len(t, metrics, 1, "should have exactly one http request metric")
	require.Equal(t, expectedHttpTags, metrics[0].Tags, "http request metric should have correct tags")
}

func TestClient5xx(t *testing.T) {
	// Create a test server that returns an error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	// Create stats store
	statsStore, err := memstats.New()
	require.NoError(t, err)

	// Create a test config
	conf := config.New()
	conf.Set("INSTANCE_ID", "test-instance")
	conf.Set("clientName", "test-client")

	// Create the client
	c := client.New(server.URL, client.PathMetrics, conf, logger.NOP, statsStore)

	// Create a test metric
	metric := &types.Metric{
		InstanceDetails: types.InstanceDetails{
			WorkspaceID: "test-workspace",
			InstanceID:  "test-instance",
		},
		StatusDetails: []*types.StatusDetail{
			{
				Status:     "success",
				Count:      100,
				StatusCode: 200,
			},
		},
	}

	// Send the metric and expect an error
	err = c.Send(context.Background(), metric)
	require.Error(t, err)
	require.Contains(t, err.Error(), "received response: statusCode:500")

	// Get server hostname
	serverURL, _ := url.Parse(server.URL)

	// Expected tags for HTTP metrics
	expectedHttpTags := stats.Tags{
		"workspaceId": "test-workspace",
		"clientName":  "test-client",
		"instanceId":  "test-instance",
		"endpoint":    serverURL.Host,
		"status":      "500",
	}

	// Verify HTTP request metric
	metrics := statsStore.GetByName(client.StatHttpRequest)
	require.Len(t, metrics, 1, "should have exactly one http request metric")
	require.Equal(t, expectedHttpTags, metrics[0].Tags, "http request metric should have correct tags")
}
