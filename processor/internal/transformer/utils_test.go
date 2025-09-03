package utils

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-server/processor/types"
)

func TestWithProcTransformReqTimeStat(t *testing.T) {
	t.Run("successful request with correct timing and labels", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		labels := types.TransformerMetricLabels{
			Endpoint:         "transformer.example.com",
			DestinationType:  "GOOGLE_ANALYTICS",
			SourceType:       "webhook",
			Language:         "js",
			Stage:            "processor",
			WorkspaceID:      "workspace-123",
			SourceID:         "source-456",
			DestinationID:    "dest-789",
			TransformationID: "transform-101",
			Mirroring:        false,
		}

		executionTime := 100 * time.Millisecond
		requestFunc := func() error {
			time.Sleep(executionTime)
			return nil
		}

		wrappedFunc := WithProcTransformReqTimeStat(requestFunc, statsStore, labels)
		start := time.Now()
		err = wrappedFunc()
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.GreaterOrEqual(t, elapsed, executionTime)

		// Verify the metric was recorded
		expectedTags := map[string]string{
			"endpoint":         "transformer.example.com",
			"destinationType":  "GOOGLE_ANALYTICS",
			"sourceType":       "webhook",
			"language":         "js",
			"stage":            "processor",
			"workspaceId":      "workspace-123",
			"destinationId":    "dest-789",
			"sourceId":         "source-456",
			"transformationId": "transform-101",
			"mirroring":        "false",
			"dest_type":        "GOOGLE_ANALYTICS", // legacy tag
			"dest_id":          "dest-789",         // legacy tag
			"src_id":           "source-456",       // legacy tag
			"success":          "true",
		}

		metric := statsStore.Get("processor_transformer_request_time", expectedTags)
		require.NotNil(t, metric)
		// Just check that some timing was recorded, not the exact value
		require.Greater(t, metric.LastDuration(), time.Duration(0))
	})

	t.Run("failed request with error and timing", func(t *testing.T) {
		statsStore, err := memstats.New()
		require.NoError(t, err)

		labels := types.TransformerMetricLabels{
			Endpoint:        "transformer.example.com",
			DestinationType: "WEBHOOK",
			SourceType:      "android",
			Language:        "python",
			Stage:           "router",
			WorkspaceID:     "workspace-456",
			SourceID:        "source-789",
			DestinationID:   "dest-123",
			Mirroring:       true,
		}

		expectedError := errors.New("transformation failed")
		executionTime := 50 * time.Millisecond
		requestFunc := func() error {
			time.Sleep(executionTime)
			return expectedError
		}

		wrappedFunc := WithProcTransformReqTimeStat(requestFunc, statsStore, labels)
		start := time.Now()
		err = wrappedFunc()
		elapsed := time.Since(start)

		require.Error(t, err)
		require.Equal(t, expectedError, err)
		require.GreaterOrEqual(t, elapsed, executionTime)

		// Verify the metric was recorded with success=false
		expectedTags := map[string]string{
			"endpoint":         "transformer.example.com",
			"destinationType":  "WEBHOOK",
			"sourceType":       "android",
			"language":         "python",
			"stage":            "router",
			"workspaceId":      "workspace-456",
			"destinationId":    "dest-123",
			"sourceId":         "source-789",
			"transformationId": "",
			"mirroring":        "true",
			"dest_type":        "WEBHOOK",    // legacy tag
			"dest_id":          "dest-123",   // legacy tag
			"src_id":           "source-789", // legacy tag
			"success":          "false",
		}

		metric := statsStore.Get("processor_transformer_request_time", expectedTags)
		require.NotNil(t, metric)
		require.GreaterOrEqual(t, metric.LastDuration(), time.Duration(executionTime.Nanoseconds()))
	})
}
