package types

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPUReportedMetricToEDReportsDB(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		var metric *PUReportedMetric
		params := ErrorMetricParams{
			WorkspaceID:             "test-workspace",
			Namespace:               "test-namespace",
			InstanceID:              "test-instance",
			DestType:                "test-dest-type",
			DestinationDefinitionID: "test-dest-def-id",
			ErrorDetails: ErrorDetails{
				Code:    "TEST_ERROR",
				Message: "Test error message",
			},
		}
		result := PUReportedMetricToEDReportsDB(metric, params)
		require.Nil(t, result)
	})

	t.Run("empty metric with params", func(t *testing.T) {
		metric := &PUReportedMetric{}
		params := ErrorMetricParams{
			WorkspaceID:             "test-workspace",
			Namespace:               "test-namespace",
			InstanceID:              "test-instance",
			DestType:                "test-dest-type",
			DestinationDefinitionID: "test-dest-def-id",
			ErrorDetails: ErrorDetails{
				Code:    "TEST_ERROR",
				Message: "Test error message",
			},
		}
		result := PUReportedMetricToEDReportsDB(metric, params)

		require.NotNil(t, result)
		require.Equal(t, params.WorkspaceID, result.WorkspaceID)
		require.Equal(t, params.Namespace, result.Namespace)
		require.Equal(t, params.InstanceID, result.InstanceID)
		require.Equal(t, params.DestType, result.DestType)
		require.Equal(t, params.DestinationDefinitionID, result.DestinationDefinitionId)
		require.Equal(t, params.ErrorDetails.Code, result.ErrorCode)
		require.Equal(t, params.ErrorDetails.Message, result.ErrorMessage)
		require.Equal(t, metric.PU, result.PU)
		require.Equal(t, metric.SourceID, result.SourceID)
		require.Equal(t, metric.DestinationID, result.DestinationID)
		require.Equal(t, metric.SourceDefinitionID, result.SourceDefinitionId)
	})

	t.Run("metric with all fields populated", func(t *testing.T) {
		original := &PUReportedMetric{
			ConnectionDetails: ConnectionDetails{
				SourceID:                "test-source",
				DestinationID:           "test-destination",
				SourceTaskRunID:         "task-run-123",
				SourceJobID:             "job-123",
				SourceJobRunID:          "job-run-123",
				SourceDefinitionID:      "source-def-123",
				DestinationDefinitionID: "dest-def-123",
				SourceCategory:          "test-category",
				TransformationID:        "transform-123",
				TransformationVersionID: "transform-ver-123",
				TrackingPlanID:          "tracking-123",
				TrackingPlanVersion:     1,
			},
			PUDetails: PUDetails{
				InPU:       "input-pu",
				PU:         "processing-unit",
				TerminalPU: true,
				InitialPU:  false,
			},
			StatusDetail: &StatusDetail{
				Status:         "success",
				Count:          100,
				StatusCode:     200,
				SampleResponse: "response data",
				EventName:      "test_event",
				EventType:      "track",
				ErrorType:      "none",
				ViolationCount: 0,
				SampleEvent:    json.RawMessage(`{"event": "test", "data": "value"}`),
				StatTags: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
				FailedMessages: []*FailedMessage{
					{
						MessageID:  "msg-1",
						ReceivedAt: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
					},
					{
						MessageID:  "msg-2",
						ReceivedAt: time.Date(2023, 1, 2, 12, 0, 0, 0, time.UTC),
					},
				},
			},
		}

		params := ErrorMetricParams{
			WorkspaceID:             "test-workspace",
			Namespace:               "test-namespace",
			InstanceID:              "test-instance",
			DestType:                "test-dest-type",
			DestinationDefinitionID: "test-dest-def-id",
			ErrorDetails: ErrorDetails{
				Code:    "CUSTOM_ERROR",
				Message: "Custom error message",
			},
		}

		result := PUReportedMetricToEDReportsDB(original, params)

		// Verify the result is not the same pointer as original
		require.NotSame(t, original, result)

		// Verify EDInstanceDetails
		require.Equal(t, params.WorkspaceID, result.WorkspaceID)
		require.Equal(t, params.Namespace, result.Namespace)
		require.Equal(t, params.InstanceID, result.InstanceID)

		// Verify EDConnectionDetails
		require.Equal(t, original.SourceID, result.SourceID)
		require.Equal(t, original.DestinationID, result.DestinationID)
		require.Equal(t, original.SourceDefinitionID, result.SourceDefinitionId)
		require.Equal(t, params.DestinationDefinitionID, result.DestinationDefinitionId)
		require.Equal(t, params.DestType, result.DestType)

		// Verify ReportMetadata
		require.NotZero(t, result.ReportedAt)

		// Verify PU
		require.Equal(t, original.PU, result.PU)

		// Verify EDErrorDetails
		require.Equal(t, original.StatusDetail.StatusCode, result.StatusCode)
		require.Equal(t, params.ErrorDetails.Code, result.ErrorCode)
		require.Equal(t, params.ErrorDetails.Message, result.ErrorMessage)
		require.Equal(t, original.StatusDetail.EventType, result.EventType)
		require.Equal(t, original.StatusDetail.EventName, result.EventName)
		require.Equal(t, original.StatusDetail.SampleResponse, result.SampleResponse)
		require.Equal(t, original.StatusDetail.SampleEvent, result.SampleEvent)
		require.Equal(t, original.StatusDetail.Count, result.ErrorCount)
		require.Equal(t, original.StatusDetail.Count, result.Count)
	})

	t.Run("metric with nil StatusDetail", func(t *testing.T) {
		original := &PUReportedMetric{
			ConnectionDetails: ConnectionDetails{
				SourceID:      "test-source",
				DestinationID: "test-destination",
			},
			PUDetails: PUDetails{
				InPU: "input-pu",
				PU:   "processing-unit",
			},
			StatusDetail: nil,
		}

		params := ErrorMetricParams{
			WorkspaceID:             "test-workspace",
			Namespace:               "test-namespace",
			InstanceID:              "test-instance",
			DestType:                "test-dest-type",
			DestinationDefinitionID: "test-dest-def-id",
			ErrorDetails: ErrorDetails{
				Code:    "NIL_ERROR",
				Message: "Nil status detail error",
			},
		}

		result := PUReportedMetricToEDReportsDB(original, params)

		require.NotNil(t, result)
		require.Equal(t, original.SourceID, result.SourceID)
		require.Equal(t, original.DestinationID, result.DestinationID)
		require.Equal(t, original.PU, result.PU)
		require.Equal(t, params.ErrorDetails.Code, result.ErrorCode)
		require.Equal(t, params.ErrorDetails.Message, result.ErrorMessage)
		// StatusCode should be zero value when StatusDetail is nil
		require.Equal(t, 0, result.StatusCode)
	})

	t.Run("modification isolation", func(t *testing.T) {
		original := &PUReportedMetric{
			ConnectionDetails: ConnectionDetails{
				SourceID:      "original-source",
				DestinationID: "original-destination",
			},
			PUDetails: PUDetails{
				InPU: "original-input",
				PU:   "original-pu",
			},
			StatusDetail: &StatusDetail{
				Status:         "original-status",
				Count:          100,
				StatusCode:     200,
				SampleResponse: "original-response",
				SampleEvent:    json.RawMessage(`{"original": "data"}`),
				StatTags: map[string]string{
					"original": "value",
				},
			},
		}

		params := ErrorMetricParams{
			WorkspaceID:             "test-workspace",
			Namespace:               "test-namespace",
			InstanceID:              "test-instance",
			DestType:                "test-dest-type",
			DestinationDefinitionID: "test-dest-def-id",
			ErrorDetails: ErrorDetails{
				Code:    "ORIGINAL_ERROR",
				Message: "Original error message",
			},
		}

		result := PUReportedMetricToEDReportsDB(original, params)

		// Modify the original
		original.SourceID = "modified-source"
		original.InPU = "modified-input"
		original.StatusDetail.Status = "modified-status"
		original.StatusDetail.Count = 200
		original.StatusDetail.SampleResponse = "modified-response"
		original.StatusDetail.SampleEvent = json.RawMessage(`{"modified": "data"}`)
		original.StatusDetail.StatTags["modified"] = "new-value"

		// Verify the result is not affected by modifications to original
		require.Equal(t, "original-source", result.SourceID)
		require.Equal(t, "original-pu", result.PU)
		require.Equal(t, 200, result.StatusCode)   // This comes from original.StatusDetail.StatusCode
		require.Equal(t, int64(100), result.Count) // This comes from original.StatusDetail.Count
		require.Equal(t, "original-response", result.SampleResponse)
		require.Equal(t, json.RawMessage(`{"original": "data"}`), result.SampleEvent)
		require.Equal(t, "ORIGINAL_ERROR", result.ErrorCode)
		require.Equal(t, "Original error message", result.ErrorMessage)
	})

	t.Run("ReportedAt timestamp", func(t *testing.T) {
		metric := &PUReportedMetric{
			PUDetails: PUDetails{
				PU: "test-pu",
			},
		}
		params := ErrorMetricParams{
			WorkspaceID:             "test-workspace",
			Namespace:               "test-namespace",
			InstanceID:              "test-instance",
			DestType:                "test-dest-type",
			DestinationDefinitionID: "test-dest-def-id",
			ErrorDetails: ErrorDetails{
				Code:    "TIMESTAMP_TEST",
				Message: "Timestamp test",
			},
		}

		before := time.Now().UTC().Unix() / 60
		result := PUReportedMetricToEDReportsDB(metric, params)
		after := time.Now().UTC().Unix() / 60

		require.GreaterOrEqual(t, result.ReportedAt, before)
		require.LessOrEqual(t, result.ReportedAt, after)
	})
}

func TestErrorMetricParams(t *testing.T) {
	t.Run("struct creation", func(t *testing.T) {
		params := ErrorMetricParams{
			WorkspaceID:             "test-workspace",
			Namespace:               "test-namespace",
			InstanceID:              "test-instance",
			DestType:                "test-dest-type",
			DestinationDefinitionID: "test-dest-def-id",
			ErrorDetails: ErrorDetails{
				Code:    "TEST_CODE",
				Message: "Test message",
			},
		}

		require.Equal(t, "test-workspace", params.WorkspaceID)
		require.Equal(t, "test-namespace", params.Namespace)
		require.Equal(t, "test-instance", params.InstanceID)
		require.Equal(t, "test-dest-type", params.DestType)
		require.Equal(t, "test-dest-def-id", params.DestinationDefinitionID)
		require.Equal(t, "TEST_CODE", params.ErrorDetails.Code)
		require.Equal(t, "Test message", params.ErrorDetails.Message)
	})
}
