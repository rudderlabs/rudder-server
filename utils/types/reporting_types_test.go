package types

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPUReportedMetric_DeepCopy(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		var metric *PUReportedMetric
		result := metric.DeepCopy()
		require.Nil(t, result)
	})

	t.Run("empty metric", func(t *testing.T) {
		metric := &PUReportedMetric{}
		result := metric.DeepCopy()

		require.NotNil(t, result)
		require.Equal(t, metric.ConnectionDetails, result.ConnectionDetails)
		require.Equal(t, metric.PUDetails, result.PUDetails)
		require.Nil(t, result.StatusDetail)
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
				ErrorDetails: ErrorDetails{
					Code:    "SUCCESS",
					Message: "Operation completed successfully",
				},
				SampleEvent: json.RawMessage(`{"event": "test", "data": "value"}`),
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

		result := original.DeepCopy()

		// Verify the copy is not the same pointer
		require.NotSame(t, original, result)
		require.NotSame(t, original.StatusDetail, result.StatusDetail)

		// Verify all fields are copied correctly
		require.Equal(t, original.ConnectionDetails, result.ConnectionDetails)
		require.Equal(t, original.PUDetails, result.PUDetails)
		require.Equal(t, original.StatusDetail.Status, result.StatusDetail.Status)
		require.Equal(t, original.StatusDetail.Count, result.StatusDetail.Count)
		require.Equal(t, original.StatusDetail.StatusCode, result.StatusDetail.StatusCode)
		require.Equal(t, original.StatusDetail.SampleResponse, result.StatusDetail.SampleResponse)
		require.Equal(t, original.StatusDetail.EventName, result.StatusDetail.EventName)
		require.Equal(t, original.StatusDetail.EventType, result.StatusDetail.EventType)
		require.Equal(t, original.StatusDetail.ErrorType, result.StatusDetail.ErrorType)
		require.Equal(t, original.StatusDetail.ViolationCount, result.StatusDetail.ViolationCount)
		require.Equal(t, original.StatusDetail.ErrorDetails, result.StatusDetail.ErrorDetails)

		// Verify SampleEvent is deep copied
		require.Equal(t, original.StatusDetail.SampleEvent, result.StatusDetail.SampleEvent)
		// Note: json.RawMessage is a slice, so we can't use NotSame directly
		// Instead, we verify the content is the same but the underlying slice is different

		// Verify StatTags is deep copied
		require.Equal(t, original.StatusDetail.StatTags, result.StatusDetail.StatTags)
		// Note: maps are reference types, so we verify the content is the same

		// Verify FailedMessages is deep copied
		require.Equal(t, len(original.StatusDetail.FailedMessages), len(result.StatusDetail.FailedMessages))
		for i, msg := range original.StatusDetail.FailedMessages {
			require.Equal(t, msg.MessageID, result.StatusDetail.FailedMessages[i].MessageID)
			require.Equal(t, msg.ReceivedAt, result.StatusDetail.FailedMessages[i].ReceivedAt)
			require.NotSame(t, msg, result.StatusDetail.FailedMessages[i])
		}
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

		result := original.DeepCopy()

		require.NotNil(t, result)
		require.Equal(t, original.ConnectionDetails, result.ConnectionDetails)
		require.Equal(t, original.PUDetails, result.PUDetails)
		require.Nil(t, result.StatusDetail)
	})

	t.Run("StatusDetail with nil complex fields", func(t *testing.T) {
		original := &PUReportedMetric{
			StatusDetail: &StatusDetail{
				Status:     "success",
				Count:      100,
				StatusCode: 200,
				// SampleEvent, StatTags, and FailedMessages are nil
			},
		}

		result := original.DeepCopy()

		require.NotNil(t, result.StatusDetail)
		require.Equal(t, original.StatusDetail.Status, result.StatusDetail.Status)
		require.Equal(t, original.StatusDetail.Count, result.StatusDetail.Count)
		require.Equal(t, original.StatusDetail.StatusCode, result.StatusDetail.StatusCode)
		require.Nil(t, result.StatusDetail.SampleEvent)
		require.Nil(t, result.StatusDetail.StatTags)
		require.Nil(t, result.StatusDetail.FailedMessages)
	})

	t.Run("StatusDetail with empty complex fields", func(t *testing.T) {
		original := &PUReportedMetric{
			StatusDetail: &StatusDetail{
				Status:         "success",
				Count:          100,
				StatusCode:     200,
				SampleEvent:    json.RawMessage{},
				StatTags:       map[string]string{},
				FailedMessages: []*FailedMessage{},
			},
		}

		result := original.DeepCopy()

		require.NotNil(t, result.StatusDetail)
		require.Equal(t, original.StatusDetail.Status, result.StatusDetail.Status)
		require.Equal(t, original.StatusDetail.Count, result.StatusDetail.Count)
		require.Equal(t, original.StatusDetail.StatusCode, result.StatusDetail.StatusCode)
		require.Equal(t, original.StatusDetail.SampleEvent, result.StatusDetail.SampleEvent)
		require.Equal(t, original.StatusDetail.StatTags, result.StatusDetail.StatTags)
		require.Equal(t, original.StatusDetail.FailedMessages, result.StatusDetail.FailedMessages)
		// Note: We can't use NotSame for slices and maps directly, but we verify content equality
	})

	t.Run("StatusDetail with nil FailedMessages elements", func(t *testing.T) {
		original := &PUReportedMetric{
			StatusDetail: &StatusDetail{
				Status: "success",
				FailedMessages: []*FailedMessage{
					nil,
					{
						MessageID:  "msg-1",
						ReceivedAt: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
					},
					nil,
				},
			},
		}

		result := original.DeepCopy()

		require.NotNil(t, result.StatusDetail)
		require.Equal(t, len(original.StatusDetail.FailedMessages), len(result.StatusDetail.FailedMessages))
		require.Nil(t, result.StatusDetail.FailedMessages[0])
		require.NotNil(t, result.StatusDetail.FailedMessages[1])
		require.Equal(t, original.StatusDetail.FailedMessages[1].MessageID, result.StatusDetail.FailedMessages[1].MessageID)
		require.Nil(t, result.StatusDetail.FailedMessages[2])
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
				FailedMessages: []*FailedMessage{
					{
						MessageID:  "original-msg",
						ReceivedAt: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
					},
				},
			},
		}

		result := original.DeepCopy()

		// Modify the original
		original.SourceID = "modified-source"
		original.InPU = "modified-input"
		original.StatusDetail.Status = "modified-status"
		original.StatusDetail.Count = 200
		original.StatusDetail.SampleResponse = "modified-response"
		original.StatusDetail.SampleEvent = json.RawMessage(`{"modified": "data"}`)
		original.StatusDetail.StatTags["modified"] = "new-value"
		original.StatusDetail.FailedMessages[0].MessageID = "modified-msg"

		// Verify the copy is not affected
		require.Equal(t, "original-source", result.SourceID)
		require.Equal(t, "original-input", result.InPU)
		require.Equal(t, "original-status", result.StatusDetail.Status)
		require.Equal(t, int64(100), result.StatusDetail.Count)
		require.Equal(t, "original-response", result.StatusDetail.SampleResponse)
		require.Equal(t, json.RawMessage(`{"original": "data"}`), result.StatusDetail.SampleEvent)
		require.Equal(t, "value", result.StatusDetail.StatTags["original"])
		require.NotContains(t, result.StatusDetail.StatTags, "modified")
		require.Equal(t, "original-msg", result.StatusDetail.FailedMessages[0].MessageID)
	})
}
