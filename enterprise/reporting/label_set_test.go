package reporting

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/types"
)

const someEventName = "some-event-name"

func createMetricObject(eventName, errorMessage string) types.PUReportedMetric {
	metric := types.PUReportedMetric{
		ConnectionDetails: types.ConnectionDetails{
			SourceID:      "some-source-id",
			DestinationID: "some-destination-id",
		},
		PUDetails: types.PUDetails{
			InPU: "some-in-pu",
			PU:   "some-pu",
		},
		StatusDetail: &types.StatusDetail{
			Status:         "some-status",
			Count:          3,
			StatusCode:     0,
			SampleResponse: `{"some-sample-response-key": "some-sample-response-value"}`,
			SampleEvent:    []byte(`{"some-sample-event-key": "some-sample-event-value"}`),
			EventName:      eventName,
			EventType:      "some-event-type",
		},
	}
	if errorMessage != "" {
		metric.StatusDetail.ErrorDetails = types.ErrorDetails{
			Code:    "some-error-code",
			Message: errorMessage,
		}
	}
	return metric
}

func TestNewLabelSet(t *testing.T) {
	t.Run("should create the correct LabelSet from types.PUReportedMetric", func(t *testing.T) {
		inputMetric := createMetricObject(someEventName, "")
		bucket := int64(28889820)
		labelSet := NewLabelSet(inputMetric, bucket)

		assert.Equal(t, "some-source-id", labelSet.SourceID)
		assert.Equal(t, someEventName, labelSet.EventName) // Default value
	})
}

func TestGenerateHash(t *testing.T) {
	tests := []struct {
		name              string
		metric1           types.PUReportedMetric
		metric2           types.PUReportedMetric
		bucket1           int64
		bucket2           int64
		shouldHashesMatch bool
	}{
		{
			name:              "same hash for same LabelSet for metrics",
			metric1:           createMetricObject(someEventName, ""),
			metric2:           createMetricObject(someEventName, ""),
			bucket1:           28889820,
			bucket2:           28889820,
			shouldHashesMatch: true,
		},
		{
			name:              "different hash for label set with different event name for metrics",
			metric1:           createMetricObject(someEventName, ""),
			metric2:           createMetricObject("some-event-name-2", ""),
			bucket1:           28889820,
			bucket2:           28889820,
			shouldHashesMatch: false,
		},
		{
			name:              "different hash for label set with different buckets for metrics",
			metric1:           createMetricObject(someEventName, ""),
			metric2:           createMetricObject(someEventName, ""),
			bucket1:           28889000,
			bucket2:           28889820,
			shouldHashesMatch: false,
		},
		{
			name:              "same hash for same LabelSet for errors",
			metric1:           createMetricObject(someEventName, "Some error message"),
			metric2:           createMetricObject(someEventName, "Some error message"),
			bucket1:           28889820,
			bucket2:           28889820,
			shouldHashesMatch: true,
		},
		{
			name:              "different hash for different LabelSet with different messages for errors",
			metric1:           createMetricObject(someEventName, "Some error message 1"),
			metric2:           createMetricObject(someEventName, "Some error message 2"),
			bucket1:           28889820,
			bucket2:           28889820,
			shouldHashesMatch: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			labelSet1 := NewLabelSet(test.metric1, test.bucket1)
			labelSet2 := NewLabelSet(test.metric2, test.bucket2)

			hash1 := labelSet1.generateHash()
			hash2 := labelSet2.generateHash()

			require.Equal(t, test.shouldHashesMatch, hash1 == hash2)
		})
	}
}
