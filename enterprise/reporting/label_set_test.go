package reporting

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-server/utils/types"
)

const someEventName = "some-event-name"

func createMetricObject(eventName string, withError bool) types.PUReportedMetric {
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
	if withError {
		metric.StatusDetail.ErrorDetails = types.ErrorDetails{
			Code:    "some-error-code",
			Message: "some-error-message",
		}
	}
	return metric
}

func TestNewLabelSet(t *testing.T) {
	t.Run("should create the correct LabelSet from types.PUReportedMetric", func(t *testing.T) {
		inputMetric := createMetricObject(someEventName, false)
		bucket := int64(28889820)
		labelSet := NewLabelSet(inputMetric, bucket)

		assert.Equal(t, "some-source-id", labelSet.SourceID)
		assert.Equal(t, someEventName, labelSet.EventName) // Default value
	})
}

func TestGenerateHash(t *testing.T) {
	t.Run("same hash for same LabelSet for metrics", func(t *testing.T) {
		inputMetric1 := createMetricObject(someEventName, false)
		bucket := int64(28889820)
		labelSet1 := NewLabelSet(inputMetric1, bucket)

		inputMetric2 := createMetricObject(someEventName, false)
		labelSet2 := NewLabelSet(inputMetric2, bucket)

		hash1 := labelSet1.generateHash()
		hash2 := labelSet2.generateHash()

		assert.Equal(t, hash1, hash2)
	})

	t.Run("different hash for different LabelSet for metrics", func(t *testing.T) {
		inputMetric1 := createMetricObject("some-event-name-1", false)
		bucket := int64(28889820)
		labelSet1 := NewLabelSet(inputMetric1, bucket)

		inputMetric2 := createMetricObject("some-event-name-2", false)
		labelSet2 := NewLabelSet(inputMetric2, bucket)

		hash1 := labelSet1.generateHash()
		hash2 := labelSet2.generateHash()

		assert.NotEqual(t, hash1, hash2)
	})

	t.Run("same hash for same LabelSet for errors", func(t *testing.T) {
		inputMetric1 := createMetricObject(someEventName, true)
		bucket := int64(28889820)
		labelSet1 := NewLabelSet(inputMetric1, bucket)

		inputMetric2 := createMetricObject(someEventName, true)
		labelSet2 := NewLabelSet(inputMetric2, bucket)

		hash1 := labelSet1.generateHash()
		hash2 := labelSet2.generateHash()

		assert.Equal(t, hash1, hash2)
	})

	t.Run("different hash for different LabelSet for errors", func(t *testing.T) {
		inputMetric1 := createMetricObject("some-event-name-1", true)
		bucket := int64(28889820)
		labelSet1 := NewLabelSet(inputMetric1, bucket)

		inputMetric2 := createMetricObject("some-event-name-2", true)
		labelSet2 := NewLabelSet(inputMetric2, bucket)

		hash1 := labelSet1.generateHash()
		hash2 := labelSet2.generateHash()

		assert.NotEqual(t, hash1, hash2)
	})
}
