package reporting

import (
	"testing"

	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/stretchr/testify/assert"
)

func createMetricObject(eventName string) types.PUReportedMetric {
	return types.PUReportedMetric{
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
}

func TestNewLabelSet(t *testing.T) {
	t.Run("should create the correct LabelSet from types.PUReportedMetric", func(t *testing.T) {
		inputMetric := createMetricObject("some-event-name")
		labelSet := NewLabelSet(inputMetric)

		assert.Equal(t, "some-source-id", labelSet.SourceID)
		assert.Equal(t, "some-event-name", labelSet.EventName) // Default value
	})

	t.Run("should create the correct LabelSet with custom EventName", func(t *testing.T) {
		inputMetric := createMetricObject("custom-event-name")
		labelSet := NewLabelSet(inputMetric)

		assert.Equal(t, "some-source-id", labelSet.SourceID)
		assert.Equal(t, "custom-event-name", labelSet.EventName) // Custom event name
	})
}

func TestGenerateHash(t *testing.T) {
	t.Run("same hash for same LabelSet", func(t *testing.T) {
		inputMetric1 := createMetricObject("some-event-name")
		labelSet1 := NewLabelSet(inputMetric1)

		inputMetric2 := createMetricObject("some-event-name")
		labelSet2 := NewLabelSet(inputMetric2)

		hash1 := labelSet1.generateHash()
		hash2 := labelSet2.generateHash()

		assert.Equal(t, hash1, hash2)
	})

	t.Run("different hash for different LabelSet", func(t *testing.T) {
		inputMetric1 := createMetricObject("some-event-name-1")
		labelSet1 := NewLabelSet(inputMetric1)

		inputMetric2 := createMetricObject("some-event-name-2")
		labelSet2 := NewLabelSet(inputMetric2)

		hash1 := labelSet1.generateHash()
		hash2 := labelSet2.generateHash()

		assert.NotEqual(t, hash1, hash2)
	})
}
