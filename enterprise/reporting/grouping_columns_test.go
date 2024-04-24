package reporting

import (
	"testing"

	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/stretchr/testify/assert"
)

func createMetricObject() types.PUReportedMetric {
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
			EventName:      "some-event-name",
			EventType:      "some-event-type",
		},
	}
}

func TestNewGroupingColumns(t *testing.T) {
	t.Run("should create the correct grouping columns object from types.PUReportedMetric passed", func(t *testing.T) {
		inputMetric := createMetricObject()
		groupingColumns := NewGroupingColumns(inputMetric)
		assert.Equal(t, "some-source-id", groupingColumns.SourceID)
		assert.Equal(t, "some-event-name", groupingColumns.EventName)
	})
}

func TestGenerateHash(t *testing.T) {
	t.Run("same hash for same grouping columns", func(t *testing.T) {
		inputMetric1 := createMetricObject()
		groupingColumns1 := NewGroupingColumns(inputMetric1)

		inputMetric2 := createMetricObject()
		groupingColumns2 := NewGroupingColumns(inputMetric2)

		hash1 := groupingColumns1.generateHash()
		hash2 := groupingColumns2.generateHash()

		assert.Equal(t, hash1, hash2)
	})

	t.Run("different hash for different grouping columns", func(t *testing.T) {
		inputMetric1 := createMetricObject()
		inputMetric1.StatusDetail.EventName = "some-event-name-1"
		groupingColumns1 := NewGroupingColumns(inputMetric1)

		inputMetric2 := createMetricObject()
		inputMetric2.StatusDetail.EventName = "some-event-name-2"
		groupingColumns2 := NewGroupingColumns(inputMetric2)

		hash1 := groupingColumns1.generateHash()
		hash2 := groupingColumns2.generateHash()

		assert.NotEqual(t, hash1, hash2)
	})
}
