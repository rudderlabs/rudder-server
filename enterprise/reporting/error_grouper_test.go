package reporting

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestErrorGrouper_GroupErrors(t *testing.T) {
	edr := &ErrorDetailReporter{
		groupingThreshold: config.GetReloadableFloat64Var(0.8, "Reporting.errorReporting.groupingThreshold"),
	}

	// Create test metrics
	metrics := []*types.PUReportedMetric{
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:      "source1",
				DestinationID: "dest1",
			},
			PUDetails: types.PUDetails{
				PU: "processor",
			},
			StatusDetail: &types.StatusDetail{
				EventType: "track",
				ErrorDetails: types.ErrorDetails{
					Message: "Database connection failed: timeout",
					Code:    "DB_TIMEOUT",
				},
				Count: 5,
			},
		},
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:      "source1",
				DestinationID: "dest1",
			},
			PUDetails: types.PUDetails{
				PU: "processor",
			},
			StatusDetail: &types.StatusDetail{
				EventType: "track",
				ErrorDetails: types.ErrorDetails{
					Message: "Database connection failed: timeout",
					Code:    "DB_TIMEOUT",
				},
				Count: 3,
			},
		},
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:      "source1",
				DestinationID: "dest1",
			},
			PUDetails: types.PUDetails{
				PU: "processor",
			},
			StatusDetail: &types.StatusDetail{
				EventType: "track",
				ErrorDetails: types.ErrorDetails{
					Message: "Authentication failed: invalid credentials",
					Code:    "AUTH_ERROR",
				},
				Count: 2,
			},
		},
	}

	// Group errors by connection
	groups := edr.groupByConnection(metrics)

	groupKey := "source1::dest1::processor::track"
	groupMetrics := groups[groupKey]
	require.Len(t, groupMetrics, 3, "Should have 3 metrics in the group")

	// Merge metrics by exact error message match
	mergedGroups := edr.mergeMetricGroupsByErrorMessage(groups)
	mergedMetrics := mergedGroups[groupKey]
	require.Len(t, mergedMetrics, 2, "Should have 2 metrics after merging identical error messages")

	for _, metric := range mergedMetrics {
		switch metric.StatusDetail.ErrorDetails.Code {
		case "DB_TIMEOUT":
			require.Equal(t, int64(8), metric.StatusDetail.Count, "DB group should have total count of 8")
		case "AUTH_ERROR":
			require.Equal(t, int64(2), metric.StatusDetail.Count, "Auth group should have total count of 2")
		default:
			t.Fatalf("Unexpected error code: %s", metric.StatusDetail.ErrorDetails.Code)
		}
	}
}

func TestErrorGrouper_GroupByConnection(t *testing.T) {
	edr := &ErrorDetailReporter{
		groupingThreshold: config.GetReloadableFloat64Var(0.8, "Reporting.errorReporting.groupingThreshold"),
	}

	// Create test metrics with different connection details
	metrics := []*types.PUReportedMetric{
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:      "source1",
				DestinationID: "dest1",
			},
			PUDetails: types.PUDetails{
				PU: "processor",
			},
			StatusDetail: &types.StatusDetail{
				EventType: "track",
				ErrorDetails: types.ErrorDetails{
					Message: "Error 1",
				},
			},
		},
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:      "source1",
				DestinationID: "dest1",
			},
			PUDetails: types.PUDetails{
				PU: "processor",
			},
			StatusDetail: &types.StatusDetail{
				EventType: "track",
				ErrorDetails: types.ErrorDetails{
					Message: "Error 2",
				},
			},
		},
		{
			ConnectionDetails: types.ConnectionDetails{
				SourceID:      "source2",
				DestinationID: "dest1",
			},
			PUDetails: types.PUDetails{
				PU: "processor",
			},
			StatusDetail: &types.StatusDetail{
				EventType: "track",
				ErrorDetails: types.ErrorDetails{
					Message: "Error 3",
				},
			},
		},
	}

	// Group by connection
	connectionGroups := edr.groupByConnection(metrics)

	// Should have 2 connection groups
	require.Len(t, connectionGroups, 2, "Should group by connection details")

	// Check that metrics are grouped correctly
	key1 := "source1::dest1::processor::track"
	key2 := "source2::dest1::processor::track"

	require.Contains(t, connectionGroups, key1, "Should have group for source1")
	require.Contains(t, connectionGroups, key2, "Should have group for source2")
	require.Len(t, connectionGroups[key1], 2, "source1 group should have 2 metrics")
	require.Len(t, connectionGroups[key2], 1, "source2 group should have 1 metric")
}

func TestMergeMetricGroupsByErrorMessage(t *testing.T) {
	edr := &ErrorDetailReporter{
		groupingThreshold: config.GetReloadableFloat64Var(0.8, "Reporting.errorReporting.groupingThreshold"),
	}

	// Test data: multiple groups with similar error messages within the same connection
	groups := map[string][]*types.PUReportedMetric{
		"source1::dest1::processor::track": {
			{
				ConnectionDetails: types.ConnectionDetails{
					SourceID:      "source1",
					DestinationID: "dest1",
				},
				PUDetails: types.PUDetails{PU: "processor"},
				StatusDetail: &types.StatusDetail{
					EventType: "track",
					ErrorDetails: types.ErrorDetails{
						Message: "Database connection failed",
						Code:    "DB_ERROR",
					},
					Count: 5,
				},
			},
			{
				ConnectionDetails: types.ConnectionDetails{
					SourceID:      "source1",
					DestinationID: "dest1",
				},
				PUDetails: types.PUDetails{PU: "processor"},
				StatusDetail: &types.StatusDetail{
					EventType: "track",
					ErrorDetails: types.ErrorDetails{
						Message: "Database connection failed",
						Code:    "DB_ERROR",
					},
					Count: 3,
				},
			},
			{
				ConnectionDetails: types.ConnectionDetails{
					SourceID:      "source1",
					DestinationID: "dest1",
				},
				PUDetails: types.PUDetails{PU: "processor"},
				StatusDetail: &types.StatusDetail{
					EventType: "track",
					ErrorDetails: types.ErrorDetails{
						Message: "Authentication failed",
						Code:    "AUTH_ERROR",
					},
					Count: 2,
				},
			},
		},
		"source2::dest1::processor::track": {
			{
				ConnectionDetails: types.ConnectionDetails{
					SourceID:      "source2",
					DestinationID: "dest1",
				},
				PUDetails: types.PUDetails{PU: "processor"},
				StatusDetail: &types.StatusDetail{
					EventType: "track",
					ErrorDetails: types.ErrorDetails{
						Message: "Rate limited",
						Code:    "RATE_LIMITED",
					},
					Count: 1,
				},
			},
		},
	}

	// Merge metrics by error message similarity
	merged := edr.mergeMetricGroupsByErrorMessage(groups)
	metricsOfSource1 := merged["source1::dest1::processor::track"]
	metricsOfSource2 := merged["source2::dest1::processor::track"]

	require.Len(t, metricsOfSource1, 2, "Should have 2 metrics in source1::dest1::processor::track after merging")
	require.Len(t, metricsOfSource2, 1, "Should have 1 metric in source2::dest1::processor::track")

	for _, metric := range metricsOfSource1 {
		if metric.StatusDetail.ErrorDetails.Code == "DB_ERROR" {
			require.Equal(t, int64(8), metric.StatusDetail.Count, "Should have 8 metrics for DB_ERROR after merging")
		}
		if metric.StatusDetail.ErrorDetails.Code == "AUTH_ERROR" {
			require.Equal(t, int64(2), metric.StatusDetail.Count, "Should have 2 metrics for AUTH_ERROR")
		}
	}
}

func TestGenerateMetricGroupKey(t *testing.T) {
	edr := &ErrorDetailReporter{}

	metric := &types.PUReportedMetric{
		ConnectionDetails: types.ConnectionDetails{
			SourceID:      "test-source",
			DestinationID: "test-dest",
		},
		PUDetails: types.PUDetails{
			PU: "test-processor",
		},
		StatusDetail: &types.StatusDetail{
			EventType: "test-event",
		},
	}

	key := edr.generateMetricGroupKey(metric)
	expectedKey := "test-source::test-dest::test-processor::test-event"
	require.Equal(t, expectedKey, key, "Generated key should match expected format")
}
