package reporting

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestErrorGrouper_GroupErrors(t *testing.T) {
	edr := &ErrorDetailReporter{}

	// Create test metrics
	metrics := []*types.EDReportsDB{
		{
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:      "source1",
				DestinationID: "dest1",
			},
			PU: "processor",
			EDErrorDetails: types.EDErrorDetails{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					EventType:    "track",
					ErrorMessage: "Database connection failed: timeout",
					ErrorCode:    "DB_TIMEOUT",
				},
				ErrorCount: 5,
			},
			Count: 5,
		},
		{
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:      "source1",
				DestinationID: "dest1",
			},
			PU: "processor",
			EDErrorDetails: types.EDErrorDetails{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					EventType:    "track",
					ErrorMessage: "Database connection failed: timeout",
					ErrorCode:    "DB_TIMEOUT",
				},
				ErrorCount: 3,
			},
			Count: 3,
		},
		{
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:      "source1",
				DestinationID: "dest1",
			},
			PU: "processor",
			EDErrorDetails: types.EDErrorDetails{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					EventType:    "track",
					ErrorMessage: "Authentication failed: invalid credentials",
					ErrorCode:    "AUTH_ERROR",
				},
				ErrorCount: 2,
			},
			Count: 2,
		},
	}

	// Group errors by connection
	groups := edr.groupByConnection(metrics)

	groupKey := types.ErrorDetailGroupKey{
		SourceID:      "source1",
		DestinationID: "dest1",
		PU:            "processor",
		EventType:     "track",
	}
	groupMetrics := groups[groupKey]
	require.Len(t, groupMetrics, 3, "Should have 3 metrics in the group")

	// Merge metrics by exact error message match
	mergedGroups := edr.mergeMetricGroupsByErrorMessage(groups)
	mergedMetrics := mergedGroups[groupKey]
	require.Len(t, mergedMetrics, 2, "Should have 2 metrics after merging identical error messages")

	for _, metric := range mergedMetrics {
		switch metric.ErrorCode {
		case "DB_TIMEOUT":
			require.Equal(t, int64(8), metric.Count, "DB group should have total count of 8")
		case "AUTH_ERROR":
			require.Equal(t, int64(2), metric.Count, "Auth group should have total count of 2")
		default:
			t.Fatalf("Unexpected error code: %s", metric.ErrorCode)
		}
	}
}

func TestErrorGrouper_GroupByConnection(t *testing.T) {
	edr := &ErrorDetailReporter{}

	// Create test metrics with different connection details
	metrics := []*types.EDReportsDB{
		{
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:      "source1",
				DestinationID: "dest1",
			},
			PU: "processor",
			EDErrorDetails: types.EDErrorDetails{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					EventType:    "track",
					ErrorMessage: "Error 1",
				},
			},
		},
		{
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:      "source1",
				DestinationID: "dest1",
			},
			PU: "processor",
			EDErrorDetails: types.EDErrorDetails{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					EventType:    "track",
					ErrorMessage: "Error 2",
				},
			},
		},
		{
			EDConnectionDetails: types.EDConnectionDetails{
				SourceID:      "source2",
				DestinationID: "dest1",
			},
			PU: "processor",
			EDErrorDetails: types.EDErrorDetails{
				EDErrorDetailsKey: types.EDErrorDetailsKey{
					EventType:    "track",
					ErrorMessage: "Error 3",
				},
			},
		},
	}

	// Group by connection
	connectionGroups := edr.groupByConnection(metrics)

	// Should have 2 connection groups
	require.Len(t, connectionGroups, 2, "Should group by connection details")

	// Check that metrics are grouped correctly
	key1 := types.ErrorDetailGroupKey{
		SourceID:      "source1",
		DestinationID: "dest1",
		PU:            "processor",
		EventType:     "track",
	}
	key2 := types.ErrorDetailGroupKey{
		SourceID:      "source2",
		DestinationID: "dest1",
		PU:            "processor",
		EventType:     "track",
	}

	require.Contains(t, connectionGroups, key1, "Should have group for source1")
	require.Contains(t, connectionGroups, key2, "Should have group for source2")
	require.Len(t, connectionGroups[key1], 2, "source1 group should have 2 metrics")
	require.Len(t, connectionGroups[key2], 1, "source2 group should have 1 metric")
}

func TestMergeMetricGroupsByErrorMessage(t *testing.T) {
	edr := &ErrorDetailReporter{}

	// Test data: multiple groups with similar error messages within the same connection
	key1 := types.ErrorDetailGroupKey{
		SourceID:      "source1",
		DestinationID: "dest1",
		PU:            "processor",
		EventType:     "track",
	}
	key2 := types.ErrorDetailGroupKey{
		SourceID:      "source2",
		DestinationID: "dest1",
		PU:            "processor",
		EventType:     "track",
	}
	groups := map[types.ErrorDetailGroupKey][]*types.EDReportsDB{
		key1: {
			{
				EDConnectionDetails: types.EDConnectionDetails{
					SourceID:      "source1",
					DestinationID: "dest1",
				},
				PU: "processor",
				EDErrorDetails: types.EDErrorDetails{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						EventType:    "track",
						ErrorMessage: "Database connection failed",
						ErrorCode:    "DB_ERROR",
					},
					ErrorCount: 5,
				},
				Count: 5,
			},
			{
				EDConnectionDetails: types.EDConnectionDetails{
					SourceID:      "source1",
					DestinationID: "dest1",
				},
				PU: "processor",
				EDErrorDetails: types.EDErrorDetails{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						EventType:    "track",
						ErrorMessage: "Database connection failed",
						ErrorCode:    "DB_ERROR",
					},
					ErrorCount: 3,
				},
				Count: 3,
			},
			{
				EDConnectionDetails: types.EDConnectionDetails{
					SourceID:      "source1",
					DestinationID: "dest1",
				},
				PU: "processor",
				EDErrorDetails: types.EDErrorDetails{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						EventType:    "track",
						ErrorMessage: "Authentication failed",
						ErrorCode:    "AUTH_ERROR",
					},
					ErrorCount: 2,
				},
				Count: 2,
			},
		},
		key2: {
			{
				EDConnectionDetails: types.EDConnectionDetails{
					SourceID:      "source2",
					DestinationID: "dest1",
				},
				PU: "processor",
				EDErrorDetails: types.EDErrorDetails{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						EventType:    "track",
						ErrorMessage: "Rate limited",
						ErrorCode:    "RATE_LIMITED",
					},
					ErrorCount: 1,
				},
				Count: 1,
			},
		},
	}

	// Merge metrics by error message similarity
	merged := edr.mergeMetricGroupsByErrorMessage(groups)
	metricsOfSource1 := merged[key1]
	metricsOfSource2 := merged[key2]

	require.Len(t, metricsOfSource1, 2, "Should have 2 metrics in source1::dest1::processor::track after merging")
	require.Len(t, metricsOfSource2, 1, "Should have 1 metric in source2::dest1::processor::track")

	for _, metric := range metricsOfSource1 {
		if metric.ErrorCode == "DB_ERROR" {
			require.Equal(t, int64(8), metric.Count, "Should have 8 metrics for DB_ERROR after merging")
		}
		if metric.ErrorCode == "AUTH_ERROR" {
			require.Equal(t, int64(2), metric.Count, "Should have 2 metrics for AUTH_ERROR")
		}
	}
}

func TestGenerateMetricGroupKey(t *testing.T) {
	edr := &ErrorDetailReporter{}

	metric := &types.EDReportsDB{
		EDConnectionDetails: types.EDConnectionDetails{
			SourceID:      "test-source",
			DestinationID: "test-dest",
		},
		PU: "test-processor",
		EDErrorDetails: types.EDErrorDetails{
			EDErrorDetailsKey: types.EDErrorDetailsKey{
				EventType: "test-event",
			},
		},
	}

	key := edr.generateMetricGroupKey(metric)
	expectedKey := types.ErrorDetailGroupKey{
		SourceID:      "test-source",
		DestinationID: "test-dest",
		PU:            "test-processor",
		EventType:     "test-event",
	}
	require.Equal(t, expectedKey, key, "Generated key should match expected format")
}
