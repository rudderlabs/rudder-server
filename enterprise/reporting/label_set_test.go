package reporting

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/types"
)

const someEventName = "some-event-name"

func createMetricObject(eventName string) types.PUReportedMetric {
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
	return metric
}

func TestNewLabelSet(t *testing.T) {
	t.Run("should create the correct LabelSet from types.PUReportedMetric", func(t *testing.T) {
		inputMetric := createMetricObject(someEventName)
		bucket := int64(28889820)
		labelSet := NewLabelSet(inputMetric, bucket)

		assert.Equal(t, "some-source-id", labelSet.SourceID)
		assert.Equal(t, someEventName, labelSet.EventName) // Default value
	})
}

func TestGenerateHashForMetrics(t *testing.T) {
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
			metric1:           createMetricObject(someEventName),
			metric2:           createMetricObject(someEventName),
			bucket1:           28889820,
			bucket2:           28889820,
			shouldHashesMatch: true,
		},
		{
			name:              "different hash for label set with different event name for metrics",
			metric1:           createMetricObject(someEventName),
			metric2:           createMetricObject("some-event-name-2"),
			bucket1:           28889820,
			bucket2:           28889820,
			shouldHashesMatch: false,
		},
		{
			name:              "different hash for label set with different buckets for metrics",
			metric1:           createMetricObject(someEventName),
			metric2:           createMetricObject(someEventName),
			bucket1:           28889000,
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

func TestNewLabelSetFromEDReportsDB(t *testing.T) {
	tests := []struct {
		name              string
		error1            types.EDReportsDB
		error2            types.EDReportsDB
		bucket1           int64
		bucket2           int64
		shouldHashesMatch bool
		expectedLabelSet  *LabelSet // nil means don't verify field mapping
	}{
		{
			name: "basic error object with field mapping and hash verification",
			error1: types.EDReportsDB{
				EDInstanceDetails: types.EDInstanceDetails{
					WorkspaceID: "test-workspace",
					Namespace:   "test-namespace",
					InstanceID:  "test-instance",
				},
				EDConnectionDetails: types.EDConnectionDetails{
					SourceID:                "test-source",
					DestinationID:           "test-destination",
					SourceDefinitionId:      "test-source-def",
					DestinationDefinitionId: "test-dest-def",
					DestType:                "test-dest-type",
				},
				EDErrorDetails: types.EDErrorDetails{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						StatusCode:   500,
						ErrorCode:    "TEST_ERROR",
						ErrorMessage: "Test error message",
						EventType:    "track",
						EventName:    "test_event",
					},
					SampleResponse: "test response",
					SampleEvent:    []byte(`{"test": "event"}`),
					ErrorCount:     10,
				},
				PU:    "test-pu",
				Count: 10,
			},
			error2: types.EDReportsDB{
				EDInstanceDetails: types.EDInstanceDetails{
					WorkspaceID: "test-workspace",
					Namespace:   "test-namespace",
					InstanceID:  "test-instance",
				},
				EDConnectionDetails: types.EDConnectionDetails{
					SourceID:                "test-source",
					DestinationID:           "test-destination",
					SourceDefinitionId:      "test-source-def",
					DestinationDefinitionId: "test-dest-def",
					DestType:                "test-dest-type",
				},
				EDErrorDetails: types.EDErrorDetails{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						StatusCode:   500,
						ErrorCode:    "TEST_ERROR",
						ErrorMessage: "Test error message",
						EventType:    "track",
						EventName:    "test_event",
					},
					SampleResponse: "test response",
					SampleEvent:    []byte(`{"test": "event"}`),
					ErrorCount:     10,
				},
				PU:    "test-pu",
				Count: 10,
			},
			bucket1:           28889820,
			bucket2:           28889820,
			shouldHashesMatch: true,
			expectedLabelSet: &LabelSet{
				WorkspaceID:             "test-workspace",
				SourceDefinitionID:      "test-source-def",
				SourceCategory:          "",
				SourceID:                "test-source",
				DestinationDefinitionID: "test-dest-def",
				DestinationID:           "test-destination",
				SourceTaskRunID:         "",
				SourceJobID:             "",
				SourceJobRunID:          "",
				TransformationID:        "",
				TransformationVersionID: "",
				TrackingPlanID:          "",
				TrackingPlanVersion:     0,
				InPU:                    "",
				PU:                      "test-pu",
				Status:                  "",
				TerminalState:           false,
				InitialState:            false,
				StatusCode:              500,
				EventName:               "test_event",
				EventType:               "track",
				ErrorType:               "",
				ErrorCode:               "TEST_ERROR",
				ErrorMessage:            "Test error message",
				Bucket:                  int64(28889820),
			},
		},
		{
			name: "different hash for different error messages",
			error1: types.EDReportsDB{
				EDInstanceDetails: types.EDInstanceDetails{
					WorkspaceID: "workspace1",
					Namespace:   "test-namespace",
					InstanceID:  "test-instance",
				},
				EDConnectionDetails: types.EDConnectionDetails{
					SourceID:                "source1",
					DestinationID:           "dest1",
					SourceDefinitionId:      "test-source-def",
					DestinationDefinitionId: "test-dest-def",
					DestType:                "test-dest-type",
				},
				EDErrorDetails: types.EDErrorDetails{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						StatusCode:   500,
						ErrorCode:    "ERROR_1",
						ErrorMessage: "Some error message 1",
						EventType:    "track",
						EventName:    someEventName,
					},
					SampleResponse: `{"some-sample-response-key": "some-sample-response-value"}`,
					SampleEvent:    []byte(`{"some-sample-event-key": "some-sample-event-value"}`),
					ErrorCount:     1,
				},
				PU:    "pu1",
				Count: 1,
			},
			error2: types.EDReportsDB{
				EDInstanceDetails: types.EDInstanceDetails{
					WorkspaceID: "workspace1",
					Namespace:   "test-namespace",
					InstanceID:  "test-instance",
				},
				EDConnectionDetails: types.EDConnectionDetails{
					SourceID:                "source1",
					DestinationID:           "dest1",
					SourceDefinitionId:      "test-source-def",
					DestinationDefinitionId: "test-dest-def",
					DestType:                "test-dest-type",
				},
				EDErrorDetails: types.EDErrorDetails{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						StatusCode:   500,
						ErrorCode:    "ERROR_1",
						ErrorMessage: "Some error message 2",
						EventType:    "track",
						EventName:    someEventName,
					},
					SampleResponse: `{"some-sample-response-key": "some-sample-response-value"}`,
					SampleEvent:    []byte(`{"some-sample-event-key": "some-sample-event-value"}`),
					ErrorCount:     1,
				},
				PU:    "pu1",
				Count: 1,
			},
			bucket1:           28889820,
			bucket2:           28889820,
			shouldHashesMatch: false,
			expectedLabelSet:  nil, // Only test hash generation
		},
		{
			name: "different hash for different workspaces",
			error1: types.EDReportsDB{
				EDInstanceDetails: types.EDInstanceDetails{
					WorkspaceID: "workspace1",
					Namespace:   "test-namespace",
					InstanceID:  "test-instance",
				},
				EDConnectionDetails: types.EDConnectionDetails{
					SourceID:                "source1",
					DestinationID:           "dest1",
					SourceDefinitionId:      "test-source-def",
					DestinationDefinitionId: "test-dest-def",
					DestType:                "test-dest-type",
				},
				EDErrorDetails: types.EDErrorDetails{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						StatusCode:   500,
						ErrorCode:    "ERROR_1",
						ErrorMessage: "Some error message",
						EventType:    "track",
						EventName:    someEventName,
					},
					SampleResponse: `{"some-sample-response-key": "some-sample-response-value"}`,
					SampleEvent:    []byte(`{"some-sample-event-key": "some-sample-event-value"}`),
					ErrorCount:     1,
				},
				PU:    "pu1",
				Count: 1,
			},
			error2: types.EDReportsDB{
				EDInstanceDetails: types.EDInstanceDetails{
					WorkspaceID: "workspace2",
					Namespace:   "test-namespace",
					InstanceID:  "test-instance",
				},
				EDConnectionDetails: types.EDConnectionDetails{
					SourceID:                "source1",
					DestinationID:           "dest1",
					SourceDefinitionId:      "test-source-def",
					DestinationDefinitionId: "test-dest-def",
					DestType:                "test-dest-type",
				},
				EDErrorDetails: types.EDErrorDetails{
					EDErrorDetailsKey: types.EDErrorDetailsKey{
						StatusCode:   500,
						ErrorCode:    "ERROR_1",
						ErrorMessage: "Some error message",
						EventType:    "track",
						EventName:    someEventName,
					},
					SampleResponse: `{"some-sample-response-key": "some-sample-response-value"}`,
					SampleEvent:    []byte(`{"some-sample-event-key": "some-sample-event-value"}`),
					ErrorCount:     1,
				},
				PU:    "pu1",
				Count: 1,
			},
			bucket1:           28889820,
			bucket2:           28889820,
			shouldHashesMatch: false,
			expectedLabelSet:  nil, // Only test hash generation
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			labelSet1 := NewLabelSetFromEDReportsDB(test.error1, test.bucket1)
			labelSet2 := NewLabelSetFromEDReportsDB(test.error2, test.bucket2)

			// Verify field mapping if expectedLabelSet is provided
			if test.expectedLabelSet != nil {
				require.Equal(t, test.expectedLabelSet.WorkspaceID, labelSet1.WorkspaceID)
				require.Equal(t, test.expectedLabelSet.SourceDefinitionID, labelSet1.SourceDefinitionID)
				require.Equal(t, test.expectedLabelSet.SourceCategory, labelSet1.SourceCategory)
				require.Equal(t, test.expectedLabelSet.SourceID, labelSet1.SourceID)
				require.Equal(t, test.expectedLabelSet.DestinationDefinitionID, labelSet1.DestinationDefinitionID)
				require.Equal(t, test.expectedLabelSet.DestinationID, labelSet1.DestinationID)
				require.Equal(t, test.expectedLabelSet.SourceTaskRunID, labelSet1.SourceTaskRunID)
				require.Equal(t, test.expectedLabelSet.SourceJobID, labelSet1.SourceJobID)
				require.Equal(t, test.expectedLabelSet.SourceJobRunID, labelSet1.SourceJobRunID)
				require.Equal(t, test.expectedLabelSet.TransformationID, labelSet1.TransformationID)
				require.Equal(t, test.expectedLabelSet.TransformationVersionID, labelSet1.TransformationVersionID)
				require.Equal(t, test.expectedLabelSet.TrackingPlanID, labelSet1.TrackingPlanID)
				require.Equal(t, test.expectedLabelSet.TrackingPlanVersion, labelSet1.TrackingPlanVersion)
				require.Equal(t, test.expectedLabelSet.InPU, labelSet1.InPU)
				require.Equal(t, test.expectedLabelSet.PU, labelSet1.PU)
				require.Equal(t, test.expectedLabelSet.Status, labelSet1.Status)
				require.Equal(t, test.expectedLabelSet.TerminalState, labelSet1.TerminalState)
				require.Equal(t, test.expectedLabelSet.InitialState, labelSet1.InitialState)
				require.Equal(t, test.expectedLabelSet.StatusCode, labelSet1.StatusCode)
				require.Equal(t, test.expectedLabelSet.EventName, labelSet1.EventName)
				require.Equal(t, test.expectedLabelSet.EventType, labelSet1.EventType)
				require.Equal(t, test.expectedLabelSet.ErrorType, labelSet1.ErrorType)
				require.Equal(t, test.expectedLabelSet.ErrorCode, labelSet1.ErrorCode)
				require.Equal(t, test.expectedLabelSet.ErrorMessage, labelSet1.ErrorMessage)
				require.Equal(t, test.expectedLabelSet.Bucket, labelSet1.Bucket)
			}

			// Verify hash generation
			hash1 := labelSet1.generateHash()
			hash2 := labelSet2.generateHash()

			require.Equal(t, test.shouldHashesMatch, hash1 == hash2)
		})
	}
}
