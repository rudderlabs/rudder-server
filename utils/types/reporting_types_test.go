package types_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-server/utils/types"
)

func TestMetricJSONMarshaling(t *testing.T) {
	expectedJSON := `{
		"workspaceId": "SomeWorkspaceId",
		"namespace": "SomeNamespace",
		"instanceId": "1",
		"sourceId": "SomeSourceId",
		"destinationId": "SomeDestinationId",
		"DestinationDefinitionId": "SomeDestinationDefinitionId",
		"sourceDefinitionId": "SomeSourceDefinitionId",
		"sourceTaskRunId": "",
		"sourceJobId": "",
		"sourceJobRunId": "",
		"sourceCategory": "SomeSourceCategory",
		"inReportedBy": "router",
		"reportedBy": "router",
		"transformationId": "",
		"transformationVersionId": "2",
		"terminalState": true,
		"initialState": false,
		"reportedAt": 1730712600000,
		"trackingPlanId": "1",
		"trackingPlanVersion": 1,
		"bucket": 1730712600000,
		"reports": [
			{
				"state": "failed",
				"count": 20,
				"errorType": "this is errorType",
				"statusCode": 400,
				"violationCount": 12,
				"sampleResponse": "error email not valid",
				"sampleEvent": {"key": "value-1"},
				"eventName": "SomeEventName1",
				"eventType": "SomeEventType"
			},
			{
				"state": "failed",
				"count": 20,
				"errorType": "this is errorType",
				"statusCode": 400,
				"violationCount": 12,
				"sampleResponse": "error email not valid",
				"sampleEvent": {"key": "value-1"},
				"eventName": "SomeEventName2",
				"eventType": "SomeEventType"
			}
		]
	}`

	// Populate the Metric struct
	metric := types.Metric{
		InstanceDetails: types.InstanceDetails{
			WorkspaceID: "SomeWorkspaceId",
			Namespace:   "SomeNamespace",
			InstanceID:  "1",
		},
		ConnectionDetails: types.ConnectionDetails{
			SourceID:                "SomeSourceId",
			DestinationID:           "SomeDestinationId",
			SourceDefinitionID:      "SomeSourceDefinitionId",
			DestinationDefinitionID: "SomeDestinationDefinitionId",
			SourceTaskRunID:         "",
			SourceJobID:             "",
			SourceJobRunID:          "",
			SourceCategory:          "SomeSourceCategory",
			TransformationID:        "",
			TransformationVersionID: "2",
			TrackingPlanID:          "1",
			TrackingPlanVersion:     1,
		},
		PUDetails: types.PUDetails{
			InPU:       "router",
			PU:         "router",
			TerminalPU: true,
			InitialPU:  false,
		},
		ReportMetadata: types.ReportMetadata{
			ReportedAt:        1730712600000,
			SampleEventBucket: 1730712600000,
		},
		StatusDetails: []*types.StatusDetail{
			{
				Status:         "failed",
				Count:          20,
				StatusCode:     400,
				SampleResponse: "error email not valid",
				SampleEvent:    json.RawMessage(`{"key": "value-1"}`),
				EventName:      "SomeEventName1",
				EventType:      "SomeEventType",
				ErrorType:      "this is errorType",
				ViolationCount: 12,
				StatTags: map[string]string{
					"category": "validation",
				},
				FailedMessages: []*types.FailedMessage{
					{
						MessageID:  "1",
						ReceivedAt: time.Now(),
					},
				},
			},
			{
				Status:         "failed",
				Count:          20,
				StatusCode:     400,
				SampleResponse: "error email not valid",
				SampleEvent:    json.RawMessage(`{"key": "value-1"}`),
				EventName:      "SomeEventName2",
				EventType:      "SomeEventType",
				ErrorType:      "this is errorType",
				ViolationCount: 12,
				StatTags: map[string]string{
					"category": "autentication",
				},
			},
		},
	}

	marshaledJSON, err := jsonrs.Marshal(metric)
	require.NoError(t, err)
	require.JSONEq(t, expectedJSON, string(marshaledJSON))
}

func TestAssertKeysSubset(t *testing.T) {
	testcases := []struct {
		name       string
		subset     map[string]string
		superset   map[string]string
		shouldPass bool
	}{
		{
			name:       "maps_are_identical",
			subset:     map[string]string{"key1": "value1", "key2": "value2"},
			superset:   map[string]string{"key1": "value1", "key2": "value2"},
			shouldPass: true,
		},
		{
			name:       "subset_is_proper_subset_of_superset",
			subset:     map[string]string{"key1": "value1", "key2": "value2"},
			superset:   map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"},
			shouldPass: true,
		},
		{
			name:       "both_maps_are_empty",
			subset:     map[string]string{},
			superset:   map[string]string{},
			shouldPass: true,
		},
		{
			name:       "subset_is_empty_and_superset_has_keys",
			subset:     map[string]string{},
			superset:   map[string]string{"key1": "value1", "key2": "value2"},
			shouldPass: true,
		},
		{
			name:       "subset_has_keys_and_superset_is_empty",
			subset:     map[string]string{"key1": "value1"},
			superset:   map[string]string{},
			shouldPass: false,
		},
		{
			name:       "same_keys_have_different_values",
			subset:     map[string]string{"key1": "original", "key2": "data"},
			superset:   map[string]string{"key1": "modified", "key2": "info"},
			shouldPass: true,
		},
		{
			name:       "subset_has_extra_keys",
			subset:     map[string]string{"key1": "value1", "key2": "value2", "extra": "value3"},
			superset:   map[string]string{"key1": "value1", "key2": "value2"},
			shouldPass: false,
		},
		{
			name:       "maps_have_no_common_keys",
			subset:     map[string]string{"x": "1", "y": "2"},
			superset:   map[string]string{"a": "1", "b": "2"},
			shouldPass: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.shouldPass {
				types.AssertKeysSubset(tc.subset, tc.superset)
			} else {
				require.Panics(t, func() {
					types.AssertKeysSubset(tc.subset, tc.superset)
				})
			}
		})
	}
}
