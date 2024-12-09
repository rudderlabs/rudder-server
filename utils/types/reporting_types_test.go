package types_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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

	marshaledJSON, err := json.Marshal(metric)
	require.NoError(t, err)
	require.JSONEq(t, expectedJSON, string(marshaledJSON))
}
