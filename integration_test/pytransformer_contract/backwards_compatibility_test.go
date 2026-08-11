package pytransformer_contract

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
	utilstypes "github.com/rudderlabs/rudder-server/utils/types"
)

// TestBackwardsCompatibility compares responses from the baseline
// rudder-pytransformer against the candidate one for various edge cases.
//
// Both containers are started once and shared across all subtests: each fetches
// transformation code per request from the shared mock config backend, so one
// container per version serves every versionID.
func TestBackwardsCompatibility(t *testing.T) {
	type subtest struct {
		name                string
		versionID           string
		config              configBackendEntry
		run                 func(t *testing.T, env *bcTestEnv)
		skipRetryCountMatch bool
	}

	subtests := []subtest{
		{
			name:      "TransformationCodeNotFound",
			versionID: "bc-not-found-v1",
			// config is zero-value — versionId is NOT registered in config backend.
			// Config backend returns 404 for unknown versionIds, so the error
			// happens before the transformation ever executes.
			run: func(t *testing.T, env *bcTestEnv) {
				const versionId = "bc-not-found-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionId),
					makeEvent("msg-2", versionId),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.True(t, len(baselineResp.FailedEvents) > 0, "baseline: expected at least 1 failed event")
				require.True(t, len(candidateResp.FailedEvents) > 0, "candidate: expected at least 1 failed event")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical error responses for unknown versionId")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "EventExpansion",
			versionID: "bc-event-expansion-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    # Return a list to expand one event into multiple events
    return [
        {"messageId": "exp-1", "type": "track", "event": "Click", "original": event.get("messageId")},
        {"messageId": "exp-2", "type": "track", "event": "View", "original": event.get("messageId")},
        {"type": "track", "event": "NoMessageId", "original": event.get("messageId")},
    ]
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionId = "bc-event-expansion-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionId),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Both should expand 1 input event into 3 output events (including one without messageId)
				require.Equal(t, 3, len(baselineResp.Events), "baseline: 3 expanded events expected")
				require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
				require.Equal(t, 3, len(candidateResp.Events), "candidate: 3 expanded events expected")
				require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical expanded event responses")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "ErrorMessageFormat",
			versionID: "bc-error-format-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    # Raise an error to trigger per-event error handling.
    # Call through a helper function to create a multi-line stack trace.
    return helper(event)

def helper(event):
    raise ValueError("intentional error for testing")
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionId = "bc-error-format-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionId),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")

				baselineError := baselineResp.FailedEvents[0].Error
				candidateError := candidateResp.FailedEvents[0].Error

				t.Logf("Baseline error message:\n%s", baselineError)
				t.Logf("Candidate error message:\n%s", candidateError)

				require.Contains(t, baselineError, "intentional error for testing", "baseline: error should contain the message")
				require.Contains(t, candidateError, "intentional error for testing", "candidate: error should contain the message")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical error messages")
				} else {
					t.Errorf("Responses differ:\n%s", diff)

					if len(baselineError) > len(candidateError) {
						t.Logf("Baseline error is longer (%d chars vs %d chars), likely contains stack trace",
							len(baselineError), len(candidateError))
					}
				}
			},
		},
		{
			name:      "BatchErrorFormat",
			versionID: "bc-batch-error-format-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    raise ValueError("intentional batch error for testing")
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionId = "bc-batch-error-format-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionId),
					makeEvent("msg-2", versionId),
					makeEvent("msg-3", versionId),
				}

				t.Log("Sending 3 events to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending 3 events to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				for i, fe := range baselineResp.FailedEvents {
					t.Logf("Baseline FailedEvent[%d]: statusCode=%d, error=%q, messageId=%q, messageIds=%v",
						i, fe.StatusCode, fe.Error, fe.Metadata.MessageID, fe.Metadata.MessageIDs)
				}
				for i, fe := range candidateResp.FailedEvents {
					t.Logf("Candidate FailedEvent[%d]: statusCode=%d, error=%q, messageId=%q, messageIds=%v",
						i, fe.StatusCode, fe.Error, fe.Metadata.MessageID, fe.Metadata.MessageIDs)
				}

				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Logf("Both versions returned %d failed events", len(baselineResp.FailedEvents))
				} else {
					t.Logf("Baseline: %d failed events, Candidate: %d failed events",
						len(baselineResp.FailedEvents), len(candidateResp.FailedEvents))
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "TransformEventNonDictReturn",
			versionID: "bc-non-dict-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    return "this is a string, not a dict"
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionId = "bc-non-dict-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionId),
				}

				t.Log("Sending request to baseline pytransformer...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate (rudder-pytransformer)...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				t.Logf("Baseline error: statusCode=%d, error=%q", baselineResp.FailedEvents[0].StatusCode, baselineResp.FailedEvents[0].Error)

				if len(candidateResp.FailedEvents) > 0 {
					t.Logf("Candidate error: statusCode=%d, error=%q", candidateResp.FailedEvents[0].StatusCode, candidateResp.FailedEvents[0].Error)
				}
				if len(candidateResp.Events) > 0 {
					t.Logf("Candidate success: statusCode=%d, output=%v", candidateResp.Events[0].StatusCode, candidateResp.Events[0].Output)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Responses are equal")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "MetadataPreservesOnlyPresentKeys",
			versionID: "bc-metadata-keys-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    m = metadata(event)
    for key, value in m.items():
        if not event.get(key):
            event[key] = value
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionId = "bc-metadata-keys-v1"

				events := []types.TransformerEvent{
					{
						Message: types.SingularEventT{
							"messageId": "msg-1",
							"type":      "track",
							"event":     "Test Event",
						},
						Metadata: types.Metadata{
							SourceID:      "src-1",
							DestinationID: "dest-1",
							WorkspaceID:   "ws-1",
							MessageID:     "msg-1",
							PartitionID:   "ws-1-42",
							InstanceID:    "2",
						},
						Destination: backendconfig.DestinationT{
							Transformations: []backendconfig.TransformationT{
								{VersionID: versionId, ID: "transformation-1", Language: "pythonfaas"},
							},
						},
					},
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")
				require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

				candidateOutput := candidateResp.Events[0].Output
				require.Equal(t, "src-1", candidateOutput["sourceId"])
				require.Equal(t, "2", candidateOutput["instanceId"])
				require.Equal(t, "ws-1-42", candidateOutput["partitionId"])

				for _, key := range []string{
					"eventName",
					"messageIds",
					"recordId",
					"sourceJobId",
					"sourceJobRunId",
					"sourceTaskRunId",
					"trackingPlanId",
					"trackingPlanVersion",
				} {
					_, ok := candidateOutput[key]
					require.Falsef(t, ok, "key %q should stay absent when it wasn't present in the input metadata", key)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Responses are equal")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "MetadataMergeAllKeys",
			versionID: "bc-metadata-merge-all-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    event.update(metadata(event))
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-metadata-merge-all-v1"
				events := []types.TransformerEvent{
					{
						Message: types.SingularEventT{
							"messageId": "msg-1",
							"type":      "track",
							"event":     "Test Event",
						},
						Metadata: types.Metadata{
							SourceID:                "src-1",
							DestinationID:           "dest-1",
							WorkspaceID:             "ws-1",
							MessageID:               "msg-1",
							InstanceID:              "2",
							PartitionID:             "ws-1-42",
							Namespace:               "test-ns",
							RudderID:                "rudder-id-1",
							ReceivedAt:              "2024-01-01T00:00:00.000Z",
							EventName:               "Test Event",
							EventType:               "track",
							SourceName:              "test-source",
							SourceType:              "javascript",
							DestinationName:         "test-dest",
							DestinationType:         "AM",
							SourceDefinitionID:      "src-def-1",
							DestinationDefinitionID: "dest-def-1",
							TransformationID:        "tf-1",
							TransformationVersionID: versionID,
						},
						Destination: backendconfig.DestinationT{
							Transformations: []backendconfig.TransformationT{
								{VersionID: versionID, ID: "transformation-1", Language: "pythonfaas"},
							},
						},
					},
				}
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")
				require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")
				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Responses are equal")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "ExecutionTimeGeneratedTimestampField",
			versionID: "bc-execution-time-field-v1",
			config: configBackendEntry{code: `
from datetime import datetime

def transformEvent(event, metadata):
    event["metadata"] = metadata(event)
    event["source"] = event["metadata"]["sourceId"]
    event["rudderstackTransformedUtc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-execution-time-field-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				baselineStartedAt := time.Now().UTC()
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				baselineFinishedAt := time.Now().UTC()

				candidateStartedAt := time.Now().UTC()
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				candidateFinishedAt := time.Now().UTC()

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")
				require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

				baselineTimestamp, ok := baselineResp.Events[0].Output["rudderstackTransformedUtc"].(string)
				require.True(t, ok, "baseline: generated timestamp should be present")
				candidateTimestamp, ok := candidateResp.Events[0].Output["rudderstackTransformedUtc"].(string)
				require.True(t, ok, "candidate: generated timestamp should be present")

				baselineParsed, err := time.ParseInLocation(time.DateTime, baselineTimestamp, time.UTC)
				require.NoError(t, err, "baseline: generated timestamp should parse")
				candidateParsed, err := time.ParseInLocation(time.DateTime, candidateTimestamp, time.UTC)
				require.NoError(t, err, "candidate: generated timestamp should parse")

				require.False(t, baselineParsed.Before(baselineStartedAt.Add(-time.Second)), "baseline timestamp should be created during the old request window")
				require.False(t, baselineParsed.After(baselineFinishedAt.Add(time.Second)), "baseline timestamp should be created during the old request window")
				require.False(t, candidateParsed.Before(candidateStartedAt.Add(-time.Second)), "candidate timestamp should be created during the new request window")
				require.False(t, candidateParsed.After(candidateFinishedAt.Add(time.Second)), "candidate timestamp should be created during the new request window")
				if baselineTimestamp != candidateTimestamp {
					t.Logf("Timestamps differ as expected: old=%s new=%s", baselineTimestamp, candidateTimestamp)
				} else {
					t.Log("Timestamps happened to land in the same second — datetime matching path not exercised this run")
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Responses are equal once datetime matching is applied")
				} else {
					t.Errorf("Responses differ despite datetime matching:\n%s", diff)
				}

				delete(baselineResp.Events[0].Output, "rudderstackTransformedUtc")
				delete(candidateResp.Events[0].Output, "rudderstackTransformedUtc")

				require.Equal(t, baselineResp, candidateResp, "execution-time field should be the only raw response difference")
			},
		},
		{
			name:      "BatchMetadata",
			versionID: "bc-batch-metadata-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    # Pass through all events unchanged to check if there is difference in metadata with transformBatch
    return events
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-batch-metadata-v1"

				// Send events with non-curated metadata fields set
				events := []types.TransformerEvent{
					{
						Message: types.SingularEventT{
							"messageId": "msg-1",
							"type":      "track",
							"event":     "Test Event",
						},
						Metadata: types.Metadata{
							SourceID:      "src-1",
							DestinationID: "dest-1",
							WorkspaceID:   "ws-1",
							MessageID:     "msg-1",
							// Non-curated fields that should be stripped by baseline but kept by candidate:
							TraceParent: "00-trace-id-span-id-01",
						},
						Destination: backendconfig.DestinationT{
							Transformations: []backendconfig.TransformationT{
								{VersionID: versionID, ID: "transformation-1", Language: "pythonfaas"},
							},
						},
					},
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Both should succeed
				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineMeta := baselineResp.Events[0].Metadata
				candidateMeta := candidateResp.Events[0].Metadata

				t.Logf("Baseline metadata: TraceParent=%q, SourceID=%q, MessageID=%q",
					baselineMeta.TraceParent, baselineMeta.SourceID, baselineMeta.MessageID)
				t.Logf("Candidate metadata: TraceParent=%q, SourceID=%q, MessageID=%q",
					candidateMeta.TraceParent, candidateMeta.SourceID, candidateMeta.MessageID)

				// Control: both should have curated fields
				require.Equal(t, "src-1", baselineMeta.SourceID, "baseline: sourceId should be present")
				require.Equal(t, "src-1", candidateMeta.SourceID, "candidate: sourceId should be present")

				// Compare: responses should be equal after Go unmarshaling (non-curated metadata differences are not observable)
				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Responses are equal")
					t.Log("This means the metadata difference is not observable after Go unmarshaling")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
					t.Logf("Baseline TraceParent=%q (expected empty), Candidate TraceParent=%q (expected set)",
						baselineMeta.TraceParent, candidateMeta.TraceParent)
				}
			},
		},
		{
			name:      "FilterWithNone",
			versionID: "bc-filter-with-none",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    # Filter some events (return None) to trigger 298 filter detection.
    if event["messageId"] != "body-msg-2":
        return None
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-filter-with-none"

				events := []types.TransformerEvent{
					makeEvent("body-msg-1", versionID),
					makeEvent("body-msg-2", versionID),
				}

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Len(t, baselineResp.FailedEvents, 1, "baseline: 1 failed event expected")
				require.Len(t, baselineResp.Events, 1, "baseline: 1 event expected")
				require.EqualValues(t, utilstypes.FilterEventCode, baselineResp.FailedEvents[0].StatusCode, "baseline: 298 filter detected")

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Compare: responses may differ in the 298 response metadata
				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Responses are equal")
					t.Log("This means the messageId source difference doesn't affect the output")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}

				// Send more, all should be filtered
				events = []types.TransformerEvent{
					makeEvent("body-msg-3", versionID),
					makeEvent("body-msg-4", versionID),
				}

				baselineResp = env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Len(t, baselineResp.FailedEvents, 2)
				require.Len(t, baselineResp.Events, 0)
				require.EqualValues(t, utilstypes.FilterEventCode, baselineResp.FailedEvents[0].StatusCode, "baseline: 298 filter detected")

				candidateResp = env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Compare: responses may differ in the 298 response metadata
				diff, equal = baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Responses are equal")
					t.Log("This means the messageId source difference doesn't affect the output")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "FilterWithDifferentMessageIds",
			versionID: "bc-filter-with-diff-msg-ids",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    # Filter all events (return None) to trigger 298 filter detection.
    return None
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-filter-with-diff-msg-ids"

				// Craft events where message.messageId and metadata.MessageID differ.
				// This is not normal in production (rudder-server copies them), but
				// exposes the difference in how input messageIds are tracked.
				events := []types.TransformerEvent{
					{
						Message: types.SingularEventT{
							"messageId": "body-msg-1", // message body messageId
							"type":      "track",
							"event":     "Test Event",
						},
						Metadata: types.Metadata{
							SourceID:      "src-1",
							DestinationID: "dest-1",
							WorkspaceID:   "ws-1",
							MessageID:     "meta-msg-1", // metadata messageId (different!)
						},
						Destination: backendconfig.DestinationT{
							Transformations: []backendconfig.TransformationT{
								{VersionID: versionID, ID: "transformation-1", Language: "pythonfaas"},
							},
						},
					},
					{
						Message: types.SingularEventT{
							"messageId": "body-msg-2",
							"type":      "track",
							"event":     "Test Event 2",
						},
						Metadata: types.Metadata{
							SourceID:      "src-1",
							DestinationID: "dest-1",
							WorkspaceID:   "ws-1",
							MessageID:     "meta-msg-2", // metadata messageId (different!)
						},
						Destination: backendconfig.DestinationT{
							Transformations: []backendconfig.TransformationT{
								{VersionID: versionID, ID: "transformation-1", Language: "pythonfaas"},
							},
						},
					},
				}

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// When all events are filtered, both versions should produce 298 responses.
				// The X-Feature-Filter-Code header is set by the usertransformer client.
				// Status 298 goes to FailedEvents (not 200).
				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Responses are equal")
					t.Log("This means the messageId source difference doesn't affect the output")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "ConfigBackendEmptyBody",
			versionID: "bc-cb-empty-body-v1",
			config:    configBackendEntry{statusCode: http.StatusOK, body: ""},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cb-empty-body-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.True(t, len(baselineResp.FailedEvents) > 0, "baseline: expected at least 1 failed event")
				require.True(t, len(candidateResp.FailedEvents) > 0, "candidate: expected at least 1 failed event")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for empty config backend body")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "ConfigBackendUnexpectedBody",
			versionID: "bc-cb-unexpected-body-v1",
			config:    configBackendEntry{statusCode: http.StatusOK, body: "hello world"},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cb-unexpected-body-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.True(t, len(baselineResp.FailedEvents) > 0, "baseline: expected at least 1 failed event")
				require.True(t, len(candidateResp.FailedEvents) > 0, "candidate: expected at least 1 failed event")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for unexpected config backend body")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "ConfigBackendNotFound",
			versionID: "bc-cb-not-found-v1",
			config:    configBackendEntry{statusCode: http.StatusNotFound},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cb-not-found-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.True(t, len(baselineResp.FailedEvents) > 0, "baseline: expected at least 1 failed event")
				require.True(t, len(candidateResp.FailedEvents) > 0, "candidate: expected at least 1 failed event")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for 404 config backend")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:                "ConfigBackendInternalError",
			versionID:           "bc-cb-500-v1",
			config:              configBackendEntry{statusCode: http.StatusInternalServerError},
			skipRetryCountMatch: true,
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cb-500-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				// Both versions return 503 + retry for a config backend 5xx, so the retries are exhausted and the
				// events fail on either side.
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.True(t, len(baselineResp.FailedEvents) > 0, "baseline: expected at least 1 failed event")

				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.True(t, len(candidateResp.FailedEvents) > 0, "candidate: expected at least 1 failed event")

				// Only compare status codes — error messages may differ between versions
				require.Equal(t, len(baselineResp.FailedEvents), len(candidateResp.FailedEvents), "failed event count mismatch")
				for i := range baselineResp.FailedEvents {
					require.Equal(t, baselineResp.FailedEvents[i].StatusCode, candidateResp.FailedEvents[i].StatusCode,
						"status code mismatch for failed event %d", i)
				}
			},
		},
		{
			name:                "ConfigBackend5xx",
			versionID:           "bc-cb-5xx-v1",
			skipRetryCountMatch: true,
			// config is zero-value — the individual 5xx versionIDs are registered separately below.
			run: func(t *testing.T, env *bcTestEnv) {
				for _, statusCode := range []int{
					http.StatusNotImplemented,
					http.StatusBadGateway,
					http.StatusServiceUnavailable,
				} {
					t.Run(strconv.Itoa(statusCode), func(t *testing.T) {
						versionID := fmt.Sprintf("bc-cb-%d-v1", statusCode)

						events := []types.TransformerEvent{
							makeEvent("msg-1", versionID),
							makeEvent("msg-2", versionID),
						}

						// Both versions return 503 + retry for a config backend 5xx, so the
						// retries are exhausted and the events fail on either side.
						baselineResp := env.BaselineClient.Transform(context.Background(), events)
						t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
						require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
						require.True(t, len(baselineResp.FailedEvents) > 0, "baseline: expected at least 1 failed event")

						candidateResp := env.CandidateClient.Transform(context.Background(), events)
						t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
						require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
						require.True(t, len(candidateResp.FailedEvents) > 0, "candidate: expected at least 1 failed event")

						// Only compare status codes — error messages may differ between versions
						require.Equal(t, len(baselineResp.FailedEvents), len(candidateResp.FailedEvents), "failed event count mismatch")
						for i := range baselineResp.FailedEvents {
							require.Equal(t, baselineResp.FailedEvents[i].StatusCode, candidateResp.FailedEvents[i].StatusCode,
								"status code mismatch for failed event %d", i)
						}
					})
				}
			},
		},
		{
			name:      "ConfigBackendBadRequest",
			versionID: "bc-cb-400-v1",
			config:    configBackendEntry{statusCode: http.StatusBadRequest},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cb-400-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.True(t, len(baselineResp.FailedEvents) > 0, "baseline: expected at least 1 failed event")
				require.True(t, len(candidateResp.FailedEvents) > 0, "candidate: expected at least 1 failed event")

				normalizeResponseErrors(&baselineResp)
				normalizeResponseErrors(&candidateResp)
				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for 400 config backend")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "PartialErrors",
			versionID: "bc-partial-errors-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    msg_id = event.get("messageId", "")
    if msg_id in ("msg-3", "msg-8"):
        return "this is a string, not a dict"
    event["processed"] = True
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionId = "bc-partial-errors-v1"

				// 10 events: msg-3 and msg-8 will return strings (errors),
				// the other 8 will succeed with processed=True.
				events := make([]types.TransformerEvent, 10)
				for i := range events {
					events[i] = makeEvent(fmt.Sprintf("msg-%d", i+1), versionId)
				}

				t.Log("Sending 10 events to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending 10 events to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// 8 events should succeed, 2 should fail
				require.Equal(t, 8, len(baselineResp.Events), "baseline: 8 success events expected")
				require.Equal(t, 2, len(baselineResp.FailedEvents), "baseline: 2 failed events expected")
				require.Equal(t, 8, len(candidateResp.Events), "candidate: 8 success events expected")
				require.Equal(t, 2, len(candidateResp.FailedEvents), "candidate: 2 failed events expected")

				// Log error details
				for i, fe := range baselineResp.FailedEvents {
					t.Logf("Baseline FailedEvent[%d]: statusCode=%d, error=%q", i, fe.StatusCode, fe.Error)
				}
				for i, fe := range candidateResp.FailedEvents {
					t.Logf("Candidate FailedEvent[%d]: statusCode=%d, error=%q", i, fe.StatusCode, fe.Error)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for partial errors")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "MessageIdTampering",
			versionID: "bc-msgid-tamper-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    event["originalMessageId"] = event.get("messageId", "")
    event["messageId"] = "tampered-" + event.get("messageId", "")
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionId = "bc-msgid-tamper-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionId),
					makeEvent("msg-2", versionId),
					makeEvent("msg-3", versionId),
				}

				t.Log("Sending 3 events to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending 3 events to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// All 3 should succeed
				require.Equal(t, 3, len(baselineResp.Events), "baseline: 3 success events expected")
				require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
				require.Equal(t, 3, len(candidateResp.Events), "candidate: 3 success events expected")
				require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

				// Verify tampering was applied
				for i, ev := range baselineResp.Events {
					output := ev.Output
					t.Logf("Baseline Event[%d]: messageId=%v, originalMessageId=%v, metadata.messageId=%v",
						i, output["messageId"], output["originalMessageId"], ev.Metadata.MessageID)
				}
				for i, ev := range candidateResp.Events {
					output := ev.Output
					t.Logf("Candidate Event[%d]: messageId=%v, originalMessageId=%v, metadata.messageId=%v",
						i, output["messageId"], output["originalMessageId"], ev.Metadata.MessageID)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for messageId tampering")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "ConfigBackendHTTP200With5xxBody",
			versionID: "bc-cb-200-body-500-v1",
			config:    configBackendEntry{statusCode: http.StatusOK, body: `{"statusCode":500,"error":"Internal Server Error"}`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cb-200-body-500-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.True(t, len(baselineResp.FailedEvents) > 0, "baseline: expected at least 1 failed event")
				require.True(t, len(candidateResp.FailedEvents) > 0, "candidate: expected at least 1 failed event")

				// Only compare status codes — error messages may differ between versions
				require.Equal(t, len(baselineResp.FailedEvents), len(candidateResp.FailedEvents), "failed event count mismatch")
				for i := range baselineResp.FailedEvents {
					require.Equal(t, baselineResp.FailedEvents[i].StatusCode, candidateResp.FailedEvents[i].StatusCode,
						"status code mismatch for failed event %d", i)
				}
			},
		},
		{
			name:      "ConfigBackendHTTP200With4xxBody",
			versionID: "bc-cb-200-body-400-v1",
			config:    configBackendEntry{statusCode: http.StatusOK, body: `{"statusCode":400,"error":"Bad Request"}`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cb-200-body-400-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.True(t, len(baselineResp.FailedEvents) > 0, "baseline: expected at least 1 failed event")
				require.True(t, len(candidateResp.FailedEvents) > 0, "candidate: expected at least 1 failed event")

				// Only compare status codes — error messages may differ between versions
				require.Equal(t, len(baselineResp.FailedEvents), len(candidateResp.FailedEvents), "failed event count mismatch")
				for i := range baselineResp.FailedEvents {
					require.Equal(t, baselineResp.FailedEvents[i].StatusCode, candidateResp.FailedEvents[i].StatusCode,
						"status code mismatch for failed event %d", i)
				}
			},
		},
		{
			name:      "BatchEventExpansion",
			versionID: "bc-batch-expansion-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
	result = []
	for event in events:
		click_event = event.copy()
		click_event["event"] = "Click"
		view_event = event.copy()
		view_event["event"] = "View"

		result.extend([click_event, view_event])

	return result
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-batch-expansion-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				t.Log("Sending 2 events to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending 2 events to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Both should expand 2 input events into 4 output events (2 per input)
				require.Equal(t, 4, len(baselineResp.Events), "baseline: 4 expanded events expected")
				require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
				require.Equal(t, 4, len(candidateResp.Events), "candidate: 4 expanded events expected")
				require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical expanded batch event responses")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "BatchEventExpansionNewMessageId",
			versionID: "bc-batch-expansion-new-msgid-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
	result = []
	for event in events:
		click_event = event.copy()
		click_event["event"] = "Click"
		click_event["messageId"] = "new-click-msg-id"

		view_event = event.copy()
		view_event["event"] = "View"
		view_event["messageId"] = "new-view-msg-id"

		result.extend([click_event, view_event])

	return result
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-batch-expansion-new-msgid-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				t.Log("Sending 2 events to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending 2 events to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 2, len(baselineResp.FailedEvents), "baseline: 2 failed events expected")

				require.Equal(t, 0, len(candidateResp.Events), "candidate: 0 success events expected (KeyError)")
				require.Equal(t, 2, len(candidateResp.FailedEvents), "candidate: 2 failed events expected")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical expanded batch event responses")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "BatchPartialDrop",
			versionID: "bc-batch-partial-drop-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    # Drop events where messageId is "msg-2", keep the rest
    return [e for e in events if e.get("messageId") != "msg-2"]
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-batch-partial-drop-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
					makeEvent("msg-3", versionID),
				}

				t.Log("Sending 3 events to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending 3 events to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// msg-2 should be dropped, msg-1 and msg-3 should pass through
				require.Equal(t, 2, len(baselineResp.Events), "baseline: 2 success events expected")
				require.Equal(t, 2, len(candidateResp.Events), "candidate: 2 success events expected")

				for i, ev := range baselineResp.Events {
					t.Logf("Baseline Event[%d]: messageId=%v", i, ev.Output["messageId"])
				}
				for i, ev := range candidateResp.Events {
					t.Logf("Candidate Event[%d]: messageId=%v", i, ev.Output["messageId"])
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for batch partial drop")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "BatchReturnNone",
			versionID: "bc-batch-return-none-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    return None
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-batch-return-none-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
					makeEvent("msg-2", versionID),
				}

				t.Log("Sending 2 events to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending 2 events to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.True(t, len(baselineResp.FailedEvents) == 2, "baseline: expected 2 failed events")
				require.True(t, len(candidateResp.FailedEvents) == 2, "candidate: expected 2 failed events")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for mixed indentation")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "MixedIndentation",
			versionID: "bc-mixed-indent-v1",
			config:    configBackendEntry{code: "def transformEvent(event, metadata):\n    event[\"processed\"] = True\n\treturn event\n"},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionId = "bc-mixed-indent-v1"

				events := []types.TransformerEvent{
					makeEvent("msg-1", versionId),
					makeEvent("msg-2", versionId),
				}

				t.Log("Sending 2 events to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending 2 events to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Both should fail (compilation error)
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.True(t, len(baselineResp.FailedEvents) > 0, "baseline: expected at least 1 failed event")
				require.True(t, len(candidateResp.FailedEvents) > 0, "candidate: expected at least 1 failed event")

				// Log error details for debugging
				for i, fe := range baselineResp.FailedEvents {
					t.Logf("Baseline FailedEvent[%d]: statusCode=%d, error=%q", i, fe.StatusCode, fe.Error)
				}
				for i, fe := range candidateResp.FailedEvents {
					t.Logf("Candidate FailedEvent[%d]: statusCode=%d, error=%q", i, fe.StatusCode, fe.Error)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for mixed indentation")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "CredentialValidKey",
			versionID: "bc-cred-valid-key-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    value = getCredential('testKey1')
    event['credential'] = value
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cred-valid-key-v1"

				creds := []types.Credential{
					{ID: "testId1", Key: "testKey1", Value: "testValue1", IsSecret: false},
					{ID: "testId2", Key: "testKey2", Value: "testValue2", IsSecret: false},
				}
				events := []types.TransformerEvent{
					makeEventWithCredentials("msg-1", versionID, creds),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")
				require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

				// Verify credential value was retrieved correctly
				require.Equal(t, "testValue1", baselineResp.Events[0].Output["credential"], "baseline: credential value should be testValue1")
				require.Equal(t, "testValue1", candidateResp.Events[0].Output["credential"], "candidate: credential value should be testValue1")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for credential with valid key")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "CredentialMissingKey",
			versionID: "bc-cred-missing-key-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    value = getCredential('nonExistentKey')
    event['credential'] = value
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cred-missing-key-v1"

				creds := []types.Credential{
					{ID: "testId1", Key: "testKey1", Value: "testValue1", IsSecret: false},
				}
				events := []types.TransformerEvent{
					makeEventWithCredentials("msg-1", versionID, creds),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// getCredential returns None for missing keys — event should succeed with null value
				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				require.Nil(t, baselineResp.Events[0].Output["credential"], "baseline: credential should be nil for missing key")
				require.Nil(t, candidateResp.Events[0].Output["credential"], "candidate: credential should be nil for missing key")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for credential with missing key")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "CredentialNoArguments",
			versionID: "bc-cred-no-args-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    value = getCredential()
    event['credential'] = value
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cred-no-args-v1"

				creds := []types.Credential{
					{ID: "testId1", Key: "testKey1", Value: "testValue1", IsSecret: false},
				}
				events := []types.TransformerEvent{
					makeEventWithCredentials("msg-1", versionID, creds),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// getCredential() with no args raises TypeError — event should fail
				require.Equal(t, 0, len(baselineResp.Events), "baseline: no success events expected")
				require.Equal(t, 1, len(baselineResp.FailedEvents), "baseline: 1 failed event expected")
				require.Equal(t, 0, len(candidateResp.Events), "candidate: no success events expected")
				require.Equal(t, 1, len(candidateResp.FailedEvents), "candidate: 1 failed event expected")

				baselineError := baselineResp.FailedEvents[0].Error
				candidateError := candidateResp.FailedEvents[0].Error
				t.Logf("Baseline error: %q", baselineError)
				t.Logf("Candidate error: %q", candidateError)

				require.Contains(t, baselineError, "Key should be valid and defined", "baseline: error should mention invalid key")
				require.Contains(t, candidateError, "Key should be valid and defined", "candidate: error should mention invalid key")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for credential with no arguments")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "CredentialNonStringKeys",
			versionID: "bc-cred-non-string-keys-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    event['credentialValueForNoneKey'] = getCredential(None)
    event['credentialValueForNumkey'] = getCredential(1)
    event['credentialValueForBoolkey'] = getCredential(True)
    event['credentialValueForObjkey'] = getCredential({})
    event['credentialValueForArrkey'] = getCredential([])
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cred-non-string-keys-v1"

				creds := []types.Credential{
					{ID: "testId1", Key: "testKey1", Value: "testValue1", IsSecret: false},
					{ID: "testId2", Key: "testKey2", Value: "testValue2", IsSecret: false},
				}
				events := []types.TransformerEvent{
					makeEventWithCredentials("msg-1", versionID, creds),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Non-string keys should return None (not raise errors)
				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")
				require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

				// All non-string keys (including None) should return None
				baselineOutput := baselineResp.Events[0].Output
				candidateOutput := candidateResp.Events[0].Output
				for _, key := range []string{"credentialValueForNoneKey", "credentialValueForNumkey", "credentialValueForBoolkey", "credentialValueForObjkey", "credentialValueForArrkey"} {
					require.Nilf(t, baselineOutput[key], "baseline: %s should be nil", key)
					require.Nilf(t, candidateOutput[key], "candidate: %s should be nil", key)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for credential with non-string keys")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "CredentialEmptyList",
			versionID: "bc-cred-empty-list-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    value = getCredential('anyKey')
    event['credential'] = value
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cred-empty-list-v1"

				// Send event with empty credentials list
				events := []types.TransformerEvent{
					makeEventWithCredentials("msg-1", versionID, []types.Credential{}),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// With empty credentials, getCredential returns None for any key
				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				require.Nil(t, baselineResp.Events[0].Output["credential"], "baseline: credential should be nil with empty credentials")
				require.Nil(t, candidateResp.Events[0].Output["credential"], "candidate: credential should be nil with empty credentials")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for credential with empty list")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "CredentialNoCredentials",
			versionID: "bc-cred-no-creds-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    value = getCredential('anyKey')
    event['credential'] = value
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cred-no-creds-v1"

				// Send event with no credentials at all (nil)
				events := []types.TransformerEvent{
					makeEvent("msg-1", versionID),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// With no credentials field, getCredential returns None for any key
				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				require.Nil(t, baselineResp.Events[0].Output["credential"], "baseline: credential should be nil with no credentials")
				require.Nil(t, candidateResp.Events[0].Output["credential"], "candidate: credential should be nil with no credentials")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for credential with no credentials field")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "CredentialBatchTransform",
			versionID: "bc-cred-batch-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        value = getCredential('testKey1')
        event['credential'] = value
    return events
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cred-batch-v1"

				creds := []types.Credential{
					{ID: "testId1", Key: "testKey1", Value: "testValue1", IsSecret: false},
					{ID: "testId2", Key: "testKey2", Value: "testValue2", IsSecret: false},
				}
				events := []types.TransformerEvent{
					makeEventWithCredentials("msg-1", versionID, creds),
					makeEventWithCredentials("msg-2", versionID, creds),
				}

				t.Log("Sending 2 events to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending 2 events to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				// Both events should succeed with credential from first event
				require.Equal(t, 2, len(baselineResp.Events), "baseline: 2 success events expected")
				require.Equal(t, 0, len(baselineResp.FailedEvents), "baseline: no failed events expected")
				require.Equal(t, 2, len(candidateResp.Events), "candidate: 2 success events expected")
				require.Equal(t, 0, len(candidateResp.FailedEvents), "candidate: no failed events expected")

				// Both events should have the same credential value
				for i := range baselineResp.Events {
					require.Equal(t, "testValue1", baselineResp.Events[i].Output["credential"],
						"baseline: event %d credential should be testValue1", i)
					require.Equal(t, "testValue1", candidateResp.Events[i].Output["credential"],
						"candidate: event %d credential should be testValue1", i)
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for credential in batch transform")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "CredentialFromFirstEventOnly",
			versionID: "bc-cred-first-event-v1",
			config: configBackendEntry{code: `
def transformBatch(events, metadata):
    for event in events:
        event['cred1'] = getCredential('key1')
        event['cred2'] = getCredential('key2')
    return events
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cred-first-event-v1"

				// First event has key1, second event has key2.
				// Both versions extract credentials from the first event only.
				events := []types.TransformerEvent{
					makeEventWithCredentials("msg-1", versionID, []types.Credential{
						{ID: "id1", Key: "key1", Value: "value1", IsSecret: false},
					}),
					makeEventWithCredentials("msg-2", versionID, []types.Credential{
						{ID: "id2", Key: "key2", Value: "value2", IsSecret: false},
					}),
				}

				t.Log("Sending 2 events to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending 2 events to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 2, len(baselineResp.Events), "baseline: 2 success events expected")
				require.Equal(t, 2, len(candidateResp.Events), "candidate: 2 success events expected")

				// key1 from first event should be available, key2 from second event should not
				for i, ev := range baselineResp.Events {
					t.Logf("Baseline Event[%d]: cred1=%v, cred2=%v", i, ev.Output["cred1"], ev.Output["cred2"])
				}
				for i, ev := range candidateResp.Events {
					t.Logf("Candidate Event[%d]: cred1=%v, cred2=%v", i, ev.Output["cred1"], ev.Output["cred2"])
				}

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for credentials from first event only")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "CredentialMultipleAccess",
			versionID: "bc-cred-multi-access-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    event['cred1'] = getCredential('testKey1')
    event['cred2'] = getCredential('testKey2')
    event['cred1_again'] = getCredential('testKey1')
    event['missing'] = getCredential('noSuchKey')
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cred-multi-access-v1"

				creds := []types.Credential{
					{ID: "testId1", Key: "testKey1", Value: "testValue1", IsSecret: false},
					{ID: "testId2", Key: "testKey2", Value: "testValue2", IsSecret: false},
				}
				events := []types.TransformerEvent{
					makeEventWithCredentials("msg-1", versionID, creds),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				baselineOutput := baselineResp.Events[0].Output
				candidateOutput := candidateResp.Events[0].Output

				// Verify multiple credential accesses and repeated access
				require.Equal(t, "testValue1", baselineOutput["cred1"], "baseline: cred1 should be testValue1")
				require.Equal(t, "testValue2", baselineOutput["cred2"], "baseline: cred2 should be testValue2")
				require.Equal(t, "testValue1", baselineOutput["cred1_again"], "baseline: cred1_again should be testValue1")
				require.Nil(t, baselineOutput["missing"], "baseline: missing should be nil")

				require.Equal(t, "testValue1", candidateOutput["cred1"], "candidate: cred1 should be testValue1")
				require.Equal(t, "testValue2", candidateOutput["cred2"], "candidate: cred2 should be testValue2")
				require.Equal(t, "testValue1", candidateOutput["cred1_again"], "candidate: cred1_again should be testValue1")
				require.Nil(t, candidateOutput["missing"], "candidate: missing should be nil")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for multiple credential accesses")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "CredentialSecretFlag",
			versionID: "bc-cred-secret-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    event['secretCred'] = getCredential('secretKey')
    event['publicCred'] = getCredential('publicKey')
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cred-secret-v1"

				// isSecret flag should not affect getCredential behavior — both should be accessible
				creds := []types.Credential{
					{ID: "id1", Key: "secretKey", Value: "secretValue", IsSecret: true},
					{ID: "id2", Key: "publicKey", Value: "publicValue", IsSecret: false},
				}
				events := []types.TransformerEvent{
					makeEventWithCredentials("msg-1", versionID, creds),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				// Both secret and public credentials should be accessible via getCredential
				require.Equal(t, "secretValue", baselineResp.Events[0].Output["secretCred"], "baseline: secret credential should be accessible")
				require.Equal(t, "publicValue", baselineResp.Events[0].Output["publicCred"], "baseline: public credential should be accessible")
				require.Equal(t, "secretValue", candidateResp.Events[0].Output["secretCred"], "candidate: secret credential should be accessible")
				require.Equal(t, "publicValue", candidateResp.Events[0].Output["publicCred"], "candidate: public credential should be accessible")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for credentials with isSecret flag")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "CredentialWithSpecialCharacters",
			versionID: "bc-cred-special-chars-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    event['cred'] = getCredential('key-with-special.chars_123')
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cred-special-chars-v1"

				creds := []types.Credential{
					{ID: "id1", Key: "key-with-special.chars_123", Value: "special-value!@#$%", IsSecret: false},
				}
				events := []types.TransformerEvent{
					makeEventWithCredentials("msg-1", versionID, creds),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				require.Equal(t, "special-value!@#$%", baselineResp.Events[0].Output["cred"], "baseline: credential value with special chars")
				require.Equal(t, "special-value!@#$%", candidateResp.Events[0].Output["cred"], "candidate: credential value with special chars")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for credentials with special characters")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "CredentialDuplicateKeys",
			versionID: "bc-cred-dup-keys-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    event['cred'] = getCredential('dupKey')
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cred-dup-keys-v1"

				// When duplicate keys exist, the last one wins (both versions
				// iterate the credentials slice in order, overwriting earlier values).
				creds := []types.Credential{
					{ID: "id1", Key: "dupKey", Value: "firstValue", IsSecret: false},
					{ID: "id2", Key: "dupKey", Value: "secondValue", IsSecret: false},
				}
				events := []types.TransformerEvent{
					makeEventWithCredentials("msg-1", versionID, creds),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				// Assert that the last credential wins when keys are duplicated
				require.Equal(t, "secondValue", baselineResp.Events[0].Output["cred"], "baseline: last credential value should win for duplicate keys")
				require.Equal(t, "secondValue", candidateResp.Events[0].Output["cred"], "candidate: last credential value should win for duplicate keys")

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for duplicate credential keys")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "CredentialEmptyValue",
			versionID: "bc-cred-empty-value-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    event['cred'] = getCredential('emptyKey')
    event['credIsNone'] = getCredential('emptyKey') is None
    event['credIsEmpty'] = getCredential('emptyKey') == ''
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-cred-empty-value-v1"

				creds := []types.Credential{
					{ID: "id1", Key: "emptyKey", Value: "", IsSecret: false},
				}
				events := []types.TransformerEvent{
					makeEventWithCredentials("msg-1", versionID, creds),
				}

				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))

				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))

				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")

				// An empty string value should be returned (not None)
				baselineOutput := baselineResp.Events[0].Output
				candidateOutput := candidateResp.Events[0].Output
				t.Logf("Baseline: cred=%v, credIsNone=%v, credIsEmpty=%v", baselineOutput["cred"], baselineOutput["credIsNone"], baselineOutput["credIsEmpty"])
				t.Logf("Candidate: cred=%v, credIsNone=%v, credIsEmpty=%v", candidateOutput["cred"], candidateOutput["credIsNone"], candidateOutput["credIsEmpty"])

				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Both versions produce identical responses for credential with empty value")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			// Same merge as MetadataMergeAllKeys above, but with every metadata field populated:
			// that one leaves most of them unset, so this pins how the fully-populated shape merges.
			name:      "MetadataMergeAllKeysPopulated",
			versionID: "bc-metadata-merge-all-populated-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    event.update(metadata(event))
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-metadata-merge-all-populated-v1"
				events := []types.TransformerEvent{
					{
						Message: types.SingularEventT{
							"messageId":   "msg-1",
							"type":        "track",
							"event":       "Test Event",
							"instanceId":  "1",
							"partitionId": "ws-1-42",
						},
						Metadata: types.Metadata{
							JobID: 1234, WorkspaceID: "ws-1", MessageID: "msg-1",
							SourceID: "src-1", SourceName: "test-source", SourceType: "javascript",
							DestinationID: "dest-1", DestinationName: "test-dest", DestinationType: "AM",
							InstanceID: "1", PartitionID: "ws-1-42", Namespace: "test-ns",
							RudderID: "rudder-id-1", ReceivedAt: "2024-01-01T00:00:00.000Z",
							EventName: "Test Event", EventType: "track",
							SourceDefinitionID: "src-def-1", DestinationDefinitionID: "dest-def-1",
							TransformationID: "tf-1", TransformationVersionID: versionID,
						},
						Destination: backendconfig.DestinationT{
							Transformations: []backendconfig.TransformationT{
								{VersionID: versionID, ID: "transformation-1", Language: "pythonfaas"},
							},
						},
					},
				}
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")
				baselineOutput := baselineResp.Events[0].Output
				candidateOutput := candidateResp.Events[0].Output
				t.Logf("Baseline output keys: %d", len(baselineOutput))
				t.Logf("Candidate output keys: %d", len(candidateOutput))
				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Responses are equal")
				} else {
					t.Errorf("Responses differ: %s\n", diff)
				}
			},
		},
		{
			// MetadataMergeIfFalsy tests the production user pattern where metadata
			// values are added to the event only when the event doesn't already have
			// a truthy value for that key: `if not event.get(k): event[k] = v`.
			// This is sensitive to None vs "" vs 0 differences in the metadata dict.
			name:      "MetadataMergeIfFalsy",
			versionID: "bc-metadata-merge-falsy-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    meta = metadata(event)
    for k, v in meta.items():
        if not event.get(k):
            event[k] = v
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-metadata-merge-falsy-v1"
				events := []types.TransformerEvent{
					{
						Message: types.SingularEventT{
							"messageId":   "msg-1",
							"type":        "track",
							"event":       "Test Event",
							"instanceId":  "1",
							"partitionId": "ws-1-42",
						},
						Metadata: types.Metadata{
							JobID: 1234, WorkspaceID: "ws-1", MessageID: "msg-1",
							SourceID: "src-1", SourceName: "test-source", SourceType: "javascript",
							DestinationID: "dest-1", DestinationName: "test-dest", DestinationType: "AM",
							InstanceID: "1", PartitionID: "ws-1-42", Namespace: "test-ns",
							RudderID: "rudder-id-1", ReceivedAt: "2024-01-01T00:00:00.000Z",
							EventName: "Test Event", EventType: "track",
							SourceDefinitionID: "src-def-1", DestinationDefinitionID: "dest-def-1",
							TransformationID: "tf-1", TransformationVersionID: versionID,
						},
						Destination: backendconfig.DestinationT{
							Transformations: []backendconfig.TransformationT{
								{VersionID: versionID, ID: "transformation-1", Language: "pythonfaas"},
							},
						},
					},
				}
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")
				baselineOutput := baselineResp.Events[0].Output
				candidateOutput := candidateResp.Events[0].Output
				t.Logf("Baseline output keys: %d", len(baselineOutput))
				t.Logf("Candidate output keys: %d", len(candidateOutput))
				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Responses are equal")
				} else {
					t.Errorf("Responses differ: %s\n", diff)
				}
			},
		},
		{
			// MetadataAddMetaPattern tests the production preprocessEvents.py pattern:
			//
			//   meta = self.metadata(self.event)
			//   for k, v in meta.items():
			//       if not self.event.get(k):
			//           self.event[k] = v
			//
			// The key difference from MetadataMergeIfFalsy is the event setup:
			// instanceId and partitionId are NOT present in the message body (so they
			// will be copied from metadata), userId is an empty string (falsy, so it
			// will also be overwritten), and SourceJobID/TrackingPlanID/etc. are zero
			// values which are omitted from the metadata JSON (omitempty) so the metadata
			// function returns None for them. This mirrors the production scenario that
			// exposed the diff between old and candidate.
			name:      "MetadataAddMetaPattern",
			versionID: "bc-add-meta-pattern-v1",
			config: configBackendEntry{code: `
def transformEvent(event, metadata):
    meta = metadata(event)
    for k, v in meta.items():
        if not event.get(k):
            event[k] = v
    return event
`},
			run: func(t *testing.T, env *bcTestEnv) {
				const versionID = "bc-add-meta-pattern-v1"
				events := []types.TransformerEvent{
					{
						Message: types.SingularEventT{
							"messageId": "msg-1",
							"type":      "track",
							"event":     "Test Event",
							"channel":   "web",
							"userId":    "", // empty string — falsy, will be overwritten by metadata value
							// instanceId and partitionId are intentionally absent from the message body
						},
						Metadata: types.Metadata{
							JobID: 1234, WorkspaceID: "ws-1", MessageID: "msg-1",
							SourceID: "src-1", SourceName: "test-source", SourceType: "javascript",
							DestinationID: "dest-1", DestinationName: "test-dest", DestinationType: "AM",
							// InstanceID and PartitionID are set (present in metadata JSON) but absent
							// from the message body — the pattern will add them to the event.
							InstanceID: "3", PartitionID: "ws-1-33", Namespace: "test-ns",
							RudderID: "rudder-id-1", ReceivedAt: "2024-01-01T00:00:00.000Z",
							EventName: "Test Event", EventType: "track",
							SourceDefinitionID: "src-def-1", DestinationDefinitionID: "dest-def-1",
							TransformationID: "tf-1", TransformationVersionID: versionID,
							// SourceJobID, TrackingPlanID, TrackingPlanVersion, SourceJobRunID,
							// SourceTaskRunID, RecordID and MessageIDs are left as zero values.
							// They are omitted from the metadata JSON (omitempty) so the metadata
							// function returns None for them (present in the dict, value is None).
						},
						Destination: backendconfig.DestinationT{
							Transformations: []backendconfig.TransformationT{
								{VersionID: versionID, ID: "transformation-1", Language: "pythonfaas"},
							},
						},
					},
				}
				t.Log("Sending request to baseline...")
				baselineResp := env.BaselineClient.Transform(context.Background(), events)
				t.Logf("Baseline: Events=%d, FailedEvents=%d", len(baselineResp.Events), len(baselineResp.FailedEvents))
				t.Log("Sending request to candidate...")
				candidateResp := env.CandidateClient.Transform(context.Background(), events)
				t.Logf("Candidate: Events=%d, FailedEvents=%d", len(candidateResp.Events), len(candidateResp.FailedEvents))
				require.Equal(t, 1, len(baselineResp.Events), "baseline: 1 success event expected")
				require.Equal(t, 1, len(candidateResp.Events), "candidate: 1 success event expected")
				baselineOutput := baselineResp.Events[0].Output
				candidateOutput := candidateResp.Events[0].Output
				t.Logf("Baseline output keys: %d", len(baselineOutput))
				t.Logf("Candidate output keys: %d", len(candidateOutput))
				diff, equal := baselineResp.Equal(&candidateResp)
				if equal {
					t.Log("Responses are equal")
				} else {
					t.Errorf("Responses differ: %s\n", diff)
				}
			},
		},
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	// Collect all config backend entries for the shared config backend.
	// Subtests with zero-value config are not registered — the config backend
	// returns 404 for those versionIds, which tests error handling paths.
	allEntries := make(map[string]configBackendEntry, len(subtests))
	for _, st := range subtests {
		if st.config != (configBackendEntry{}) {
			_, dup := allEntries[st.versionID]
			require.Falsef(t, dup, "duplicate versionID %q: with shared containers the versionID is the only "+
				"isolation boundary between subtests, so a collision serves one subtest's code to another", st.versionID)
			allEntries[st.versionID] = st.config
		}
	}
	// Register individual 5xx entries for the ConfigBackend5xx grouped subtest.
	for _, code := range []int{http.StatusNotImplemented, http.StatusBadGateway, http.StatusServiceUnavailable} {
		allEntries[fmt.Sprintf("bc-cb-%d-v1", code)] = configBackendEntry{statusCode: code}
	}
	configBackend := newContractConfigBackend(t, allEntries)
	t.Cleanup(configBackend.Close)

	// Both rudder-pytransformer versions read from the same config backend, so
	// one container per version serves every subtest.
	var (
		wg                        sync.WaitGroup
		baselineURL, candidateURL string
	)
	wg.Go(func() {
		baselineURL = startBaselinePytransformer(t, pool, configBackend.URL)
	})
	wg.Go(func() {
		candidateURL = startCandidatePytransformer(t, pool, configBackend.URL)
	})
	wg.Wait()

	// Run subtests sequentially. Each subtest gets a fresh bcTestEnv with its own
	// memstats stores so that retry counts are isolated per subtest.
	for _, st := range subtests {
		t.Run(st.name, func(t *testing.T) {
			env := newBCTestEnv(t, baselineURL, candidateURL,
				withFailOnError(),
				withLimitedRetryableHTTPRetries(),
			)

			st.run(t, env)
			if !st.skipRetryCountMatch {
				env.assertRetryCountsMatch(t)
			}
		})
	}
}
