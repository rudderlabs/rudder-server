package pytransformer_contract

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/types"
)

func Test_TransformBatchErrorFormat(t *testing.T) {
	const versionID = "bc-batch-error-format-v1"

	pythonCode := `
def transformBatch(events, metadata):
    raise ValueError("intentional batch error for testing")
`

	env := setupBackwardsCompatibilityTest(t, versionID, pythonCode)

	// Send 3 events in the same group (same sourceId/destinationId)
	events := []types.TransformerEvent{
		makeEvent("msg-1", versionID),
		makeEvent("msg-2", versionID),
		makeEvent("msg-3", versionID),
	}

	t.Log("Sending 3 events to old architecture...")
	oldResp := env.OldClient.Transform(context.Background(), events)
	t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

	t.Log("Sending 3 events to new architecture...")
	newResp := env.NewClient.Transform(context.Background(), events)
	t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

	// Log details of each failed event
	for i, fe := range oldResp.FailedEvents {
		t.Logf("Old arch FailedEvent[%d]: statusCode=%d, error=%q, messageId=%q, messageIds=%v",
			i, fe.StatusCode, fe.Error, fe.Metadata.MessageID, fe.Metadata.MessageIDs)
	}
	for i, fe := range newResp.FailedEvents {
		t.Logf("New arch FailedEvent[%d]: statusCode=%d, error=%q, messageId=%q, messageIds=%v",
			i, fe.StatusCode, fe.Error, fe.Metadata.MessageID, fe.Metadata.MessageIDs)
	}

	// Both architectures should have no success events
	require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
	require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")

	// Compare: responses should differ in FailedEvents count
	diff, equal := oldResp.Equal(&newResp)
	if equal {
		t.Log("Finding 2 not confirmed at contract test level: responses are equal")
		t.Logf("Both architectures returned %d failed events", len(oldResp.FailedEvents))
	} else {
		t.Logf("Old arch: %d failed events, New arch: %d failed events",
			len(oldResp.FailedEvents), len(newResp.FailedEvents))
		t.Errorf("Responses differ:\n%s", diff)
	}
}
