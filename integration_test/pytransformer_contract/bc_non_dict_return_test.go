package pytransformer_contract

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/types"
)

func TestBC_TransformEventNonDictReturn(t *testing.T) {
	const versionID = "bc1-non-dict-v1"

	pythonCode := `
def transformEvent(event, metadata):
    return "this is a string, not a dict"
`

	env := setupBackwardsCompatibilityTest(t, versionID, pythonCode)

	events := []types.TransformerEvent{
		makeEvent("msg-1", versionID),
	}

	t.Log("Sending request to old architecture (rudder-transformer + openfaas)...")
	oldResp := env.OldClient.Transform(context.Background(), events)
	t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

	t.Log("Sending request to new architecture (rudder-pytransformer)...")
	newResp := env.NewClient.Transform(context.Background(), events)
	t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

	// Old arch should return a failed event
	require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
	require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
	t.Logf("Old arch error: statusCode=%d, error=%q", oldResp.FailedEvents[0].StatusCode, oldResp.FailedEvents[0].Error)

	// Log new arch response for analysis
	if len(newResp.FailedEvents) > 0 {
		t.Logf("New arch error: statusCode=%d, error=%q", newResp.FailedEvents[0].StatusCode, newResp.FailedEvents[0].Error)
	}
	if len(newResp.Events) > 0 {
		t.Logf("New arch success: statusCode=%d, output=%v", newResp.Events[0].StatusCode, newResp.Events[0].Output)
	}

	// Compare: responses should not differ
	diff, equal := oldResp.Equal(&newResp)
	if equal {
		t.Log("Finding 1 not confirmed at contract test level: responses are equal")
		t.Log("This means openfaas and pytransformer handle non-dict returns the same way")
	} else {
		t.Errorf("Finding 1 confirmed: responses differ:\n%s", diff)
	}
}
