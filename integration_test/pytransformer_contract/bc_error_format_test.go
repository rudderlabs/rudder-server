package pytransformer_contract

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/types"
)

func TestBC_ErrorMessageFormat(t *testing.T) {
	const versionID = "bc-error-format-v1"

	pythonCode := `
def transformEvent(event, metadata):
    # Raise an error to trigger per-event error handling.
    # Call through a helper function to create a multi-line stack trace.
    return helper(event)

def helper(event):
    raise ValueError("intentional error for testing")
`

	env := setupBackwardsCompatibilityTest(t, versionID, pythonCode)

	events := []types.TransformerEvent{
		makeEvent("msg-1", versionID),
	}

	t.Log("Sending request to old architecture...")
	oldResp := env.OldClient.Transform(context.Background(), events)
	t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

	t.Log("Sending request to new architecture...")
	newResp := env.NewClient.Transform(context.Background(), events)
	t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

	// Both should return per-event errors
	require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
	require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
	require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
	require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")

	oldError := oldResp.FailedEvents[0].Error
	newError := newResp.FailedEvents[0].Error

	t.Logf("Old arch error message:\n%s", oldError)
	t.Logf("New arch error message:\n%s", newError)

	// Both should contain the error message
	require.Contains(t, oldError, "intentional error for testing", "old arch: error should contain the message")
	require.Contains(t, newError, "intentional error for testing", "new arch: error should contain the message")

	// Compare: error format should differ
	diff, equal := oldResp.Equal(&newResp)
	if equal {
		t.Log("Both architectures produce identical error messages")
	} else {
		t.Errorf("Responses differ:\n%s", diff)

		// Analyze the difference: old arch should have stack trace, new arch should not
		if len(oldError) > len(newError) {
			t.Logf("Old arch error is longer (%d chars vs %d chars), likely contains stack trace",
				len(oldError), len(newError))
		}
	}
}
