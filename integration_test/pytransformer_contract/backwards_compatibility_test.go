package pytransformer_contract

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/processor/usertransformer"
)

// TestBackwardsCompatibility compares responses from the old architecture
// (rudder-transformer + openfaas-flask-base) against the new architecture
// (rudder-pytransformer) for various edge cases.
//
// rudder-transformer and rudder-pytransformer are started once and shared
// across all subtests. Each subtest gets its own openfaas-flask-base container
// since openfaas loads transformation code at startup (one version per container).
func TestBackwardsCompatibility(t *testing.T) {
	type subtest struct {
		name       string
		versionID  string
		pythonCode string
		run        func(t *testing.T, env *bcTestEnv)
	}

	subtests := []subtest{
		{
			name:      "ErrorMessageFormat",
			versionID: "bc-error-format-v1",
			pythonCode: `
def transformEvent(event, metadata):
    # Raise an error to trigger per-event error handling.
    # Call through a helper function to create a multi-line stack trace.
    return helper(event)

def helper(event):
    raise ValueError("intentional error for testing")
`,
			run: func(t *testing.T, env *bcTestEnv) {
				events := []types.TransformerEvent{
					makeEvent("msg-1", "bc-error-format-v1"),
				}

				t.Log("Sending request to old architecture...")
				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

				t.Log("Sending request to new architecture...")
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")
				require.Equal(t, 1, len(newResp.FailedEvents), "new arch: 1 failed event expected")

				oldError := oldResp.FailedEvents[0].Error
				newError := newResp.FailedEvents[0].Error

				t.Logf("Old arch error message:\n%s", oldError)
				t.Logf("New arch error message:\n%s", newError)

				require.Contains(t, oldError, "intentional error for testing", "old arch: error should contain the message")
				require.Contains(t, newError, "intentional error for testing", "new arch: error should contain the message")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Both architectures produce identical error messages")
				} else {
					t.Errorf("Responses differ:\n%s", diff)

					if len(oldError) > len(newError) {
						t.Logf("Old arch error is longer (%d chars vs %d chars), likely contains stack trace",
							len(oldError), len(newError))
					}
				}
			},
		},
		{
			name:      "TransformBatchErrorFormat",
			versionID: "bc-batch-error-format-v1",
			pythonCode: `
def transformBatch(events, metadata):
    raise ValueError("intentional batch error for testing")
`,
			run: func(t *testing.T, env *bcTestEnv) {
				events := []types.TransformerEvent{
					makeEvent("msg-1", "bc-batch-error-format-v1"),
					makeEvent("msg-2", "bc-batch-error-format-v1"),
					makeEvent("msg-3", "bc-batch-error-format-v1"),
				}

				t.Log("Sending 3 events to old architecture...")
				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

				t.Log("Sending 3 events to new architecture...")
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				for i, fe := range oldResp.FailedEvents {
					t.Logf("Old arch FailedEvent[%d]: statusCode=%d, error=%q, messageId=%q, messageIds=%v",
						i, fe.StatusCode, fe.Error, fe.Metadata.MessageID, fe.Metadata.MessageIDs)
				}
				for i, fe := range newResp.FailedEvents {
					t.Logf("New arch FailedEvent[%d]: statusCode=%d, error=%q, messageId=%q, messageIds=%v",
						i, fe.StatusCode, fe.Error, fe.Metadata.MessageID, fe.Metadata.MessageIDs)
				}

				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 0, len(newResp.Events), "new arch: no success events expected")

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Logf("Both architectures returned %d failed events", len(oldResp.FailedEvents))
				} else {
					t.Logf("Old arch: %d failed events, New arch: %d failed events",
						len(oldResp.FailedEvents), len(newResp.FailedEvents))
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
		{
			name:      "TransformEventNonDictReturn",
			versionID: "bc-non-dict-v1",
			pythonCode: `
def transformEvent(event, metadata):
    return "this is a string, not a dict"
`,
			run: func(t *testing.T, env *bcTestEnv) {
				events := []types.TransformerEvent{
					makeEvent("msg-1", "bc-non-dict-v1"),
				}

				t.Log("Sending request to old architecture (rudder-transformer + openfaas)...")
				oldResp := env.OldClient.Transform(context.Background(), events)
				t.Logf("Old arch: Events=%d, FailedEvents=%d", len(oldResp.Events), len(oldResp.FailedEvents))

				t.Log("Sending request to new architecture (rudder-pytransformer)...")
				newResp := env.NewClient.Transform(context.Background(), events)
				t.Logf("New arch: Events=%d, FailedEvents=%d", len(newResp.Events), len(newResp.FailedEvents))

				require.Equal(t, 0, len(oldResp.Events), "old arch: no success events expected")
				require.Equal(t, 1, len(oldResp.FailedEvents), "old arch: 1 failed event expected")
				t.Logf("Old arch error: statusCode=%d, error=%q", oldResp.FailedEvents[0].StatusCode, oldResp.FailedEvents[0].Error)

				if len(newResp.FailedEvents) > 0 {
					t.Logf("New arch error: statusCode=%d, error=%q", newResp.FailedEvents[0].StatusCode, newResp.FailedEvents[0].Error)
				}
				if len(newResp.Events) > 0 {
					t.Logf("New arch success: statusCode=%d, output=%v", newResp.Events[0].StatusCode, newResp.Events[0].Output)
				}

				diff, equal := oldResp.Equal(&newResp)
				if equal {
					t.Log("Responses are equal")
				} else {
					t.Errorf("Responses differ:\n%s", diff)
				}
			},
		},
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// Collect all transformation codes for the shared config backend.
	allTransformations := make(map[string]string, len(subtests))
	for _, st := range subtests {
		allTransformations[st.versionID] = st.pythonCode
	}
	configBackend := newContractConfigBackend(t, allTransformations)
	t.Cleanup(configBackend.Close)

	// Create a mock OpenFaaS gateway with a dynamic proxy target.
	// The target URL is updated before each subtest to point to that
	// subtest's openfaas-flask-base container.
	var (
		gatewayMu        sync.Mutex
		gatewayTargetURL string
	)
	getGatewayTarget := func() string {
		gatewayMu.Lock()
		defer gatewayMu.Unlock()
		return gatewayTargetURL
	}
	setGatewayTarget := func(url string) {
		gatewayMu.Lock()
		defer gatewayMu.Unlock()
		gatewayTargetURL = url
	}
	mockGateway, _ := newMockOpenFaaSGateway(t, getGatewayTarget)
	t.Cleanup(mockGateway.Close)

	// Start shared rudder-transformer.
	transformerPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	transformerURL := fmt.Sprintf("http://localhost:%d", transformerPort)
	transformerContainer := startRudderTransformer(t, pool, transformerPort, configBackend.URL, mockGateway.URL)
	t.Cleanup(func() {
		if err := pool.Purge(transformerContainer); err != nil {
			t.Logf("Failed to purge rudder-transformer: %v", err)
		}
	})

	// Start shared rudder-pytransformer.
	pyTransformerPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	pyTransformerURL := fmt.Sprintf("http://localhost:%d", pyTransformerPort)
	pyTransformerContainer := startRudderPytransformer(t, pool, pyTransformerPort, configBackend.URL)
	t.Cleanup(func() {
		if err := pool.Purge(pyTransformerContainer); err != nil {
			t.Logf("Failed to purge rudder-pytransformer: %v", err)
		}
	})

	// Wait for shared services to be healthy.
	t.Log("Waiting for shared services to be healthy...")
	waitForHealthy(t, pool, transformerURL, "rudder-transformer")
	waitForHealthy(t, pool, pyTransformerURL, "rudder-pytransformer")

	// Create shared clients.
	oldArchConf := config.New()
	oldArchConf.Set("Processor.UserTransformer.maxRetry", 1)
	oldArchConf.Set("USER_TRANSFORM_URL", transformerURL)

	newArchConf := config.New()
	newArchConf.Set("Processor.UserTransformer.maxRetry", 1)
	newArchConf.Set("PYTHON_TRANSFORM_URL", pyTransformerURL)

	var (
		oldArchLogger = logger.NOP
		newArchLogger = logger.NOP
	)
	if testing.Verbose() {
		oldArchLogger = logger.NewLogger().Child("old-arch")
		newArchLogger = logger.NewLogger().Child("new-arch")
	}
	env := &bcTestEnv{
		OldClient: usertransformer.New(oldArchConf, oldArchLogger, stats.NOP),
		NewClient: usertransformer.New(newArchConf, newArchLogger, stats.NOP),
	}

	// Run subtests sequentially. Each subtest spins up its own
	// openfaas-flask-base since openfaas loads code at startup.
	for _, st := range subtests {
		t.Run(st.name, func(t *testing.T) {
			openFaasPort, err := kithelper.GetFreePort()
			require.NoError(t, err)
			openFaasURL := fmt.Sprintf("http://localhost:%d", openFaasPort)

			t.Logf("Starting openfaas-flask-base for %s (versionID=%s)...", st.name, st.versionID)
			container := startOpenFaasFlask(t, pool, openFaasPort, st.versionID, configBackend.URL)
			t.Cleanup(func() {
				if err := pool.Purge(container); err != nil {
					t.Logf("Failed to purge openfaas-flask-base: %v", err)
				}
			})
			waitForOpenFaasFlask(t, pool, openFaasURL)

			// Point the mock gateway to this subtest's openfaas container.
			setGatewayTarget(openFaasURL)

			st.run(t, env)
		})
	}
}
