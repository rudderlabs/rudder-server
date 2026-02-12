package pytransformer_contract

import (
	"context"
	"fmt"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/processor/usertransformer"
)

// TestBaseContract is the base contract test that compares responses from the
// old architecture (rudder-transformer + openfaas-flask-base) against the new
// architecture (rudder-pytransformer).
//
// This test:
// 1. Starts a mock config backend serving Python transformation code
// 2. Starts openfaas-flask-base with the transformation pre-loaded
// 3. Starts a mock OpenFaaS gateway that proxies invocations to openfaas-flask-base
// 4. Starts rudder-transformer connected to the mock gateway
// 5. Starts rudder-pytransformer connected to the mock config backend
// 6. Uses the actual user_transformer.Client to send /customTransform to both
// 7. Asserts the OpenFaaS gateway was invoked (proving the old architecture path)
// 8. Compares the responses for equivalence
//
// Copy this test and change pythonCode + events to create new contract test cases.
//
// To be able to run these tests, make sure you're able to pull Docker images from ECR (see Notion docs).
func TestBaseContract(t *testing.T) {
	const versionID = "contract-test-v1"

	pythonCode := `
def transformEvent(event, metadata):
    event['foo'] = 'bar'
    return event
`

	// Language "pythonfaas" is required: the user_transformer.Client reads it from
	// Destination.Transformations[0].Language to decide URL routing.
	// - When PYTHON_TRANSFORM_URL is empty, python falls through to USER_TRANSFORM_URL (old architecture).
	// - When PYTHON_TRANSFORM_URL is set, python routes there (new architecture).
	events := []types.TransformerEvent{
		{
			Message: types.SingularEventT{
				"messageId":  "msg-1",
				"type":       "track",
				"event":      "Test Event",
				"properties": map[string]any{"key": "value"},
			},
			Metadata: types.Metadata{
				SourceID:      "src-1",
				DestinationID: "dest-1",
				WorkspaceID:   "ws-1",
				MessageID:     "msg-1",
			},
			Destination: backendconfig.DestinationT{
				Transformations: []backendconfig.TransformationT{
					{VersionID: versionID, ID: "transformation-1", Language: "pythonfaas"},
				},
			},
		},
		{
			Message: types.SingularEventT{
				"messageId": "msg-2",
				"type":      "identify",
				"traits":    map[string]any{"name": "Test User"},
			},
			Metadata: types.Metadata{
				SourceID:      "src-1",
				DestinationID: "dest-1",
				WorkspaceID:   "ws-1",
				MessageID:     "msg-2",
			},
			Destination: backendconfig.DestinationT{
				Transformations: []backendconfig.TransformationT{
					{VersionID: versionID, ID: "transformation-1", Language: "pythonfaas"},
				},
			},
		},
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	t.Log("Starting mock config backend...")
	configBackend := newContractConfigBackend(t, map[string]string{
		versionID: pythonCode,
	})
	defer configBackend.Close()
	t.Logf("Config backend at %s", configBackend.URL)

	t.Log("Allocating free ports...")
	openFaasPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	transformerPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	pyTransformerPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	openFaasURL := fmt.Sprintf("http://localhost:%d", openFaasPort)
	transformerURL := fmt.Sprintf("http://localhost:%d", transformerPort)
	pyTransformerURL := fmt.Sprintf("http://localhost:%d", pyTransformerPort)

	t.Log("Starting openfaas-flask-base container...")
	openFaasContainer := startOpenFaasFlask(t, pool, openFaasPort, versionID, configBackend.URL)
	defer func() {
		if err := pool.Purge(openFaasContainer); err != nil {
			t.Logf("Failed to purge openfaas-flask-base container: %v", err)
		}
	}()
	waitForOpenFaasFlask(t, pool, openFaasURL)

	t.Log("Starting mock OpenFaaS gateway...")
	mockGateway, openFaaSInvocations := newMockOpenFaaSGateway(t, func() string { return openFaasURL })
	defer mockGateway.Close()
	t.Logf("Mock OpenFaaS gateway at %s", mockGateway.URL)

	t.Log("Starting rudder-transformer container...")
	transformerContainer := startRudderTransformer(t, pool, transformerPort, configBackend.URL, mockGateway.URL)
	defer func() {
		if err := pool.Purge(transformerContainer); err != nil {
			t.Logf("Failed to purge rudder-transformer container: %v", err)
		}
	}()

	t.Log("Starting rudder-pytransformer container...")
	pyTransformerContainer := startRudderPytransformer(t, pool, pyTransformerPort, configBackend.URL)
	defer func() {
		if err := pool.Purge(pyTransformerContainer); err != nil {
			t.Logf("Failed to purge rudder-pytransformer container: %v", err)
		}
	}()

	t.Log("Waiting for transformers to be healthy...")
	waitForHealthy(t, pool, transformerURL, "rudder-transformer")
	waitForHealthy(t, pool, pyTransformerURL, "rudder-pytransformer")

	// Old architecture: PYTHON_TRANSFORM_URL is empty, so the client falls through
	// to USER_TRANSFORM_URL for python transformations (same as production before pytransformer).
	t.Log("Sending request to rudder-transformer (old architecture)...")
	oldArchConf := config.New()
	oldArchConf.Set("USER_TRANSFORM_URL", transformerURL)
	oldClient := usertransformer.New(oldArchConf, logger.NOP, stats.NOP)
	oldResp := oldClient.Transform(context.Background(), events)
	t.Logf("Old architecture returned %d events, %d failed", len(oldResp.Events), len(oldResp.FailedEvents))

	t.Log("Asserting OpenFaaS gateway was invoked by rudder-transformer...")
	require.Greater(t, openFaaSInvocations.Load(), int64(0),
		"expected OpenFaaS gateway to be invoked at least once by rudder-transformer")
	t.Logf("OpenFaaS gateway was invoked %d times", openFaaSInvocations.Load())

	// New architecture: PYTHON_TRANSFORM_URL is set, so the client routes python
	// transformations directly to rudder-pytransformer.
	t.Log("Sending request to rudder-pytransformer (new architecture)...")
	newArchConf := config.New()
	newArchConf.Set("PYTHON_TRANSFORM_URL", pyTransformerURL)
	newClient := usertransformer.New(newArchConf, logger.NOP, stats.NOP)
	newResp := newClient.Transform(context.Background(), events)
	t.Logf("New architecture returned %d events, %d failed", len(newResp.Events), len(newResp.FailedEvents))

	t.Log("Comparing responses...")
	diff, equal := oldResp.Equal(&newResp)
	require.True(t, equal, "responses differ:\n%s", diff)

	t.Log("Contract test passed: old and new architectures return equivalent responses")
}
