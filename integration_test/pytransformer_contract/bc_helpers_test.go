package pytransformer_contract

import (
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

// bcTestEnv holds clients for both the old architecture (rudder-transformer + openfaas)
// and the new architecture (rudder-pytransformer) to compare their responses.
type bcTestEnv struct {
	OldClient *usertransformer.Client // rudder-transformer + openfaas (old architecture)
	NewClient *usertransformer.Client // rudder-pytransformer (new architecture)
}

// setupBackwardsCompatibilityTest creates the full Docker-based test environment with both
// old and new architectures ready to receive requests.
//
// It starts:
//   - Mock config backend serving transformation code
//   - openfaas-flask-base container (loads code at startup)
//   - Mock OpenFaaS gateway proxying to openfaas-flask-base
//   - rudder-transformer container
//   - rudder-pytransformer container
func setupBackwardsCompatibilityTest(t *testing.T, versionID, pythonCode string) *bcTestEnv {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	configBackend := newContractConfigBackend(t, map[string]string{
		versionID: pythonCode,
	})
	t.Cleanup(configBackend.Close)

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
	t.Cleanup(func() {
		if err := pool.Purge(openFaasContainer); err != nil {
			t.Logf("Failed to purge openfaas-flask-base container: %v", err)
		}
	})
	waitForOpenFaasFlask(t, pool, openFaasURL)

	t.Log("Starting mock OpenFaaS gateway...")
	mockGateway, _ := newMockOpenFaaSGateway(t, openFaasURL)
	t.Cleanup(mockGateway.Close)

	t.Log("Starting rudder-transformer container...")
	transformerContainer := startRudderTransformer(t, pool, transformerPort, configBackend.URL, mockGateway.URL)
	t.Cleanup(func() {
		if err := pool.Purge(transformerContainer); err != nil {
			t.Logf("Failed to purge rudder-transformer container: %v", err)
		}
	})

	t.Log("Starting rudder-pytransformer container...")
	pyTransformerContainer := startRudderPytransformer(t, pool, pyTransformerPort, configBackend.URL)
	t.Cleanup(func() {
		if err := pool.Purge(pyTransformerContainer); err != nil {
			t.Logf("Failed to purge rudder-pytransformer container: %v", err)
		}
	})

	t.Log("Waiting for transformers to be healthy...")
	waitForHealthy(t, pool, transformerURL, "rudder-transformer")
	waitForHealthy(t, pool, pyTransformerURL, "rudder-pytransformer")

	// Old architecture: PYTHON_TRANSFORM_URL is empty, so the client falls through
	// to USER_TRANSFORM_URL for python transformations (same as production before pytransformer).
	oldArchConf := config.New()
	oldArchConf.Set("Processor.UserTransformer.maxRetry", 1)
	oldArchConf.Set("USER_TRANSFORM_URL", transformerURL)

	// New architecture: PYTHON_TRANSFORM_URL is set, so the client routes python
	// transformations directly to rudder-pytransformer.
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
	return &bcTestEnv{
		OldClient: usertransformer.New(oldArchConf, oldArchLogger, stats.NOP),
		NewClient: usertransformer.New(newArchConf, newArchLogger, stats.NOP),
	}
}

// makeEvent creates a TransformerEvent for backwards compatibility testing with minimal required fields.
func makeEvent(messageID, versionID string) types.TransformerEvent {
	return types.TransformerEvent{
		Message: types.SingularEventT{
			"messageId": messageID,
			"type":      "track",
			"event":     "Test Event",
		},
		Metadata: types.Metadata{
			SourceID:      "src-1",
			DestinationID: "dest-1",
			WorkspaceID:   "ws-1",
			MessageID:     messageID,
		},
		Destination: backendconfig.DestinationT{
			Transformations: []backendconfig.TransformationT{
				{VersionID: versionID, ID: "transformation-1", Language: "pythonfaas"},
			},
		},
	}
}
