package pytransformer_contract

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/processor/usertransformer"
)

// TestBaseContract is the base contract test that compares responses from the
// baseline rudder-pytransformer (the last released version) against the
// candidate one (main).
//
// This test:
//  1. Starts a mock config backend serving Python transformation code
//  2. Starts both rudder-pytransformer versions against that config backend
//  3. Uses the actual user_transformer.Client to send /customTransform to both
//  4. Compares the responses for equivalence
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

	// Language "pythonfaas" is what production stores for Python transformations:
	// the user_transformer.Client reads it from Destination.Transformations[0].Language
	// to decide URL routing, and any "python" prefix routes to rudder-pytransformer.
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
	pool.MaxWait = 2 * time.Minute

	t.Log("Starting mock config backend...")
	configBackend := newContractConfigBackend(t, map[string]configBackendEntry{
		versionID: {code: pythonCode},
	})
	defer configBackend.Close()
	t.Logf("Config backend at %s", configBackend.URL)

	var (
		wg                        sync.WaitGroup
		baselineURL, candidateURL string
	)
	wg.Go(func() {
		baselineURL = startBaselinePytransformer(t, pool, configBackend.URL)
	})
	wg.Go(func() {
		candidateURL = startRudderPytransformer(t, pool, configBackend.URL)
	})
	wg.Wait()

	newClientFor := func(url string) *usertransformer.Client {
		conf := config.New()
		setPytransformerRouting(conf, url)
		return usertransformer.New(conf, logger.NOP, stats.NOP)
	}

	t.Logf("Sending request to rudder-pytransformer:%s (baseline)...", baselinePytransformerTag())
	baselineResp := newClientFor(baselineURL).Transform(context.Background(), events)
	t.Logf("Baseline returned %d events, %d failed", len(baselineResp.Events), len(baselineResp.FailedEvents))

	t.Logf("Sending request to rudder-pytransformer:%s (candidate)...", candidatePytransformerTag())
	candidateResp := newClientFor(candidateURL).Transform(context.Background(), events)
	t.Logf("Candidate returned %d events, %d failed", len(candidateResp.Events), len(candidateResp.FailedEvents))

	t.Log("Comparing responses...")
	diff, equal := baselineResp.Equal(&candidateResp)
	require.True(t, equal, "responses differ:\n%s", diff)

	t.Log("Contract test passed: baseline and candidate return equivalent responses")
}
