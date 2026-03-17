package pytransformer_contract

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/processor/types"
)

const statusMirrorFiltered = 297

const (
	versionHTTPCall        = "mf-http-call-v1"
	versionNoHTTPCall      = "mf-no-http-call-v1"
	versionCaughtException = "mf-caught-exception-v1"
)

// TestMirrorFilter tests the mirror filtering feature of rudder-pytransformer.
// When MIRROR_FILTER_ENABLED=true, transformations that make external HTTP calls
// return HTTP 297 (StatusMirrorFiltered), while transformations that don't make
// HTTP calls continue to work normally.
//
// These tests send raw HTTP requests to pytransformer (not through usertransformer.Client)
// because the sendBatch panic guard rejects HTTP 297 outside of mirroring mode.
func TestMirrorFilter(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// Register transformation codes in the mock config backend
	allEntries := map[string]configBackendEntry{
		versionHTTPCall: {code: `
import requests

def transformEvent(event, metadata):
    requests.get("https://api.example.com/data")
    event['fetched'] = True
    return event
`},
		versionNoHTTPCall: {code: `
def transformEvent(event, metadata):
    event['foo'] = 'bar'
    return event
`},
		versionCaughtException: {code: `
import requests

def transformEvent(event, metadata):
    try:
        requests.get("https://evil.com/steal")
    except Exception:
        event['caught'] = True
    return event
`},
	}

	configBackend := newContractConfigBackend(t, allEntries)
	defer configBackend.Close()

	// Start pytransformer WITH mirror filter enabled
	pyFilteredContainer, pyFilteredURL := startRudderPytransformer(
		t, pool, configBackend.URL, "MIRROR_FILTER_ENABLED=true",
	)
	defer func() { _ = pool.Purge(pyFilteredContainer) }()
	waitForHealthy(t, pool, pyFilteredURL, "pytransformer-filtered")

	// Start pytransformer WITHOUT mirror filter (default)
	pyNormalContainer, pyNormalURL := startRudderPytransformer(
		t, pool, configBackend.URL,
	)
	defer func() { _ = pool.Purge(pyNormalContainer) }()
	waitForHealthy(t, pool, pyNormalURL, "pytransformer-normal")

	// sendTransform sends events to pytransformer and returns the HTTP status code and parsed response items.
	sendTransform := func(t *testing.T, baseURL string, events []types.TransformerEvent) (int, []types.TransformerResponse) {
		t.Helper()
		// Convert TransformerEvent to UserTransformerEvent (the format pytransformer expects)
		payload := make([]any, len(events))
		for i, ev := range events {
			payload[i] = map[string]any{
				"message":  ev.Message,
				"metadata": ev.Metadata,
				"destination": map[string]any{
					"Transformations": ev.Destination.Transformations,
				},
			}
		}
		body, err := jsonrs.Marshal(payload)
		require.NoError(t, err)

		req, err := http.NewRequest("POST", baseURL+"/customTransform", bytes.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()

		respBody, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		var items []types.TransformerResponse
		if len(respBody) > 0 {
			err = jsonrs.Unmarshal(respBody, &items)
			require.NoError(t, err, "failed to parse response: %s", string(respBody))
		}
		return resp.StatusCode, items
	}

	t.Run("FilterEnabled_HTTPCallReturns297", func(t *testing.T) {
		events := []types.TransformerEvent{
			makeEvent("msg-1", versionHTTPCall),
			makeEvent("msg-2", versionHTTPCall),
		}

		httpStatus, items := sendTransform(t, pyFilteredURL, events)
		t.Logf("HTTP status=%d, items=%d", httpStatus, len(items))

		require.Equal(t, statusMirrorFiltered, httpStatus, "HTTP status should be 297")
		require.Empty(t, items, "body should be empty for mirror-filtered responses")
	})

	t.Run("FilterEnabled_NoHTTPCallReturns200", func(t *testing.T) {
		events := []types.TransformerEvent{
			makeEvent("msg-1", versionNoHTTPCall),
		}

		httpStatus, items := sendTransform(t, pyFilteredURL, events)
		t.Logf("HTTP status=%d, items=%d", httpStatus, len(items))

		require.Equal(t, http.StatusOK, httpStatus, "HTTP status should be 200")
		require.Equal(t, 1, len(items), "one transformed event expected")
		require.Equal(t, 200, items[0].StatusCode, "per-event status should be 200")
		require.Equal(t, "bar", items[0].Output["foo"], "transformation should have run normally")
	})

	t.Run("FilterEnabled_CaughtExceptionStillFiltered", func(t *testing.T) {
		events := []types.TransformerEvent{
			makeEvent("msg-1", versionCaughtException),
		}

		httpStatus, items := sendTransform(t, pyFilteredURL, events)
		t.Logf("HTTP status=%d, items=%d", httpStatus, len(items))

		// Even though user code catches the exception, the flag is already set
		require.Equal(t, statusMirrorFiltered, httpStatus, "should be 297 even when exception is caught")
		require.Empty(t, items, "body should be empty for mirror-filtered responses")
	})

	t.Run("FilterDisabled_HTTPCallProcessesNormally", func(t *testing.T) {
		events := []types.TransformerEvent{
			makeEvent("msg-1", versionHTTPCall),
		}

		httpStatus, items := sendTransform(t, pyNormalURL, events)
		t.Logf("HTTP status=%d, items=%d", httpStatus, len(items))

		// Without filter, HTTP call will fail with connection error but NOT 297
		require.NotEqual(t, statusMirrorFiltered, httpStatus, "HTTP status should NOT be 297")
		for _, item := range items {
			require.NotEqual(t, statusMirrorFiltered, item.StatusCode,
				"per-event status should NOT be 297 without filter")
		}
	})
}
