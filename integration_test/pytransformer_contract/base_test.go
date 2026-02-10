package pytransformer_contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/stretchr/testify/require"
)

// TestBaseContract is the base contract test that compares responses from the
// old architecture (rudder-transformer + openfaas-flask-base) against the new
// architecture (rudder-pytransformer).
//
// This test:
// 1. Builds the openfaas-flask-base Docker image from local source
// 2. Starts a mock config backend serving Python transformation code
// 3. Starts openfaas-flask-base with the transformation pre-loaded
// 4. Starts a mock OpenFaaS gateway that proxies invocations to openfaas-flask-base
// 5. Starts rudder-transformer connected to the mock gateway
// 6. Starts rudder-pytransformer connected to the mock config backend
// 7. Sends the same /customTransform request to both
// 8. Compares the responses for equivalence
//
// Copy this test and change pythonCode + testEvents to create new contract test cases.
//
// To be able to run these tests, make sure you're able to pull Docker images from ECR (see Notion docs).
func TestBaseContract(t *testing.T) {
	// --- Test configuration ---
	const versionID = "contract-test-v1"

	pythonCode := `
def transformEvent(event, metadata):
    event['foo'] = 'bar'
    return event
`

	testEvents := []map[string]any{
		{
			"messageId": "msg-1",
			"type":      "track",
			"event":     "Test Event",
			"properties": map[string]any{
				"key": "value",
			},
			"_metadata": map[string]any{
				"sourceId":      "src-1",
				"destinationId": "dest-1",
				"workspaceId":   "ws-1",
				"messageId":     "msg-1",
			},
		},
		{
			"messageId": "msg-2",
			"type":      "identify",
			"traits": map[string]any{
				"name": "Test User",
			},
			"_metadata": map[string]any{
				"sourceId":      "src-1",
				"destinationId": "dest-1",
				"workspaceId":   "ws-1",
				"messageId":     "msg-2",
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
	mockGateway := newMockOpenFaaSGateway(t, openFaasURL)
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

	requestBody := buildCustomTransformRequest(versionID, testEvents)
	t.Logf("Request payload: %s", string(requestBody))

	t.Log("Sending request to rudder-transformer (old architecture)...")
	oldResp := sendCustomTransform(t, transformerURL, requestBody)
	t.Logf("Old architecture returned %d responses", len(oldResp))

	t.Log("Sending request to rudder-pytransformer (new architecture)...")
	newResp := sendCustomTransform(t, pyTransformerURL, requestBody)
	t.Logf("New architecture returned %d responses", len(newResp))

	t.Log("Comparing responses...")
	compareResponses(t, oldResp, newResp)

	t.Log("Contract test passed: old and new architectures return equivalent responses")
}

// transformerResponse is the response format returned by both
// rudder-transformer and rudder-pytransformer from /customTransform.
type transformerResponse struct {
	Output     map[string]any `json:"output"`
	Metadata   map[string]any `json:"metadata"`
	StatusCode int            `json:"statusCode"`
	Error      string         `json:"error,omitempty"`
	StatTags   map[string]any `json:"statTags,omitempty"`
}

// newContractConfigBackend creates a mock config backend that serves
// transformation code for both rudder-transformer and rudder-pytransformer.
//
// The response includes language: "pythonfaas" so rudder-transformer routes
// to the OpenFaaS path. rudder-pytransformer and openfaas-flask-base only
// use the "code" field.
func newContractConfigBackend(t *testing.T, transformations map[string]string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/transformation/getByVersionId":
			versionID := r.URL.Query().Get("versionId")
			code, ok := transformations[versionID]
			if !ok {
				t.Logf("ConfigBackend: unknown versionId %q", versionID)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			t.Logf("ConfigBackend: serving code for versionId %q", versionID)
			w.Header().Set("Content-Type", "application/json")
			resp := map[string]any{
				"id":             uuid.NewString(),
				"createdAt":      "2024-01-01T00:00:00.000Z",
				"updatedAt":      "2024-01-01T00:00:00.000Z",
				"versionId":      versionID,
				"name":           "Contract test transformation",
				"description":    "",
				"code":           code,
				"language":       "pythonfaas",
				"codeVersion":    "1",
				"secretsVersion": nil,
				"imports":        []any{},
				"secrets":        map[string]any{},
			}
			if err := json.NewEncoder(w).Encode(resp); err != nil {
				t.Errorf("ConfigBackend: failed to encode response: %v", err)
			}
		case "/transformationLibrary/getByVersionId":
			t.Logf("ConfigBackend: library request for %s (not configured)", r.URL.Query().Get("versionId"))
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Logf("ConfigBackend: unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

// newMockOpenFaaSGateway creates a mock OpenFaaS gateway that:
// - Accepts function deployment requests (POST /system/functions)
// - Reports functions as healthy (GET /function/*)
// - Proxies function invocations (POST /function/*) to the openfaas-flask-base container
func newMockOpenFaaSGateway(t *testing.T, openfaasFlaskURL string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("MockOpenFaaS: %s %s", r.Method, r.URL.Path)

		switch {
		// Deploy function
		case r.Method == http.MethodPost && r.URL.Path == "/system/functions":
			w.WriteHeader(http.StatusOK)

		// Update function
		case r.Method == http.MethodPut && r.URL.Path == "/system/functions":
			w.WriteHeader(http.StatusOK)

		// Delete function
		case r.Method == http.MethodDelete && r.URL.Path == "/system/functions":
			w.WriteHeader(http.StatusOK)

		// List functions
		case r.Method == http.MethodGet && r.URL.Path == "/system/functions":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("[]"))

		// Get function info
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/system/function/"):
			name := strings.TrimPrefix(r.URL.Path, "/system/function/")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":     name,
				"replicas": 1,
			})

		// Health check or invoke function
		case strings.HasPrefix(r.URL.Path, "/function/"):
			if r.Method == http.MethodGet {
				// Health check (X-REQUEST-TYPE: HEALTH-CHECK)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"service": "UP"}`))
				return
			}
			if r.Method == http.MethodPost {
				// Invoke: proxy to openfaas-flask-base
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Logf("MockOpenFaaS: failed to read body: %v", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				proxyReq, err := http.NewRequest(http.MethodPost, openfaasFlaskURL+"/", bytes.NewReader(body))
				if err != nil {
					t.Logf("MockOpenFaaS: failed to create proxy request: %v", err)
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				proxyReq.Header.Set("Content-Type", "application/json")

				resp, err := http.DefaultClient.Do(proxyReq)
				if err != nil {
					t.Logf("MockOpenFaaS: failed to proxy to openfaas-flask-base: %v", err)
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				defer func() { _ = resp.Body.Close() }()

				respBody, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Logf("MockOpenFaaS: failed to read proxy response: %v", err)
					w.WriteHeader(http.StatusBadGateway)
					return
				}

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(resp.StatusCode)
				_, _ = w.Write(respBody)
				return
			}
			w.WriteHeader(http.StatusMethodNotAllowed)

		default:
			t.Logf("MockOpenFaaS: unhandled %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

// startOpenFaasFlask starts an openfaas-flask-base container with transformation code
// loaded at startup via --vid and --config-backend-url.
func startOpenFaasFlask(
	t *testing.T, pool *dockertest.Pool,
	port int, versionID, configBackendURL string,
) *dockertest.Resource {
	t.Helper()
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "422074288268.dkr.ecr.us-east-1.amazonaws.com/rudderstack/openfaas-flask",
		Tag:        "latest",
		Env: []string{
			fmt.Sprintf("fprocess=python index.py --vid %s --config-backend-url %s", versionID, configBackendURL),
			fmt.Sprintf("port=%d", port),
		},
	}, func(hc *docker.HostConfig) {
		hc.NetworkMode = "host"
	})
	require.NoError(t, err, "failed to start openfaas-flask-base container")
	return container
}

// startRudderTransformer starts a rudder-transformer container configured to use
// the mock config backend and mock OpenFaaS gateway.
func startRudderTransformer(
	t *testing.T, pool *dockertest.Pool,
	port int, configBackendURL, openfaasGatewayURL string,
) *dockertest.Resource {
	t.Helper()
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "rudderstack/rudder-transformer",
		Tag:        "latest",
		Env: []string{
			"CONFIG_BACKEND_URL=" + configBackendURL,
			"OPENFAAS_GATEWAY_URL=" + openfaasGatewayURL,
			"PORT=" + strconv.Itoa(port),
			"NODE_OPTIONS=--no-node-snapshot",
		},
	}, func(hc *docker.HostConfig) {
		hc.NetworkMode = "host"
	})
	require.NoError(t, err, "failed to start rudder-transformer container")
	return container
}

// startRudderPytransformer starts a rudder-pytransformer container configured
// to use the mock config backend.
func startRudderPytransformer(
	t *testing.T, pool *dockertest.Pool,
	port int, configBackendURL string,
) *dockertest.Resource {
	t.Helper()
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "422074288268.dkr.ecr.us-east-1.amazonaws.com/rudderstack/rudder-pytransformer",
		Tag:        "main",
		Env: []string{
			"CONFIG_BACKEND_URL=" + configBackendURL,
			"GUNICORN_WORKERS=1",
			"GUNICORN_TIMEOUT=120",
			"GUNICORN_BIND=0.0.0.0:" + strconv.Itoa(port),
		},
	}, func(hc *docker.HostConfig) {
		hc.NetworkMode = "host"
	})
	require.NoError(t, err, "failed to start rudder-pytransformer container")
	return container
}

// waitForHealthy polls a service's /health endpoint until it returns 200 OK.
func waitForHealthy(t *testing.T, pool *dockertest.Pool, baseURL, name string) {
	t.Helper()
	t.Logf("Waiting for %s at %s to be healthy...", name, baseURL)
	err := pool.Retry(func() error {
		resp, err := http.Get(baseURL + "/health")
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("%s health check failed: %d - %s", name, resp.StatusCode, string(body))
		}
		return nil
	})
	require.NoError(t, err, "%s failed to become healthy", name)
	t.Logf("%s is healthy at %s", name, baseURL)
}

// waitForOpenFaasFlask polls the openfaas-flask-base fwatchdog health endpoint.
// fwatchdog responds to GET / with X-REQUEST-TYPE: HEALTH-CHECK header.
func waitForOpenFaasFlask(t *testing.T, pool *dockertest.Pool, baseURL string) {
	t.Helper()
	t.Logf("Waiting for openfaas-flask-base at %s to be healthy...", baseURL)
	err := pool.Retry(func() error {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/", nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-REQUEST-TYPE", "HEALTH-CHECK")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("openfaas-flask health check failed: %d - %s", resp.StatusCode, string(body))
		}
		return nil
	})
	require.NoError(t, err, "openfaas-flask-base failed to become healthy")
	t.Logf("openfaas-flask-base is healthy at %s", baseURL)
}

// buildCustomTransformRequest builds a /customTransform request payload.
func buildCustomTransformRequest(versionID string, events []map[string]any) []byte {
	var payload []map[string]any
	for _, event := range events {
		payload = append(payload, map[string]any{
			"message":  event,
			"metadata": event["_metadata"],
			"destination": map[string]any{
				"Transformations": []map[string]any{
					{"VersionID": versionID},
				},
			},
			"credentials": []any{},
			"libraries":   []any{},
		})
	}

	data, err := json.Marshal(payload)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal request: %v", err))
	}
	return data
}

// sendCustomTransform sends a POST /customTransform request and returns parsed responses.
func sendCustomTransform(t *testing.T, baseURL string, body []byte) []transformerResponse {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, baseURL+"/customTransform", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("X-Feature-Filter-Code", "?1")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"expected 200 from /customTransform, got %d: %s", resp.StatusCode, string(respBody))

	// Verify apiVersion header
	apiVersion := resp.Header.Get("apiVersion")
	require.Equal(t, "2", apiVersion, "expected apiVersion: 2 header")

	var responses []transformerResponse
	require.NoError(t, json.Unmarshal(respBody, &responses),
		"failed to unmarshal response: %s", string(respBody))
	return responses
}

// compareResponses asserts that responses from the old architecture (rudder-transformer)
// and new architecture (rudder-pytransformer) are equivalent.
func compareResponses(t *testing.T, old, new []transformerResponse) {
	t.Helper()

	require.Equal(t, len(old), len(new),
		"response count mismatch: old=%d, new=%d", len(old), len(new))

	// Sort both by messageId for deterministic comparison
	sortByMessageID := func(responses []transformerResponse) {
		sort.Slice(responses, func(i, j int) bool {
			iID := getMessageID(responses[i])
			jID := getMessageID(responses[j])
			return iID < jID
		})
	}
	sortByMessageID(old)
	sortByMessageID(new)

	for i := range old {
		o := old[i]
		n := new[i]
		msgID := getMessageID(o)

		// Status codes must match exactly
		require.Equal(t, o.StatusCode, n.StatusCode,
			"statusCode mismatch for messageId %s: old=%d, new=%d", msgID, o.StatusCode, n.StatusCode)

		// Output must match (for success responses)
		if o.StatusCode == 200 {
			require.Equal(t, o.Output, n.Output,
				"output mismatch for messageId %s", msgID)
		}

		// Error presence must match
		if o.Error != "" {
			require.NotEmpty(t, n.Error,
				"old has error %q for messageId %s but new has no error", o.Error, msgID)
		} else {
			require.Empty(t, n.Error,
				"old has no error for messageId %s but new has error %q", msgID, n.Error)
		}

		// Metadata key fields must match
		for _, key := range []string{"sourceId", "destinationId", "workspaceId"} {
			require.Equal(t, o.Metadata[key], n.Metadata[key],
				"metadata.%s mismatch for messageId %s", key, msgID)
		}
	}
}

// getMessageID extracts the messageId from a response (checks output first, then metadata).
func getMessageID(r transformerResponse) string {
	if r.Output != nil {
		if id, ok := r.Output["messageId"].(string); ok {
			return id
		}
	}
	if r.Metadata != nil {
		if id, ok := r.Metadata["messageId"].(string); ok {
			return id
		}
	}
	return ""
}
