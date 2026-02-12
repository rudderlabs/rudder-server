package pytransformer_contract

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/registry"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/processor/usertransformer"
)

// bcTestEnv holds clients for both the old architecture (rudder-transformer + openfaas)
// and the new architecture (rudder-pytransformer) to compare their responses.
type bcTestEnv struct {
	OldClient *usertransformer.Client // rudder-transformer + openfaas (old architecture)
	NewClient *usertransformer.Client // rudder-pytransformer (new architecture)
	OldStats  *memstats.Store         // stats store for old architecture client
	NewStats  *memstats.Store         // stats store for new architecture client
}

// newBCTestEnv creates a bcTestEnv with fresh memstats stores per subtest.
// Fresh stores are needed because memstats accumulates counts and cannot be reset.
func newBCTestEnv(t *testing.T, transformerURL, pyTransformerURL string) *bcTestEnv {
	t.Helper()

	oldStats, err := memstats.New()
	require.NoError(t, err)
	newStats, err := memstats.New()
	require.NoError(t, err)

	oldArchConf := config.New()
	oldArchConf.Set("Processor.UserTransformer.maxRetry", 2)
	oldArchConf.Set("Processor.UserTransformer.cpDownEndlessRetries", false)
	oldArchConf.Set("Processor.UserTransformer.maxRetryBackoffInterval", 1*time.Millisecond)
	oldArchConf.Set("USER_TRANSFORM_URL", transformerURL)

	newArchConf := config.New()
	newArchConf.Set("Processor.UserTransformer.maxRetry", 2)
	newArchConf.Set("Processor.UserTransformer.cpDownEndlessRetries", false)
	newArchConf.Set("Processor.UserTransformer.maxRetryBackoffInterval", 1*time.Millisecond)
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
		OldClient: usertransformer.New(oldArchConf, oldArchLogger, oldStats),
		NewClient: usertransformer.New(newArchConf, newArchLogger, newStats),
		OldStats:  oldStats,
		NewStats:  newStats,
	}
}

// getRetryCount returns the total retry count for a stat name from a memstats store.
// Returns 0 if the stat was never recorded.
func getRetryCount(store *memstats.Store, name string) int {
	m := store.Get(name, nil)
	if m == nil {
		return 0
	}
	return int(m.LastValue())
}

// assertRetryCountsMatch asserts that both architectures triggered the same number of retries.
func (env *bcTestEnv) assertRetryCountsMatch(t *testing.T) {
	t.Helper()

	oldCPRetries := getRetryCount(env.OldStats, "processor_user_transformer_cp_down_retries")
	newCPRetries := getRetryCount(env.NewStats, "processor_user_transformer_cp_down_retries")
	t.Logf("CP down retries: old=%d, new=%d", oldCPRetries, newCPRetries)
	require.Equal(t, oldCPRetries, newCPRetries, "CP down retry counts should match between old and new arch")

	oldHTTPRetries := getRetryCount(env.OldStats, "processor_user_transformer_http_retries")
	newHTTPRetries := getRetryCount(env.NewStats, "processor_user_transformer_http_retries")
	t.Logf("HTTP retries: old=%d, new=%d", oldHTTPRetries, newHTTPRetries)
	require.Equal(t, oldHTTPRetries, newHTTPRetries, "HTTP retry counts should match between old and new arch")
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

// configBackendEntry controls what the mock config backend returns for a given versionId.
//
// When statusCode is 0 (default), the entry is treated as a normal transformation:
// HTTP 200 with the standard JSON envelope wrapping the code field.
//
// When statusCode is non-zero, the config backend returns that status code with body
// as the raw response body (no JSON envelope).
type configBackendEntry struct {
	statusCode int
	body       string
	code       string
}

// newContractConfigBackend creates a mock config backend that serves
// transformation code for both rudder-transformer and rudder-pytransformer.
//
// The response includes language: "pythonfaas" so rudder-transformer routes
// to the OpenFaaS path. rudder-pytransformer and openfaas-flask-base only
// use the "code" field.
func newContractConfigBackend(t *testing.T, entries map[string]configBackendEntry) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/transformation/getByVersionId":
			versionID := r.URL.Query().Get("versionId")
			entry, ok := entries[versionID]
			if !ok {
				t.Logf("ConfigBackend: unknown versionId %q", versionID)
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if entry.statusCode != 0 {
				t.Logf("ConfigBackend: returning %d for versionId %q", entry.statusCode, versionID)
				w.WriteHeader(entry.statusCode)
				if entry.body != "" {
					_, _ = w.Write([]byte(entry.body))
				}
				return
			}
			if entry.code == "" && entry.body != "" {
				t.Logf("ConfigBackend: returning 200 with raw body for versionId %q", versionID)
				_, _ = w.Write([]byte(entry.body))
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
				"code":           entry.code,
				"language":       "pythonfaas",
				"codeVersion":    "1",
				"secretsVersion": nil,
				"imports":        []any{},
				"secrets":        map[string]any{},
			}
			if err := jsonrs.NewEncoder(w).Encode(resp); err != nil {
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
// - Proxies function invocations (POST /function/*) to the URL returned by getTarget()
//
// getTarget is called on each invocation, allowing the target to change between subtests.
// Returns the server and an atomic counter tracking POST /function/* invocations.
func newMockOpenFaaSGateway(t *testing.T, getTarget func() string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	invocations := &atomic.Int64{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			_ = jsonrs.NewEncoder(w).Encode(map[string]any{
				"name":     name,
				"replicas": 1,
			})

		// Health check or invoke function
		case strings.HasPrefix(r.URL.Path, "/function/"):
			if r.Method == http.MethodGet {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"service": "UP"}`))
				return
			}
			if r.Method == http.MethodPost {
				invocations.Add(1)
				targetURL := getTarget()
				if targetURL == "" {
					t.Log("MockOpenFaaS: no target URL set")
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}

				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Logf("MockOpenFaaS: failed to read body: %v", err)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				proxyReq, err := http.NewRequest(http.MethodPost, targetURL+"/", bytes.NewReader(body))
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
	return srv, invocations
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
		Auth:       registry.AuthConfiguration(),
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
		Tag:        "latest",
		Auth:       registry.AuthConfiguration(),
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

// normalizeJSON re-marshals a JSON string so keys are in deterministic (sorted) order.
// For non-200 responses, the Go usertransformer client stores the raw JSON body as the
// Error string. Different transformers (JS vs Python) may serialize JSON keys in different
// orders, so we normalize before comparison.
func normalizeJSON(s string) string {
	var v any
	if err := jsonrs.Unmarshal([]byte(s), &v); err != nil {
		return s // not valid JSON, return as-is
	}
	b, err := jsonrs.Marshal(v)
	if err != nil {
		return s
	}
	return string(b)
}

// normalizeResponseErrors normalizes JSON Error strings in a Response for comparison.
func normalizeResponseErrors(r *types.Response) {
	for i := range r.Events {
		r.Events[i].Error = normalizeJSON(r.Events[i].Error)
	}
	for i := range r.FailedEvents {
		r.FailedEvents[i].Error = normalizeJSON(r.FailedEvents[i].Error)
	}
}
