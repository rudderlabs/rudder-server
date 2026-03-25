package pytransformer_contract

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	minioclient "github.com/minio/minio-go/v7"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	dockertesthelper "github.com/rudderlabs/rudder-go-kit/testhelper/docker"
	miniodocker "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/registry"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/processor/usertransformer"
)

// containerConfig holds platform-specific Docker container configuration.
// On Linux, containers use host networking (sharing the host's network namespace).
// On macOS, containers use bridge networking with port bindings and host.docker.internal.
type containerConfig struct {
	hostPort     int // allocated host port (Linux only)
	ExtraHosts   []string
	PortBindings map[docker.Port][]docker.PortBinding
	hostConfigFn func(*docker.HostConfig)
}

// newContainerConfig returns the appropriate Docker configuration for the current platform.
// Default is host networking (Linux, CI, production). On macOS, Docker Desktop does not
// support host networking so we fall back to bridge networking with port bindings.
func newContainerConfig(t *testing.T, containerPort string) containerConfig {
	t.Helper()
	if runtime.GOOS == "darwin" {
		return containerConfig{
			ExtraHosts: []string{"host.docker.internal:host-gateway"},
			PortBindings: map[docker.Port][]docker.PortBinding{
				docker.Port(containerPort + "/tcp"): {{HostIP: "127.0.0.1", HostPort: "0"}},
			},
			hostConfigFn: func(hc *docker.HostConfig) {},
		}
	}
	port, err := kithelper.GetFreePort()
	require.NoError(t, err)
	return containerConfig{
		hostPort: port,
		hostConfigFn: func(hc *docker.HostConfig) {
			hc.NetworkMode = "host"
		},
	}
}

// portStr returns the port to pass as a container environment variable.
func (c containerConfig) portStr(containerPort string) string {
	if runtime.GOOS == "darwin" {
		return containerPort
	}
	return strconv.Itoa(c.hostPort)
}

// url returns the URL to reach the container from the host test process.
func (c containerConfig) url(container *dockertest.Resource, containerPort string) string {
	if runtime.GOOS == "darwin" {
		return fmt.Sprintf("http://%s:%s",
			container.GetBoundIP(containerPort+"/tcp"),
			container.GetPort(containerPort+"/tcp"),
		)
	}
	return fmt.Sprintf("http://localhost:%d", c.hostPort)
}

// toContainerURL rewrites a host URL for use inside a Docker container.
// On macOS (bridge networking): replaces localhost/127.0.0.1 with host.docker.internal.
// Default (host networking): returns the URL as-is since containers share the host namespace.
func toContainerURL(url string) string {
	if runtime.GOOS == "darwin" {
		return dockertesthelper.ToInternalDockerHost(url)
	}
	return url
}

// bcTestEnv holds clients for both the old architecture (rudder-transformer + openfaas)
// and the new architecture (rudder-pytransformer) to compare their responses.
type bcTestEnv struct {
	OldClient *usertransformer.Client // rudder-transformer + openfaas (old architecture)
	NewClient *usertransformer.Client // rudder-pytransformer (new architecture)
	OldStats  *memstats.Store         // stats store for old architecture client
	NewStats  *memstats.Store         // stats store for new architecture client
}

type bcTestEnvOpt func(oldConf, newConf *config.Config)

// withFailOnError configures the test env to return error responses (instead of
// panicking) when transformer retries are exhausted. Required for tests where
// the new architecture triggers retries (e.g. geolocation 5xx → HTTP 503).
func withFailOnError() bcTestEnvOpt {
	return func(oldConf, newConf *config.Config) {
		oldConf.Set("Processor.UserTransformer.failOnError", true)
		newConf.Set("Processor.UserTransformer.failOnError", true)
	}
}

// withLimitedRetryableHTTPRetries caps the retryable HTTP client retries so that
// 503 + X-Rudder-Should-Retry responses don't retry indefinitely in tests.
func withLimitedRetryableHTTPRetries() bcTestEnvOpt {
	return func(oldConf, newConf *config.Config) {
		for _, c := range []*config.Config{oldConf, newConf} {
			c.Set("Transformer.Client.UserTransformer.retryRudderErrors.maxRetry", 2)
			c.Set("Transformer.Client.UserTransformer.retryRudderErrors.maxInterval", 1*time.Millisecond)
		}
	}
}

// newBCTestEnv creates a bcTestEnv with fresh memstats stores per subtest.
// Fresh stores are needed because memstats accumulates counts and cannot be reset.
func newBCTestEnv(t *testing.T, transformerURL, pyTransformerURL string, opts ...bcTestEnvOpt) *bcTestEnv {
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

	for _, opt := range opts {
		opt(oldArchConf, newArchConf)
	}

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

// makeEventWithCredentials creates a TransformerEvent with credentials attached.
func makeEventWithCredentials(messageID, versionID string, credentials []types.Credential) types.TransformerEvent {
	ev := makeEvent(messageID, versionID)
	ev.Credentials = credentials
	return ev
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
// loaded at startup via --vid and --config-backend-url. Optional extra environment
// variables can be passed (e.g. "geolocation_url=http://...").
// Returns the container resource and the URL to reach it from the host.
func startOpenFaasFlask(
	t *testing.T, pool *dockertest.Pool,
	versionID, configBackendURL string,
	extraEnv ...string,
) (*dockertest.Resource, string) {
	t.Helper()
	const containerPort = "8080"
	cfg := newContainerConfig(t, containerPort)
	env := []string{
		fmt.Sprintf("fprocess=python index.py --vid %s --config-backend-url %s", versionID, toContainerURL(configBackendURL)),
		fmt.Sprintf("port=%s", cfg.portStr(containerPort)),
	}
	for _, e := range extraEnv {
		env = append(env, toContainerURL(e))
	}
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "422074288268.dkr.ecr.us-east-1.amazonaws.com/rudderstack/openfaas-flask",
		Tag:          "latest",
		Auth:         registry.AuthConfiguration(),
		Env:          env,
		ExtraHosts:   cfg.ExtraHosts,
		PortBindings: cfg.PortBindings,
	}, cfg.hostConfigFn)
	require.NoError(t, err, "failed to start openfaas-flask-base container")
	return container, cfg.url(container, containerPort)
}

// startRudderTransformer starts a rudder-transformer container configured to use
// the mock config backend and mock OpenFaaS gateway.
// Returns the container resource and the URL to reach it from the host.
func startRudderTransformer(
	t *testing.T, pool *dockertest.Pool,
	configBackendURL, openfaasGatewayURL string,
) (*dockertest.Resource, string) {
	t.Helper()
	const containerPort = "9090"
	cfg := newContainerConfig(t, containerPort)
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "rudderstack/rudder-transformer",
		Tag:        "latest",
		Env: []string{
			"CONFIG_BACKEND_URL=" + toContainerURL(configBackendURL),
			"OPENFAAS_GATEWAY_URL=" + toContainerURL(openfaasGatewayURL),
			"PORT=" + cfg.portStr(containerPort),
			"NODE_OPTIONS=--no-node-snapshot",
		},
		ExtraHosts:   cfg.ExtraHosts,
		PortBindings: cfg.PortBindings,
	}, cfg.hostConfigFn)
	require.NoError(t, err, "failed to start rudder-transformer container")
	return container, cfg.url(container, containerPort)
}

// startRudderPytransformer starts a rudder-pytransformer container configured
// to use the mock config backend. Optional extra environment variables can be
// passed (e.g. "GEOLOCATION_URL=http://...").
// Returns the container resource and the URL to reach it from the host.
func startRudderPytransformer(
	t *testing.T, pool *dockertest.Pool,
	configBackendURL string,
	extraEnv ...string,
) (*dockertest.Resource, string) {
	t.Helper()
	const containerPort = "8080"
	cfg := newContainerConfig(t, containerPort)
	env := []string{
		"CONFIG_BACKEND_URL=" + toContainerURL(configBackendURL),
		"UVICORN_PORT=" + cfg.portStr(containerPort),
	}
	// With host networking (Linux/CI) all containers share the same network
	// namespace, so the Prometheus metrics server (default port 9091) must use
	// a unique port to avoid "Address already in use" collisions.
	if runtime.GOOS != "darwin" {
		metricsPort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		env = append(env, "METRICS_PORT="+strconv.Itoa(metricsPort))
	}
	for _, e := range extraEnv {
		env = append(env, toContainerURL(e))
	}
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "422074288268.dkr.ecr.us-east-1.amazonaws.com/rudderstack/rudder-pytransformer",
		Tag:          "latest",
		Auth:         registry.AuthConfiguration(),
		Env:          env,
		ExtraHosts:   cfg.ExtraHosts,
		PortBindings: cfg.PortBindings,
	}, cfg.hostConfigFn)
	require.NoError(t, err, "failed to start rudder-pytransformer container")
	return container, cfg.url(container, containerPort)
}

// waitForHealthy polls a service's /health endpoint until it returns 200 OK.
// If a container is provided and the health check fails, container state and
// logs are dumped to the test output to aid CI debugging.
func waitForHealthy(t *testing.T, pool *dockertest.Pool, baseURL, name string, containers ...*dockertest.Resource) {
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
	if err != nil && len(containers) > 0 {
		dumpContainerLogs(t, pool, containers[0], name)
	}
	require.NoError(t, err, "%s failed to become healthy", name)
	t.Logf("%s is healthy at %s", name, baseURL)
}

// dumpContainerLogs inspects a container's state and prints its last 100 log
// lines. This is called when a health check fails so CI output contains enough
// context to diagnose startup issues.
func dumpContainerLogs(t *testing.T, pool *dockertest.Pool, container *dockertest.Resource, name string) {
	t.Helper()

	info, err := pool.Client.InspectContainer(container.Container.ID)
	if err != nil {
		t.Logf("Failed to inspect %s container: %v", name, err)
	} else {
		t.Logf("%s container state: Running=%v, ExitCode=%d, Status=%s",
			name, info.State.Running, info.State.ExitCode, info.State.Status)
	}

	var buf bytes.Buffer
	err = pool.Client.Logs(docker.LogsOptions{
		Container:    container.Container.ID,
		OutputStream: &buf,
		ErrorStream:  &buf,
		Stdout:       true,
		Stderr:       true,
		Tail:         "100",
	})
	if err != nil {
		t.Logf("Failed to fetch %s container logs: %v", name, err)
		return
	}
	t.Logf("=== %s container logs ===\n%s=== end %s logs ===", name, buf.String(), name)
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

// startRudderGeolocation starts a MinIO container, uploads the test MMDB file,
// then starts a rudder-geolocation container configured to download the database
// from MinIO on startup. The container serves the /geoip/{ip} endpoint.
// Returns the container resource and the URL to reach it from the host.
func startRudderGeolocation(t *testing.T, pool *dockertest.Pool) (*dockertest.Resource, string) {
	t.Helper()

	minioResource, err := miniodocker.Setup(pool, t)
	require.NoError(t, err, "failed to start MinIO")

	_, err = minioResource.Client.FPutObject(
		t.Context(),
		minioResource.BucketName,
		"city_test.mmdb",
		"../../services/geolocation/testdata/city_test.mmdb",
		minioclient.PutObjectOptions{},
	)
	require.NoError(t, err, "failed to upload city_test.mmdb to MinIO")

	const containerPort = "8080"
	cfg := newContainerConfig(t, containerPort)
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "422074288268.dkr.ecr.us-east-1.amazonaws.com/rudderstack/rudder-geolocation",
		Tag:        "main",
		Auth:       registry.AuthConfiguration(),
		Env: []string{
			"PORT=" + cfg.portStr(containerPort),
			"BUCKET=" + minioResource.BucketName,
			"KEY=city_test.mmdb",
			"OUTPUT_PATH=/tmp/city.mmdb",
			"REGION=us-east-1",
			"S3_ENDPOINT=" + toContainerURL("http://"+minioResource.Endpoint),
			"S3_FORCE_PATH_STYLE=true",
			"AWS_ACCESS_KEY_ID=" + minioResource.AccessKeyID,
			"AWS_SECRET_ACCESS_KEY=" + minioResource.AccessKeySecret,
		},
		ExtraHosts:   cfg.ExtraHosts,
		PortBindings: cfg.PortBindings,
	}, cfg.hostConfigFn)
	require.NoError(t, err, "failed to start rudder-geolocation container")

	return container, cfg.url(container, containerPort)
}

// waitForGeolocation polls the rudder-geolocation service until it responds to
// a /geoip request. We try a well-known public IP (1.2.3.4) to verify the
// service is up and can resolve IPs.
func waitForGeolocation(t *testing.T, pool *dockertest.Pool, baseURL string) {
	t.Helper()
	t.Logf("Waiting for rudder-geolocation at %s to be healthy...", baseURL)
	err := pool.Retry(func() error {
		resp, err := http.Get(baseURL + "/geoip/1.2.3.4")
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("rudder-geolocation not ready: status %d", resp.StatusCode)
		}
		return nil
	})
	require.NoError(t, err, "rudder-geolocation failed to become healthy")
	t.Logf("rudder-geolocation is healthy at %s", baseURL)
}

// mockGeoConfig holds configurable behavior for the mock geolocation service.
// Use setResponse to change the HTTP status code and body between subtests.
type mockGeoConfig struct {
	mu         sync.Mutex
	statusCode int
	closeConn  bool
}

func (c *mockGeoConfig) setResponse(statusCode int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statusCode = statusCode
	c.closeConn = false
}

func (c *mockGeoConfig) setConnectionClose() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeConn = true
}

// newConfigurableMockGeolocationService creates a mock geolocation HTTP server
// whose /geoip/* responses can be changed between subtests via mockGeoConfig.
// Health check endpoints (/ and /health) always return 200 OK.
func newConfigurableMockGeolocationService(t *testing.T) (*httptest.Server, *mockGeoConfig) {
	t.Helper()
	cfg := &mockGeoConfig{statusCode: http.StatusOK}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Health check — always responds OK so containers stay healthy.
		if r.URL.Path == "/" || r.URL.Path == "/health" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"service":"geolocation","status":"ok"}`))
			return
		}

		cfg.mu.Lock()
		code := cfg.statusCode
		closeConn := cfg.closeConn
		cfg.mu.Unlock()

		if closeConn {
			hj, ok := w.(http.Hijacker)
			if ok {
				conn, _, err := hj.Hijack()
				if err == nil {
					_ = conn.Close()
				}
				return
			} else {
				t.Log("MockGeolocation: failed to hijack connection")
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
	}))
	return server, cfg
}
