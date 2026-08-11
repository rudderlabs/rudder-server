package pytransformer_contract

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"sync"
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
// On macOS, containers use bridge networking with port bindings and
// host.docker.internal.
type containerConfig struct {
	bridge       bool // bridge networking (vs host networking)
	hostPort     int  // allocated host port (host networking only)
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
			bridge:     true,
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
	if c.bridge {
		return containerPort
	}
	return strconv.Itoa(c.hostPort)
}

// url returns the URL to reach the container from the host test process.
func (c containerConfig) url(container *dockertest.Resource, containerPort string) string {
	if c.bridge {
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

// bcTestEnv holds clients for the two rudder-pytransformer builds under
// comparison: the baseline (last released version) and the candidate (main).
type bcTestEnv struct {
	BaselineClient  *usertransformer.Client // baseline rudder-pytransformer
	CandidateClient *usertransformer.Client // candidate rudder-pytransformer
	BaselineStats   *memstats.Store         // stats store for the baseline client
	CandidateStats  *memstats.Store         // stats store for the candidate client
}

type bcTestEnvOpt func(baselineConf, candidateConf *config.Config)

// withFailOnError configures the test env to return error responses (instead of
// panicking) when transformer retries are exhausted. Required for tests where
// pytransformer triggers retries (e.g. geolocation 5xx → HTTP 503).
func withFailOnError() bcTestEnvOpt {
	return func(baselineConf, candidateConf *config.Config) {
		baselineConf.Set("Processor.UserTransformer.failOnError", true)
		candidateConf.Set("Processor.UserTransformer.failOnError", true)
	}
}

// withLimitedRetryableHTTPRetries caps the retryable HTTP client retries so that
// 503 + X-Rudder-Should-Retry responses don't retry indefinitely in tests.
func withLimitedRetryableHTTPRetries() bcTestEnvOpt {
	return func(baselineConf, candidateConf *config.Config) {
		for _, c := range []*config.Config{baselineConf, candidateConf} {
			c.Set("Transformer.Client.UserTransformer.retryRudderErrors.maxRetry", 2)
			c.Set("Transformer.Client.UserTransformer.retryRudderErrors.maxInterval", 1*time.Millisecond)
		}
	}
}

// setPytransformerRouting points a client at one pytransformer container the way
// production addresses PyT: the per-workspace path. PerWorkspacePyTBaseURL only
// substitutes "{workspaceID}", so a template without that placeholder resolves
// to the container URL verbatim and one container serves every workspace id.
func setPytransformerRouting(conf *config.Config, pyTransformerURL string) {
	conf.Set("Processor.UserTransformer.perWorkspacePyTEnabled", true)
	conf.Set("Processor.UserTransformer.perWorkspacePyTURLTemplate", pyTransformerURL)
	// The per-workspace path classifies ECONNREFUSED/502/503 from PyT as a cold
	// start, and cold starts retry forever by default so a Deployment scaling off
	// zero is waited out rather than failed. Tests must not hang on that: several
	// subtests make pytransformer answer 503 deliberately.
	conf.Set("Processor.UserTransformer.perWorkspacePyTEndlessRetries", false)
}

// newBCTestEnv creates a bcTestEnv with fresh memstats stores per subtest.
// Fresh stores are needed because memstats accumulates counts and cannot be reset.
func newBCTestEnv(t *testing.T, baselineURL, candidateURL string, opts ...bcTestEnvOpt) *bcTestEnv {
	t.Helper()

	baselineStats, err := memstats.New()
	require.NoError(t, err)
	candidateStats, err := memstats.New()
	require.NoError(t, err)

	baselineConf := config.New()
	baselineConf.Set("Processor.UserTransformer.maxRetry", 2)
	baselineConf.Set("Processor.UserTransformer.cpDownEndlessRetries", false)
	baselineConf.Set("Processor.UserTransformer.maxRetryBackoffInterval", 1*time.Millisecond)
	setPytransformerRouting(baselineConf, baselineURL)

	candidateConf := config.New()
	candidateConf.Set("Processor.UserTransformer.maxRetry", 2)
	candidateConf.Set("Processor.UserTransformer.cpDownEndlessRetries", false)
	candidateConf.Set("Processor.UserTransformer.maxRetryBackoffInterval", 1*time.Millisecond)
	setPytransformerRouting(candidateConf, candidateURL)

	for _, opt := range opts {
		opt(baselineConf, candidateConf)
	}

	var (
		baselineLogger  = logger.NOP
		candidateLogger = logger.NOP
	)
	if testing.Verbose() {
		baselineLogger = logger.NewLogger().Child("baseline")
		candidateLogger = logger.NewLogger().Child("candidate")
	}

	return &bcTestEnv{
		BaselineClient:  usertransformer.New(baselineConf, baselineLogger, baselineStats),
		CandidateClient: usertransformer.New(candidateConf, candidateLogger, candidateStats),
		BaselineStats:   baselineStats,
		CandidateStats:  candidateStats,
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

// assertRetryCountsMatch asserts that both PyT versions triggered the same number of retries.
func (env *bcTestEnv) assertRetryCountsMatch(t *testing.T) {
	t.Helper()

	oldCPRetries := getRetryCount(env.BaselineStats, "processor_user_transformer_cp_down_retries")
	newCPRetries := getRetryCount(env.CandidateStats, "processor_user_transformer_cp_down_retries")
	t.Logf("CP down retries: baseline=%d, candidate=%d", oldCPRetries, newCPRetries)
	require.Equal(t, oldCPRetries, newCPRetries, "CP down retry counts should match between baseline and candidate")

	oldHTTPRetries := getRetryCount(env.BaselineStats, "processor_user_transformer_http_retries")
	newHTTPRetries := getRetryCount(env.CandidateStats, "processor_user_transformer_http_retries")
	t.Logf("HTTP retries: baseline=%d, candidate=%d", oldHTTPRetries, newHTTPRetries)
	require.Equal(t, oldHTTPRetries, newHTTPRetries, "HTTP retry counts should match between baseline and candidate")
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
			SourceID:         "src-1",
			DestinationID:    "dest-1",
			WorkspaceID:      "ws-1",
			MessageID:        messageID,
			TransformationID: versionID,
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

// makeEvents creates n TransformerEvents for versionID, optionally carrying library version ids.
//
// Message ids are prefixed with versionID so a test sharing one mock config backend across
// subtests can scope its assertions to its own requests.
func makeEvents(versionID string, n int, libraryVersionIDs ...string) []types.TransformerEvent {
	libraries := make([]backendconfig.LibraryT, len(libraryVersionIDs))
	for i, id := range libraryVersionIDs {
		libraries[i] = backendconfig.LibraryT{VersionID: id}
	}

	events := make([]types.TransformerEvent, n)
	for i := range events {
		events[i] = makeEvent(fmt.Sprintf("%s-msg-%d", versionID, i+1), versionID)
		events[i].Libraries = libraries
	}
	return events
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
// transformation code to both rudder-pytransformer containers under comparison.
//
// The response includes language: "pythonfaas", which is what production still
// stores for Python transformations; rudder-pytransformer only uses the "code"
// field.
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

// The suite always compares two rudder-pytransformer builds — a candidate
// against a baseline — so a behaviour change between two PyT versions fails here
// the same way it would surface in a mirrored production comparison.
//
// Which pair depends on where the suite runs:
//
//   - Locally: the image you just built with "make build-ecr-latest" in the
//     rudder-pytransformer repo (tagged "main") against the latest release. This
//     is the question a developer is asking — does my change regress anything?
//   - On CI: the latest release against the one before it. CI has no locally
//     built image, so comparing against "main" would compare a release to
//     whatever last landed on that branch.
//
// Either side can be overridden to point the suite at any pair:
//
//	PYTRANSFORMER_CANDIDATE_TAG=0.11.0 PYTRANSFORMER_BASELINE_TAG=0.10.0 go test ...
const (
	pytransformerImage = "422074288268.dkr.ecr.us-east-1.amazonaws.com/rudderstack/rudder-pytransformer"

	// localBuildTag is the tag "make build-ecr-latest" writes in the
	// rudder-pytransformer repo, so a locally built image is picked up as-is.
	localBuildTag = "main"
	// latestReleaseTag and previousReleaseTag are the two most recent released
	// rudder-pytransformer versions. Bump both when a new version ships.
	latestReleaseTag   = "0.10.2"
	previousReleaseTag = "0.10.1"
)

// runningInCI reports whether the suite is running on CI rather than a
// developer machine. Every mainstream CI provider sets CI=true.
func runningInCI() bool {
	inCI, err := strconv.ParseBool(os.Getenv("CI"))
	return err == nil && inCI
}

// candidatePytransformerTag returns the tag under test: the local build when a
// developer runs the suite, the latest release on CI.
func candidatePytransformerTag() string {
	if tag := os.Getenv("PYTRANSFORMER_CANDIDATE_TAG"); tag != "" {
		return tag
	}
	if runningInCI() {
		return latestReleaseTag
	}
	return localBuildTag
}

// baselinePytransformerTag returns the tag the candidate is compared against:
// the latest release locally, the release before it on CI.
func baselinePytransformerTag() string {
	if tag := os.Getenv("PYTRANSFORMER_BASELINE_TAG"); tag != "" {
		return tag
	}
	if runningInCI() {
		return previousReleaseTag
	}
	return latestReleaseTag
}

// startRudderPytransformer starts the candidate rudder-pytransformer container
// configured to use the mock config backend. Optional extra environment
// variables can be passed (e.g. "GEOLOCATION_URL=http://...").
// Returns the URL to reach it from the host.
func startRudderPytransformer(
	t *testing.T, pool *dockertest.Pool,
	configBackendURL string,
	extraEnv ...string,
) string {
	t.Helper()
	return startRudderPytransformerWithTag(t, pool, candidatePytransformerTag(), configBackendURL, extraEnv...)
}

// startBaselinePytransformer starts the baseline rudder-pytransformer container,
// i.e. the released version the candidate is compared against.
func startBaselinePytransformer(
	t *testing.T, pool *dockertest.Pool,
	configBackendURL string,
	extraEnv ...string,
) string {
	t.Helper()
	baseline, candidate := baselinePytransformerTag(), candidatePytransformerTag()
	if baseline == candidate {
		// Panic rather than t.Fatal: most callers start the two containers from a wg.Go goroutine
		panic(fmt.Sprintf(
			"baseline and candidate both resolved to rudder-pytransformer:%s — every comparison in this suite "+
				"would pass without testing anything. Bump latestReleaseTag/previousReleaseTag, or set "+
				"PYTRANSFORMER_BASELINE_TAG and PYTRANSFORMER_CANDIDATE_TAG to different tags.", baseline))
	}
	t.Logf("Comparing rudder-pytransformer baseline=%s against candidate=%s", baseline, candidate)
	return startRudderPytransformerWithTag(t, pool, baseline, configBackendURL, extraEnv...)
}

// startRudderPytransformerWithTag starts a rudder-pytransformer container at an
// explicit image tag.
func startRudderPytransformerWithTag(
	t *testing.T, pool *dockertest.Pool,
	tag, configBackendURL string,
	extraEnv ...string,
) string {
	t.Helper()
	const containerPort = "8080"
	cfg := newContainerConfig(t, containerPort)
	env := []string{
		"CONFIG_BACKEND_URL=" + toContainerURL(configBackendURL),
		"UVICORN_PORT=" + cfg.portStr(containerPort),
	}
	// With host networking (Linux/CI) all containers share the same network
	// namespace. This helper does not scrape Prometheus metrics, so let the OS
	// pick an unused metrics port inside the container instead of racing on a
	// host-side "free port" probe.
	if runtime.GOOS != "darwin" {
		env = append(env, "METRICS_PORT=0")
	}
	for _, e := range extraEnv {
		env = append(env, toContainerURL(e))
	}

	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   pytransformerImage,
		Tag:          tag,
		Auth:         registry.AuthConfiguration(),
		Env:          env,
		ExtraHosts:   cfg.ExtraHosts,
		PortBindings: cfg.PortBindings,
	}, cfg.hostConfigFn)
	require.NoErrorf(t, err, "failed to start rudder-pytransformer:%s container", tag)

	t.Cleanup(func() {
		if err := pool.Purge(container); err != nil {
			t.Logf("Failed to purge pytransformer:%s container: %v", tag, err)
		}
	})

	pyURL := cfg.url(container, containerPort)
	waitForHealthy(t, pool, pyURL, "rudder-pytransformer:"+tag, container)

	return pyURL
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
	delay      time.Duration
}

func (c *mockGeoConfig) setResponse(statusCode int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statusCode = statusCode
	c.closeConn = false
	c.delay = 0
}

func (c *mockGeoConfig) setConnectionClose() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeConn = true
	c.delay = 0
}

// setSlow makes the mock /geoip/* handler block for "delay" before responding
// with HTTP 200. Used to simulate a hung geolocation backend so the
// pytransformer's GEOLOCATION_TIMEOUT_SECS deadline fires.
func (c *mockGeoConfig) setSlow(delay time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statusCode = http.StatusOK
	c.closeConn = false
	c.delay = delay
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
		delay := cfg.delay
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

		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				// Client gave up — stop waiting so we don't leak a goroutine.
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
	}))
	return server, cfg
}
