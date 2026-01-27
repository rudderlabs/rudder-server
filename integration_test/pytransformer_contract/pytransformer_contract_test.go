package pytransformer_contract

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
)

// TestPyTransformerContract tests that rudder-pytransformer can be used as a drop-in
// replacement for rudder-transformer's /customTransform endpoint.
//
// This test:
// 1. Starts a rudder-pytransformer Docker container
// 2. Mocks a config backend that returns Python transformation code
// 3. Uses transformertest for features/destination transforms
// 4. Sends events through rudder-server
// 5. Verifies that the Python transformation is applied (adds foo=bar to events)
func TestPyTransformerContract(t *testing.T) {
	// Python transformation code that adds foo=bar to each event
	pythonTransformCode := `
def transformEvent(event, metadata):
    event['foo'] = 'bar'
    return event
`

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	// 1. Start PostgreSQL container
	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	// 2. Create mock webhook destination to receive events
	// Note: The mock transformer handles delivery, so this just needs to accept requests
	webhookServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer webhookServer.Close()

	// 3. Create mock config backend for Python transformation code
	// This is what rudder-pytransformer calls to fetch the transformation code
	pyConfigBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/transformation/getByVersionId":
			versionID := r.URL.Query().Get("versionId")
			t.Logf("PyTransformer requested transformation code for versionId: %s", versionID)
			if versionID == "version-1" {
				w.Header().Set("Content-Type", "application/json")
				_ = jsonrs.NewEncoder(w).Encode(map[string]string{
					"code": pythonTransformCode,
				})
			} else {
				t.Errorf("Unknown versionId requested: %s", versionID)
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			t.Errorf("PyConfigBackend: unexpected path: %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer pyConfigBackend.Close()

	// 4. Get a free port for pytransformer
	pyTransformerPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	pyTransformerURL := fmt.Sprintf("http://localhost:%d", pyTransformerPort)

	// 5. Start rudder-pytransformer container with host network (Linux)
	pyTransformerContainer, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "rudderlabs/rudder-pytransformer",
		Tag:        "poc", // TODO change to "latest" once merged to "main"
		Env: []string{
			"CONFIG_BACKEND_URL=" + pyConfigBackend.URL,
			"GUNICORN_WORKERS=1",
			"GUNICORN_TIMEOUT=120",
			fmt.Sprintf("GUNICORN_BIND=0.0.0.0:%d", pyTransformerPort),
		},
	}, func(hc *docker.HostConfig) {
		hc.NetworkMode = "host"
	})
	require.NoError(t, err)
	defer func() {
		if err := pyTransformerContainer.Close(); err != nil {
			t.Logf("Failed to close pytransformer container: %v", err)
		}
	}()

	// Wait for pytransformer to be healthy
	t.Logf("Waiting for pytransformer at %s to be healthy...", pyTransformerURL)
	err = pool.Retry(func() error {
		resp, err := http.Get(pyTransformerURL + "/health")
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("health check failed: %d - %s", resp.StatusCode, string(body))
		}
		return nil
	})
	require.NoError(t, err, "pytransformer failed to become healthy")
	t.Logf("PyTransformer is healthy at %s", pyTransformerURL)

	// 6. Create mock transformer for features and destination transforms
	// This handles everything except /customTransform (which goes to pytransformer)
	trServer := transformertest.NewBuilder().Build()
	defer trServer.Close()

	// 7. Create workspace config with WEBHOOK destination
	bcServer := backendconfigtest.NewBuilder().
		WithWorkspaceConfig(
			backendconfigtest.NewConfigBuilder().
				WithSource(
					backendconfigtest.NewSourceBuilder().
						WithID("source-1").
						WithWriteKey("writekey-1").
						WithConnection(
							backendconfigtest.NewDestinationBuilder("WEBHOOK").
								WithID("destination-1").
								WithUserTransformation("transformation-1", "version-1").
								WithConfigOption("webhookUrl", webhookServer.URL).
								Build()).
						Build()).
				Build()).
		Build()
	defer bcServer.Close()

	// 8. Start rudder-server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gwPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		err := runRudderServer(
			t, ctx, gwPort, postgresContainer, bcServer.URL, trServer.URL, pyTransformerURL, t.TempDir(),
		)
		if err != nil {
			t.Logf("rudder-server exited with error: %v", err)
		}
		return err
	})

	// 9. Wait for rudder-server to be ready
	url := fmt.Sprintf("http://localhost:%d", gwPort)
	health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
	t.Logf("rudder-server is ready at %s", url)

	// 10. Send events to rudder-server
	eventsCount := 5
	t.Logf("Sending %d identify events...", eventsCount)
	err = sendEvents(eventsCount, "identify", "writekey-1", url)
	require.NoError(t, err)

	// 11. Verify events were processed successfully
	t.Logf("Waiting for events to be processed...")
	requireJobsCount(t, ctx, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
	requireJobsCount(t, ctx, postgresContainer.DB, "rt", jobsdb.Succeeded.State, eventsCount)

	// 12. Verify transformation was applied by checking router job payloads
	// The Python transformation should have added foo=bar to each event
	requireTransformationApplied(t, ctx, postgresContainer.DB, eventsCount)

	t.Logf("All %d events processed successfully through pytransformer with foo=bar transformation!", eventsCount)

	// 13. Cleanup
	cancel()
	require.NoError(t, wg.Wait())
}

func runRudderServer(
	t testing.TB,
	ctx context.Context,
	port int,
	postgresContainer *postgres.Resource,
	cbURL, transformerURL, userTransformURL,
	tmpDir string,
) (err error) {
	t.Setenv("CONFIG_BACKEND_URL", cbURL)
	t.Setenv("WORKSPACE_TOKEN", "token")
	// DEST_TRANSFORM_URL for destination transforms, features, etc.
	t.Setenv("DEST_TRANSFORM_URL", transformerURL)
	// USER_TRANSFORM_URL specifically for /customTransform (pytransformer)
	t.Setenv("USER_TRANSFORM_URL", userTransformURL)

	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.host"), postgresContainer.Host)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.port"), postgresContainer.Port)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.user"), postgresContainer.User)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.name"), postgresContainer.Database)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DB.password"), postgresContainer.Password)

	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Warehouse.mode"), "off")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "DestinationDebugger.disableEventDeliveryStatusUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "SourceDebugger.disableEventUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "TransformationDebugger.disableTransformationStatusUploads"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.backup.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "JobsDB.migrateDSLoopSleepDuration"), "60m")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "archival.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Reporting.syncer.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "BatchRouter.pingFrequency"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "BatchRouter.uploadFreq"), "1s")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.webPort"), strconv.Itoa(port))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "RUDDER_TMPDIR"), os.TempDir())
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.storagePath"), path.Join(tmpDir, "/recovery_data.json"))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "recovery.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Profiler.Enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.enableSuppressUserFeature"), "false")

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "DUMMY"})
	c := r.Run(ctx, []string{"pytransformer-contract"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return err
}

func sendEvents(
	num int,
	eventType, writeKey,
	url string,
) error {
	for i := 0; i < num; i++ {
		err := func() error {
			payload := []byte(fmt.Sprintf(`
			{
			  "batch": [
				{
				  "userId": %[1]q,
				  "type": %[2]q,
				  "context": {
					"traits": {
					  "trait1": "new-val"
					},
					"ip": "14.5.67.21",
					"library": {
					  "name": "http"
					}
				  },
				  "timestamp": "2020-02-02T00:23:09.544Z"
				}
			  ]
			}`,
				rand.String(10),
				eventType,
			))
			req, err := http.NewRequest(http.MethodPost, url+"/v1/batch", bytes.NewReader(payload))
			if err != nil {
				return err
			}
			req.SetBasicAuth(writeKey, "password")

			resp, err := http.DefaultClient.Do(req)
			defer func() { kithttputil.CloseResponse(resp) }()
			if err != nil {
				return err
			}

			if resp.StatusCode != http.StatusOK {
				b, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("failed to send event to rudder server, status code: %d: %s", resp.StatusCode, string(b))
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func requireJobsCount(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	queue, state string,
	expectedCount int,
) {
	t.Helper()

	query := fmt.Sprintf(`
		SELECT
		  count(*)
		FROM
		  unionjobsdbmetadata('%s', 1)
		WHERE
		  job_state = '%s';
	`,
		queue,
		state,
	)
	require.Eventuallyf(t, func() bool {
		var jobsCount int
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&jobsCount))
		t.Logf("%s %sJobCount: %d (expecting %d)", queue, state, jobsCount, expectedCount)
		return jobsCount == expectedCount
	},
		60*time.Second,
		1*time.Second,
		"%d %s events should be in %s state", expectedCount, queue, state,
	)
}

// requireTransformationApplied verifies that the Python transformation was applied
// by checking the router job payloads for the presence of "foo": "bar"
func requireTransformationApplied(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	expectedCount int,
) {
	t.Helper()

	// Query the router jobs to get event payloads
	rows, err := db.QueryContext(ctx, `SELECT event_payload FROM rt_jobs_1`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var transformedCount int
	for rows.Next() {
		var payload string
		require.NoError(t, rows.Scan(&payload))

		var event map[string]any
		require.NoError(t, jsonrs.Unmarshal([]byte(payload), &event))

		// Check if foo=bar was added by the Python transformation
		fooValue, exists := event["foo"]
		if exists && fooValue == "bar" {
			transformedCount++
			t.Logf("Event %d has foo=%v (transformation applied)", transformedCount, fooValue)
		} else {
			t.Errorf("Event missing foo=bar transformation: foo exists=%v, value=%v", exists, fooValue)
		}
	}
	require.NoError(t, rows.Err())

	require.Equal(t, expectedCount, transformedCount,
		"all %d events should have foo=bar transformation applied", expectedCount)
}
