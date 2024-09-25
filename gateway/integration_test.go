package gateway

import (
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	whUtil "github.com/rudderlabs/rudder-server/testhelper/webhook"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

func TestGatewayIntegration(t *testing.T) {
	for _, appType := range []string{app.GATEWAY, app.EMBEDDED} {
		t.Run(appType, func(t *testing.T) {
			testGatewayByAppType(t, appType)
		})
	}
}

func testGatewayByAppType(t *testing.T, appType string) {
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		group                errgroup.Group
		postgresContainer    *postgres.Resource
		transformerContainer *transformertest.Resource
		workspaceToken       = "workspace-token"
	)

	group.Go(func() (err error) {
		postgresContainer, err = postgres.Setup(pool, t)
		if err != nil {
			return fmt.Errorf("could not start postgres: %v", err)
		}
		return nil
	})
	group.Go(func() (err error) {
		transformerContainer, err = transformertest.Setup(pool, t)
		if err != nil {
			return fmt.Errorf("could not start transformer: %v", err)
		}
		return nil
	})
	require.NoError(t, group.Wait())

	webhook := whUtil.NewRecorder()
	t.Cleanup(webhook.Close)

	writeKey := rand.String(27)
	workspaceID := rand.String(27)
	marshalledWorkspaces := testhelper.FillTemplateAndReturn(t, "../integration_test/multi_tenant_test/testdata/mtGatewayTest02.json", map[string]string{
		"writeKey":    writeKey,
		"workspaceId": workspaceID,
		"webhookUrl":  webhook.Server.URL,
	})
	require.NoError(t, err)
	sourceID := "xxxyyyzzEaEurW247ad9WYZLUyk" // sourceID from the workspace config template

	beConfigRouter := chi.NewMux()
	if testing.Verbose() {
		beConfigRouter.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Logf("BackendConfig server call: %+v", r)
				next.ServeHTTP(w, r)
			})
		})
	}

	backedConfigHandler := func(w http.ResponseWriter, r *http.Request) {
		u, _, ok := r.BasicAuth()
		require.True(t, ok, "Auth should be present")
		require.Equalf(t, workspaceToken, u,
			"Expected HTTP basic authentication to be %q, got %q instead",
			workspaceToken, u)

		n, err := w.Write(marshalledWorkspaces.Bytes())
		require.NoError(t, err)
		require.Equal(t, marshalledWorkspaces.Len(), n)
	}
	controlPlaneHandler := func(w http.ResponseWriter, r *http.Request) {}

	beConfigRouter.Get("/workspaceConfig", backedConfigHandler)
	beConfigRouter.Post("/data-plane/v1/workspaces/{workspaceID}/settings", controlPlaneHandler)
	beConfigRouter.NotFound(func(w http.ResponseWriter, r *http.Request) {
		require.FailNowf(t, "backend config", "unexpected request to backend config, not found: %+v", r.URL)
		w.WriteHeader(http.StatusNotFound)
	})

	backendConfigSrv := httptest.NewServer(beConfigRouter)
	t.Logf("BackendConfig server listening on: %s", backendConfigSrv.URL)
	t.Cleanup(backendConfigSrv.Close)

	httpPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	debugPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	rudderTmpDir, err := os.MkdirTemp("", "rudder_server_*_test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(rudderTmpDir) })

	releaseName := t.Name() + "_" + appType
	envArr := []string{
		fmt.Sprintf("APP_TYPE=%s", appType),
		fmt.Sprintf("INSTANCE_ID=%s", "rudderstackmt-v0-rudderstack-0"),
		fmt.Sprintf("RELEASE_NAME=%s", releaseName),
		fmt.Sprintf("JOBS_DB_HOST=%s", postgresContainer.Host),
		fmt.Sprintf("JOBS_DB_PORT=%s", postgresContainer.Port),
		fmt.Sprintf("JOBS_DB_USER=%s", postgresContainer.User),
		fmt.Sprintf("JOBS_DB_DB_NAME=%s", postgresContainer.Database),
		fmt.Sprintf("JOBS_DB_PASSWORD=%s", postgresContainer.Password),
		fmt.Sprintf("CONFIG_BACKEND_URL=%s", backendConfigSrv.URL),
		fmt.Sprintf("RSERVER_GATEWAY_WEB_PORT=%d", httpPort),
		fmt.Sprintf("RSERVER_GATEWAY_ADMIN_WEB_PORT=%d", httpAdminPort),
		fmt.Sprintf("RSERVER_PROFILER_PORT=%d", debugPort),
		fmt.Sprintf("RSERVER_ENABLE_STATS=%s", "false"),
		fmt.Sprintf("RUDDER_TMPDIR=%s", rudderTmpDir),
		fmt.Sprintf("DEST_TRANSFORM_URL=%s", transformerContainer.TransformerURL),
		fmt.Sprintf("WORKSPACE_TOKEN=%s", workspaceToken),
	}
	if testing.Verbose() {
		envArr = append(envArr, "LOG_LEVEL=debug")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		cmd := exec.CommandContext(ctx, "go", "run", "../main.go")
		cmd.Env = append(os.Environ(), envArr...)
		stdout, err := cmd.StdoutPipe()
		require.NoError(t, err)
		stderr, err := cmd.StderrPipe()
		require.NoError(t, err)

		defer func() {
			_ = stdout.Close()
			_ = stderr.Close()
		}()
		require.NoError(t, cmd.Start())
		if testing.Verbose() {
			go func() { _, _ = io.Copy(os.Stdout, stdout) }()
			go func() { _, _ = io.Copy(os.Stderr, stderr) }()
		}

		if err = cmd.Wait(); err != nil {
			if err.Error() != "signal: killed" {
				t.Errorf("Error running main.go: %v", err)
				return
			}
		}
		t.Log("main.go exited")
	}()
	t.Cleanup(func() { cancel(); <-done })

	healthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	resp, err := http.Get(healthEndpoint)
	require.ErrorContains(t, err, "connection refused")
	require.Nil(t, resp)
	defer func() { httputil.CloseResponse(resp) }()

	// Checking now that the configuration has been processed and the server can start
	t.Log("Checking health endpoint at", healthEndpoint)
	health.WaitUntilReady(ctx, t,
		healthEndpoint,
		3*time.Minute,
		100*time.Millisecond,
		t.Name(),
	)

	cleanupGwJobs := func() {
		_, _ = postgresContainer.DB.ExecContext(ctx, `DELETE FROM gw_job_status_1 WHERE job_id in (SELECT job_id from gw_jobs_1 WHERE workspace_id = $1)`, workspaceID)
		_, _ = postgresContainer.DB.ExecContext(ctx, `DELETE FROM gw_jobs_1 WHERE workspace_id = $1`, workspaceID)
	}

	// Test basic Gateway happy path
	t.Run("events are received in gateway", func(t *testing.T) {
		require.Empty(t, webhook.Requests(), "webhook should have no requests before sending the events")
		sendEventsToGateway(t, httpPort, writeKey, sourceID, workspaceID)
		t.Cleanup(cleanupGwJobs)

		var (
			eventPayload string
			message      map[string]interface{}
		)
		require.Eventually(t, func() bool {
			return postgresContainer.DB.QueryRowContext(ctx,
				"SELECT event_payload FROM gw_jobs_1 WHERE workspace_id = $1", workspaceID,
			).Scan(&eventPayload) == nil
		}, time.Minute, 50*time.Millisecond)
		require.NoError(t, json.Unmarshal([]byte(eventPayload), &message))

		var userId string
		err = postgresContainer.DB.QueryRowContext(ctx,
			"SELECT user_id FROM gw_jobs_1 WHERE workspace_id = $1", workspaceID,
		).Scan(&userId)
		require.NoError(t, err)
		require.Equal(t, "anonymousId_header<<>>anonymousId_1<<>>identified_user_id", userId)

		batch, ok := message["batch"].([]interface{})
		require.True(t, ok)
		require.Len(t, batch, 1)
		require.Equal(t, message["writeKey"], writeKey)
		for _, msg := range batch {
			m, ok := msg.(map[string]interface{})
			require.True(t, ok)
			require.Equal(t, "anonymousId_1", m["anonymousId"])
			require.Equal(t, "identified_user_id", m["userId"])
			require.Equal(t, "identify", m["type"])
			require.Equal(t, "1", m["eventOrderNo"])
			require.Equal(t, "messageId_1", m["messageId"])
		}

		// Only the Gateway is running, so we don't expect any destinations to be hit.
		require.Empty(t, webhook.Requests(), "webhook should have no requests because there is no processor")
	})

	if appType == app.EMBEDDED {
		// Trigger normal mode for the processor to start
		t.Run("switch to normal mode", func(t *testing.T) {
			sendEventsToGateway(t, httpPort, writeKey, sourceID, workspaceID)
			t.Cleanup(cleanupGwJobs)

			require.Eventuallyf(t, func() bool {
				return webhook.RequestsCount() == 2
			}, 60*time.Second, 100*time.Millisecond, "Webhook should have received %d requests on %d", webhook.RequestsCount(), httpPort)
		})

		// Trigger degraded mode, the Gateway should still work
		t.Run("switch to degraded mode", func(t *testing.T) {
			sendEventsToGateway(t, httpPort, writeKey, sourceID, workspaceID)
			t.Cleanup(cleanupGwJobs)

			var count int
			err = postgresContainer.DB.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM gw_jobs_1 WHERE workspace_id = $1", workspaceID,
			).Scan(&count)
			require.NoError(t, err)
			require.Equal(t, 2, count)

			var userId string
			err = postgresContainer.DB.QueryRowContext(ctx,
				"SELECT user_id FROM gw_jobs_1 WHERE workspace_id = $1", workspaceID,
			).Scan(&userId)
			require.NoError(t, err)
			require.Equal(t, "anonymousId_header<<>>anonymousId_1<<>>identified_user_id", userId)
		})
	}
}

func sendEventsToGateway(t *testing.T, httpPort int, writeKey, sourceID, workspaceID string) {
	event := `{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
		"type": "identify",
		"eventOrderNo":"1",
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
	}`
	payload1 := strings.NewReader(event)
	sendEvent(t, httpPort, payload1, "identify", writeKey)
	internalBatchPayload := fmt.Sprintf(`[{
			"properties": {
				"messageID": "messageID",
				"routingKey": "anonymousId_header<<>>anonymousId_1<<>>identified_user_id",
				"workspaceID": %q,
				"userID": "identified_user_id",
				"sourceID": %q,
				"sourceJobRunID": "sourceJobRunID",
				"sourceTaskRunID": "sourceTaskRunID",
				"receivedAt": "2024-01-01T01:01:01.000000001Z",
				"requestIP": "1.1.1.1",
				"traceID": "traceID"
			},
			"payload": %s
			}]`, workspaceID, sourceID, event)
	payload2 := strings.NewReader(internalBatchPayload)
	sendInternalBatch(t, httpPort, payload2)
}

func sendEvent(t *testing.T, httpPort int, payload *strings.Reader, callType, writeKey string) {
	t.Helper()
	t.Logf("Sending %s Event", callType)

	var (
		httpClient = &http.Client{}
		method     = "POST"
		url        = fmt.Sprintf("http://localhost:%d/v1/%s", httpPort, callType)
	)

	req, err := http.NewRequest(method, url, payload)
	require.NoError(t, err)

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Basic %s", b64.StdEncoding.EncodeToString(
		[]byte(fmt.Sprintf("%s:", writeKey)),
	)))
	req.Header.Add("AnonymousId", "anonymousId_header")

	res, err := httpClient.Do(req)
	require.NoError(t, err)
	defer func() { httputil.CloseResponse(res) }()

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	t.Logf("Event Sent Successfully: (%s)", body)
}

func sendInternalBatch(t *testing.T, httpPort int, payload *strings.Reader) {
	t.Helper()
	t.Logf("Sending Internal Batch")

	var (
		httpClient = &http.Client{}
		method     = "POST"
		url        = fmt.Sprintf("http://localhost:%d/internal/v1/batch", httpPort)
	)

	req, err := http.NewRequest(method, url, payload)
	require.NoError(t, err)

	req.Header.Add("Content-Type", "application/json")

	res, err := httpClient.Do(req)
	require.NoError(t, err)
	defer func() { httputil.CloseResponse(res) }()

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	t.Logf("Internal Batch Sent Successfully: (%s)", body)
}
