package multi_tenant_test

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
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"

	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	thEtcd "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/etcd"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	transformertest "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/transformer"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/app"
	th "github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	whUtil "github.com/rudderlabs/rudder-server/testhelper/webhook"
	"github.com/rudderlabs/rudder-server/utils/httputil"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

func TestMultiTenant(t *testing.T) {
	for _, appType := range []string{app.GATEWAY, app.EMBEDDED} {
		t.Run(appType, func(t *testing.T) {
			testMultiTenantByAppType(t, appType)
		})
	}
}

func requireAuth(t *testing.T, secret string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, _, ok := r.BasicAuth()
		require.True(t, ok, "Auth should be present")
		require.Equalf(t, secret, u,
			"Expected HTTP basic authentication to be %q, got %q instead",
			secret, u)

		handler(w, r)
	}
}

func testMultiTenantByAppType(t *testing.T, appType string) {
	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		group                errgroup.Group
		etcdContainer        *thEtcd.Resource
		postgresContainer    *postgres.Resource
		transformerContainer *transformertest.Resource
		serverInstanceID     = "1"
		workspaceNamespace   = "test-workspace-namespace"

		hostedServiceSecret = "service-secret"
	)

	group.Go(func() (err error) {
		postgresContainer, err = postgres.Setup(pool, t)
		return err
	})
	group.Go(func() (err error) {
		etcdContainer, err = thEtcd.Setup(pool, t)
		return err
	})
	group.Go(func() (err error) {
		transformerContainer, err = transformertest.Setup(pool, t)
		return err
	})
	require.NoError(t, group.Wait())

	webhook := whUtil.NewRecorder()
	t.Cleanup(webhook.Close)

	writeKey := rand.String(27)
	workspaceID := rand.String(27)
	marshalledWorkspaces := th.FillTemplateAndReturn(t, "testdata/mtGatewayTest01.json", map[string]string{
		"writeKey":    writeKey,
		"workspaceId": workspaceID,
		"webhookUrl":  webhook.Server.URL,
	})
	require.NoError(t, err)

	beConfigRouter := chi.NewRouter()
	if testing.Verbose() {
		beConfigRouter.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Logf("BackendConfig server call: %+v", r)
				next.ServeHTTP(w, r)
			})
		})
	}

	beConfigRouter.Get("/data-plane/v1/namespaces/"+workspaceNamespace+"/config", requireAuth(t, hostedServiceSecret, func(w http.ResponseWriter, r *http.Request) {
		n, err := w.Write(marshalledWorkspaces.Bytes())
		require.NoError(t, err)
		require.Equal(t, marshalledWorkspaces.Len(), n)
	}))
	beConfigRouter.Post("/data-plane/v1/namespaces/"+workspaceNamespace+"/settings", requireAuth(t, hostedServiceSecret, func(w http.ResponseWriter, r *http.Request) {
		expectBody, err := os.ReadFile("testdata/expected_features.json")
		require.NoError(t, err)

		actualBody, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		require.JSONEq(t, string(expectBody), string(actualBody))

		w.WriteHeader(http.StatusNoContent)
	}))
	beConfigRouter.NotFound(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.FailNowf(t, "backend config", "unexpected request to backend config, not found: %+v", r.URL)
		w.WriteHeader(http.StatusNotFound)
	}))

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

	var (
		done        = make(chan struct{})
		releaseName = t.Name() + "_" + appType
	)
	go func() {
		defer close(done)
		defer cancel()
		cmd := exec.CommandContext(ctx, "go", "run", "../../main.go")
		cmd.Env = append(os.Environ(),
			"APP_TYPE="+appType,
			"INSTANCE_ID=rudderstackmt-v0-rudderstack-1",
			"RELEASE_NAME="+releaseName,
			"ETCD_HOSTS="+etcdContainer.Hosts[0],
			"JOBS_DB_HOST="+postgresContainer.Host,
			"JOBS_DB_PORT="+postgresContainer.Port,
			"JOBS_DB_USER="+postgresContainer.User,
			"JOBS_DB_DB_NAME="+postgresContainer.Database,
			"JOBS_DB_PASSWORD="+postgresContainer.Password,
			"CONFIG_BACKEND_URL="+backendConfigSrv.URL,
			"RSERVER_GATEWAY_WEB_PORT="+strconv.Itoa(httpPort),
			"RSERVER_GATEWAY_ADMIN_WEB_PORT="+strconv.Itoa(httpAdminPort),
			"RSERVER_PROFILER_PORT="+strconv.Itoa(debugPort),
			"RSERVER_ENABLE_STATS=false",
			"RSERVER_BACKEND_CONFIG_USE_HOSTED_BACKEND_CONFIG=false",
			"RUDDER_TMPDIR="+rudderTmpDir,
			"DEPLOYMENT_TYPE="+string(deployment.MultiTenantType),
			"DEST_TRANSFORM_URL="+transformerContainer.TransformerURL,
			"HOSTED_SERVICE_SECRET="+hostedServiceSecret,
			"WORKSPACE_NAMESPACE="+workspaceNamespace,
			"RSERVER_WAREHOUSE_MODE=off",
		)
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

	// The Gateway will not become healthy until we trigger a valid configuration via ETCD
	healthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	resp, err := http.Get(healthEndpoint)
	require.ErrorContains(t, err, "connection refused")
	require.Nil(t, resp)
	if err == nil {
		defer func() { httputil.CloseResponse(resp) }()
	}

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
		sendEventsToGateway(t, httpPort, writeKey)
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
		// Triger normal mode for the processor to start
		t.Run("switch to normal mode", func(t *testing.T) {
			serverModeReqKey := getETCDServerModeReqKey(releaseName, serverInstanceID)
			t.Logf("Server mode ETCD key: %s", serverModeReqKey)

			_, err := etcdContainer.Client.Put(ctx, serverModeReqKey, `{"mode":"NORMAL","ack_key":"test-ack/normal"}`)
			require.NoError(t, err)
			t.Log("Triggering degraded mode")

			select {
			case ack := <-etcdContainer.Client.Watch(ctx, "test-ack/", clientv3.WithPrefix(), clientv3.WithRev(1)):
				require.Len(t, ack.Events, 1)
				require.Equal(t, "test-ack/normal", string(ack.Events[0].Kv.Key))
				require.Equal(t, `{"status":"NORMAL"}`, string(ack.Events[0].Kv.Value))
			case <-time.After(60 * time.Second):
				t.Fatal("Timeout waiting for server-mode test-ack")
			}
			sendEventsToGateway(t, httpPort, writeKey)
			t.Cleanup(cleanupGwJobs)
			t.Logf("Message sent to gateway")
			require.Eventually(t, func() bool {
				pgcont := postgresContainer
				_ = pgcont.Port
				return len(webhook.Requests()) == 1
			}, 60*time.Second, 100*time.Millisecond)
		})

		// Trigger degraded mode, the Gateway should still work
		t.Run("switch to degraded mode", func(t *testing.T) {
			serverModeReqKey := getETCDServerModeReqKey(releaseName, serverInstanceID)
			t.Logf("Server mode ETCD key: %s", serverModeReqKey)

			_, err := etcdContainer.Client.Put(ctx, serverModeReqKey, `{"mode":"DEGRADED","ack_key":"test-ack-2/2"}`)
			require.NoError(t, err)
			t.Log("Triggering degraded mode")

			select {
			case ack := <-etcdContainer.Client.Watch(ctx, "test-ack-2/", clientv3.WithPrefix(), clientv3.WithRev(1)):
				require.Len(t, ack.Events, 1)
				require.Equal(t, "test-ack-2/2", string(ack.Events[0].Kv.Key))
				require.Equal(t, `{"status":"DEGRADED"}`, string(ack.Events[0].Kv.Value))
			case <-time.After(60 * time.Second):
				t.Fatal("Timeout waiting for server-mode test-ack-2")
			}

			sendEventsToGateway(t, httpPort, writeKey)
			t.Cleanup(cleanupGwJobs)
			var count int
			err = postgresContainer.DB.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM gw_jobs_1 WHERE workspace_id = $1", workspaceID,
			).Scan(&count)
			require.NoError(t, err)
			require.Equal(t, 1, count)
		})
	}
}

func getETCDServerModeReqKey(releaseName, instance string) string {
	return fmt.Sprintf("/%s/SERVER/%s/MODE", releaseName, instance)
}

func sendEventsToGateway(t *testing.T, httpPort int, writeKey string) {
	payload1 := strings.NewReader(`{
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
	}`)
	sendEvent(t, httpPort, payload1, "identify", writeKey)
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
