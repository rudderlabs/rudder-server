package router_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	trand "github.com/rudderlabs/rudder-server/testhelper/rand"
	"github.com/rudderlabs/rudder-server/testhelper/workspaceConfig"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

func Test_RouterDestIsolation(t *testing.T) {
	type webhookCount struct {
		count   *uint64
		webhook *httptest.Server
	}

	generatePayloads := func(t *testing.T, count int) [][]byte {
		payloads := make([][]byte, count)
		for i := 0; i < count; i++ {
			testBody, err := os.ReadFile("./../scripts/batch.json")
			require.NoError(t, err)
			payloads[i] = testBody
		}
		return payloads
	}

	createNewWebhook := func(t *testing.T, statusCode int) webhookCount {
		var count uint64 = 0
		webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(statusCode)
			_, err := w.Write([]byte(`{"message": "some transformed message"}`))

			atomic.AddUint64(&count, 1)
			require.NoError(t, err)
		}))
		t.Cleanup(webhook.Close)
		return webhookCount{
			&count,
			webhook,
		}
	}

	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	var (
		group                errgroup.Group
		postgresContainer    *destination.PostgresResource
		transformerContainer *destination.TransformerResource
	)
	group.Go(func() (err error) {
		postgresContainer, err = destination.SetupPostgres(pool, t)
		return err
	})
	group.Go(func() (err error) {
		transformerContainer, err = destination.SetupTransformer(pool, t)
		return err
	})
	require.NoError(t, group.Wait())

	writeKey := trand.String(27)
	workspaceID := trand.String(27)
	webhook1 := createNewWebhook(t, 500)
	defer webhook1.webhook.Close()
	webhook2 := createNewWebhook(t, 200)
	defer webhook2.webhook.Close()

	templateCtx := map[string]any{
		"webhookUrl1": webhook1.webhook.URL,
		"webhookUrl2": webhook2.webhook.URL,
		"writeKey":    writeKey,
		"workspaceId": workspaceID,
	}
	configJsonPath := workspaceConfig.CreateTempFile(t, "testdata/destIdIsolationTestTemplate.json", templateCtx)

	httpPort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	debugPort, err := testhelper.GetFreePort()
	require.NoError(t, err)
	rudderTmpDir, err := os.MkdirTemp("", "rudder_server_*_test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(rudderTmpDir) })

	t.Setenv("JOBS_DB_PORT", postgresContainer.Port)
	t.Setenv("JOBS_DB_USER", postgresContainer.User)
	t.Setenv("JOBS_DB_DB_NAME", postgresContainer.Database)
	t.Setenv("JOBS_DB_PASSWORD", postgresContainer.Password)
	t.Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	t.Setenv("RSERVER_PROFILER_PORT", strconv.Itoa(debugPort))
	t.Setenv("RSERVER_WAREHOUSE_MODE", "off")
	t.Setenv("RSERVER_ENABLE_STATS", "false")
	t.Setenv("RSERVER_JOBS_DB_BACKUP_ENABLED", "false")
	t.Setenv("RUDDER_TMPDIR", rudderTmpDir)
	t.Setenv("DEST_TRANSFORM_URL", transformerContainer.TransformURL)
	t.Setenv("RSERVER_MODE", "normal")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", configJsonPath)
	t.Setenv("RSERVER_ROUTER_WEBHOOK_ISOLATE_DEST_ID", "true")
	t.Setenv("RSERVER_ROUTER_JOB_QUERY_BATCH_SIZE", "10")

	if testing.Verbose() {
		t.Setenv("LOG_LEVEL", "DEBUG")
	}

	svcDone := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("server panicked: %v", r)
				close(svcDone)
			}
		}()
		r := runner.New(runner.ReleaseInfo{})
		c := r.Run(ctx, []string{"eventorder-test-rudder-server"})
		t.Logf("server stopped: %d", c)
		if c != 0 {
			t.Errorf("server exited with a non-0 exit code: %d", c)
		}
		close(svcDone)
	}()
	t.Cleanup(func() { <-svcDone })

	healthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	health.WaitUntilReady(ctx, t,
		healthEndpoint,
		200*time.Second,
		100*time.Millisecond,
		t.Name(),
	)
	batches := generatePayloads(t, 100)
	client := &http.Client{}
	for _, payload := range batches {
		url := fmt.Sprintf("http://localhost:%d/v1/batch", httpPort)
		req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
		require.NoError(t, err, "should be able to create a new request")
		req.SetBasicAuth(writeKey, "password")
		resp, err := client.Do(req)
		require.NoError(t, err, "should be able to send the request to gateway")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		func() { httputil.CloseResponse(resp) }()
	}
	require.Eventually(t, func() bool {
		return atomic.LoadUint64(webhook2.count) == 100 && atomic.LoadUint64(webhook1.count) < 100
	}, 30*time.Second, 1*time.Second, "should have received all the events")
}
