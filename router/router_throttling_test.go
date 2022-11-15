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
	"sync"
	"sync/atomic"
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
)

func Test_RouterThrottling(t *testing.T) {
	type webhookCount struct {
		count     *int64
		buckets   map[int64]int
		bucketsMu *sync.Mutex
		webhook   *httptest.Server
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

	createNewWebhook := func(t *testing.T) webhookCount {
		var (
			count     int64
			buckets   = make(map[int64]int)
			bucketsMu sync.Mutex
		)
		webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write([]byte(`{"message":"some transformed message"}`))
			atomic.AddInt64(&count, 1)
			require.NoError(t, err)

			bucketsMu.Lock()
			buckets[time.Now().Unix()]++
			bucketsMu.Unlock()
		}))
		t.Cleanup(webhook.Close)
		return webhookCount{
			count:     &count,
			buckets:   buckets,
			bucketsMu: &bucketsMu,
			webhook:   webhook,
		}
	}

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
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
		return
	})
	group.Go(func() (err error) {
		transformerContainer, err = destination.SetupTransformer(pool, t)
		return
	})
	require.NoError(t, group.Wait())

	writeKey := trand.String(27)
	workspaceID := trand.String(27)
	webhook1 := createNewWebhook(t)
	defer webhook1.webhook.Close()
	webhook2 := createNewWebhook(t)
	defer webhook2.webhook.Close()

	templateCtx := map[string]string{
		"webhookUrl1": webhook1.webhook.URL,
		"webhookUrl2": webhook2.webhook.URL,
		"writeKey":    writeKey,
		"workspaceId": workspaceID,
	}
	configJsonPath := workspaceConfig.CreateTempFile(t, "./testdata/throttlingTestTemplate.json", templateCtx)

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
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_FROM_FILE", "true")
	t.Setenv("RSERVER_BACKEND_CONFIG_CONFIG_JSONPATH", configJsonPath)
	t.Setenv("RSERVER_ROUTER_WEBHOOK_ISOLATE_DEST_ID", "true")
	t.Setenv("RSERVER_ROUTER_JOB_QUERY_BATCH_SIZE", "1000")
	t.Setenv("RSERVER_ROUTER_THROTTLER_TEST1_LIMIT", "20")
	t.Setenv("RSERVER_ROUTER_THROTTLER_TEST1_TIME_WINDOW", "1")
	t.Setenv("RSERVER_ROUTER_THROTTLER_TEST2_LIMIT", "50")
	t.Setenv("RSERVER_ROUTER_THROTTLER_TEST2_TIME_WINDOW", "1")

	if testing.Verbose() {
		t.Setenv("LOG_LEVEL", "DEBUG")
	}

	svcDone := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("server panicked: %v", r)
				close(svcDone)
			}
		}()
		r := runner.New(runner.ReleaseInfo{})
		exitCode := r.Run(ctx, []string{"eventorder-test-rudder-server"})
		if exitCode != 0 {
			t.Errorf("server exited with a non-0 exit code: %d", exitCode)
		} else {
			t.Log("server stopped")
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
	noOfEvents := 100
	batches := generatePayloads(t, noOfEvents)
	client := &http.Client{}
	for _, payload := range batches {
		url := fmt.Sprintf("http://localhost:%d/v1/batch", httpPort)
		req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
		require.NoError(t, err, "should be able to create a new request")
		req.SetBasicAuth(writeKey, "password")
		resp, err := client.Do(req)
		require.NoError(t, err, "should be able to send the request to gateway")
		require.Equal(t, http.StatusOK, resp.StatusCode)
		_ = resp.Body.Close()
	}

	require.Eventuallyf(t,
		func() bool {
			return atomic.LoadInt64(webhook1.count) == int64(noOfEvents) &&
				atomic.LoadInt64(webhook2.count) == int64(noOfEvents)
		},
		30*time.Second, 1*time.Second, "should have received all the events, got w1 %d and w2 %d",
		atomic.LoadInt64(webhook1.count), atomic.LoadInt64(webhook2.count),
	)

	require.Len(t, webhook1.buckets, noOfEvents/20)
	for _, rate := range webhook1.buckets {
		// throttling cost is 1 and rate is 20 per second, so we expect 20 in each bucket
		require.Equal(t, rate, 20)
	}
	require.Len(t, webhook2.buckets, noOfEvents/25)
	for _, rate := range webhook2.buckets {
		// throttling cost is 2 and rate is 50 per second, so we expect 25 in each bucket
		require.Equal(t, rate, 25)
	}
}
