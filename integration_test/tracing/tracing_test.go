package tracing

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/rudderlabs/rudder-go-kit/stats/testhelper/tracemodel"
	"github.com/rudderlabs/rudder-go-kit/testhelper/assert"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/jobsdb"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
)

func TestTracing(t *testing.T) {
	t.Run("gateway-processor-router tracing", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

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
									Build()).
							Build()).
					Build()).
			Build()
		defer bcServer.Close()

		trServer := transformertest.NewBuilder().Build()
		defer trServer.Close()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		postgresContainer, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		zipkin, err := resource.SetupZipkin(pool, t)
		require.NoError(t, err)

		zipkinURL := "http://localhost:" + zipkin.Port + "/api/v2/spans"
		zipkinTracesURL := "http://localhost:" + zipkin.Port + "/api/v2/traces?limit=100&serviceName=" + app.EMBEDDED

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		prometheusPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, prometheusPort, postgresContainer, zipkinURL, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 12
		startTIme := time.Now()

		err = sendEvents(eventsCount, "identify", "writekey-1", url)
		require.NoError(t, err)

		requireJobsCount(t, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
		requireJobsCount(t, postgresContainer.DB, "rt", jobsdb.Succeeded.State, eventsCount)

		getTracesReq, err := http.NewRequest(http.MethodGet, zipkinTracesURL, nil)
		require.NoError(t, err)

		spansBody := assert.RequireEventuallyStatusCode(t, http.StatusOK, getTracesReq)

		var zipkinTraces [][]tracemodel.ZipkinTrace
		require.NoError(t, json.Unmarshal([]byte(spansBody), &zipkinTraces))

		stages := []string{"gw.webrequesthandler", "proc.processjobsfordest", "proc.transformations", "proc.store", "rt.pickup"}

		require.Len(t, zipkinTraces, eventsCount)
		for _, zipkinTrace := range zipkinTraces {
			require.Len(t, zipkinTrace, len(stages))

			for i, trace := range zipkinTrace {
				require.Equal(t, stages[i], trace.Name)
				require.Greater(t, trace.Duration, int64(0))
				require.Greater(t, trace.Timestamp, startTIme.UnixMicro())
				require.NotEmpty(t, trace.ID)
				require.Equal(t, "go", trace.Tags["telemetry.sdk.language"])
				require.Equal(t, "opentelemetry", trace.Tags["telemetry.sdk.name"])
				require.Equal(t, otel.Version(), trace.Tags["telemetry.sdk.version"])
				require.Equal(t, app.EMBEDDED, trace.Tags["service.name"])
			}

			require.Empty(t, zipkinTrace[0].ParentID)

			for _, childTrace := range zipkinTrace[1:] {
				require.Equal(t, zipkinTrace[0].ID, childTrace.ParentID)
				require.Equal(t, zipkinTrace[0].TraceID, childTrace.TraceID)
			}
		}

		for _, trace := range lo.Filter(lo.Flatten(zipkinTraces), func(trace tracemodel.ZipkinTrace, _ int) bool {
			return trace.Name == "gw.webrequesthandler"
		}) {
			require.Equal(t, "batch", trace.Tags["reqType"])
			require.Equal(t, "/v1/batch", trace.Tags["path"])
			require.Equal(t, "source-1", trace.Tags["sourceId"])
			require.Equal(t, "gateway", trace.Tags["otel.library.name"])
		}
		for _, trace := range lo.Filter(lo.Flatten(zipkinTraces), func(trace tracemodel.ZipkinTrace, _ int) bool {
			return trace.Name == "proc.processjobsfordest"
		}) {
			require.Equal(t, "source-1", trace.Tags["sourceId"])
			require.Equal(t, "processor", trace.Tags["otel.library.name"])
		}
		for _, trace := range lo.Filter(lo.Flatten(zipkinTraces), func(trace tracemodel.ZipkinTrace, _ int) bool {
			return trace.Name == "proc.transformations"
		}) {
			require.Equal(t, "source-1", trace.Tags["sourceId"])
			require.Equal(t, "destination-1", trace.Tags["destinationId"])
			require.Equal(t, "processor", trace.Tags["otel.library.name"])
		}
		for _, trace := range lo.Filter(lo.Flatten(zipkinTraces), func(trace tracemodel.ZipkinTrace, _ int) bool {
			return trace.Name == "proc.store"
		}) {
			require.Equal(t, "source-1", trace.Tags["sourceId"])
			require.Equal(t, "destination-1", trace.Tags["destinationId"])
			require.Equal(t, "processor", trace.Tags["otel.library.name"])
		}
		for _, trace := range lo.Filter(lo.Flatten(zipkinTraces), func(trace tracemodel.ZipkinTrace, _ int) bool {
			return trace.Name == "rt.pickup"
		}) {
			require.Equal(t, "WEBHOOK", trace.Tags["customVal"])
			require.Equal(t, "source-1", trace.Tags["sourceId"])
			require.Equal(t, "destination-1", trace.Tags["destinationId"])
			require.Equal(t, "router", trace.Tags["otel.library.name"])
		}

		cancel()
		require.NoError(t, wg.Wait())
	})

	t.Run("status error at gateway", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID("source-1").
							WithWriteKey("writekey-1").
							Build()).
					Build()).
			Build()
		defer bcServer.Close()

		trServer := transformertest.NewBuilder().Build()
		defer trServer.Close()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		postgresContainer, err := resource.SetupPostgres(pool, t)
		require.NoError(t, err)
		zipkin, err := resource.SetupZipkin(pool, t)
		require.NoError(t, err)

		zipkinURL := "http://localhost:" + zipkin.Port + "/api/v2/spans"
		zipkinTracesURL := "http://localhost:" + zipkin.Port + "/api/v2/traces?limit=100&serviceName=" + app.EMBEDDED

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		prometheusPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			config.Set("Gateway.maxReqSizeInKB", 0)

			err := runRudderServer(ctx, gwPort, prometheusPort, postgresContainer, zipkinURL, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 1
		startTIme := time.Now()

		err = sendEvents(eventsCount, "identify", "writekey-1", url)
		require.Error(t, err)

		getTracesReq, err := http.NewRequest(http.MethodGet, zipkinTracesURL, nil)
		require.NoError(t, err)

		spansBody := assert.RequireEventuallyStatusCode(t, http.StatusOK, getTracesReq)

		var zipkinTraces [][]tracemodel.ZipkinTrace
		require.NoError(t, json.Unmarshal([]byte(spansBody), &zipkinTraces))

		stages := []string{"gw.webrequesthandler"}

		require.Len(t, zipkinTraces, eventsCount)
		for _, zipkinTrace := range zipkinTraces {
			require.Len(t, zipkinTrace, len(stages))

			for i, trace := range zipkinTrace {
				require.Equal(t, stages[i], trace.Name)
				require.Greater(t, trace.Duration, int64(0))
				require.Greater(t, trace.Timestamp, startTIme.UnixMicro())
				require.NotEmpty(t, trace.ID)
				require.Equal(t, "go", trace.Tags["telemetry.sdk.language"])
				require.Equal(t, "opentelemetry", trace.Tags["telemetry.sdk.name"])
				require.Equal(t, otel.Version(), trace.Tags["telemetry.sdk.version"])
				require.Equal(t, app.EMBEDDED, trace.Tags["service.name"])
			}
		}

		for _, trace := range lo.Filter(lo.Flatten(zipkinTraces), func(trace tracemodel.ZipkinTrace, _ int) bool {
			return trace.Name == "gw.webrequesthandler"
		}) {
			require.Equal(t, "batch", trace.Tags["reqType"])
			require.Equal(t, "/v1/batch", trace.Tags["path"])
			require.Equal(t, "source-1", trace.Tags["sourceId"])
			require.Equal(t, "gateway", trace.Tags["otel.library.name"])
			require.Equal(t, "ERROR", trace.Tags["otel.status_code"])
			require.Equal(t, response.RequestBodyTooLarge, trace.Tags["error"])
		}

		cancel()
		require.NoError(t, wg.Wait())
	})
	t.Run("traceParent should be present in gateway", func(t *testing.T) {})
	t.Run("traceParent should be present in router", func(t *testing.T) {})
	t.Run("traceParent should be present in destination", func(t *testing.T) {})
	t.Run("multiplexing in user transformations", func(t *testing.T) {})
	t.Run("multiplexing in destination transformations", func(t *testing.T) {})
}

func runRudderServer(
	ctx context.Context,
	port int,
	prometheusPort int,
	postgresContainer *resource.PostgresResource,
	zipkinURL, cbURL, transformerURL, tmpDir string,
) (err error) {
	config.Set("CONFIG_BACKEND_URL", cbURL)
	config.Set("WORKSPACE_TOKEN", "token")
	config.Set("DB.port", postgresContainer.Port)
	config.Set("DB.user", postgresContainer.User)
	config.Set("DB.name", postgresContainer.Database)
	config.Set("DB.password", postgresContainer.Password)
	config.Set("DEST_TRANSFORM_URL", transformerURL)

	config.Set("Warehouse.mode", "off")
	config.Set("DestinationDebugger.disableEventDeliveryStatusUploads", true)
	config.Set("SourceDebugger.disableEventUploads", true)
	config.Set("TransformationDebugger.disableTransformationStatusUploads", true)
	config.Set("JobsDB.backup.enabled", false)
	config.Set("JobsDB.migrateDSLoopSleepDuration", "60m")
	config.Set("archival.Enabled", false)
	config.Set("Reporting.syncer.enabled", false)
	config.Set("Reporting.errorIndexReporting.enabled", true)
	config.Set("Reporting.errorIndexReporting.syncer.enabled", true)
	config.Set("Reporting.errorIndexReporting.SleepDuration", "1s")
	config.Set("Reporting.errorIndexReporting.minWorkerSleep", "1s")
	config.Set("Reporting.errorIndexReporting.uploadFrequency", "1s")
	config.Set("BatchRouter.mainLoopFreq", "1s")
	config.Set("BatchRouter.uploadFreq", "1s")
	config.Set("Gateway.webPort", strconv.Itoa(port))
	config.Set("RUDDER_TMPDIR", os.TempDir())
	config.Set("recovery.storagePath", path.Join(tmpDir, "/recovery_data.json"))
	config.Set("recovery.enabled", false)
	config.Set("Profiler.Enabled", false)
	config.Set("Gateway.enableSuppressUserFeature", false)

	config.Set("enableStats", true)
	config.Set("RuntimeStats.enabled", false)
	config.Set("OpenTelemetry.enabled", true)
	config.Set("OpenTelemetry.traces.endpoint", zipkinURL)
	config.Set("OpenTelemetry.traces.samplingRate", 1.0)
	config.Set("OpenTelemetry.traces.withSyncer", true)
	config.Set("OpenTelemetry.traces.withZipkin", true)
	config.Set("OpenTelemetry.metrics.prometheus.enabled", true)
	config.Set("OpenTelemetry.metrics.prometheus.port", prometheusPort)
	config.Set("OpenTelemetry.metrics.exportInterval", 10*time.Millisecond)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "DUMMY"})
	c := r.Run(ctx, []string{"rudder-tracing"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return
}

// nolint: unparam
func requireJobsCount(
	t *testing.T,
	db *sql.DB,
	queue, state string,
	expectedCount int,
) {
	t.Helper()

	require.Eventually(t, func() bool {
		var jobsCount int
		require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM unionjobsdbmetadata('%s',1) WHERE job_state = '%s' AND parameters->>'traceparent' is not NULL;`, queue, state)).Scan(&jobsCount))
		t.Logf("%s %sJobCount: %d", queue, state, jobsCount)
		return jobsCount == expectedCount
	},
		20*time.Second,
		1*time.Second,
		fmt.Sprintf("%d %s events should be in %s state", expectedCount, queue, state),
	)
}

// nolint: unparam
func sendEvents(
	num int,
	eventType, writeKey,
	url string,
) error {
	for i := 0; i < num; i++ {
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

		resp, err := (&http.Client{}).Do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to send event to rudder server, status code: %d: %s", resp.StatusCode, string(b))
		}
		func() { kithttputil.CloseResponse(resp) }()
	}
	return nil
}
