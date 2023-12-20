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
	"slices"
	"strconv"
	"testing"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/rudderlabs/rudder-server/processor/transformer"

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

type testConfig struct {
	zipkinURL        string
	zipkinTracesURL  string
	postgresResource *resource.PostgresResource
	gwPort           int
	prometheusPort   int
}

func TestTracing(t *testing.T) {
	t.Run("gateway-processor-router tracing", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		tc := setup(t)

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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, tc.gwPort, tc.prometheusPort, tc.postgresResource, tc.zipkinURL, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", tc.gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 12

		err := sendEvents(eventsCount, "identify", "writekey-1", url)
		require.NoError(t, err)

		requireJobsCount(t, ctx, tc.postgresResource.DB, "gw", jobsdb.Succeeded.State, eventsCount)
		requireJobsCount(t, ctx, tc.postgresResource.DB, "rt", jobsdb.Succeeded.State, eventsCount)

		zipkinTraces := getZipkinTraces(t, tc.zipkinTracesURL)
		require.Len(t, zipkinTraces, eventsCount)
		for _, zipkinTrace := range zipkinTraces {
			requireTags(t, zipkinTrace, "gw.webrequesthandler", map[string]string{"reqType": "batch", "path": "/v1/batch", "sourceId": "source-1", "otel.library.name": "gateway"}, 1)
			requireTags(t, zipkinTrace, "proc.processjobsfordest", map[string]string{"sourceId": "source-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "proc.transformations", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "proc.store", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
			requireTags(t, zipkinTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
		}

		cancel()
		require.NoError(t, wg.Wait())
	})
	t.Run("zipkin down", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		tc := setup(t)
		zipkinDownURL := "http://localhost:1234/api/v2/spans"

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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, tc.gwPort, tc.prometheusPort, tc.postgresResource, zipkinDownURL, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", tc.gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 12

		err := sendEvents(eventsCount, "identify", "writekey-1", url)
		require.NoError(t, err)

		requireJobsCount(t, ctx, tc.postgresResource.DB, "gw", jobsdb.Succeeded.State, eventsCount)
		requireJobsCount(t, ctx, tc.postgresResource.DB, "rt", jobsdb.Succeeded.State, eventsCount)

		zipkinTraces := getZipkinTraces(t, tc.zipkinTracesURL)
		require.Empty(t, zipkinTraces)

		cancel()
		require.NoError(t, wg.Wait())
	})
	t.Run("gateway-processor-router with transformations", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		tc := setup(t)

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
									WithDefinitionConfigOption("transformAtV1", "router").
									Build()).
							Build()).
					Build()).
			Build()
		defer bcServer.Close()

		trServer := transformertest.NewBuilder().WithRouterTransform("WEBHOOK").Build()
		defer trServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			config.Set("Router.guaranteeUserEventOrder", false)
			config.Set("Router.WEBHOOK.enableBatching", false)

			err := runRudderServer(ctx, tc.gwPort, tc.prometheusPort, tc.postgresResource, tc.zipkinURL, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", tc.gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 12

		err := sendEvents(eventsCount, "identify", "writekey-1", url)
		require.NoError(t, err)

		requireJobsCount(t, ctx, tc.postgresResource.DB, "gw", jobsdb.Succeeded.State, eventsCount)
		requireJobsCount(t, ctx, tc.postgresResource.DB, "rt", jobsdb.Succeeded.State, eventsCount)

		zipkinTraces := getZipkinTraces(t, tc.zipkinTracesURL)
		require.Len(t, zipkinTraces, eventsCount)
		for _, zipkinTrace := range zipkinTraces {
			requireTags(t, zipkinTrace, "gw.webrequesthandler", map[string]string{"reqType": "batch", "path": "/v1/batch", "sourceId": "source-1", "otel.library.name": "gateway"}, 1)
			requireTags(t, zipkinTrace, "proc.processjobsfordest", map[string]string{"sourceId": "source-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "proc.transformations", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "proc.store", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
			requireTags(t, zipkinTrace, "rt.transform", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
			requireTags(t, zipkinTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
		}

		cancel()
		require.NoError(t, wg.Wait())
	})
	t.Run("gateway-processor-router with batch transformations", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		tc := setup(t)

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

		trServer := transformertest.NewBuilder().WithRouterTransform("WEBHOOK").Build()
		defer trServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			config.Set("Router.guaranteeUserEventOrder", false)
			config.Set("Router.WEBHOOK.enableBatching", true)

			err := runRudderServer(ctx, tc.gwPort, tc.prometheusPort, tc.postgresResource, tc.zipkinURL, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", tc.gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 12

		err := sendEvents(eventsCount, "identify", "writekey-1", url)
		require.NoError(t, err)

		requireJobsCount(t, ctx, tc.postgresResource.DB, "gw", jobsdb.Succeeded.State, eventsCount)
		requireJobsCount(t, ctx, tc.postgresResource.DB, "rt", jobsdb.Succeeded.State, eventsCount)

		zipkinTraces := getZipkinTraces(t, tc.zipkinTracesURL)
		require.Len(t, zipkinTraces, eventsCount)
		for _, zipkinTrace := range zipkinTraces {
			requireTags(t, zipkinTrace, "gw.webrequesthandler", map[string]string{"reqType": "batch", "path": "/v1/batch", "sourceId": "source-1", "otel.library.name": "gateway"}, 1)
			requireTags(t, zipkinTrace, "proc.processjobsfordest", map[string]string{"sourceId": "source-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "proc.transformations", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "proc.store", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
			requireTags(t, zipkinTrace, "rt.batchtransform", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
			requireTags(t, zipkinTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
		}

		cancel()
		require.NoError(t, wg.Wait())
	})
	t.Run("failed at gateway", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		tc := setup(t)

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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			config.Set("Gateway.maxReqSizeInKB", 0)

			err := runRudderServer(ctx, tc.gwPort, tc.prometheusPort, tc.postgresResource, tc.zipkinURL, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", tc.gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 12

		for i := 0; i < eventsCount; i++ {
			err := sendEvents(1, "identify", "writekey-1", url)
			require.Error(t, err)
		}

		zipkinTraces := getZipkinTraces(t, tc.zipkinTracesURL)
		require.Len(t, zipkinTraces, eventsCount)
		for _, zipkinTrace := range zipkinTraces {
			requireTags(t, zipkinTrace, "gw.webrequesthandler", map[string]string{"reqType": "batch", "path": "/v1/batch", "sourceId": "source-1", "otel.library.name": "gateway", "otel.status_code": "ERROR", "error": response.RequestBodyTooLarge}, 1)
		}

		cancel()
		require.NoError(t, wg.Wait())
	})
	t.Run("multiplexing in transformations", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		tc := setup(t)

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
									Build()).
							Build()).
					Build()).
			Build()
		defer bcServer.Close()

		trServer := transformertest.NewBuilder().
			WithUserTransformHandler(func(request []transformer.TransformerEvent) (response []transformer.TransformerResponse) {
				for i := range request {
					req := request[i]
					response = append(response, transformer.TransformerResponse{
						Metadata:   req.Metadata,
						Output:     req.Message,
						StatusCode: http.StatusOK,
					})
					response = append(response, transformer.TransformerResponse{
						Metadata:   req.Metadata,
						Output:     req.Message,
						StatusCode: http.StatusOK,
					})
				}
				return
			}).
			Build()
		defer trServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			config.Set("Router.jobQueryBatchSize", 1)

			err := runRudderServer(ctx, tc.gwPort, tc.prometheusPort, tc.postgresResource, tc.zipkinURL, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", tc.gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 3

		err := sendEvents(eventsCount, "identify", "writekey-1", url)
		require.NoError(t, err)

		requireJobsCount(t, ctx, tc.postgresResource.DB, "gw", jobsdb.Succeeded.State, eventsCount)
		requireJobsCount(t, ctx, tc.postgresResource.DB, "rt", jobsdb.Succeeded.State, 2*eventsCount)

		zipkinTraces := getZipkinTraces(t, tc.zipkinTracesURL)
		require.Len(t, zipkinTraces, eventsCount)
		for _, zipkinTrace := range zipkinTraces {
			requireTags(t, zipkinTrace, "gw.webrequesthandler", map[string]string{"reqType": "batch", "path": "/v1/batch", "sourceId": "source-1", "otel.library.name": "gateway"}, 1)
			requireTags(t, zipkinTrace, "proc.processjobsfordest", map[string]string{"sourceId": "source-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "proc.transformations", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "proc.store", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.library.name": "router"}, 2)  // 2 because of multiplexing
			requireTags(t, zipkinTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.library.name": "router"}, 2) // 2 because of multiplexing
		}

		cancel()
		require.NoError(t, wg.Wait())
	})
	t.Run("one source multiple destinations", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		tc := setup(t)

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
							WithConnection(
								backendconfigtest.NewDestinationBuilder("WEBHOOK").
									WithID("destination-2").
									Build()).
							WithConnection(
								backendconfigtest.NewDestinationBuilder("WEBHOOK").
									WithID("destination-3").
									Build()).
							Build()).
					Build()).
			Build()
		defer bcServer.Close()

		trServer := transformertest.NewBuilder().Build()
		defer trServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, tc.gwPort, tc.prometheusPort, tc.postgresResource, tc.zipkinURL, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", tc.gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 3

		err := sendEvents(eventsCount, "identify", "writekey-1", url)
		require.NoError(t, err)

		requireJobsCount(t, ctx, tc.postgresResource.DB, "gw", jobsdb.Succeeded.State, eventsCount)
		requireJobsCount(t, ctx, tc.postgresResource.DB, "rt", jobsdb.Succeeded.State, 3*eventsCount)

		zipkinTraces := getZipkinTraces(t, tc.zipkinTracesURL)
		require.Len(t, zipkinTraces, eventsCount)
		for _, zipkinTrace := range zipkinTraces {
			requireTags(t, zipkinTrace, "gw.webrequesthandler", map[string]string{"reqType": "batch", "path": "/v1/batch", "sourceId": "source-1", "otel.library.name": "gateway"}, 1)
			requireTags(t, zipkinTrace, "proc.processjobsfordest", map[string]string{"sourceId": "source-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "proc.transformations", map[string]string{"sourceId": "source-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "proc.store", map[string]string{"sourceId": "source-1", "otel.library.name": "processor"}, 1)
			requireTags(t, zipkinTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
			requireTags(t, zipkinTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-2", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
			requireTags(t, zipkinTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-3", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
			requireTags(t, zipkinTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
			requireTags(t, zipkinTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-2", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
			requireTags(t, zipkinTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-3", "destType": "WEBHOOK", "otel.library.name": "router"}, 1)
		}

		cancel()
		require.NoError(t, wg.Wait())
	})
}

func setup(t testing.TB) testConfig {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	zipkinResource, err := resource.SetupZipkin(pool, t)
	require.NoError(t, err)
	postgresResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)

	zipkinURL := "http://localhost:" + zipkinResource.Port + "/api/v2/spans"
	zipkinTracesURL := "http://localhost:" + zipkinResource.Port + "/api/v2/traces?limit=100&serviceName=" + app.EMBEDDED

	gwPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	prometheusPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	return testConfig{
		zipkinURL:        zipkinURL,
		zipkinTracesURL:  zipkinTracesURL,
		postgresResource: postgresResource,
		gwPort:           gwPort,
		prometheusPort:   prometheusPort,
	}
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

// nolint: unparam, bodyclose
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

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}

		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to send event to rudder server, status code: %d: %s", resp.StatusCode, string(b))
		}
		kithttputil.CloseResponse(resp)
	}
	return nil
}

// nolint: unparam
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
		  job_state = '%s'
		  AND parameters ->> 'traceparent' is not NULL;
	`,
		queue,
		state,
	)
	require.Eventuallyf(t, func() bool {
		var jobsCount int
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&jobsCount))
		t.Logf("%s %sJobCount: %d", queue, state, jobsCount)
		return jobsCount == expectedCount
	},
		10*time.Second,
		1*time.Second,
		"%d %s events should be in %s state", expectedCount, queue, state,
	)
}

func getZipkinTraces(t *testing.T, zipkinTracesURL string) [][]tracemodel.ZipkinTrace {
	t.Helper()

	getTracesReq, err := http.NewRequest(http.MethodGet, zipkinTracesURL, nil)
	require.NoError(t, err)

	spansBody := assert.RequireEventuallyStatusCode(t, http.StatusOK, getTracesReq)

	var zipkinTraces [][]tracemodel.ZipkinTrace
	require.NoError(t, json.Unmarshal([]byte(spansBody), &zipkinTraces))

	for _, zipkinTrace := range zipkinTraces {
		slices.SortFunc(zipkinTrace, func(a, b tracemodel.ZipkinTrace) int {
			return int(a.Timestamp - b.Timestamp)
		})
	}
	return zipkinTraces
}

func requireTags(t *testing.T, zipkinTraces []tracemodel.ZipkinTrace, traceName string, traceTags map[string]string, expectedCount int) {
	t.Helper()

	// Add common tags
	expectedTags := lo.Assign(traceTags, map[string]string{
		"service.name":           app.EMBEDDED,
		"telemetry.sdk.language": "go",
		"telemetry.sdk.name":     "opentelemetry",
		"telemetry.sdk.version":  otel.Version(),
	})
	filteredTraces := lo.Filter(zipkinTraces, func(trace tracemodel.ZipkinTrace, index int) bool {
		if trace.Name != traceName {
			return false
		}
		for key, value := range expectedTags {
			if trace.Tags[key] != value {
				return false
			}
		}
		return true
	})
	require.Len(t, filteredTraces, expectedCount)
}
