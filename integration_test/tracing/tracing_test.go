package tracing

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/jaeger"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/app"
	"github.com/rudderlabs/rudder-server/gateway/response"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
)

type testConfig struct {
	jaegerResource   *jaeger.Resource
	postgresResource *postgres.Resource
	gwPort           int
	prometheusPort   int
}

func TestTracing(t *testing.T) {
	t.Setenv("RSERVER_ROUTER_BATCHING_SUPPORTED_DESTINATIONS", "WEBHOOK")
	t.Run("gateway-processor-router tracing", func(t *testing.T) {
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
			err := runRudderServer(t, ctx, cancel, tc.gwPort, tc.prometheusPort, tc.postgresResource, tc.jaegerResource.OTLPEndpoint, bcServer.URL, trServer.URL, t.TempDir())
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

		allTraces, err := tc.jaegerResource.GetTraces(app.EMBEDDED)
		require.NoError(t, err)
		jaegerTraces := filterTracesByOperation(allTraces, "gw.webRequestHandler")
		require.Len(t, jaegerTraces, eventsCount)
		for _, jaegerTrace := range jaegerTraces {
			requireTags(t, jaegerTrace, "gw.webRequestHandler", map[string]string{"reqType": "batch", "path": "/v1/batch", "sourceId": "source-1", "otel.scope.name": "gateway"}, 1)
			requireTags(t, jaegerTrace, "proc.preprocessStage", map[string]string{"sourceId": "source-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.user_transformations", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.destination_transformations", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.store", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
			requireTags(t, jaegerTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
		}

		cancel()
		require.NoError(t, wg.Wait())
	})
	t.Run("jaeger down", func(t *testing.T) {
		tc := setup(t)
		jaegerDownURL := "localhost:1234"

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
			err := runRudderServer(t, ctx, cancel, tc.gwPort, tc.prometheusPort, tc.postgresResource, jaegerDownURL, bcServer.URL, trServer.URL, t.TempDir())
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

		jaegerTraces, err := tc.jaegerResource.GetTraces(app.EMBEDDED)
		require.NoError(t, err)
		require.Empty(t, jaegerTraces)

		cancel()
		require.NoError(t, wg.Wait())
	})
	t.Run("gateway-processor-router with transformations", func(t *testing.T) {
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
			t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Router.guaranteeUserEventOrder"), "false")
			t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Router.WEBHOOK.enableBatching"), "false")

			err := runRudderServer(t, ctx, cancel, tc.gwPort, tc.prometheusPort, tc.postgresResource, tc.jaegerResource.OTLPEndpoint, bcServer.URL, trServer.URL, t.TempDir())
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

		allTraces, err := tc.jaegerResource.GetTraces(app.EMBEDDED)
		require.NoError(t, err)
		jaegerTraces := filterTracesByOperation(allTraces, "gw.webRequestHandler")
		require.Len(t, jaegerTraces, eventsCount)
		for _, jaegerTrace := range jaegerTraces {
			requireTags(t, jaegerTrace, "gw.webRequestHandler", map[string]string{"reqType": "batch", "path": "/v1/batch", "sourceId": "source-1", "otel.scope.name": "gateway"}, 1)
			requireTags(t, jaegerTrace, "proc.preprocessStage", map[string]string{"sourceId": "source-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.user_transformations", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.destination_transformations", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.store", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
			requireTags(t, jaegerTrace, "rt.transform", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
			requireTags(t, jaegerTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
		}

		cancel()
		require.NoError(t, wg.Wait())
	})
	t.Run("gateway-processor-router with batch transformations", func(t *testing.T) {
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
			t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Router.guaranteeUserEventOrder"), "false")
			t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Router.WEBHOOK.enableBatching"), "true")

			err := runRudderServer(t, ctx, cancel, tc.gwPort, tc.prometheusPort, tc.postgresResource, tc.jaegerResource.OTLPEndpoint, bcServer.URL, trServer.URL, t.TempDir())
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

		allTraces, err := tc.jaegerResource.GetTraces(app.EMBEDDED)
		require.NoError(t, err)
		jaegerTraces := filterTracesByOperation(allTraces, "gw.webRequestHandler")
		require.Len(t, jaegerTraces, eventsCount)
		for _, jaegerTrace := range jaegerTraces {
			requireTags(t, jaegerTrace, "gw.webRequestHandler", map[string]string{"reqType": "batch", "path": "/v1/batch", "sourceId": "source-1", "otel.scope.name": "gateway"}, 1)
			requireTags(t, jaegerTrace, "proc.preprocessStage", map[string]string{"sourceId": "source-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.user_transformations", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.destination_transformations", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.store", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
			requireTags(t, jaegerTrace, "rt.batchTransform", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
			requireTags(t, jaegerTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
		}

		cancel()
		require.NoError(t, wg.Wait())
	})
	t.Run("failed at gateway", func(t *testing.T) {
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
			t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Gateway.maxReqSizeInKB"), "0")

			err := runRudderServer(t, ctx, cancel, tc.gwPort, tc.prometheusPort, tc.postgresResource, tc.jaegerResource.OTLPEndpoint, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", tc.gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 12

		for range eventsCount {
			err := sendEvents(1, "identify", "writekey-1", url)
			require.Error(t, err)
		}

		allTraces, err := tc.jaegerResource.GetTraces(app.EMBEDDED)
		require.NoError(t, err)
		jaegerTraces := filterTracesByOperation(allTraces, "gw.webRequestHandler")
		require.Len(t, jaegerTraces, eventsCount)
		for _, jaegerTrace := range jaegerTraces {
			requireTags(t, jaegerTrace, "gw.webRequestHandler", map[string]string{"reqType": "batch", "path": "/v1/batch", "sourceId": "source-1", "otel.scope.name": "gateway", "otel.status_code": "ERROR", "otel.status_description": response.RequestBodyTooLarge}, 1)
		}

		cancel()
		require.NoError(t, wg.Wait())
	})
	t.Run("multiplexing in processor transformations", func(t *testing.T) {
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
			WithUserTransformHandler(func(request []types.TransformerEvent) (response []types.TransformerResponse) {
				for i := range request {
					req := request[i]
					response = append(response, types.TransformerResponse{
						Metadata:   req.Metadata,
						Output:     req.Message,
						StatusCode: http.StatusOK,
					})
					response = append(response, types.TransformerResponse{
						Metadata:   req.Metadata,
						Output:     req.Message,
						StatusCode: http.StatusOK,
					})
				}
				return response
			}).
			Build()
		defer trServer.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "Router.jobQueryBatchSize"), "1")

			err := runRudderServer(t, ctx, cancel, tc.gwPort, tc.prometheusPort, tc.postgresResource, tc.jaegerResource.OTLPEndpoint, bcServer.URL, trServer.URL, t.TempDir())
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

		allTraces, err := tc.jaegerResource.GetTraces(app.EMBEDDED)
		require.NoError(t, err)
		jaegerTraces := filterTracesByOperation(allTraces, "gw.webRequestHandler")
		require.Len(t, jaegerTraces, eventsCount)
		for _, jaegerTrace := range jaegerTraces {
			requireTags(t, jaegerTrace, "gw.webRequestHandler", map[string]string{"reqType": "batch", "path": "/v1/batch", "sourceId": "source-1", "otel.scope.name": "gateway"}, 1)
			requireTags(t, jaegerTrace, "proc.preprocessStage", map[string]string{"sourceId": "source-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.user_transformations", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.destination_transformations", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.store", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.scope.name": "router"}, 2)  // 2 because of multiplexing
			requireTags(t, jaegerTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.scope.name": "router"}, 2) // 2 because of multiplexing
		}

		cancel()
		require.NoError(t, wg.Wait())
	})
	t.Run("one source multiple destinations", func(t *testing.T) {
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
			err := runRudderServer(t, ctx, cancel, tc.gwPort, tc.prometheusPort, tc.postgresResource, tc.jaegerResource.OTLPEndpoint, bcServer.URL, trServer.URL, t.TempDir())
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

		allTraces, err := tc.jaegerResource.GetTraces(app.EMBEDDED)
		require.NoError(t, err)
		jaegerTraces := filterTracesByOperation(allTraces, "gw.webRequestHandler")
		require.Len(t, jaegerTraces, eventsCount)
		for _, jaegerTrace := range jaegerTraces {
			requireTags(t, jaegerTrace, "gw.webRequestHandler", map[string]string{"reqType": "batch", "path": "/v1/batch", "sourceId": "source-1"}, 1)
			requireTags(t, jaegerTrace, "proc.preprocessStage", map[string]string{"sourceId": "source-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.user_transformations", map[string]string{"sourceId": "source-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.destination_transformations", map[string]string{"sourceId": "source-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "proc.store", map[string]string{"sourceId": "source-1", "otel.scope.name": "processor"}, 1)
			requireTags(t, jaegerTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
			requireTags(t, jaegerTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-2", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
			requireTags(t, jaegerTrace, "rt.pickup", map[string]string{"sourceId": "source-1", "destinationId": "destination-3", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
			requireTags(t, jaegerTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-1", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
			requireTags(t, jaegerTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-2", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
			requireTags(t, jaegerTrace, "rt.process", map[string]string{"sourceId": "source-1", "destinationId": "destination-3", "destType": "WEBHOOK", "otel.scope.name": "router"}, 1)
		}

		cancel()
		require.NoError(t, wg.Wait())
	})
}

func setup(t testing.TB) testConfig {
	t.Helper()

	config.Reset()
	t.Cleanup(config.Reset)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	jaegerResource, err := jaeger.Setup(pool, t)
	require.NoError(t, err)
	postgresResource, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	gwPort, err := kithelper.GetFreePort()
	require.NoError(t, err)
	prometheusPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	return testConfig{
		jaegerResource:   jaegerResource,
		postgresResource: postgresResource,
		gwPort:           gwPort,
		prometheusPort:   prometheusPort,
	}
}

func runRudderServer(
	t testing.TB,
	ctx context.Context,
	cancel context.CancelFunc,
	port int,
	prometheusPort int,
	postgresContainer *postgres.Resource,
	otlpEndpoint, cbURL, transformerURL, tmpDir string,
) (err error) {
	t.Setenv("CONFIG_BACKEND_URL", cbURL)
	t.Setenv("WORKSPACE_TOKEN", "token")
	t.Setenv("DEST_TRANSFORM_URL", transformerURL)

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

	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "enableStats"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "RuntimeStats.enabled"), "false")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "OpenTelemetry.enabled"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "OpenTelemetry.traces.endpoint"), otlpEndpoint)
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "OpenTelemetry.traces.samplingRate"), "1.0")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "OpenTelemetry.traces.withSyncer"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "OpenTelemetry.traces.withOTLPHTTP"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "OpenTelemetry.metrics.prometheus.enabled"), "true")
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "OpenTelemetry.metrics.prometheus.port"), strconv.Itoa(prometheusPort))
	t.Setenv(config.ConfigKeyToEnv(config.DefaultEnvPrefix, "OpenTelemetry.metrics.exportInterval"), "10ms")

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "DUMMY"})
	c := r.Run(ctx, cancel, []string{"rudder-tracing"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return err
}

// nolint: unparam, bodyclose
func sendEvents(
	num int,
	eventType, writeKey,
	url string,
) error {
	for range num {
		payload := fmt.Appendf(nil, `
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
		)
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
		30*time.Second,
		1*time.Second,
		"%d %s events should be in %s state", expectedCount, queue, state,
	)
}

// filterTracesByOperation returns only traces that contain at least one span with the given operation name.
func filterTracesByOperation(traces []jaeger.Trace, operationName string) []jaeger.Trace { // nolint: unparam
	return lo.Filter(traces, func(trace jaeger.Trace, _ int) bool {
		return lo.ContainsBy(trace.Spans, func(span jaeger.Span) bool {
			return span.OperationName == operationName
		})
	})
}

func requireTags(t *testing.T, jaegerTrace jaeger.Trace, traceName string, traceTags map[string]string, expectedCount int) {
	t.Helper()

	// Add common tags
	expectedTags := lo.Assign(traceTags, map[string]string{
		"service.name":           app.EMBEDDED,
		"telemetry.sdk.language": "go",
		"telemetry.sdk.name":     "opentelemetry",
		"telemetry.sdk.version":  stats.OtelVersion(),
	})
	filteredTraces := lo.Filter(jaegerTrace.Spans, func(span jaeger.Span, index int) bool {
		if span.OperationName != traceName {
			return false
		}

		// Collect all tags: span tags + process tags (resource attributes)
		allTags := span.Tags
		if proc, ok := jaegerTrace.Processes[span.ProcessID]; ok {
			allTags = append(allTags, proc.Tags...)
			// service.name is a top-level Process field, not a tag
			allTags = append(allTags, jaeger.Tag{Key: "service.name", Type: "string", Value: proc.ServiceName})
		}

		for key, value := range expectedTags {
			tag, _ := lo.Find(allTags, func(tag jaeger.Tag) bool {
				return tag.Key == key
			})
			if fmt.Sprint(tag.Value) != value {
				return false
			}
		}
		return true
	})
	require.Len(t, filteredTraces, expectedCount)
}
