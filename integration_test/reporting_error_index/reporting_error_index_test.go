package reportingfailedmessages_test

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
	"strings"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/minio"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
)

func TestReportingErrorIndex(t *testing.T) {
	t.Run("Events failed during tracking plan validation stage", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID("source-1").
							WithWriteKey("writekey-1").
							WithTrackingPlan("trackingplan-1", 1).
							WithConnection(
								backendconfigtest.NewDestinationBuilder("WEBHOOK").
									WithID("destination-1").
									Build()).
							Build()).
					Build()).
			Build()
		defer bcServer.Close()

		trServer := transformertest.NewBuilder().
			WithTrackingPlanHandler(
				transformertest.ViolationErrorTransformerHandler(
					http.StatusBadRequest,
					"tracking plan validation failed",
					[]transformer.ValidationError{{Type: "Datatype-Mismatch", Message: "must be number"}},
				),
			).
			Build()
		defer trServer.Close()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		minioResource, err := minio.Setup(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, minioResource, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 12

		err = sendEvents(eventsCount, "identify", "writekey-1", url)
		require.NoError(t, err)

		requireJobsCount(t, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
		requireJobsCount(t, postgresContainer.DB, "err_idx", jobsdb.Succeeded.State, eventsCount)
		requireMessagesCount(t, ctx, minioResource, eventsCount, []lo.Tuple2[string, string]{
			{A: "source_id", B: "source-1"},
			{A: "tracking_plan_id", B: "trackingplan-1"},
			{A: "failed_stage", B: "tracking_plan_validator"},
			{A: "event_type", B: "identify"},
		}...)

		cancel()
		require.NoError(t, wg.Wait())
	})

	t.Run("Events failed during user transformation stage", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		bcServer := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID("source-1").
							WithWriteKey("writekey-2").
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
			WithUserTransformHandler(
				transformertest.ErrorTransformerHandler(
					http.StatusBadRequest, "TypeError: Cannot read property 'uuid' of undefined",
				),
			).
			Build()
		defer trServer.Close()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		minioResource, err := minio.Setup(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, minioResource, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 12

		err = sendEvents(eventsCount, "identify", "writekey-2", url)
		require.NoError(t, err)

		requireJobsCount(t, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
		requireJobsCount(t, postgresContainer.DB, "err_idx", jobsdb.Succeeded.State, eventsCount)
		requireMessagesCount(t, ctx, minioResource, eventsCount, []lo.Tuple2[string, string]{
			{A: "source_id", B: "source-1"},
			{A: "destination_id", B: "destination-1"},
			{A: "transformation_id", B: "transformation-1"},
			{A: "failed_stage", B: "user_transformer"},
			{A: "event_type", B: "identify"},
		}...)

		cancel()
		require.NoError(t, wg.Wait())
	})

	t.Run("Events failed during event filtering stage", func(t *testing.T) {
		t.Run("empty message type", func(t *testing.T) {
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
										WithDefinitionConfigOption("supportedMessageTypes", []string{"track"}).
										Build()).
								Build()).
						Build()).
				Build()
			defer bcServer.Close()

			trServer := transformertest.NewBuilder().
				WithUserTransformHandler(transformertest.EmptyTransformerHandler).
				Build()
			defer trServer.Close()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			postgresContainer, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			minioResource, err := minio.Setup(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			gwPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			wg, ctx := errgroup.WithContext(ctx)
			wg.Go(func() error {
				err := runRudderServer(ctx, gwPort, postgresContainer, minioResource, bcServer.URL, trServer.URL, t.TempDir())
				if err != nil {
					t.Logf("rudder-server exited with error: %v", err)
				}
				return err
			})

			url := fmt.Sprintf("http://localhost:%d", gwPort)
			health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

			eventsCount := 12

			err = sendEvents(eventsCount, "", "writekey-1", url)
			require.NoError(t, err)

			requireJobsCount(t, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
			requireJobsCount(t, postgresContainer.DB, "err_idx", jobsdb.Succeeded.State, eventsCount)
			requireMessagesCount(t, ctx, minioResource, eventsCount, []lo.Tuple2[string, string]{
				{A: "source_id", B: "source-1"},
				{A: "destination_id", B: "destination-1"},
				{A: "failed_stage", B: "event_filter"},
			}...)

			cancel()
			require.NoError(t, wg.Wait())
		})

		t.Run("empty message event", func(t *testing.T) {
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
										WithConfigOption("listOfConversions", []map[string]string{
											{
												"conversions": "Test event",
											},
										}).
										Build()).
								Build()).
						Build()).
				Build()
			defer bcServer.Close()

			trServer := transformertest.NewBuilder().
				WithUserTransformHandler(transformertest.EmptyTransformerHandler).
				Build()
			defer trServer.Close()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			postgresContainer, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			minioResource, err := minio.Setup(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			gwPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			wg, ctx := errgroup.WithContext(ctx)
			wg.Go(func() error {
				err := runRudderServer(ctx, gwPort, postgresContainer, minioResource, bcServer.URL, trServer.URL, t.TempDir())
				if err != nil {
					t.Logf("rudder-server exited with error: %v", err)
				}
				return err
			})

			url := fmt.Sprintf("http://localhost:%d", gwPort)
			health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

			eventsCount := 12

			err = sendEvents(eventsCount, "", "writekey-1", url)
			require.NoError(t, err)

			requireJobsCount(t, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
			requireJobsCount(t, postgresContainer.DB, "err_idx", jobsdb.Succeeded.State, eventsCount)
			requireMessagesCount(t, ctx, minioResource, eventsCount, []lo.Tuple2[string, string]{
				{A: "source_id", B: "source-1"},
				{A: "destination_id", B: "destination-1"},
				{A: "failed_stage", B: "event_filter"},
			}...)

			cancel()
			require.NoError(t, wg.Wait())
		})
	})

	t.Run("Events failed during destination transformation stage", func(t *testing.T) {
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

		trServer := transformertest.NewBuilder().
			WithDestTransformHandler(
				"WEBHOOK",
				transformertest.ErrorTransformerHandler(http.StatusBadRequest, "dest transformation failed"),
			).
			Build()
		defer trServer.Close()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)

		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		minioResource, err := minio.Setup(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)

		wg, ctx := errgroup.WithContext(ctx)
		wg.Go(func() error {
			err := runRudderServer(ctx, gwPort, postgresContainer, minioResource, bcServer.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})

		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		eventsCount := 12

		err = sendEvents(eventsCount, "identify", "writekey-1", url)
		require.NoError(t, err)

		requireJobsCount(t, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
		requireJobsCount(t, postgresContainer.DB, "err_idx", jobsdb.Succeeded.State, eventsCount)
		requireMessagesCount(t, ctx, minioResource, eventsCount, []lo.Tuple2[string, string]{
			{A: "source_id", B: "source-1"},
			{A: "destination_id", B: "destination-1"},
			{A: "failed_stage", B: "dest_transformer"},
			{A: "event_type", B: "identify"},
		}...)

		cancel()
		require.NoError(t, wg.Wait())
	})

	t.Run("Events failed during router delivery stage", func(t *testing.T) {
		t.Run("rejected by destination itself", func(t *testing.T) {
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

			webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "aborted", http.StatusBadRequest)
			}))
			defer webhook.Close()

			trServer := transformertest.NewBuilder().
				WithDestTransformHandler(
					"WEBHOOK",
					transformertest.RESTJSONDestTransformerHandler(http.MethodPost, webhook.URL),
				).
				Build()
			defer trServer.Close()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			postgresContainer, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			minioResource, err := minio.Setup(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			gwPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			wg, ctx := errgroup.WithContext(ctx)
			wg.Go(func() error {
				err := runRudderServer(ctx, gwPort, postgresContainer, minioResource, bcServer.URL, trServer.URL, t.TempDir())
				if err != nil {
					t.Logf("rudder-server exited with error: %v", err)
				}
				return err
			})

			url := fmt.Sprintf("http://localhost:%d", gwPort)
			health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

			eventsCount := 12

			err = sendEvents(eventsCount, "identify", "writekey-1", url)
			require.NoError(t, err)

			requireJobsCount(t, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
			requireJobsCount(t, postgresContainer.DB, "rt", jobsdb.Aborted.State, eventsCount)
			requireJobsCount(t, postgresContainer.DB, "err_idx", jobsdb.Succeeded.State, eventsCount)
			requireMessagesCount(t, ctx, minioResource, eventsCount, []lo.Tuple2[string, string]{
				{A: "source_id", B: "source-1"},
				{A: "destination_id", B: "destination-1"},
				{A: "failed_stage", B: "router"},
				{A: "event_type", B: "identify"},
			}...)

			cancel()
			require.NoError(t, wg.Wait())
		})
	})

	t.Run("Events failed during batch router delivery stage", func(t *testing.T) {
		t.Run("destination id included in BatchRouter.toAbortDestinationIDs", func(t *testing.T) {
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
									backendconfigtest.NewDestinationBuilder("S3").
										WithID("destination-1").
										Build()).
								Build()).
						Build()).
				Build()
			defer bcServer.Close()

			trServer := transformertest.NewBuilder().
				WithDestTransformHandler(
					"S3",
					transformertest.MirroringTransformerHandler,
				).
				Build()
			defer trServer.Close()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			postgresContainer, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			minioResource, err := minio.Setup(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			gwPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			wg, ctx := errgroup.WithContext(ctx)
			wg.Go(func() error {
				config.Set("Router.toAbortDestinationIDs", "destination-1")

				err := runRudderServer(ctx, gwPort, postgresContainer, minioResource, bcServer.URL, trServer.URL, t.TempDir())
				if err != nil {
					t.Logf("rudder-server exited with error: %v", err)
				}
				return err
			})

			url := fmt.Sprintf("http://localhost:%d", gwPort)
			health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

			eventsCount := 12

			err = sendEvents(eventsCount, "identify", "writekey-1", url)
			require.NoError(t, err)

			requireJobsCount(t, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
			requireJobsCount(t, postgresContainer.DB, "batch_rt", jobsdb.Aborted.State, eventsCount)
			requireJobsCount(t, postgresContainer.DB, "err_idx", jobsdb.Succeeded.State, eventsCount)
			requireMessagesCount(t, ctx, minioResource, eventsCount, []lo.Tuple2[string, string]{
				{A: "source_id", B: "source-1"},
				{A: "destination_id", B: "destination-1"},
				{A: "failed_stage", B: "batch_router"},
				{A: "event_type", B: "identify"},
			}...)

			cancel()
			require.NoError(t, wg.Wait())
		})

		t.Run("invalid object storage configuration", func(t *testing.T) {
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
									backendconfigtest.NewDestinationBuilder("S3").
										WithID("destination-1").
										Build()).
								Build()).
						Build()).
				Build()
			defer bcServer.Close()

			trServer := transformertest.NewBuilder().
				WithDestTransformHandler(
					"S3",
					transformertest.MirroringTransformerHandler,
				).
				Build()
			defer trServer.Close()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			postgresContainer, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			minioResource, err := minio.Setup(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			gwPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			wg, ctx := errgroup.WithContext(ctx)
			wg.Go(func() error {
				config.Set("BatchRouter.S3.retryTimeWindow", "0s")
				config.Set("BatchRouter.S3.maxFailedCountForJob", 0)

				err := runRudderServer(ctx, gwPort, postgresContainer, minioResource, bcServer.URL, trServer.URL, t.TempDir())
				if err != nil {
					t.Logf("rudder-server exited with error: %v", err)
				}
				return err
			})

			url := fmt.Sprintf("http://localhost:%d", gwPort)
			health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

			eventsCount := 12

			err = sendEvents(eventsCount, "identify", "writekey-1", url)
			require.NoError(t, err)

			requireJobsCount(t, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
			requireJobsCount(t, postgresContainer.DB, "batch_rt", jobsdb.Aborted.State, eventsCount)
			requireJobsCount(t, postgresContainer.DB, "err_idx", jobsdb.Succeeded.State, eventsCount)
			requireMessagesCount(t, ctx, minioResource, eventsCount, []lo.Tuple2[string, string]{
				{A: "source_id", B: "source-1"},
				{A: "destination_id", B: "destination-1"},
				{A: "failed_stage", B: "batch_router"},
				{A: "event_type", B: "identify"},
			}...)

			cancel()
			require.NoError(t, wg.Wait())
		})

		t.Run("unable to ping to warehouse", func(t *testing.T) {
			config.Reset()
			defer config.Reset()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			postgresContainer, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			minioResource, err := minio.Setup(pool, t)
			require.NoError(t, err)

			bcServer := backendconfigtest.NewBuilder().
				WithWorkspaceConfig(
					backendconfigtest.NewConfigBuilder().
						WithSource(
							backendconfigtest.NewSourceBuilder().
								WithID("source-1").
								WithWriteKey("writekey-1").
								WithConnection(
									backendconfigtest.NewDestinationBuilder("POSTGRES").
										WithID("destination-1").
										WithConfigOption("bucketProvider", "MINIO").
										WithConfigOption("bucketName", minioResource.BucketName).
										WithConfigOption("accessKeyID", minioResource.AccessKeyID).
										WithConfigOption("secretAccessKey", minioResource.AccessKeySecret).
										WithConfigOption("endPoint", minioResource.Endpoint).
										Build()).
								Build()).
						Build()).
				Build()
			defer bcServer.Close()

			trServer := transformertest.NewBuilder().
				WithDestTransformHandler(
					"POSTGRES",
					transformertest.WarehouseTransformerHandler(
						"tracks", http.StatusOK, "",
					),
				).
				Build()
			defer trServer.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			gwPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			wg, ctx := errgroup.WithContext(ctx)
			wg.Go(func() error {
				config.Set("BatchRouter.warehouseServiceMaxRetryTime", "0s")

				err := runRudderServer(ctx, gwPort, postgresContainer, minioResource, bcServer.URL, trServer.URL, t.TempDir())
				if err != nil {
					t.Logf("rudder-server exited with error: %v", err)
				}
				return err
			})

			url := fmt.Sprintf("http://localhost:%d", gwPort)
			health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

			eventsCount := 12

			err = sendEvents(eventsCount, "identify", "writekey-1", url)
			require.NoError(t, err)

			requireJobsCount(t, postgresContainer.DB, "gw", jobsdb.Succeeded.State, eventsCount)
			requireJobsCount(t, postgresContainer.DB, "batch_rt", jobsdb.Aborted.State, eventsCount)
			requireJobsCount(t, postgresContainer.DB, "err_idx", jobsdb.Succeeded.State, eventsCount)
			requireMessagesCount(t, ctx, minioResource, eventsCount, []lo.Tuple2[string, string]{
				{A: "source_id", B: "source-1"},
				{A: "destination_id", B: "destination-1"},
				{A: "failed_stage", B: "batch_router"},
				{A: "event_type", B: "identify"},
			}...)

			cancel()
			require.NoError(t, wg.Wait())
		})
	})
}

func runRudderServer(
	ctx context.Context,
	port int,
	postgresContainer *postgres.Resource,
	minioResource *minio.Resource,
	cbURL, transformerURL, tmpDir string,
) (err error) {
	config.Set("CONFIG_BACKEND_URL", cbURL)
	config.Set("WORKSPACE_TOKEN", "token")
	config.Set("DB.host", postgresContainer.Host)
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

	config.Set("ErrorIndex.storage.Bucket", minioResource.BucketName)
	config.Set("ErrorIndex.storage.Endpoint", minioResource.Endpoint)
	config.Set("ErrorIndex.storage.AccessKey", minioResource.AccessKeyID)
	config.Set("ErrorIndex.storage.SecretAccessKey", minioResource.AccessKeySecret)
	config.Set("ErrorIndex.storage.S3ForcePathStyle", true)
	config.Set("ErrorIndex.storage.DisableSSL", true)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "DUMMY"})
	c := r.Run(ctx, []string{"rudder-error-reporting"})
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
		require.NoError(t, db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM unionjobsdbmetadata('%s',1) WHERE job_state = '%s';`, queue, state)).Scan(&jobsCount))
		t.Logf("%s %sJobCount: %d", queue, state, jobsCount)
		return jobsCount == expectedCount
	},
		20*time.Second,
		1*time.Second,
		fmt.Sprintf("%d %s events should be in %s state", expectedCount, queue, state),
	)
}

// nolint: unparam
func requireMessagesCount(
	t *testing.T,
	ctx context.Context,
	mr *minio.Resource,
	expectedCount int,
	filters ...lo.Tuple2[string, string],
) {
	t.Helper()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)

	_, err = db.Exec(fmt.Sprintf(`INSTALL parquet; LOAD parquet; INSTALL httpfs; LOAD httpfs;SET s3_region='%s';SET s3_endpoint='%s';SET s3_access_key_id='%s';SET s3_secret_access_key='%s';SET s3_use_ssl= false;SET s3_url_style='path';`,
		mr.Region,
		mr.Endpoint,
		mr.AccessKeyID,
		mr.AccessKeySecret,
	))
	require.NoError(t, err)

	query := fmt.Sprintf("SELECT count(*) FROM read_parquet('%s') WHERE 1 = 1", fmt.Sprintf("s3://%s/**/**/**/*.parquet", mr.BucketName))
	query += strings.Join(lo.Map(filters, func(t lo.Tuple2[string, string], _ int) string {
		return fmt.Sprintf(" AND %s = '%s'", t.A, t.B)
	}), "")

	require.Eventually(t, func() bool {
		var messagesCount int
		require.NoError(t, db.QueryRowContext(ctx, query).Scan(&messagesCount))
		t.Logf("messagesCount: %d", messagesCount)
		return messagesCount == expectedCount
	},
		10*time.Second,
		1*time.Second,
		fmt.Sprintf("%d messages should be in the bucket", expectedCount),
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
