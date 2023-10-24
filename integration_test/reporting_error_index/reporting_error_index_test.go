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
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/testhelper/destination"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"

	_ "github.com/marcboeker/go-duckdb"
)

func TestReportingErrorIndex(t *testing.T) {
	//// FIXME: destination filter should drop events using a [filtered] status instead of a [diff] status with negative count
	//t.Run("Events dropped in destination filter stage", func(t *testing.T) {
	//	config.Reset()
	//	defer config.Reset()
	//
	//	bcserver := backendconfigtest.NewBuilder().
	//		WithWorkspaceConfig(
	//			backendconfigtest.NewConfigBuilder().
	//				WithSource(
	//					backendconfigtest.NewSourceBuilder().
	//						WithID("source-1").
	//						WithWriteKey("writekey-1").
	//						Build()).
	//				Build()).
	//		Build()
	//	defer bcserver.Close()
	//
	//	trServer := transformertest.NewBuilder().Build()
	//	defer trServer.Close()
	//
	//	pool, err := dockertest.NewPool("")
	//	require.NoError(t, err)
	//	postgresContainer, err := resource.SetupPostgres(pool, t)
	//	require.NoError(t, err)
	//
	//	ctx, cancel := context.WithCancel(context.Background())
	//	defer cancel()
	//	wg, ctx := errgroup.WithContext(ctx)
	//	gwPort, err := kithelper.GetFreePort()
	//	require.NoError(t, err)
	//	wg.Go(func() error {
	//		err := runRudderServer(ctx, gwPort, postgresContainer, nil, bcserver.URL, trServer.URL, t.TempDir())
	//		if err != nil {
	//			t.Logf("rudder-server exited with error: %v", err)
	//		}
	//		return err
	//	})
	//	url := fmt.Sprintf("http://localhost:%d", gwPort)
	//	health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
	//	err = sendEvents(10, "identify", "writekey-1", url)
	//	require.NoError(t, err)
	//
	//	require.Eventually(t, func() bool {
	//		var jobsCount int
	//		require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
	//		t.Logf("gw processedJobCount: %d", jobsCount)
	//		return jobsCount == 10
	//	}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
	//
	//	require.Eventually(t, func() bool {
	//		var droppedCount sql.NullInt64
	//		require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = '' AND pu = 'destination_filter' and status = 'diff' and error_type = ''").Scan(&droppedCount))
	//		t.Logf("destination_filter diff count: %d", droppedCount.Int64)
	//		logRows(t, postgresContainer.DB, "SELECT * FROM reports")
	//		return droppedCount.Int64 == -10
	//	}, 10*time.Second, 1*time.Second, "all events should be dropped in destination_filter stage")
	//
	//	cancel()
	//	_ = wg.Wait()
	//})
	//
	//t.Run("Events dropped in tracking plan validation stage", func(t *testing.T) {
	//	config.Reset()
	//	defer config.Reset()
	//
	//	bcserver := backendconfigtest.NewBuilder().
	//		WithWorkspaceConfig(
	//			backendconfigtest.NewConfigBuilder().
	//				WithSource(
	//					backendconfigtest.NewSourceBuilder().
	//						WithID("source-1").
	//						WithWriteKey("writekey-1").
	//						WithTrackingPlan("trackingplan-1", 1).
	//						WithConnection(
	//							backendconfigtest.NewDestinationBuilder("WEBHOOK").
	//								WithID("destination-1").
	//								Build()).
	//						Build()).
	//				Build()).
	//		Build()
	//	defer bcserver.Close()
	//
	//	trServer := transformertest.NewBuilder().
	//		WithTrackingPlanHandler(
	//			transformertest.ViolationErrorTransformerHandler(
	//				http.StatusBadRequest,
	//				"tracking plan validation failed",
	//				[]transformer.ValidationError{{Type: "Datatype-Mismatch", Message: "must be number"}},
	//			),
	//		).
	//		Build()
	//	defer trServer.Close()
	//
	//	pool, err := dockertest.NewPool("")
	//	require.NoError(t, err)
	//	postgresContainer, err := resource.SetupPostgres(pool, t)
	//	require.NoError(t, err)
	//
	//	ctx, cancel := context.WithCancel(context.Background())
	//	defer cancel()
	//	wg, ctx := errgroup.WithContext(ctx)
	//	gwPort, err := kithelper.GetFreePort()
	//	require.NoError(t, err)
	//	wg.Go(func() error {
	//		err := runRudderServer(ctx, gwPort, postgresContainer, nil, bcserver.URL, trServer.URL, t.TempDir())
	//		if err != nil {
	//			t.Logf("rudder-server exited with error: %v", err)
	//		}
	//		return err
	//	})
	//	url := fmt.Sprintf("http://localhost:%d", gwPort)
	//	health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
	//	err = sendEvents(10, "identify", "writekey-1", url)
	//	require.NoError(t, err)
	//
	//	require.Eventually(t, func() bool {
	//		var jobsCount int
	//		require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
	//		t.Logf("gw processedJobCount: %d", jobsCount)
	//		return jobsCount == 10
	//	}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
	//
	//	require.Eventually(t, func() bool {
	//		var droppedCount sql.NullInt64
	//		require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = '' AND pu = 'tracking_plan_validator' and status = 'aborted' and error_type = ''").Scan(&droppedCount))
	//		t.Logf("tracking_plan_validator aborted count: %d", droppedCount.Int64)
	//		logRows(t, postgresContainer.DB, "SELECT * FROM reports")
	//		return droppedCount.Int64 == 10
	//	}, 10*time.Second, 1*time.Second, "all events should be aborted in tracking_plan_validator stage")
	//
	//	cancel()
	//	_ = wg.Wait()
	//})
	//
	//// TODO: revisit user transformation [diff] metrics?
	//t.Run("Events dropped in user transformation stage", func(t *testing.T) {
	//	t.Run("user transformer function returns an null event", func(t *testing.T) {
	//		config.Reset()
	//		defer config.Reset()
	//
	//		bcserver := backendconfigtest.NewBuilder().
	//			WithWorkspaceConfig(
	//				backendconfigtest.NewConfigBuilder().
	//					WithSource(
	//						backendconfigtest.NewSourceBuilder().
	//							WithID("source-1").
	//							WithWriteKey("writekey-1").
	//							WithConnection(
	//								backendconfigtest.NewDestinationBuilder("WEBHOOK").
	//									WithID("destination-1").
	//									WithUserTransformation("transformation-1", "version-1").
	//									Build()).
	//							Build()).
	//					Build()).
	//			Build()
	//		defer bcserver.Close()
	//
	//		trServer := transformertest.NewBuilder().
	//			WithUserTransformHandler(transformertest.EmptyTransformerHandler).
	//			Build()
	//		defer trServer.Close()
	//
	//		pool, err := dockertest.NewPool("")
	//		require.NoError(t, err)
	//		postgresContainer, err := resource.SetupPostgres(pool, t)
	//		require.NoError(t, err)
	//
	//		ctx, cancel := context.WithCancel(context.Background())
	//		defer cancel()
	//		wg, ctx := errgroup.WithContext(ctx)
	//		gwPort, err := kithelper.GetFreePort()
	//		require.NoError(t, err)
	//		wg.Go(func() error {
	//			err := runRudderServer(ctx, gwPort, postgresContainer, nil, bcserver.URL, trServer.URL, t.TempDir())
	//			if err != nil {
	//				t.Logf("rudder-server exited with error: %v", err)
	//			}
	//			return err
	//		})
	//		url := fmt.Sprintf("http://localhost:%d", gwPort)
	//		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
	//		err = sendEvents(10, "identify", "writekey-1", url)
	//		require.NoError(t, err)
	//
	//		require.Eventually(t, func() bool {
	//			var jobsCount int
	//			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
	//			t.Logf("gw processedJobCount: %d", jobsCount)
	//			return jobsCount == 10
	//		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
	//
	//		require.Eventually(t, func() bool {
	//			var droppedCount sql.NullInt64
	//			require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = 'destination-1' AND pu = 'user_transformer' and status = 'diff' and error_type = ''").Scan(&droppedCount))
	//			t.Logf("user_transformer aborted/diff count: %d", droppedCount.Int64)
	//			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
	//			return droppedCount.Int64 == -10
	//		}, 10*time.Second, 1*time.Second, "all events should be aborted in user_transformer stage")
	//
	//		cancel()
	//		_ = wg.Wait()
	//	})
	//})
	//
	//t.Run("Events dropped in event filtering stage", func(t *testing.T) {
	//	t.Run("unsupported message type", func(t *testing.T) {
	//		config.Reset()
	//		defer config.Reset()
	//
	//		bcserver := backendconfigtest.NewBuilder().
	//			WithWorkspaceConfig(
	//				backendconfigtest.NewConfigBuilder().
	//					WithSource(
	//						backendconfigtest.NewSourceBuilder().
	//							WithID("source-1").
	//							WithWriteKey("writekey-1").
	//							WithConnection(
	//								backendconfigtest.NewDestinationBuilder("WEBHOOK").
	//									WithID("destination-1").
	//									WithDefinitionConfigOption("supportedMessageTypes", []string{"track"}).
	//									Build()).
	//							Build()).
	//					Build()).
	//			Build()
	//		defer bcserver.Close()
	//
	//		trServer := transformertest.NewBuilder().
	//			WithUserTransformHandler(transformertest.EmptyTransformerHandler).
	//			Build()
	//		defer trServer.Close()
	//
	//		pool, err := dockertest.NewPool("")
	//		require.NoError(t, err)
	//		postgresContainer, err := resource.SetupPostgres(pool, t)
	//		require.NoError(t, err)
	//
	//		ctx, cancel := context.WithCancel(context.Background())
	//		defer cancel()
	//		wg, ctx := errgroup.WithContext(ctx)
	//		gwPort, err := kithelper.GetFreePort()
	//		require.NoError(t, err)
	//		wg.Go(func() error {
	//			err := runRudderServer(ctx, gwPort, postgresContainer, nil, bcserver.URL, trServer.URL, t.TempDir())
	//			if err != nil {
	//				t.Logf("rudder-server exited with error: %v", err)
	//			}
	//			return err
	//		})
	//		url := fmt.Sprintf("http://localhost:%d", gwPort)
	//		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
	//		err = sendEvents(10, "identify", "writekey-1", url)
	//		require.NoError(t, err)
	//
	//		require.Eventually(t, func() bool {
	//			var jobsCount int
	//			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
	//			t.Logf("gw processedJobCount: %d", jobsCount)
	//			return jobsCount == 10
	//		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
	//
	//		require.Eventually(t, func() bool {
	//			var droppedCount sql.NullInt64
	//			require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = 'destination-1' AND pu = 'event_filter' and status = 'filtered' and error_type = ''").Scan(&droppedCount))
	//			t.Logf("event_filter filtered count: %d", droppedCount.Int64)
	//			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
	//			return droppedCount.Int64 == 10
	//		}, 10*time.Second, 1*time.Second, "all events should be filtered in event_filter stage")
	//
	//		cancel()
	//		_ = wg.Wait()
	//	})
	//})
	//
	//t.Run("Events dropped in destination transformation stage", func(t *testing.T) {
	//	config.Reset()
	//	defer config.Reset()
	//
	//	bcserver := backendconfigtest.NewBuilder().
	//		WithWorkspaceConfig(
	//			backendconfigtest.NewConfigBuilder().
	//				WithSource(
	//					backendconfigtest.NewSourceBuilder().
	//						WithID("source-1").
	//						WithWriteKey("writekey-1").
	//						WithConnection(
	//							backendconfigtest.NewDestinationBuilder("WEBHOOK").
	//								WithID("destination-1").
	//								Build()).
	//						Build()).
	//				Build()).
	//		Build()
	//	defer bcserver.Close()
	//
	//	trServer := transformertest.NewBuilder().
	//		WithDestTransformHandler(
	//			"WEBHOOK",
	//			transformertest.ErrorTransformerHandler(http.StatusBadRequest, "dest transformation failed"),
	//		).
	//		Build()
	//	defer trServer.Close()
	//
	//	pool, err := dockertest.NewPool("")
	//	require.NoError(t, err)
	//	postgresContainer, err := resource.SetupPostgres(pool, t)
	//	require.NoError(t, err)
	//
	//	ctx, cancel := context.WithCancel(context.Background())
	//	defer cancel()
	//	wg, ctx := errgroup.WithContext(ctx)
	//	gwPort, err := kithelper.GetFreePort()
	//	require.NoError(t, err)
	//	wg.Go(func() error {
	//		err := runRudderServer(ctx, gwPort, postgresContainer, nil, bcserver.URL, trServer.URL, t.TempDir())
	//		if err != nil {
	//			t.Logf("rudder-server exited with error: %v", err)
	//		}
	//		return err
	//	})
	//	url := fmt.Sprintf("http://localhost:%d", gwPort)
	//	health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
	//	err = sendEvents(10, "identify", "writekey-1", url)
	//	require.NoError(t, err)
	//
	//	require.Eventually(t, func() bool {
	//		var jobsCount int
	//		require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
	//		t.Logf("gw processedJobCount: %d", jobsCount)
	//		return jobsCount == 10
	//	}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
	//
	//	require.Eventually(t, func() bool {
	//		var droppedCount sql.NullInt64
	//		require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = 'destination-1' AND pu = 'dest_transformer' and status = 'aborted' and error_type = ''").Scan(&droppedCount))
	//		t.Logf("tracking_plan_validator aborted count: %d", droppedCount.Int64)
	//		logRows(t, postgresContainer.DB, "SELECT * FROM reports")
	//		return droppedCount.Int64 == 10
	//	}, 10*time.Second, 1*time.Second, "all events should be aborted in dest_transformer stage")
	//
	//	cancel()
	//	_ = wg.Wait()
	//})
	//
	//t.Run("Events dropped in router delivery stage", func(t *testing.T) {
	//	t.Run("rejected by destination itself", func(t *testing.T) {
	//		config.Reset()
	//		defer config.Reset()
	//
	//		bcserver := backendconfigtest.NewBuilder().
	//			WithWorkspaceConfig(
	//				backendconfigtest.NewConfigBuilder().
	//					WithSource(
	//						backendconfigtest.NewSourceBuilder().
	//							WithID("source-1").
	//							WithWriteKey("writekey-1").
	//							WithConnection(
	//								backendconfigtest.NewDestinationBuilder("WEBHOOK").
	//									WithID("destination-1").
	//									Build()).
	//							Build()).
	//					Build()).
	//			Build()
	//		defer bcserver.Close()
	//
	//		webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	//			http.Error(w, "aborted", http.StatusBadRequest)
	//		}))
	//		defer webhook.Close()
	//
	//		trServer := transformertest.NewBuilder().
	//			WithDestTransformHandler(
	//				"WEBHOOK",
	//				transformertest.RESTJSONDestTransformerHandler(http.MethodPost, webhook.URL),
	//			).
	//			Build()
	//		defer trServer.Close()
	//
	//		pool, err := dockertest.NewPool("")
	//		require.NoError(t, err)
	//		postgresContainer, err := resource.SetupPostgres(pool, t)
	//		require.NoError(t, err)
	//
	//		ctx, cancel := context.WithCancel(context.Background())
	//		defer cancel()
	//		wg, ctx := errgroup.WithContext(ctx)
	//		gwPort, err := kithelper.GetFreePort()
	//		require.NoError(t, err)
	//		wg.Go(func() error {
	//			err := runRudderServer(ctx, gwPort, postgresContainer, nil, bcserver.URL, trServer.URL, t.TempDir())
	//			if err != nil {
	//				t.Logf("rudder-server exited with error: %v", err)
	//			}
	//			return err
	//		})
	//		url := fmt.Sprintf("http://localhost:%d", gwPort)
	//		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
	//		err = sendEvents(10, "identify", "writekey-1", url)
	//		require.NoError(t, err)
	//
	//		require.Eventually(t, func() bool {
	//			var jobsCount int
	//			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
	//			t.Logf("gw processedJobCount: %d", jobsCount)
	//			return jobsCount == 10
	//		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")
	//
	//		require.Eventually(t, func() bool {
	//			var jobsCount int
	//			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('rt',1) WHERE job_state = 'aborted'").Scan(&jobsCount))
	//			t.Logf("rt abortedJobCount: %d", jobsCount)
	//			return jobsCount == 10
	//		}, 20*time.Second, 1*time.Second, "all events should be aborted in router")
	//
	//		require.Eventually(t, func() bool {
	//			var droppedCount sql.NullInt64
	//			require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = 'destination-1' AND pu = 'router' and status = 'aborted' and error_type = ''").Scan(&droppedCount))
	//			t.Logf("router aborted count: %d", droppedCount.Int64)
	//			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
	//			return droppedCount.Int64 == 10
	//		}, 10*time.Second, 1*time.Second, "all events should be aborted in router stage")
	//
	//		cancel()
	//		_ = wg.Wait()
	//	})
	//})

	t.Run("Events dropped in batch router delivery stage", func(t *testing.T) {
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

			webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "aborted", http.StatusBadRequest)
			}))
			defer webhook.Close()

			trServer := transformertest.NewBuilder().
				WithDestTransformHandler(
					"S3",
					transformertest.MirroringTransformerHandler,
				).
				Build()
			defer trServer.Close()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)

			postgresContainer, err := resource.SetupPostgres(pool, t)
			require.NoError(t, err)
			minioResource, err := destination.SetupMINIO(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			wg, ctx := errgroup.WithContext(ctx)

			gwPort, err := kithelper.GetFreePort()
			require.NoError(t, err)

			wg.Go(func() error {
				config.Set("BatchRouter.toAbortDestinationIDs", "destination-1")

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

			require.Eventually(t, func() bool {
				var jobsCount int
				require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
				t.Logf("gw processedJobCount: %d", jobsCount)
				return jobsCount == eventsCount
			}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")

			require.Eventually(t, func() bool {
				var jobsCount int
				require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = 'aborted'").Scan(&jobsCount))
				t.Logf("batch_rt abortedJobCount: %d", jobsCount)
				return jobsCount == eventsCount
			}, 20*time.Second, 1*time.Second, "all events should be aborted in batch router")

			require.Eventually(t, func() bool {
				var jobsCount int
				require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('err_idx',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
				t.Logf("err_idx abortedJobCount: %d", jobsCount)
				return jobsCount == eventsCount
			}, 30*time.Second, 1*time.Second, "all events should be succeeded in error index")

			duckDB, err := sql.Open("duckdb", "")
			require.NoError(t, err)
			defer func() { _ = duckDB.Close() }()

			_, err = duckDB.Exec(fmt.Sprintf(`
		INSTALL parquet;
		LOAD parquet;
		INSTALL httpfs;
		LOAD httpfs;
		SET s3_region='%s';
		SET s3_endpoint='%s';
		SET s3_access_key_id='%s';
		SET s3_secret_access_key='%s';
		set s3_use_ssl= false;
		set s3_url_style='path';
	`,
				minioResource.SiteRegion,
				minioResource.Endpoint,
				minioResource.AccessKey,
				minioResource.SecretKey,
			))
			require.NoError(t, err)

			filePath := fmt.Sprintf("s3://%s/**/**/**/*.parquet", minioResource.BucketName)
			query := fmt.Sprintf("SELECT count(*) FROM read_parquet('%s');", filePath)

			var count int64
			err = duckDB.QueryRowContext(ctx, query).Scan(&count)
			require.NoError(t, err)
			require.EqualValues(t, eventsCount, count)

			cancel()
			_ = wg.Wait()
		})
	})
}

func runRudderServer(ctx context.Context, port int, postgresContainer *resource.PostgresResource, minioResource *destination.MINIOResource, cbURL, transformerURL, tmpDir string) (err error) {
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

	config.Set("JOBS_BACKUP_STORAGE_PROVIDER", "MINIO")
	config.Set("ErrorIndex.Storage.Bucket", minioResource.BucketName)
	config.Set("MINIO_ENDPOINT", minioResource.Endpoint)
	config.Set("MINIO_ACCESS_KEY_ID", minioResource.AccessKey)
	config.Set("MINIO_SECRET_ACCESS_KEY", minioResource.SecretKey)
	config.Set("MINIO_USE_SSL", "false")

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "TOKEN"})
	c := r.Run(ctx, []string{"proc-isolation-test-rudder-server"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return
}

func sendEvents(num int, eventType, writeKey, url string) error { // nolint:unparam
	for i := 0; i < num; i++ {
		payload := []byte(fmt.Sprintf(`{"batch": [{
			"userId": %[1]q,
			"type": %[2]q,
			"context":
			{
				"traits":
				{
					"trait1": "new-val"
				},
				"ip": "14.5.67.21",
				"library":
				{
					"name": "http"
				}
			},
			"timestamp": "2020-02-02T00:23:09.544Z"
			}]}`,
			rand.String(10),
			eventType))
		req, err := http.NewRequest("POST", url+"/v1/batch", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.SetBasicAuth(writeKey, "password")
		client := &http.Client{}
		resp, err := client.Do(req)
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

//func logRows(t *testing.T, db *sql.DB, query string) { // nolint:unparam
//	rows, err := db.Query(query) // nolint:rowserrcheck
//	defer func() { _ = rows.Close() }()
//	if err != nil {
//		var b strings.Builder
//		_ = sqlutil.PrintRowsToTable(rows, &b)
//		t.Log(b.String())
//	}
//}
