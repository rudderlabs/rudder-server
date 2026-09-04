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
	"sync/atomic"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/sqlutil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/runner"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
)

func TestReportingDroppedEvents(t *testing.T) {
	t.Run("Events dropped in destination filter stage", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		bcserver := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID("source-1").
							WithWriteKey("writekey-1").
							Build()).
					Build()).
			Build()
		defer bcserver.Close()

		trServer := transformertest.NewBuilder().Build()
		defer trServer.Close()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wg, ctx := errgroup.WithContext(ctx)
		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		wg.Go(func() error {
			err := runRudderServer(ctx, cancel, gwPort, postgresContainer, bcserver.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
		err = sendEvents(10, "identify", "writekey-1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 10
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")

		require.Eventually(t, func() bool {
			var filteredCount sql.NullInt64
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = '' AND pu = 'destination_filter' and status = 'filtered' and error_type = ''").Scan(&filteredCount))
			t.Logf("destination_filter filtered count: %d", filteredCount.Int64)
			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
			return filteredCount.Int64 == 10
		}, 10*time.Second, 1*time.Second, "all events should be dropped in destination_filter stage")

		cancel()
		_ = wg.Wait()
	})

	t.Run("Events dropped in tracking plan validation stage", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		bcserver := backendconfigtest.NewBuilder().
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
		defer bcserver.Close()

		trServer := transformertest.NewBuilder().
			WithTrackingPlanHandler(
				transformertest.ViolationErrorTransformerHandler(
					http.StatusBadRequest,
					"tracking plan validation failed",
					[]types.ValidationError{{Type: "Datatype-Mismatch", Message: "must be number"}},
				),
			).
			Build()
		defer trServer.Close()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wg, ctx := errgroup.WithContext(ctx)
		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		wg.Go(func() error {
			err := runRudderServer(ctx, cancel, gwPort, postgresContainer, bcserver.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
		err = sendEvents(10, "identify", "writekey-1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 10
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")

		require.Eventually(t, func() bool {
			var droppedCount sql.NullInt64
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = '' AND pu = 'tracking_plan_validator' and status = 'aborted' and error_type = ''").Scan(&droppedCount))
			t.Logf("tracking_plan_validator aborted count: %d", droppedCount.Int64)
			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
			return droppedCount.Int64 == 10
		}, 10*time.Second, 1*time.Second, "all events should be aborted in tracking_plan_validator stage")

		cancel()
		_ = wg.Wait()
	})

	// TODO: revisit user transformation [diff] metrics?
	t.Run("Events dropped in user transformation stage", func(t *testing.T) {
		t.Run("user transformer function returns an null event", func(t *testing.T) {
			config.Reset()
			defer config.Reset()

			bcserver := backendconfigtest.NewBuilder().
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
			defer bcserver.Close()

			trServer := transformertest.NewBuilder().
				WithUserTransformHandler(transformertest.EmptyTransformerHandler).
				Build()
			defer trServer.Close()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)
			postgresContainer, err := postgres.Setup(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wg, ctx := errgroup.WithContext(ctx)
			gwPort, err := kithelper.GetFreePort()
			require.NoError(t, err)
			wg.Go(func() error {
				err := runRudderServer(ctx, cancel, gwPort, postgresContainer, bcserver.URL, trServer.URL, t.TempDir())
				if err != nil {
					t.Logf("rudder-server exited with error: %v", err)
				}
				return err
			})
			url := fmt.Sprintf("http://localhost:%d", gwPort)
			health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
			err = sendEvents(10, "identify", "writekey-1", url)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				var jobsCount int
				require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
				t.Logf("gw processedJobCount: %d", jobsCount)
				return jobsCount == 10
			}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")

			require.Eventually(t, func() bool {
				var droppedCount sql.NullInt64
				require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = 'destination-1' AND pu = 'user_transformer' and status = 'diff' and error_type = ''").Scan(&droppedCount))
				t.Logf("user_transformer aborted/diff count: %d", droppedCount.Int64)
				logRows(t, postgresContainer.DB, "SELECT * FROM reports")
				return droppedCount.Int64 == -10
			}, 10*time.Second, 1*time.Second, "all events should be aborted in user_transformer stage")

			cancel()
			_ = wg.Wait()
		})
	})

	t.Run("Events dropped in event filtering stage", func(t *testing.T) {
		t.Run("unsupported message type", func(t *testing.T) {
			config.Reset()
			defer config.Reset()

			bcserver := backendconfigtest.NewBuilder().
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
			defer bcserver.Close()

			trServer := transformertest.NewBuilder().
				WithUserTransformHandler(transformertest.EmptyTransformerHandler).
				Build()
			defer trServer.Close()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)
			postgresContainer, err := postgres.Setup(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wg, ctx := errgroup.WithContext(ctx)
			gwPort, err := kithelper.GetFreePort()
			require.NoError(t, err)
			wg.Go(func() error {
				err := runRudderServer(ctx, cancel, gwPort, postgresContainer, bcserver.URL, trServer.URL, t.TempDir())
				if err != nil {
					t.Logf("rudder-server exited with error: %v", err)
				}
				return err
			})
			url := fmt.Sprintf("http://localhost:%d", gwPort)
			health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
			err = sendEvents(10, "identify", "writekey-1", url)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				var jobsCount int
				require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
				t.Logf("gw processedJobCount: %d", jobsCount)
				return jobsCount == 10
			}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")

			require.Eventually(t, func() bool {
				var droppedCount sql.NullInt64
				require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = 'destination-1' AND pu = 'event_filter' and status = 'filtered' and error_type = ''").Scan(&droppedCount))
				t.Logf("event_filter filtered count: %d", droppedCount.Int64)
				logRows(t, postgresContainer.DB, "SELECT * FROM reports")
				return droppedCount.Int64 == 10
			}, 10*time.Second, 1*time.Second, "all events should be filtered in event_filter stage")

			cancel()
			_ = wg.Wait()
		})
	})

	t.Run("Events dropped in destination transformation stage", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		bcserver := backendconfigtest.NewBuilder().
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
		defer bcserver.Close()

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

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wg, ctx := errgroup.WithContext(ctx)
		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		wg.Go(func() error {
			err := runRudderServer(ctx, cancel, gwPort, postgresContainer, bcserver.URL, trServer.URL, t.TempDir())
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
		err = sendEvents(10, "identify", "writekey-1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 10
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")

		require.Eventually(t, func() bool {
			var droppedCount sql.NullInt64
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = 'destination-1' AND pu = 'dest_transformer' and status = 'aborted' and error_type = ''").Scan(&droppedCount))
			t.Logf("tracking_plan_validator aborted count: %d", droppedCount.Int64)
			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
			return droppedCount.Int64 == 10
		}, 10*time.Second, 1*time.Second, "all events should be aborted in dest_transformer stage")

		cancel()
		_ = wg.Wait()
	})

	t.Run("Events dropped in router delivery stage", func(t *testing.T) {
		t.Run("rejected by destination itself", func(t *testing.T) {
			config.Reset()
			defer config.Reset()

			bcserver := backendconfigtest.NewBuilder().
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
			defer bcserver.Close()

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

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wg, ctx := errgroup.WithContext(ctx)
			gwPort, err := kithelper.GetFreePort()
			require.NoError(t, err)
			wg.Go(func() error {
				err := runRudderServer(ctx, cancel, gwPort, postgresContainer, bcserver.URL, trServer.URL, t.TempDir())
				if err != nil {
					t.Logf("rudder-server exited with error: %v", err)
				}
				return err
			})
			url := fmt.Sprintf("http://localhost:%d", gwPort)
			health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
			err = sendEvents(10, "identify", "writekey-1", url)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				var jobsCount int
				require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
				t.Logf("gw processedJobCount: %d", jobsCount)
				return jobsCount == 10
			}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")

			require.Eventually(t, func() bool {
				var jobsCount int
				require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('rt',1) WHERE job_state = 'aborted'").Scan(&jobsCount))
				t.Logf("rt abortedJobCount: %d", jobsCount)
				return jobsCount == 10
			}, 20*time.Second, 1*time.Second, "all events should be aborted in router")

			require.Eventually(t, func() bool {
				var droppedCount sql.NullInt64
				require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = 'destination-1' AND pu = 'router' and status = 'aborted' and error_type = ''").Scan(&droppedCount))
				t.Logf("router aborted count: %d", droppedCount.Int64)
				logRows(t, postgresContainer.DB, "SELECT * FROM reports")
				return droppedCount.Int64 == 10
			}, 10*time.Second, 1*time.Second, "all events should be aborted in router stage")

			cancel()
			_ = wg.Wait()
		})
	})

	t.Run("Events dropped in dedup stage", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		bcserver := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID("source-1").
							WithWriteKey("writekey-1").
							Build(),
					).
					Build(),
			).
			Build()
		defer bcserver.Close()

		trServer := transformertest.NewBuilder().Build()
		defer trServer.Close()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wg, ctx := errgroup.WithContext(ctx)
		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		// the dedup badger db lives under RUDDER_TMPDIR: keep it isolated and cleaned up, but
		// short enough for the admin unix socket path that also lives there (t.TempDir() is too long)
		dedupTmpDir, err := os.MkdirTemp("", "dedup")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(dedupTmpDir) })
		wg.Go(func() error {
			err := runRudderServer(ctx, cancel, gwPort, postgresContainer, bcserver.URL, trServer.URL, t.TempDir(), map[string]any{
				"Dedup.enableDedup":              true,
				"RUDDER_TMPDIR":                  dedupTmpDir,
				"Reporting.dedupMetrics.enabled": true,
			})
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
		// 1 original + 2 duplicates, all carrying the same messageId
		err = sendEventsWithMessageID(3, "message-id-1", "identify", "writekey-1", url)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			var jobsCount int
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
			t.Logf("gw processedJobCount: %d", jobsCount)
			return jobsCount == 3
		}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")

		require.Eventually(t, func() bool {
			var dedupCount, matchingDedupCount sql.NullInt64
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = '' AND pu = 'dedup'").Scan(&dedupCount))
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = '' AND pu = 'dedup' and status = 'filtered' and status_code = 298 and in_pu = '' and terminal_state = false and initial_state = false and error_type = ''").Scan(&matchingDedupCount))
			t.Logf("dedup filtered count: %d (matching: %d)", dedupCount.Int64, matchingDedupCount.Int64)
			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
			return dedupCount.Int64 == 2 && matchingDedupCount.Int64 == 2
		}, 10*time.Second, 1*time.Second, "both duplicates should be filtered in dedup stage")

		require.Eventually(t, func() bool {
			var gatewayCount sql.NullInt64
			require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = '' AND pu = 'gateway'").Scan(&gatewayCount))
			t.Logf("gateway count: %d", gatewayCount.Int64)
			return gatewayCount.Int64 == 1
		}, 10*time.Second, 1*time.Second, "only the surviving event should be counted in the gateway stage")

		cancel()
		_ = wg.Wait()
	})

	// Phase 1 of the pipeline inspector: per-destination visibility at the destination-filter
	// boundary. A single server processes four batches with the (reloadable)
	// Processor.earlyDestinationFilter flag flipped between them via config.Set, so both rollout
	// states are exercised end to end against the same reports table:
	//
	//	1. reorder off (default)     -> zero candidates drop at preprocess, old filtered/298 row
	//	2. reorder on                -> one succeeded/200 destination_enter row per candidate
	//	3. reorder on, part. exclude -> filtered_integration/298 per destination, no source-level row
	//	4. reorder on, zero cands    -> filtered_no_destination/298 with an empty destination_id
	//	5. reorder on, consent deny  -> filtered_consent/298 per destination, no source-level row
	//
	// Batches are drained before the flag is flipped: report rows are committed in the same
	// transaction as the gateway job statuses, so "all gw jobs succeeded" is a safe barrier.
	t.Run("Per destination visibility at the destination filter boundary", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		const eventsPerBatch = 10

		bcserver := backendconfigtest.NewBuilder().
			WithWorkspaceConfig(
				backendconfigtest.NewConfigBuilder().
					WithSource(
						backendconfigtest.NewSourceBuilder().
							WithID("source-1").
							WithWriteKey("writekey-1").
							WithConnection(newDestinationOfType("WEBHOOK", "destination-1")).
							WithConnection(newDestinationOfType("AM", "destination-2")).
							WithConnection(newDestinationOfType("GA", "destination-3")).
							Build(),
					).
					WithSource(
						// no connections at all: every event is a zero-candidate event
						backendconfigtest.NewSourceBuilder().
							WithID("source-2").
							WithWriteKey("writekey-2").
							Build(),
					).
					WithSource(
						// destination-5 is consent-gated on "cat-1"; the other two are not
						backendconfigtest.NewSourceBuilder().
							WithID("source-3").
							WithWriteKey("writekey-3").
							WithConnection(newDestinationOfType("WEBHOOK", "destination-4")).
							WithConnection(newConsentGatedDestinationOfType("AM", "destination-5", "cat-1")).
							WithConnection(newDestinationOfType("GA", "destination-6")).
							Build(),
					).
					Build(),
			).
			Build()
		defer bcserver.Close()

		trServer := transformertest.NewBuilder().Build()
		defer trServer.Close()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wg, ctx := errgroup.WithContext(ctx)
		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		wg.Go(func() error {
			err := runRudderServer(ctx, cancel, gwPort, postgresContainer, bcserver.URL, trServer.URL, t.TempDir(), map[string]any{
				// keep the router from retrying against the mirrored (undeliverable) payloads
				"Router.toAbortDestinationIDs": "destination-1,destination-2,destination-3,destination-4,destination-5,destination-6",
			})
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		// reportCount sums the reports rows matching where, returning -1 on any query error so
		// that it can be used inside a require.Eventually callback without asserting there.
		reportCount := func(where string) int64 {
			var count sql.NullInt64
			if err := postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE " + where).Scan(&count); err != nil {
				return -1
			}
			return count.Int64
		}
		routerJobCount := func(destinationID string) int64 {
			var count int64
			if err := postgresContainer.DB.QueryRow(
				"SELECT count(*) FROM unionjobsdbmetadata('rt',1) WHERE parameters->>'destination_id' = $1", destinationID,
			).Scan(&count); err != nil {
				return -1
			}
			return count
		}
		// drainGateway waits until every event sent so far has been processed. Reports are
		// committed in the same transaction as the gateway job statuses, so once this returns
		// every report row of the batch is visible.
		drainGateway := func(total int) {
			t.Helper()
			require.Eventually(t, func() bool {
				var jobsCount int
				if err := postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount); err != nil {
					return false
				}
				t.Logf("gw processedJobCount: %d (expecting %d)", jobsCount, total)
				return jobsCount == total
			}, 60*time.Second, 500*time.Millisecond, "all gw events should be successfully processed")
		}

		t.Run("reorder off (default): zero-candidate events drop at preprocess with the old source-level filtered row", func(t *testing.T) {
			require.NoError(t, sendEvents(eventsPerBatch, "identify", "writekey-2", url))
			drainGateway(eventsPerBatch)

			require.Eventually(t, func() bool {
				return reportCount("source_id = 'source-2' and destination_id = '' and pu = 'destination_filter' and status = 'filtered' and status_code = 298 and in_pu = 'gateway' and terminal_state = false and initial_state = false and error_type = ''") == eventsPerBatch
			}, 30*time.Second, 500*time.Millisecond, "the preprocess source-level filtered row should be emitted")

			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
			require.EqualValues(t, 0, reportCount("pu = 'destination_enter'"),
				"no destination_enter rows while Processor.earlyDestinationFilter is on")
			require.EqualValues(t, 0, reportCount("pu = 'destination_filter' and status like 'filtered_%'"),
				"no per-destination rows while Processor.earlyDestinationFilter is on")
		})

		t.Run("reorder on: one destination_enter row per candidate destination", func(t *testing.T) {
			config.Set("Processor.earlyDestinationFilter", false)

			require.NoError(t, sendEvents(eventsPerBatch, "identify", "writekey-1", url))
			drainGateway(2 * eventsPerBatch)

			require.Eventually(t, func() bool {
				return reportCount("source_id = 'source-1' and pu = 'destination_enter' and status = 'succeeded' and status_code = 200 and in_pu = '' and terminal_state = false and initial_state = false and error_type = ''") == 3*eventsPerBatch
			}, 30*time.Second, 500*time.Millisecond, "every candidate destination should get a destination_enter row")

			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
			for _, destinationID := range []string{"destination-1", "destination-2", "destination-3"} {
				require.EqualValues(t, eventsPerBatch,
					reportCount("source_id = 'source-1' and pu = 'destination_enter' and destination_id = '"+destinationID+"'"),
					"destination_enter rows should be attributed to %s", destinationID)
			}
			require.EqualValues(t, 0, reportCount("pu = 'destination_enter' and destination_id = ''"),
				"destination_enter rows must always carry a destination id")
			require.EqualValues(t, 0, reportCount("source_id = 'source-1' and pu = 'destination_filter'"),
				"nothing was excluded, so no destination_filter rows for source-1")
		})

		t.Run("reorder on: partial exclusion emits a per-destination row instead of the source level row", func(t *testing.T) {
			// opt out of destination-2's type only; destination-1 and destination-3 still deliver
			require.NoError(t, sendEventsWithIntegrations(eventsPerBatch, `{"AM": false}`, "identify", "writekey-1", url))
			drainGateway(3 * eventsPerBatch)

			require.Eventually(t, func() bool {
				return reportCount("source_id = 'source-1' and destination_id = 'destination-2' and pu = 'destination_filter' and status = 'filtered_integration' and status_code = 298 and in_pu = '' and terminal_state = false and initial_state = false and error_type = ''") == eventsPerBatch
			}, 30*time.Second, 500*time.Millisecond, "the excluded destination should get a filtered_integration row")

			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
			// excluded destinations are still candidates, so all three keep entering
			require.EqualValues(t, 2*3*eventsPerBatch,
				reportCount("source_id = 'source-1' and pu = 'destination_enter' and status = 'succeeded'"),
				"excluded destinations are candidates too and still emit destination_enter")
			require.EqualValues(t, eventsPerBatch,
				reportCount("source_id = 'source-1' and pu = 'destination_filter' and status like 'filtered_%'"),
				"only destination-2 should be filtered")
			// the cutover: the old source-level row is gone for this source
			require.EqualValues(t, 0,
				reportCount("source_id = 'source-1' and destination_id = '' and pu = 'destination_filter' and status = 'filtered'"),
				"the source-level filtered row must not be emitted once per-destination metrics are on")

			// the exclusion is a reporting outcome, not a routing change: the two survivors got
			// jobs from both batches, the excluded one only from the previous batch
			require.Eventually(t, func() bool {
				return routerJobCount("destination-1") == 2*eventsPerBatch && routerJobCount("destination-3") == 2*eventsPerBatch
			}, 30*time.Second, 500*time.Millisecond, "the surviving destinations should still be routed to")
			require.EqualValues(t, eventsPerBatch, routerJobCount("destination-2"),
				"the excluded destination should not have received this batch")
		})

		t.Run("reorder on: zero candidate events keep a source level row as filtered_no_destination", func(t *testing.T) {
			filteredBefore := reportCount("source_id = 'source-2' and pu = 'destination_filter' and status = 'filtered'")

			require.NoError(t, sendEvents(eventsPerBatch, "identify", "writekey-2", url))
			drainGateway(4 * eventsPerBatch)

			require.Eventually(t, func() bool {
				return reportCount("source_id = 'source-2' and destination_id = '' and pu = 'destination_filter' and status = 'filtered_no_destination' and status_code = 298 and in_pu = '' and terminal_state = false and initial_state = false and error_type = ''") == eventsPerBatch
			}, 30*time.Second, 500*time.Millisecond, "zero-candidate events should be reported as filtered_no_destination")

			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
			require.EqualValues(t, filteredBefore,
				reportCount("source_id = 'source-2' and pu = 'destination_filter' and status = 'filtered'"),
				"no new plain filtered rows should be emitted for source-2")
			require.EqualValues(t, 0, reportCount("source_id = 'source-2' and pu = 'destination_enter'"),
				"a source without destinations has no candidates to enter")
		})

		t.Run("reorder on: consent excludes one destination as filtered_consent instead of the source level row", func(t *testing.T) {
			config.Set("Processor.earlyDestinationFilter", false)

			// destination-5 is gated on "cat-1", which these events deny; destination-4 and
			// destination-6 carry no consent configuration and still deliver
			require.NoError(t, sendEventsWithDeniedConsent(eventsPerBatch, `["cat-1"]`, "identify", "writekey-3", url))
			drainGateway(5 * eventsPerBatch)

			require.Eventually(t, func() bool {
				return reportCount("source_id = 'source-3' and destination_id = 'destination-5' and pu = 'destination_filter' and status = 'filtered_consent' and status_code = 298 and in_pu = '' and terminal_state = false and initial_state = false and error_type = ''") == eventsPerBatch
			}, 30*time.Second, 500*time.Millisecond, "the consent-denied destination should get a filtered_consent row")

			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
			// a consent-denied destination is still a candidate, so all three keep entering
			require.EqualValues(t, 3*eventsPerBatch,
				reportCount("source_id = 'source-3' and pu = 'destination_enter' and status = 'succeeded' and status_code = 200 and in_pu = '' and terminal_state = false and initial_state = false and error_type = ''"),
				"every candidate destination, including the consent-denied one, should get a destination_enter row")
			for _, destinationID := range []string{"destination-4", "destination-5", "destination-6"} {
				require.EqualValues(t, eventsPerBatch,
					reportCount("source_id = 'source-3' and pu = 'destination_enter' and destination_id = '"+destinationID+"'"),
					"destination_enter rows should be attributed to %s", destinationID)
			}
			require.EqualValues(t, eventsPerBatch,
				reportCount("source_id = 'source-3' and pu = 'destination_filter' and status like 'filtered_%'"),
				"only destination-5 should be filtered")
			// the cutover: the old source-level row is gone for this source
			require.EqualValues(t, 0,
				reportCount("source_id = 'source-3' and destination_id = '' and pu = 'destination_filter'"),
				"the source-level filtered row must not be emitted once per-destination metrics are on")

			// consent denial is a reporting outcome for one destination only: the other two are
			// still routed to
			require.Eventually(t, func() bool {
				return routerJobCount("destination-4") == eventsPerBatch && routerJobCount("destination-6") == eventsPerBatch
			}, 30*time.Second, 500*time.Millisecond, "the consented destinations should still be routed to")
			require.EqualValues(t, 0, routerJobCount("destination-5"),
				"the consent-denied destination should not have received this batch")
		})

		cancel()
		_ = wg.Wait()
	})

	// Phase 3 of the pipeline inspector: suppressed users become dummy jobs that ride the normal
	// gateway -> jobsdb -> processor path and are reported and dropped at the top of the
	// processor's preprocess loop. This is an internal-batch-only behaviour: the public
	// (/v1/batch) path drops suppressed events outright, whatever the flag says. A single server
	// processes three batches for the same suppressed user, with the (reloadable)
	// Gateway.storeUserSuppressedEvents flag flipped between the first two:
	//
	//	1. internal batch, flag off (default) -> nothing is stored at all
	//	2. internal batch, flag on            -> one dummy gw job per suppressed event, dropped at
	//	                                         the processor with a user_suppression/filtered/298
	//	                                         row and nothing else
	//	3. public batch, flag on              -> still nothing is stored
	//
	// Report rows are committed in the same transaction as the gateway job statuses, so "exactly
	// N gw jobs succeeded" is both the barrier for the second batch and the proof that the first
	// batch stored nothing.
	t.Run("Events dropped in user suppression stage", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		const (
			eventsPerBatch = 10
			workspaceID    = "workspace-1"
			suppressedUser = "suppressed-user-1"
		)

		workspaceConfig := backendconfigtest.NewConfigBuilder().
			WithWorkspaceID(workspaceID).
			WithSource(
				backendconfigtest.NewSourceBuilder().
					WithID("source-1").
					WithWriteKey("writekey-1").
					WithWorkspaceID(workspaceID).
					WithConnection(newDestinationOfType("WEBHOOK", "destination-1")).
					Build(),
			).
			Build()

		// a single suppression with no sourceIds, i.e. the user is suppressed for every source of
		// the workspace
		cpserver, suppressionsServed := newControlPlaneServer(workspaceConfig, []model.Suppression{{
			WorkspaceID: workspaceID,
			UserID:      suppressedUser,
		}})
		defer cpserver.Close()

		trServer := transformertest.NewBuilder().Build()
		defer trServer.Close()

		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		postgresContainer, err := postgres.Setup(pool, t)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wg, ctx := errgroup.WithContext(ctx)
		gwPort, err := kithelper.GetFreePort()
		require.NoError(t, err)
		wg.Go(func() error {
			err := runRudderServer(ctx, cancel, gwPort, postgresContainer, cpserver.URL, trServer.URL, t.TempDir(), map[string]any{
				"Gateway.enableSuppressUserFeature": true,
				// the in-memory suppression repository: no badger, no backup service seeding
				"BackendConfig.Regulations.useBadgerDB":  false,
				"BackendConfig.Regulations.pollInterval": "1s",
				"SUPPRESS_USER_BACKEND_URL":              cpserver.URL,
				"Gateway.storeUserSuppressedEvents":      false,
				"Router.toAbortDestinationIDs":           "destination-1",
			})
			if err != nil {
				t.Logf("rudder-server exited with error: %v", err)
			}
			return err
		})
		url := fmt.Sprintf("http://localhost:%d", gwPort)
		health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

		// reportCount sums the reports rows matching where, returning -1 on any query error so
		// that it can be used inside a require.Eventually callback without asserting there.
		reportCount := func(where string) int64 {
			var count sql.NullInt64
			if err := postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE " + where).Scan(&count); err != nil {
				return -1
			}
			return count.Int64
		}
		gatewayJobCount := func() int64 {
			var count int64
			if err := postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1)").Scan(&count); err != nil {
				return -1
			}
			return count
		}

		// The sync loop is started asynchronously and Setup returns immediately, so events sent
		// before the first page has been added to the repository would not be suppressed at all.
		// Waiting for the *second* request is what makes this a barrier rather than a race: the
		// second sync can only start after the first page was added to the repository.
		require.Eventually(t, func() bool {
			return suppressionsServed.Load() >= 2
		}, 60*time.Second, 100*time.Millisecond, "the suppression list should have been synced")

		sendInternalBatch := func() error {
			return sendInternalBatchEventsForUser(eventsPerBatch, workspaceID, "source-1", suppressedUser, "identify", "writekey-1", url)
		}

		t.Run("flag off (default): the suppressed events are dropped at the gateway", func(t *testing.T) {
			require.NoError(t, sendInternalBatch())

			require.Never(t, func() bool {
				return gatewayJobCount() != 0
			}, 5*time.Second, 500*time.Millisecond, "nothing should be stored while Gateway.storeUserSuppressedEvents is off")
		})

		t.Run("flag on: the suppressed events are stored as dummy jobs and dropped at the processor", func(t *testing.T) {
			config.Set("Gateway.storeUserSuppressedEvents", true)

			require.NoError(t, sendInternalBatch())

			// exactly one gw job per event of this batch: the flag-off batch contributed none
			require.Eventually(t, func() bool {
				var jobsCount int
				if err := postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount); err != nil {
					return false
				}
				t.Logf("gw processedJobCount: %d (expecting %d)", jobsCount, eventsPerBatch)
				return jobsCount == eventsPerBatch
			}, 60*time.Second, 500*time.Millisecond, "all suppressed dummy jobs should be successfully processed")

			require.Eventually(t, func() bool {
				return reportCount("source_id = 'source-1' and destination_id = '' and pu = 'user_suppression' and status = 'filtered' and status_code = 298 and in_pu = '' and terminal_state = false and initial_state = false and error_type = ''") == eventsPerBatch
			}, 30*time.Second, 500*time.Millisecond, "every suppressed event should get a user_suppression filtered row")

			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
			require.EqualValues(t, eventsPerBatch, reportCount("pu = 'user_suppression'"),
				"no user_suppression rows other than the filtered ones")
			require.EqualValues(t, eventsPerBatch, gatewayJobCount(),
				"the flag-off batch must not have stored any job")

			// the drop happens before the GATEWAY billing metric and before any fan-out
			require.EqualValues(t, 0, reportCount("pu = 'gateway'"),
				"suppressed events must never reach the gateway billing metric")
			for _, pu := range []string{"destination_filter", "user_transformer", "router"} {
				require.EqualValues(t, 0, reportCount("source_id = 'source-1' and pu = '"+pu+"'"),
					"the suppressed events died at preprocess and should have no %s rows", pu)
			}
		})

		t.Run("flag on: the public path still drops suppressed events outright", func(t *testing.T) {
			require.NoError(t, sendEventsForUser(eventsPerBatch, suppressedUser, "identify", "writekey-1", url))

			// the dummy-job behaviour is internal-batch only: the public path drops suppressed
			// events regardless of the flag, so the internal batch's jobs stay the whole story
			require.Never(t, func() bool {
				return gatewayJobCount() != eventsPerBatch
			}, 5*time.Second, 500*time.Millisecond, "the public path must not store suppressed events even with the flag on")
			require.EqualValues(t, eventsPerBatch, reportCount("pu = 'user_suppression'"),
				"the public path drops before the processor, so it adds no user_suppression rows")
		})

		cancel()
		_ = wg.Wait()
	})

	t.Run("Events dropped in batch router delivery stage", func(t *testing.T) {
		t.Run("destination id included in BatchRouter.toAbortDestinationIDs", func(t *testing.T) {
			config.Reset()
			defer config.Reset()

			bcserver := backendconfigtest.NewBuilder().
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
			defer bcserver.Close()

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
			postgresContainer, err := postgres.Setup(pool, t)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wg, ctx := errgroup.WithContext(ctx)
			gwPort, err := kithelper.GetFreePort()
			require.NoError(t, err)
			wg.Go(func() error {
				config.Set("Router.toAbortDestinationIDs", "destination-1")
				err := runRudderServer(ctx, cancel, gwPort, postgresContainer, bcserver.URL, trServer.URL, t.TempDir())
				if err != nil {
					t.Logf("rudder-server exited with error: %v", err)
				}
				return err
			})
			url := fmt.Sprintf("http://localhost:%d", gwPort)
			health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())
			err = sendEvents(10, "identify", "writekey-1", url)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				var jobsCount int
				require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('gw',1) WHERE job_state = 'succeeded'").Scan(&jobsCount))
				t.Logf("gw processedJobCount: %d", jobsCount)
				return jobsCount == 10
			}, 20*time.Second, 1*time.Second, "all gw events should be successfully processed")

			require.Eventually(t, func() bool {
				var jobsCount int
				require.NoError(t, postgresContainer.DB.QueryRow("SELECT count(*) FROM unionjobsdbmetadata('batch_rt',1) WHERE job_state = 'aborted'").Scan(&jobsCount))
				t.Logf("batch_rt abortedJobCount: %d", jobsCount)
				return jobsCount == 10
			}, 20*time.Second, 1*time.Second, "all events should be aborted in batch router")

			require.Eventually(t, func() bool {
				var droppedCount sql.NullInt64
				require.NoError(t, postgresContainer.DB.QueryRow("SELECT sum(count) FROM reports WHERE source_id = 'source-1' and destination_id = 'destination-1' AND pu = 'batch_router' and status = 'aborted' and error_type = ''").Scan(&droppedCount))
				t.Logf("batch router aborted count: %d", droppedCount.Int64)
				logRows(t, postgresContainer.DB, "SELECT * FROM reports")
				return droppedCount.Int64 == 10
			}, 10*time.Second, 1*time.Second, "all events should be aborted in batch_router stage")

			cancel()
			_ = wg.Wait()
		})
	})
}

func runRudderServer(ctx context.Context, cancel context.CancelFunc, port int, postgresContainer *postgres.Resource, cbURL, transformerURL, tmpDir string, configOverrides ...map[string]any) (err error) {
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
	config.Set("JobsDB.compactionLoopSleepDuration", "60m")
	config.Set("archival.Enabled", false)
	config.Set("Reporting.syncer.enabled", false)
	config.Set("BatchRouter.pingFrequency", "1s")
	config.Set("BatchRouter.uploadFreq", "1s")
	config.Set("Gateway.webPort", strconv.Itoa(port))
	config.Set("RUDDER_TMPDIR", os.TempDir())
	config.Set("recovery.storagePath", path.Join(tmpDir, "/recovery_data.json"))
	config.Set("recovery.enabled", false)
	config.Set("Profiler.Enabled", false)
	config.Set("Gateway.enableSuppressUserFeature", false)

	for _, overrides := range configOverrides {
		for key, value := range overrides {
			config.Set(key, value)
		}
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	r := runner.New(runner.ReleaseInfo{EnterpriseToken: "TOKEN"})
	c := r.Run(ctx, cancel, []string{"proc-isolation-test-rudder-server"})
	if c != 0 {
		err = fmt.Errorf("rudder-server exited with a non-0 exit code: %d", c)
	}
	return err
}

func sendEvents(num int, eventType, writeKey, url string) error { // nolint:unparam
	for range num {
		payload := fmt.Appendf(nil, `{"batch": [{
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
			eventType)
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

// sendEventsWithMessageID sends num events all carrying the same messageId, so that every
// event after the first one is a duplicate as far as the processor's dedup stage is concerned.
func sendEventsWithMessageID(num int, messageID, eventType, writeKey, url string) error { // nolint:unparam
	for range num {
		payload := fmt.Appendf(nil, `{"batch": [{
			"userId": %[1]q,
			"messageId": %[2]q,
			"type": %[3]q,
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
			messageID,
			eventType)
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

// sendEventsWithIntegrations sends num events carrying the given `integrations` object, which is
// what the processor's client-integration filter reads to decide which destination types an event
// opts out of.
func sendEventsWithIntegrations(num int, integrations, eventType, writeKey, url string) error { // nolint:unparam
	for range num {
		payload := fmt.Appendf(nil, `{"batch": [{
			"userId": %[1]q,
			"type": %[2]q,
			"integrations": %[3]s,
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
			eventType,
			integrations)
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

// sendEventsWithDeniedConsent sends num events whose `context.consentManagement.deniedConsentIds`
// denies the given consent ids, which is what the processor's consent filter matches against a
// destination's oneTrustCookieCategories.
func sendEventsWithDeniedConsent(num int, deniedConsentIDs, eventType, writeKey, url string) error { // nolint:unparam
	for range num {
		payload := fmt.Appendf(nil, `{"batch": [{
			"userId": %[1]q,
			"type": %[2]q,
			"context":
			{
				"traits":
				{
					"trait1": "new-val"
				},
				"consentManagement":
				{
					"deniedConsentIds": %[3]s
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
			eventType,
			deniedConsentIDs)
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

// sendEventsForUser sends num events all carrying the same userId, which is what the gateway's
// suppression check is keyed on (unlike sendEvents, which randomises it).
func sendEventsForUser(num int, userID, eventType, writeKey, url string) error { // nolint:unparam
	for range num {
		payload := fmt.Appendf(nil, `{"batch": [{
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
			userID,
			eventType)
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

// sendInternalBatchEventsForUser sends num events for the given user in a single request to the
// gateway's internal batch endpoint. That path reads the suppression key off the message
// properties (workspaceID/userID/sourceID) rather than off the event payload, so both are set to
// the same user here. Request shape mirrors processor/processor_event_dropping_test.go.
func sendInternalBatchEventsForUser(num int, workspaceID, sourceID, userID, eventType, writeKey, url string) error { // nolint:unparam
	messages := make([]string, 0, num)
	for i := range num {
		messages = append(messages, fmt.Sprintf(`{
			"properties": {
				"routingKey": "a1",
				"requestType": %[1]q,
				"workspaceID": %[2]q,
				"userID": %[3]q,
				"sourceID": %[4]q,
				"requestIP": "1.2.3.4",
				"receivedAt": "2024-01-01T01:01:01.000000001Z"
			},
			"payload": {
				"userId": %[3]q,
				"anonymousId": %[3]q,
				"type": %[1]q,
				"messageId": %[5]q,
				"request_ip": "1.2.3.4",
				"rudderId": "rudder-id-1",
				"receivedAt": "2024-01-01T01:01:01.000000001Z"
			}
		}`, eventType, workspaceID, userID, sourceID, fmt.Sprintf("message-id-%d", i)))
	}
	payload := fmt.Appendf(nil, "[%s]", strings.Join(messages, ",\n"))

	req, err := http.NewRequest("POST", url+"/internal/v1/batch", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.SetBasicAuth(writeKey, "password")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { kithttputil.CloseResponse(resp) }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to send internal batch to rudder server, status code: %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

// newControlPlaneServer serves both the workspace config and the data-regulation suppressions
// endpoint from a single mux, so that a test can point CONFIG_BACKEND_URL and
// SUPPRESS_USER_BACKEND_URL at the same server (backendconfigtest's server only knows about the
// former).
//
// The suppressions endpoint hands out the given suppressions on its first call and an empty page
// afterwards, which is what tells the syncer that the sync is complete. The returned counter
// records how many times that endpoint has been hit, so that a test can wait for the suppression
// list to be loaded instead of relying on the poll interval.
func newControlPlaneServer(workspaceConfig backendconfig.ConfigT, suppressions []model.Suppression) (*httptest.Server, *atomic.Int64) {
	var served atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/workspaceConfig", func(w http.ResponseWriter, _ *http.Request) {
		response, _ := jsonrs.Marshal(workspaceConfig)
		_, _ = w.Write(response)
	})
	mux.HandleFunc(
		fmt.Sprintf("/dataplane/workspaces/%s/regulations/suppressions", workspaceConfig.WorkspaceID),
		func(w http.ResponseWriter, _ *http.Request) {
			items := []model.Suppression{}
			if served.Add(1) == 1 {
				items = suppressions
			}
			response, _ := jsonrs.Marshal(struct {
				Items []model.Suppression `json:"items"`
				Token string              `json:"token"`
			}{Items: items, Token: "suppression-token"})
			_, _ = w.Write(response)
		},
	)
	return httptest.NewServer(mux), &served
}

// newDestinationOfType builds a destination whose definition display name is set alongside its
// name. The processor keys enabled destination types by DestinationDefinition.DisplayName (and the
// client `integrations` object is matched against those same keys), but backendconfigtest's
// destination builder only sets the definition Name — without this, destinations of different
// types would all collapse into a single "" type.
func newDestinationOfType(destType, id string) backendconfig.DestinationT {
	destination := backendconfigtest.NewDestinationBuilder(destType).WithID(id).Build()
	destination.DestinationDefinition.DisplayName = destType
	return destination
}

// newConsentGatedDestinationOfType builds a destination of the given type that is gated on a
// single legacy (oneTrust) consent category: an event denying that category excludes this
// destination in the processor's consent filter. Mirrors the destination config shape
// TestClassifyDestinations' consentDeniedDest helper uses.
func newConsentGatedDestinationOfType(destType, id, consentCategory string) backendconfig.DestinationT {
	destination := backendconfigtest.NewDestinationBuilder(destType).
		WithID(id).
		WithConfigOption("oneTrustCookieCategories", []any{
			map[string]any{"oneTrustCookieCategory": consentCategory},
		}).
		Build()
	destination.DestinationDefinition.DisplayName = destType
	return destination
}

func logRows(t *testing.T, db *sql.DB, query string) { // nolint:unparam
	rows, err := db.Query(query) // nolint:rowserrcheck
	defer func() { _ = rows.Close() }()
	if err != nil {
		var b strings.Builder
		_ = sqlutil.PrintRowsToTable(rows, &b)
		t.Log(b.String())
	}
}
