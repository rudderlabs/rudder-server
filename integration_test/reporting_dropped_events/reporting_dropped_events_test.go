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

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithttputil "github.com/rudderlabs/rudder-go-kit/httputil"
	"github.com/rudderlabs/rudder-go-kit/sqlutil"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
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
									Build(),
							).
							Build(),
					).
					Build(),
			).
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
										Build(),
								).
								Build(),
						).
						Build(),
				).
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
										Build(),
								).
								Build(),
						).
						Build(),
				).
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
									Build(),
							).
							Build(),
					).
					Build(),
			).
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
										Build(),
								).
								Build(),
						).
						Build(),
				).
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
	// boundary. A single server processes four batches with the three (reloadable) flags flipped
	// between them via config.Set, so every rollout state of the plan is exercised end to end
	// against the same reports table:
	//
	//	1. reorder on, metrics off  -> the old source-level filtered/298 row, now from fan-out
	//	2. destination_enter on     -> one succeeded/200 row per candidate destination
	//	3. both on, partial exclude -> filtered_integration/298 per destination, no source-level row
	//	4. both on, zero candidates -> filtered_no_destination/298 with an empty destination_id
	//
	// Batches are drained before the flags are flipped: report rows are committed in the same
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
				// the destination filter moves to fan-out; the metrics start off, so the first
				// batch exercises the safety fallback
				"Processor.earlyDestinationFilter":              false,
				"Reporting.destinationEnterMetrics.enabled":     false,
				"Reporting.perDestinationFilterMetrics.enabled": false,
				// keep the router from retrying against the mirrored (undeliverable) payloads
				"Router.toAbortDestinationIDs": "destination-1,destination-2,destination-3",
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

		t.Run("reorder on, metrics off: the source level filtered row moves to fan-out", func(t *testing.T) {
			require.NoError(t, sendEvents(eventsPerBatch, "identify", "writekey-2", url))
			drainGateway(eventsPerBatch)

			require.Eventually(t, func() bool {
				return reportCount("source_id = 'source-2' and destination_id = '' and pu = 'destination_filter' and status = 'filtered' and status_code = 298 and in_pu = 'gateway' and terminal_state = false and initial_state = false and error_type = ''") == eventsPerBatch
			}, 30*time.Second, 500*time.Millisecond, "the fallback source-level filtered row should still be emitted, from fan-out")

			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
			require.EqualValues(t, 0, reportCount("pu = 'destination_enter'"),
				"no destination_enter rows while Reporting.destinationEnterMetrics.enabled is off")
			require.EqualValues(t, 0, reportCount("pu = 'destination_filter' and status like 'filtered_%'"),
				"no per-destination rows while Reporting.perDestinationFilterMetrics.enabled is off")
		})

		t.Run("destination_enter metrics on: one succeeded row per candidate destination", func(t *testing.T) {
			config.Set("Reporting.destinationEnterMetrics.enabled", true)

			require.NoError(t, sendEvents(eventsPerBatch, "identify", "writekey-1", url))
			drainGateway(2 * eventsPerBatch)

			require.Eventually(t, func() bool {
				return reportCount("source_id = 'source-1' and pu = 'destination_enter' and status = 'succeeded' and status_code = 200 and in_pu = 'gateway' and terminal_state = false and initial_state = false and error_type = ''") == 3*eventsPerBatch
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

		t.Run("per destination filter metrics on: partial exclusion replaces the source level row", func(t *testing.T) {
			config.Set("Reporting.perDestinationFilterMetrics.enabled", true)

			// opt out of destination-2's type only; destination-1 and destination-3 still deliver
			require.NoError(t, sendEventsWithIntegrations(eventsPerBatch, `{"AM": false}`, "identify", "writekey-1", url))
			drainGateway(3 * eventsPerBatch)

			require.Eventually(t, func() bool {
				return reportCount("source_id = 'source-1' and destination_id = 'destination-2' and pu = 'destination_filter' and status = 'filtered_integration' and status_code = 298 and in_pu = 'destination_enter' and terminal_state = false and initial_state = false and error_type = ''") == eventsPerBatch
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

		t.Run("per destination filter metrics on: zero candidate events keep a source level row", func(t *testing.T) {
			filteredBefore := reportCount("source_id = 'source-2' and pu = 'destination_filter' and status = 'filtered'")

			require.NoError(t, sendEvents(eventsPerBatch, "identify", "writekey-2", url))
			drainGateway(4 * eventsPerBatch)

			require.Eventually(t, func() bool {
				return reportCount("source_id = 'source-2' and destination_id = '' and pu = 'destination_filter' and status = 'filtered_no_destination' and status_code = 298 and in_pu = 'gateway' and terminal_state = false and initial_state = false and error_type = ''") == eventsPerBatch
			}, 30*time.Second, 500*time.Millisecond, "zero-candidate events should be reported as filtered_no_destination")

			logRows(t, postgresContainer.DB, "SELECT * FROM reports")
			require.EqualValues(t, filteredBefore,
				reportCount("source_id = 'source-2' and pu = 'destination_filter' and status = 'filtered'"),
				"no new plain filtered rows should be emitted for source-2")
			require.EqualValues(t, 0, reportCount("source_id = 'source-2' and pu = 'destination_enter'"),
				"a source without destinations has no candidates to enter")
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
										Build(),
								).
								Build(),
						).
						Build(),
				).
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

func logRows(t *testing.T, db *sql.DB, query string) { // nolint:unparam
	rows, err := db.Query(query) // nolint:rowserrcheck
	defer func() { _ = rows.Close() }()
	if err != nil {
		var b strings.Builder
		_ = sqlutil.PrintRowsToTable(rows, &b)
		t.Log(b.String())
	}
}
