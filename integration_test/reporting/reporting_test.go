package reporting_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	kithelper "github.com/rudderlabs/rudder-go-kit/testhelper"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/testhelper/backendconfigtest"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/transformertest"
)

func startTransformerServer(handler transformertest.TransformerHandler) (*httptest.Server, error) {
	return transformertest.NewBuilder().
		WithUserTransformHandler(handler).
		Build(), nil
}

func startRudderServer(t *testing.T, postgresContainer *postgres.Resource, bcURL, transformerURL string) (string, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	wg, ctx := errgroup.WithContext(ctx)
	gwPort, err := kithelper.GetFreePort()
	require.NoError(t, err)

	config.Set("Processor.Transformer.enableUpdatedEventNameReporting", true)
	config.Set("Processor.useSuccessMetricsInDiffMetrics", true)

	wg.Go(func() error {
		err := runRudderServer(ctx, gwPort, postgresContainer, bcURL, transformerURL, t.TempDir())
		if err != nil {
			t.Logf("rudder-server exited with error: %v", err)
		}
		return err
	})

	url := fmt.Sprintf("http://localhost:%d", gwPort)
	health.WaitUntilReady(ctx, t, url+"/health", 60*time.Second, 10*time.Millisecond, t.Name())

	cleanup := func() {
		cancel()
		_ = wg.Wait()
	}

	return url, cleanup
}

func verifyMetrics(t *testing.T, db *sql.DB, eventName string, expectedCounts map[string]int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		for status, expectedCount := range expectedCounts {
			var count sql.NullInt64
			query := `
				SELECT sum(count) 
				FROM reports 
				WHERE source_id = 'source-1' 
				AND destination_id = 'destination-1' 
				AND pu = 'user_transformer' 
				AND status = $1 
				AND error_type = ''
				AND event_name = $2`
			require.NoError(t, db.QueryRow(query, status, eventName).Scan(&count))
			t.Logf("%s %s count: %d (expected: %d)", eventName, status, count.Int64, expectedCount)
			if !count.Valid || count.Int64 != expectedCount {
				return false
			}
		}
		return true
	}, 10*time.Second, 1*time.Second, "metrics for %s should match expected values", eventName)
}

func setupBackendConfigServer() (*httptest.Server, error) {
	return backendconfigtest.NewBuilder().
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
		Build(), nil
}

func setupDB(t *testing.T) *postgres.Resource {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	postgresContainer, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	return postgresContainer
}

func TestReporting(t *testing.T) {
	t.Run("UserTransformation", func(t *testing.T) {
		config.Reset()
		defer config.Reset()

		bcserver, err := setupBackendConfigServer()
		require.NoError(t, err)
		defer bcserver.Close()

		t.Run("Pass through", func(t *testing.T) {
			db := setupDB(t)

			trServer, err := startTransformerServer(transformertest.MirroringTransformerHandler)
			require.NoError(t, err)
			defer trServer.Close()

			url, cleanup := startRudderServer(t, db, bcserver.URL, trServer.URL)
			defer cleanup()

			err = sendEvents(10, "track", "event_1", "writekey-1", url)
			require.NoError(t, err)
			verifyMetrics(t, db.DB, "event_1", map[string]int64{
				"succeeded": 10,
			})
		})

		t.Run("Drops due to error", func(t *testing.T) {
			db := setupDB(t)

			trServer, err := startTransformerServer(transformertest.ErrorTransformerHandler(400, "dropped"))
			require.NoError(t, err)
			defer trServer.Close()

			url, cleanup := startRudderServer(t, db, bcserver.URL, trServer.URL)
			defer cleanup()

			err = sendEvents(10, "track", "event_2", "writekey-1", url)
			require.NoError(t, err)
			verifyMetrics(t, db.DB, "event_2", map[string]int64{
				"aborted": 10,
			})
		})

		t.Run("Filter", func(t *testing.T) {
			db := setupDB(t)

			trServer, err := startTransformerServer(transformertest.FilteredTransformerHandler)
			require.NoError(t, err)
			defer trServer.Close()

			url, cleanup := startRudderServer(t, db, bcserver.URL, trServer.URL)
			defer cleanup()

			err = sendEvents(10, "track", "event_3", "writekey-1", url)
			require.NoError(t, err)
			verifyMetrics(t, db.DB, "event_3", map[string]int64{
				"filtered": 10,
			})
		})

		t.Run("Add New Event", func(t *testing.T) {
			db := setupDB(t)
			trServer, err := startTransformerServer(transformertest.AddEventHandler("event_5_new"))
			require.NoError(t, err)
			defer trServer.Close()

			url, cleanup := startRudderServer(t, db, bcserver.URL, trServer.URL)
			defer cleanup()

			err = sendEvents(10, "track", "event_5", "writekey-1", url)
			require.NoError(t, err)
			verifyMetrics(t, db.DB, "event_5", map[string]int64{
				"succeeded": 10,
			})
			verifyMetrics(t, db.DB, "event_5_new", map[string]int64{
				"succeeded": 10,
				"diff":      10,
			})
		})

		t.Run("Event Name Transformations", func(t *testing.T) {
            t.Run("to string", func(t *testing.T) {
                db := setupDB(t)
                trServer, err := startTransformerServer(transformertest.RenameEventToString("event_renamed"))
                require.NoError(t, err)
                defer trServer.Close()

                url, cleanup := startRudderServer(t, db, bcserver.URL, trServer.URL)
                defer cleanup()

                err = sendEvents(10, "track", "event_original", "writekey-1", url)
                require.NoError(t, err)
                verifyMetrics(t, db.DB, "event_original", map[string]int64{
                    "diff": -10,
                })
                verifyMetrics(t, db.DB, "event_renamed", map[string]int64{
                    "succeeded": 10,
                    "diff":      10,
                })
            })

            t.Run("to float", func(t *testing.T) {
                db := setupDB(t)
                trServer, err := startTransformerServer(transformertest.RenameEventToFloat(123.45))
                require.NoError(t, err)
                defer trServer.Close()

                url, cleanup := startRudderServer(t, db, bcserver.URL, trServer.URL)
                defer cleanup()

                err = sendEvents(10, "track", "event_original", "writekey-1", url)
                require.NoError(t, err)
                verifyMetrics(t, db.DB, "event_original", map[string]int64{
                    "diff": -10,
                })
                verifyMetrics(t, db.DB, "123.45", map[string]int64{
                    "succeeded": 10,
                    "diff":      10,
                })
            })

            t.Run("to object", func(t *testing.T) {
                db := setupDB(t)
                obj := map[string]interface{}{
                    "name": "test_event",
                    "id":   123,
                }
                trServer, err := startTransformerServer(transformertest.RenameEventToObject(obj))
                require.NoError(t, err)
                defer trServer.Close()

                url, cleanup := startRudderServer(t, db, bcserver.URL, trServer.URL)
                defer cleanup()

                err = sendEvents(10, "track", "event_original", "writekey-1", url)
                require.NoError(t, err)
                verifyMetrics(t, db.DB, "event_original", map[string]int64{
                    "diff": -10,
                })
                verifyMetrics(t, db.DB, `{"name":"test_event","id":123}`, map[string]int64{
                    "succeeded": 10,
                    "diff":      10,
                })
            })

            t.Run("remove event key", func(t *testing.T) {
                db := setupDB(t)
                trServer, err := startTransformerServer(transformertest.RemoveEventKey())
                require.NoError(t, err)
                defer trServer.Close()

                url, cleanup := startRudderServer(t, db, bcserver.URL, trServer.URL)
                defer cleanup()

                err = sendEvents(10, "track", "event_original", "writekey-1", url)
                require.NoError(t, err)
                verifyMetrics(t, db.DB, "event_original", map[string]int64{
                    "diff": -10,
                })
                verifyMetrics(t, db.DB, "", map[string]int64{
                    "succeeded": 10,
                    "diff":      10,
                })
            })
        })

		t.Run("Complex ", func(t *testing.T) {
			db := setupDB(t)

			complexHandler := func(events []transformer.TransformerEvent) []transformer.TransformerResponse {
				var responses []transformer.TransformerResponse
				for i, event := range events {
					switch i % 10 {
					case 0, 1, 2, 3, 4: // 5 pass through, 5 new events added
						responses = append(responses, transformertest.AddEventHandler("event_6_new")([]transformer.TransformerEvent{event})...)
					case 5, 6: // 2 renamed
						responses = append(responses, transformertest.RenameEventToString("event_6_renamed")([]transformer.TransformerEvent{event})...)
					case 7, 8: // 2 dropped
						responses = append(responses, transformertest.ErrorTransformerHandler(400, "dropped")([]transformer.TransformerEvent{event})...)
					case 9: // 1 filtered
						responses = append(responses, transformertest.FilteredTransformerHandler([]transformer.TransformerEvent{event})...)
					}

				}
				return responses
			}

			trServer, err := startTransformerServer(complexHandler)
			require.NoError(t, err)
			defer trServer.Close()

			url, cleanup := startRudderServer(t, db, bcserver.URL, trServer.URL)
			defer cleanup()

			err = sendEvents(10, "track", "event_6", "writekey-1", url)
			require.NoError(t, err)
			verifyMetrics(t, db.DB, "event_6", map[string]int64{
				"succeeded": 5,
				"aborted":   2,
				"filtered":  1,
				"diff":      -2,
			})
			verifyMetrics(t, db.DB, "event_6_renamed", map[string]int64{
				"succeeded": 2,
				"diff":      2,
			})
			verifyMetrics(t, db.DB, "event_6_new", map[string]int64{
				"succeeded": 5,
				"diff":      5,
			})
		})
	})
}
