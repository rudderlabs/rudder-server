package notifier_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"

	"github.com/rudderlabs/rudder-server/services/notifier"
)

func TestNotifier(t *testing.T) {
	const (
		workspaceIdentifier = "test_workspace_identifier"
		workerID            = "test_worker"
	)

	t.Run("clear jobs", func(t *testing.T) {
		db, ctx := setupDB(t), context.Background()

		publishRequest := &notifier.PublishRequest{
			Payloads: []json.RawMessage{
				json.RawMessage(`{"id":"1"}`),
				json.RawMessage(`{"id":"2"}`),
				json.RawMessage(`{"id":"3"}`),
				json.RawMessage(`{"id":"4"}`),
				json.RawMessage(`{"id":"5"}`),
			},
			JobType:      notifier.JobTypeUploadV2,
			UploadSchema: json.RawMessage(`{"UploadSchema": "1"}`),
			Priority:     50,
		}

		count := func(t testing.TB) int {
			t.Helper()

			var count int
			err := db.DB.QueryRowContext(ctx, `
				SELECT
				  COUNT(*)
				FROM
				  pg_notifier_queue;
			`).Scan(&count)
			require.NoError(t, err)
			return count
		}

		statsStore, err := memstats.New()
		require.NoError(t, err)

		notifierWithIdentifier := notifier.New(config.New(), logger.NOP, statsStore, workspaceIdentifier, true)
		require.NoError(t, notifierWithIdentifier.Setup(ctx, db.DBDsn))
		notifierWithoutIdentifier := notifier.New(config.New(), logger.NOP, statsStore, "", true)
		require.NoError(t, notifierWithoutIdentifier.Setup(ctx, db.DBDsn))

		t.Run("without workspace identifier", func(t *testing.T) {
			_, err := notifierWithIdentifier.Publish(ctx, publishRequest)
			require.NoError(t, err)
			require.Equal(t, len(publishRequest.Payloads), count(t))
			require.NoError(t, notifierWithoutIdentifier.ClearJobs(ctx))
			require.Equal(t, len(publishRequest.Payloads), count(t))
			require.NoError(t, notifierWithIdentifier.ClearJobs(ctx))
			require.Zero(t, count(t))
		})
		t.Run("with workspace identifier", func(t *testing.T) {
			_, err := notifierWithoutIdentifier.Publish(ctx, publishRequest)
			require.NoError(t, err)
			require.Equal(t, len(publishRequest.Payloads), count(t))
			require.NoError(t, notifierWithIdentifier.ClearJobs(ctx))
			require.Equal(t, len(publishRequest.Payloads), count(t))
			require.NoError(t, notifierWithoutIdentifier.ClearJobs(ctx))
			require.Equal(t, len(publishRequest.Payloads), count(t))
		})
		t.Run("context cancelled", func(t *testing.T) {
			cancelledCtx, cancel := context.WithCancel(ctx)
			cancel()
			require.ErrorIs(t, notifierWithIdentifier.ClearJobs(cancelledCtx), context.Canceled)
		})
	})
	t.Run("health check", func(t *testing.T) {
		db, ctx := setupDB(t), context.Background()

		t.Run("success", func(t *testing.T) {
			statsStore, err := memstats.New()
			require.NoError(t, err)

			n := notifier.New(config.New(), logger.NOP, statsStore, workspaceIdentifier, true)
			require.NoError(t, n.Setup(ctx, db.DBDsn))
			require.True(t, n.CheckHealth(ctx))
		})
		t.Run("context cancelled", func(t *testing.T) {
			cancelledCtx, cancel := context.WithCancel(ctx)
			cancel()

			statsStore, err := memstats.New()
			require.NoError(t, err)

			n := notifier.New(config.New(), logger.NOP, statsStore, workspaceIdentifier, true)
			require.NoError(t, n.Setup(ctx, db.DBDsn))
			require.False(t, n.CheckHealth(cancelledCtx))
		})
	})
	t.Run("basic workflow", func(t *testing.T) {
		db, ctx := setupDB(t), context.Background()

		publishRequest := &notifier.PublishRequest{
			Payloads: []json.RawMessage{
				json.RawMessage(`{"id":"1"}`),
				json.RawMessage(`{"id":"2"}`),
				json.RawMessage(`{"id":"3"}`),
				json.RawMessage(`{"id":"4"}`),
				json.RawMessage(`{"id":"5"}`),
			},
			JobType:      notifier.JobTypeUploadV2,
			UploadSchema: json.RawMessage(`{"UploadSchema": "1"}`),
			Priority:     50,
		}

		statsStore, err := memstats.New()
		require.NoError(t, err)

		c := config.New()
		c.Set("PgNotifier.maxAttempt", 1)
		c.Set("PgNotifier.maxOpenConnections", 500)
		c.Set("PgNotifier.trackBatchIntervalInS", "1s")
		c.Set("PgNotifier.maxPollSleep", "1s")
		c.Set("PgNotifier.jobOrphanTimeout", "120s")

		groupCtx, groupCancel := context.WithCancel(ctx)
		g, gCtx := errgroup.WithContext(groupCtx)

		n := notifier.New(c, logger.NOP, statsStore, workspaceIdentifier, true)
		require.NoError(t, n.Setup(groupCtx, db.DBDsn))

		const (
			totalJobs         = 12
			subscribers       = 8
			subscriberWorkers = 4
		)

		collectResponses := make(chan *notifier.PublishResponse)

		for range totalJobs {
			g.Go(func() error {
				publishCh, err := n.Publish(gCtx, publishRequest)
				require.NoError(t, err)

				response, ok := <-publishCh
				require.True(t, ok)
				require.NotNil(t, response)

				collectResponses <- response

				response, ok = <-publishCh
				require.False(t, ok)
				require.Nil(t, response)

				return nil
			})
		}
		for range subscribers {
			g.Go(func() error {
				subscriberCh := n.Subscribe(gCtx, workerID, subscriberWorkers)

				slaveGroup, slaveCtx := errgroup.WithContext(gCtx)

				for range subscriberWorkers {
					slaveGroup.Go(func() error {
						for job := range subscriberCh {
							switch job.Job.ID % 4 {
							case 0, 1, 2:
								n.UpdateClaim(slaveCtx, job, &notifier.ClaimJobResponse{
									Payload: json.RawMessage(`{"test": "payload"}`),
								})
							case 3:
								n.UpdateClaim(slaveCtx, job, &notifier.ClaimJobResponse{
									Err: errors.New("test error"),
								})
							}
						}
						return nil
					})
				}
				return slaveGroup.Wait()
			})
		}
		g.Go(func() error {
			return n.RunMaintenance(gCtx)
		})
		g.Go(func() error {
			<-groupCtx.Done()
			return n.Shutdown()
		})
		g.Go(func() error {
			responses := make([]notifier.Job, 0, totalJobs*len(publishRequest.Payloads))

			for range totalJobs {
				job := <-collectResponses
				require.NoError(t, job.Err)
				require.Len(t, job.Jobs, len(publishRequest.Payloads))

				successfulJobs := lo.Filter(job.Jobs, func(item notifier.Job, index int) bool {
					return item.Error == nil
				})
				for _, j := range successfulJobs {
					require.EqualValues(t, j.Payload, json.RawMessage(`{"test": "payload"}`))
				}
				responses = append(responses, job.Jobs...)
			}

			successCount := (3 * totalJobs * len(publishRequest.Payloads)) / 4
			failureCount := totalJobs * len(publishRequest.Payloads) / 4

			require.Len(t, lo.Filter(responses, func(item notifier.Job, index int) bool {
				return item.Error == nil
			}),
				successCount,
			)
			require.Len(t, lo.Filter(responses, func(item notifier.Job, index int) bool {
				return item.Error != nil
			}),
				failureCount,
			)

			close(collectResponses)
			groupCancel()

			return nil
		})
		require.NoError(t, g.Wait())
		require.NoError(t, n.ClearJobs(ctx))
		require.EqualValues(t, statsStore.Get("pgnotifier.publish", stats.Tags{
			"module": "pgnotifier",
		}).LastValue(), totalJobs)
		require.EqualValues(t, statsStore.Get("pg_notifier.insert_records", stats.Tags{
			"module":    "pg_notifier",
			"queueName": "pg_notifier_queue",
		}).LastValue(), totalJobs*len(publishRequest.Payloads))
	})
	t.Run("many publish jobs", func(t *testing.T) {
		db, ctx := setupDB(t), context.Background()

		const (
			batchSize         = 1
			jobs              = 25
			subscribers       = 100
			subscriberWorkers = 4
		)

		var payloads []json.RawMessage
		for i := range batchSize {
			payloads = append(payloads, json.RawMessage(fmt.Sprintf(`{"id": "%d"}`, i)))
		}

		publishRequest := &notifier.PublishRequest{
			Payloads:     payloads,
			JobType:      notifier.JobTypeUploadV2,
			UploadSchema: json.RawMessage(`{"UploadSchema": "1"}`),
			Priority:     50,
		}

		c := config.New()
		c.Set("PgNotifier.maxAttempt", 1)
		c.Set("PgNotifier.maxOpenConnections", 900)
		c.Set("PgNotifier.trackBatchIntervalInS", "1s")
		c.Set("PgNotifier.maxPollSleep", "100ms")
		c.Set("PgNotifier.jobOrphanTimeout", "120s")

		groupCtx, groupCancel := context.WithCancel(ctx)
		g, gCtx := errgroup.WithContext(groupCtx)

		statsStore, err := memstats.New()
		require.NoError(t, err)

		n := notifier.New(c, logger.NOP, statsStore, workspaceIdentifier, true)
		require.NoError(t, n.Setup(groupCtx, db.DBDsn))

		publishResponses := make(chan *notifier.PublishResponse)

		for range jobs {
			g.Go(func() error {
				publishCh, err := n.Publish(gCtx, publishRequest)
				require.NoError(t, err)
				publishResponses <- <-publishCh
				return nil
			})
		}

		for range subscribers {
			g.Go(func() error {
				subscriberCh := n.Subscribe(gCtx, workerID, subscriberWorkers)
				slaveGroup, slaveCtx := errgroup.WithContext(gCtx)
				for range subscriberWorkers {
					slaveGroup.Go(func() error {
						for job := range subscriberCh {
							n.UpdateClaim(slaveCtx, job, &notifier.ClaimJobResponse{
								Payload: json.RawMessage(`{"test": "payload"}`),
							})
						}
						return nil
					})
				}
				return slaveGroup.Wait()
			})
		}
		g.Go(func() error {
			for range jobs {
				response := <-publishResponses
				require.NoError(t, response.Err)
				require.Len(t, response.Jobs, len(publishRequest.Payloads))
			}
			close(publishResponses)
			groupCancel()
			return nil
		})
		g.Go(func() error {
			return n.RunMaintenance(gCtx)
		})
		g.Go(func() error {
			<-groupCtx.Done()
			return n.Shutdown()
		})
		require.NoError(t, g.Wait())
	})
	t.Run("bigger batches and many subscribers", func(t *testing.T) {
		db, ctx := setupDB(t), context.Background()

		const (
			batchSize         = 500
			jobs              = 1
			subscribers       = 100
			subscriberWorkers = 4
		)

		var payloads []json.RawMessage
		for i := range batchSize {
			payloads = append(payloads, json.RawMessage(fmt.Sprintf(`{"id": "%d"}`, i)))
		}

		publishRequest := &notifier.PublishRequest{
			Payloads:     payloads,
			JobType:      notifier.JobTypeUploadV2,
			UploadSchema: json.RawMessage(`{"UploadSchema": "1"}`),
			Priority:     50,
		}

		c := config.New()
		c.Set("PgNotifier.maxAttempt", 1)
		c.Set("PgNotifier.maxOpenConnections", 900)
		c.Set("PgNotifier.trackBatchIntervalInS", "1s")
		c.Set("PgNotifier.maxPollSleep", "100ms")
		c.Set("PgNotifier.jobOrphanTimeout", "120s")

		groupCtx, groupCancel := context.WithCancel(ctx)
		g, gCtx := errgroup.WithContext(groupCtx)

		statsStore, err := memstats.New()
		require.NoError(t, err)

		n := notifier.New(c, logger.NOP, statsStore, workspaceIdentifier, true)
		require.NoError(t, n.Setup(groupCtx, db.DBDsn))

		publishResponses := make(chan *notifier.PublishResponse)

		for range jobs {
			g.Go(func() error {
				publishCh, err := n.Publish(gCtx, publishRequest)
				require.NoError(t, err)
				publishResponses <- <-publishCh
				return nil
			})
		}
		for range subscribers {
			g.Go(func() error {
				subscriberCh := n.Subscribe(gCtx, workerID, subscriberWorkers)
				slaveGroup, slaveCtx := errgroup.WithContext(gCtx)
				for range subscriberWorkers {
					slaveGroup.Go(func() error {
						for job := range subscriberCh {
							n.UpdateClaim(slaveCtx, job, &notifier.ClaimJobResponse{
								Payload: json.RawMessage(`{"test": "payload"}`),
							})
						}
						return nil
					})
				}
				return slaveGroup.Wait()
			})
		}
		g.Go(func() error {
			for range jobs {
				response := <-publishResponses
				require.NoError(t, response.Err)
				require.Len(t, response.Jobs, len(publishRequest.Payloads))
			}
			close(publishResponses)
			groupCancel()
			return nil
		})
		g.Go(func() error {
			return n.RunMaintenance(gCtx)
		})
		g.Go(func() error {
			<-groupCtx.Done()
			return n.Shutdown()
		})
		require.NoError(t, g.Wait())
	})
	t.Run("round robin pickup and maintenance workers", func(t *testing.T) {
		db, ctx := setupDB(t), context.Background()

		const (
			batchSize         = 1
			jobs              = 1
			subscribers       = 1
			subscriberWorkers = 4
		)

		var payloads []json.RawMessage
		for i := range batchSize {
			payloads = append(payloads, json.RawMessage(fmt.Sprintf(`{"id": "%d"}`, i)))
		}

		publishRequest := &notifier.PublishRequest{
			Payloads:     payloads,
			JobType:      notifier.JobTypeUploadV2,
			UploadSchema: json.RawMessage(`{"UploadSchema": "1"}`),
			Priority:     50,
		}

		c := config.New()
		c.Set("PgNotifier.maxAttempt", 1)
		c.Set("PgNotifier.maxOpenConnections", 900)
		c.Set("PgNotifier.trackBatchIntervalInS", "1s")
		c.Set("PgNotifier.maxPollSleep", "100ms")
		c.Set("PgNotifier.jobOrphanTimeout", "3s")

		groupCtx, groupCancel := context.WithCancel(ctx)
		g, gCtx := errgroup.WithContext(groupCtx)

		statsStore, err := memstats.New()
		require.NoError(t, err)

		n := notifier.New(c, logger.NOP, statsStore, workspaceIdentifier, true)
		require.NoError(t, n.Setup(groupCtx, db.DBDsn))

		publishResponses := make(chan *notifier.PublishResponse)
		claimedWorkers := atomic.NewInt64(0)

		for range jobs {
			g.Go(func() error {
				publishCh, err := n.Publish(gCtx, publishRequest)
				require.NoError(t, err)
				publishResponses <- <-publishCh
				return nil
			})
		}

		for range subscribers {
			g.Go(func() error {
				subscriberCh := n.Subscribe(gCtx, workerID, subscriberWorkers)
				slaveGroup, slaveCtx := errgroup.WithContext(gCtx)
				blockSub := make(chan struct{})
				defer close(blockSub)
				for range subscriberWorkers {
					slaveGroup.Go(func() error {
						for job := range subscriberCh {
							claimedWorkers.Add(1)
							if claimedWorkers.Load() < subscriberWorkers {
								select {
								case <-blockSub:
								case <-slaveCtx.Done():
									return nil
								}
							}
							n.UpdateClaim(slaveCtx, job, &notifier.ClaimJobResponse{
								Payload: json.RawMessage(`{"test": "payload"}`),
							})
						}
						return nil
					})
				}
				return slaveGroup.Wait()
			})
		}
		g.Go(func() error {
			for range jobs {
				response := <-publishResponses
				require.NoError(t, response.Err)
				require.Len(t, response.Jobs, len(publishRequest.Payloads))
			}
			close(publishResponses)
			groupCancel()
			return nil
		})
		g.Go(func() error {
			return n.RunMaintenance(gCtx)
		})
		g.Go(func() error {
			<-groupCtx.Done()
			return n.Shutdown()
		})
		require.NoError(t, g.Wait())
	})
	t.Run("env vars", func(t *testing.T) {
		db, ctx := setupDB(t), context.Background()

		c := config.New()
		c.Set("PGNOTIFIER_DB_HOST", db.Host)
		c.Set("PGNOTIFIER_DB_USER", db.User)
		c.Set("PGNOTIFIER_DB_NAME", db.Database)
		c.Set("PGNOTIFIER_DB_PORT", db.Port)
		c.Set("PGNOTIFIER_DB_PASSWORD", db.Password)

		statsStore, err := memstats.New()
		require.NoError(t, err)

		n := notifier.New(c, logger.NOP, statsStore, workspaceIdentifier, true)
		err = n.Setup(ctx, db.DBDsn)
		require.NoError(t, err)
	})
	t.Run("maintenance workflow", func(t *testing.T) {
		db, ctx := setupDB(t), context.Background()
		c := config.New()
		c.Set("PgNotifier.jobOrphanTimeout", "1s")
		g, _ := errgroup.WithContext(ctx)
		g.Go(func() error {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			statsStore, err := memstats.New()
			require.NoError(t, err)
			n := notifier.New(c, logger.NOP, statsStore, workspaceIdentifier, true)
			require.NoError(t, n.Setup(ctx, db.DBDsn))
			require.NoError(t, n.RunMaintenance(ctxWithTimeout))
			return nil
		})
		g.Go(func() error {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			statsStore, err := memstats.New()
			require.NoError(t, err)

			n := notifier.New(c, logger.NOP, statsStore, workspaceIdentifier, true)
			require.NoError(t, n.Setup(ctx, db.DBDsn))
			require.NoError(t, n.RunMaintenance(ctxWithTimeout))
			return nil
		})
		require.NoError(t, g.Wait())
	})
}

func setupDB(t testing.TB) *postgres.Resource {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	db, err := postgres.Setup(pool, t, postgres.WithOptions("max_connections=1000"))
	require.NoError(t, err)
	return db
}
