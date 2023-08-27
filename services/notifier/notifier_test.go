package notifier_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-server/services/notifier"
	"github.com/rudderlabs/rudder-server/services/notifier/model"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
)

func setup(t testing.TB) *resource.PostgresResource {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := resource.SetupPostgres(pool, t, postgres.WithOptions("max_connections=1000"))
	require.NoError(t, err)

	t.Log("db:", pgResource.DBDsn)

	return pgResource
}

func TestNotifier(t *testing.T) {
	const (
		workspaceIdentifier = "test_workspace_identifier"
		workerID            = "test_worker"
	)

	t.Run("basic workflow", func(t *testing.T) {
		t.Parallel()

		pgResource := setup(t)
		ctx := context.Background()

		publishRequest := &model.PublishRequest{
			Payloads: []json.RawMessage{
				json.RawMessage(`{"id":"1"}`),
				json.RawMessage(`{"id":"2"}`),
				json.RawMessage(`{"id":"3"}`),
				json.RawMessage(`{"id":"4"}`),
				json.RawMessage(`{"id":"5"}`),
			},
			JobType:         model.JobTypeUpload,
			PayloadMetadata: json.RawMessage(`{"mid": "1"}`),
			Priority:        50,
		}

		statsStore := memstats.New()

		c := config.New()
		c.Set("PgNotifier.maxAttempt", 1)
		c.Set("PgNotifier.maxOpenConnections", 500)
		c.Set("PgNotifier.trackBatchIntervalInS", "1s")
		c.Set("PgNotifier.maxPollSleep", "1s")
		c.Set("PgNotifier.jobOrphanTimeout", "30s")

		groupCtx, groupCancel := context.WithCancel(ctx)
		g, gCtx := errgroup.WithContext(groupCtx)

		n, err := notifier.New(groupCtx, c, logger.NOP, statsStore, workspaceIdentifier,
			pgResource.DBDsn,
		)
		require.NoError(t, err)

		const (
			jobs              = 64
			subscribers       = 32
			subscriberWorkers = 4
		)

		publishResponses := make(chan *model.PublishResponse)

		for i := 0; i < jobs; i++ {
			g.Go(func() error {
				publishCh, err := n.Publish(gCtx, publishRequest)
				require.NoError(t, err)

				response, ok := <-publishCh
				require.True(t, ok)
				require.NotNil(t, response)

				publishResponses <- response

				response, ok = <-publishCh
				require.False(t, ok)
				require.Nil(t, response)

				return nil
			})
		}
		for i := 0; i < subscribers; i++ {
			g.Go(func() error {
				subscriberCh := n.Subscribe(gCtx, workerID, subscriberWorkers)

				slaveGroup, slaveCtx := errgroup.WithContext(gCtx)

				for j := 0; j < subscriberWorkers; j++ {
					slaveGroup.Go(func() error {
						for job := range subscriberCh {
							switch job.Job.ID % 4 {
							case 0, 1, 2:
								n.UpdateClaim(slaveCtx, job, &model.ClaimJobResponse{
									Payload: json.RawMessage(`{"test": "payload"}`),
								})
							case 3:
								n.UpdateClaim(slaveCtx, job, &model.ClaimJobResponse{
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
			return n.RunMaintenanceWorker(gCtx)
		})
		g.Go(func() error {
			return n.Wait()
		})
		g.Go(func() error {
			failedResponses := make([]*model.Job, 0)
			succeededResponses := make([]*model.Job, 0)

			for i := 0; i < jobs; i++ {
				job := <-publishResponses
				require.NoError(t, job.Err)
				require.EqualValues(t, job.JobMetadata, model.JobMetadata(publishRequest.PayloadMetadata))

				for _, n := range job.Jobs {
					if n.Error != nil {
						failedResponses = append(failedResponses, &n)
					} else {
						succeededResponses = append(succeededResponses, &n)
					}
				}
			}

			failedResCount := jobs * len(publishRequest.Payloads) / 4
			succeededResCount := (3 * jobs * len(publishRequest.Payloads)) / 4

			require.Equal(t, failedResCount, len(failedResponses))
			require.Equal(t, succeededResCount, len(succeededResponses))

			close(publishResponses)
			groupCancel()

			return nil
		})
		require.NoError(t, g.Wait())
		require.NoError(t, n.ClearJobs(ctx))
		require.EqualValues(t, statsStore.Get("pgnotifier.publish", stats.Tags{
			"module": "pgnotifier",
		}).LastValue(), jobs)
		require.EqualValues(t, statsStore.Get("pg_notifier_insert_records", stats.Tags{
			"module":    "pgnotifier",
			"queueName": "pg_notifier_queue",
		}).LastValue(), jobs*len(publishRequest.Payloads))
	})

	t.Run("bigger batches and many subscribers", func(t *testing.T) {
		t.Parallel()

		pgResource := setup(t)
		ctx := context.Background()

		const (
			batchSize         = 1000
			jobs              = 1
			subscribers       = 100
			subscriberWorkers = 10
		)

		var payloads []json.RawMessage
		for i := 0; i < batchSize; i++ {
			payloads = append(payloads, json.RawMessage(fmt.Sprintf(`{"id": "%d"}`, i)))
		}

		publishRequest := &model.PublishRequest{
			Payloads:        payloads,
			JobType:         model.JobTypeUpload,
			PayloadMetadata: json.RawMessage(`{"mid": "1"}`),
			Priority:        50,
		}

		c := config.New()
		c.Set("PgNotifier.maxAttempt", 1)
		c.Set("PgNotifier.maxOpenConnections", 900)
		c.Set("PgNotifier.trackBatchIntervalInS", "1s")
		c.Set("PgNotifier.maxPollSleep", "100ms")
		c.Set("PgNotifier.jobOrphanTimeout", "30s")

		groupCtx, groupCancel := context.WithCancel(ctx)
		g, gCtx := errgroup.WithContext(groupCtx)

		n, err := notifier.New(groupCtx, c, logger.NOP, stats.Default, workspaceIdentifier,
			pgResource.DBDsn,
		)
		require.NoError(t, err)

		publishResponses := make(chan *model.PublishResponse)

		for i := 0; i < jobs; i++ {
			g.Go(func() error {
				publishCh, err := n.Publish(gCtx, publishRequest)
				require.NoError(t, err)

				publishResponses <- <-publishCh

				return nil
			})
		}

		for i := 0; i < subscribers; i++ {
			g.Go(func() error {
				subscriberCh := n.Subscribe(gCtx, workerID, subscriberWorkers)

				slaveGroup, slaveCtx := errgroup.WithContext(gCtx)

				for j := 0; j < subscriberWorkers; j++ {
					slaveGroup.Go(func() error {
						for job := range subscriberCh {
							n.UpdateClaim(slaveCtx, job, &model.ClaimJobResponse{
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
			for i := 0; i < jobs; i++ {
				response := <-publishResponses
				require.NoError(t, response.Err)
				require.Len(t, response.Jobs, len(publishRequest.Payloads))
			}
			close(publishResponses)
			groupCancel()
			return nil
		})
		g.Go(func() error {
			return n.RunMaintenanceWorker(gCtx)
		})
		g.Go(func() error {
			return n.Wait()
		})
		require.NoError(t, g.Wait())
	})

	t.Run("round robin and maintenance workers", func(t *testing.T) {
		t.Parallel()

		pgResource := setup(t)
		ctx := context.Background()

		const (
			batchSize         = 1
			jobs              = 1
			subscribers       = 1
			subscriberWorkers = 4
		)

		var payloads []json.RawMessage
		for i := 0; i < batchSize; i++ {
			payloads = append(payloads, json.RawMessage(fmt.Sprintf(`{"id": "%d"}`, i)))
		}

		publishRequest := &model.PublishRequest{
			Payloads:        payloads,
			JobType:         model.JobTypeUpload,
			PayloadMetadata: json.RawMessage(`{"mid": "1"}`),
			Priority:        50,
		}

		c := config.New()
		c.Set("PgNotifier.maxAttempt", 1)
		c.Set("PgNotifier.maxOpenConnections", 900)
		c.Set("PgNotifier.trackBatchIntervalInS", "1s")
		c.Set("PgNotifier.maxPollSleep", "100ms")
		c.Set("PgNotifier.jobOrphanTimeout", "5s")

		groupCtx, groupCancel := context.WithCancel(ctx)
		g, gCtx := errgroup.WithContext(groupCtx)

		n, err := notifier.New(groupCtx, c, logger.NOP, stats.Default, workspaceIdentifier,
			pgResource.DBDsn,
		)
		require.NoError(t, err)

		publishResponses := make(chan *model.PublishResponse)

		for i := 0; i < jobs; i++ {
			g.Go(func() error {
				publishCh, err := n.Publish(gCtx, publishRequest)
				require.NoError(t, err)

				publishResponses <- <-publishCh

				return nil
			})
		}

		for i := 0; i < subscribers; i++ {
			g.Go(func() error {
				subscriberCh := n.Subscribe(gCtx, workerID, subscriberWorkers)

				slaveGroup, slaveCtx := errgroup.WithContext(gCtx)
				claimedWorkers := atomic.NewInt64(0)
				blockSub := make(chan struct{})

				for i := 0; i < subscriberWorkers; i++ {
					slaveGroup.Go(func() error {
						for job := range subscriberCh {
							claimedWorkers.Add(1)

							if claimedWorkers.Load() < subscriberWorkers {
								select {
								case blockSub <- struct{}{}:
								case <-slaveCtx.Done():
								}
							}

							n.UpdateClaim(slaveCtx, job, &model.ClaimJobResponse{
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
			for i := 0; i < jobs; i++ {
				response := <-publishResponses
				require.NoError(t, response.Err)
				require.Len(t, response.Jobs, len(publishRequest.Payloads))
			}
			close(publishResponses)
			groupCancel()
			return nil
		})
		g.Go(func() error {
			return n.RunMaintenanceWorker(gCtx)
		})
		g.Go(func() error {
			return n.Wait()
		})
		require.NoError(t, g.Wait())
	})

	t.Run("env vars", func(t *testing.T) {
		t.Parallel()

		pgResource := setup(t)
		ctx := context.Background()

		port, err := strconv.Atoi(pgResource.Port)
		require.NoError(t, err)

		c := config.New()
		c.Set("PGNOTIFIER_DB_HOST", pgResource.Host)
		c.Set("PGNOTIFIER_DB_USER", pgResource.User)
		c.Set("PGNOTIFIER_DB_NAME", pgResource.Database)
		c.Set("PGNOTIFIER_DB_PORT", port)
		c.Set("PGNOTIFIER_DB_PASSWORD", pgResource.Password)

		_, err = notifier.New(ctx, c, logger.NOP, stats.Default, workspaceIdentifier,
			"postgres://localhost:5432/invalid",
		)
		require.NoError(t, err)
	})
}
