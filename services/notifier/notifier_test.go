package notifier_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/samber/lo"

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
	t.Parallel()

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
			JobType:      model.JobTypeUpload,
			UploadSchema: json.RawMessage(`{"UploadSchema": "1"}`),
			Priority:     50,
		}

		statsStore := memstats.New()

		c := config.New()
		c.Set("PgNotifier.maxAttempt", 1)
		c.Set("PgNotifier.maxOpenConnections", 500)
		c.Set("PgNotifier.trackBatchIntervalInS", "1s")
		c.Set("PgNotifier.maxPollSleep", "1s")
		c.Set("PgNotifier.jobOrphanTimeout", "120s")

		groupCtx, groupCancel := context.WithCancel(ctx)
		g, gCtx := errgroup.WithContext(groupCtx)

		n := notifier.New(c, logger.NOP, statsStore, workspaceIdentifier)
		err := n.Setup(groupCtx, pgResource.DBDsn)
		require.NoError(t, err)

		const (
			totalJobs         = 12
			subscribers       = 8
			subscriberWorkers = 4
		)

		collectResponses := make(chan *model.PublishResponse)

		for i := 0; i < totalJobs; i++ {
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
			return n.RunMaintenance(gCtx)
		})
		g.Go(func() error {
			<-groupCtx.Done()
			return n.Shutdown()
		})
		g.Go(func() error {
			responses := make([]model.Job, 0, totalJobs*len(publishRequest.Payloads))

			for i := 0; i < totalJobs; i++ {
				job := <-collectResponses
				require.NoError(t, job.Err)
				require.Len(t, job.Jobs, len(publishRequest.Payloads))

				successfulJobs := lo.Filter(job.Jobs, func(item model.Job, index int) bool {
					return item.Error == nil
				})
				for _, j := range successfulJobs {
					require.EqualValues(t, j.Payload, json.RawMessage(`{"test": "payload"}`))
				}
				responses = append(responses, job.Jobs...)
			}

			successCount := (3 * totalJobs * len(publishRequest.Payloads)) / 4
			failureCount := totalJobs * len(publishRequest.Payloads) / 4

			require.Len(t, lo.Filter(responses, func(item model.Job, index int) bool {
				return item.Error == nil
			}),
				successCount,
			)
			require.Len(t, lo.Filter(responses, func(item model.Job, index int) bool {
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
		t.Parallel()

		pgResource := setup(t)
		ctx := context.Background()

		const (
			batchSize         = 1
			jobs              = 25
			subscribers       = 100
			subscriberWorkers = 4
		)

		var payloads []json.RawMessage
		for i := 0; i < batchSize; i++ {
			payloads = append(payloads, json.RawMessage(fmt.Sprintf(`{"id": "%d"}`, i)))
		}

		publishRequest := &model.PublishRequest{
			Payloads:     payloads,
			JobType:      model.JobTypeUpload,
			UploadSchema: json.RawMessage(`{"mid": "1"}`),
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

		n := notifier.New(c, logger.NOP, stats.Default, workspaceIdentifier)
		err := n.Setup(groupCtx, pgResource.DBDsn)
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
			return n.RunMaintenance(gCtx)
		})
		g.Go(func() error {
			<-groupCtx.Done()
			return n.Shutdown()
		})
		require.NoError(t, g.Wait())
	})
	t.Run("bigger batches and many subscribers", func(t *testing.T) {
		t.Parallel()

		pgResource := setup(t)
		ctx := context.Background()

		const (
			batchSize         = 500
			jobs              = 1
			subscribers       = 100
			subscriberWorkers = 4
		)

		var payloads []json.RawMessage
		for i := 0; i < batchSize; i++ {
			payloads = append(payloads, json.RawMessage(fmt.Sprintf(`{"id": "%d"}`, i)))
		}

		publishRequest := &model.PublishRequest{
			Payloads:     payloads,
			JobType:      model.JobTypeUpload,
			UploadSchema: json.RawMessage(`{"mid": "1"}`),
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

		n := notifier.New(c, logger.NOP, stats.Default, workspaceIdentifier)
		err := n.Setup(groupCtx, pgResource.DBDsn)
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
			return n.RunMaintenance(gCtx)
		})
		g.Go(func() error {
			<-groupCtx.Done()
			return n.Shutdown()
		})
		require.NoError(t, g.Wait())
	})
	t.Run("round robin pickup and maintenance workers", func(t *testing.T) {
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
			Payloads:     payloads,
			JobType:      model.JobTypeUpload,
			UploadSchema: json.RawMessage(`{"mid": "1"}`),
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

		statsStore := memstats.New()

		n := notifier.New(c, logger.NOP, statsStore, workspaceIdentifier)
		err := n.Setup(groupCtx, pgResource.DBDsn)
		require.NoError(t, err)

		publishResponses := make(chan *model.PublishResponse)

		claimedWorkers := atomic.NewInt64(0)

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

				blockSub := make(chan struct{})
				defer close(blockSub)

				for i := 0; i < subscriberWorkers; i++ {
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
			return n.RunMaintenance(gCtx)
		})
		g.Go(func() error {
			<-groupCtx.Done()
			return n.Shutdown()
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

		n := notifier.New(c, logger.NOP, stats.Default, workspaceIdentifier)
		err = n.Setup(ctx, pgResource.DBDsn)
		require.NoError(t, err)
	})
}
