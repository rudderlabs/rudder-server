package notifier_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/services/notifier"
	"github.com/rudderlabs/rudder-server/services/notifier/model"
)

func TestNotifier(t *testing.T) {
	const (
		workspaceIdentifier = "test_workspace_identifier"
		workerID            = "test_worker"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := resource.SetupPostgres(pool, t, postgres.WithOptions("max_connections=200"))
	require.NoError(t, err)

	t.Log("db:", pgResource.DBDsn)

	ctx := context.Background()

	payload := &model.PublishRequest{
		Payloads: []model.Payload{
			model.Payload(`{"id":"1"}`),
			model.Payload(`{"id":"2"}`),
			model.Payload(`{"id":"3"}`),
			model.Payload(`{"id":"4"}`),
			model.Payload(`{"id":"5"}`),
		},
		JobType:  "upload",
		Schema:   json.RawMessage(`{"sid":"1"}`),
		Priority: 50,
	}

	statsStore := memstats.New()

	c := config.New()
	c.Set("PgNotifier.db.host", pgResource.Host)
	c.Set("PgNotifier.db.user", pgResource.User)
	c.Set("PgNotifier.db.name", pgResource.Database)
	c.Set("PgNotifier.db.password", pgResource.Password)
	c.Set("PgNotifier.db.port", pgResource.Port)
	c.Set("PgNotifier.maxAttempt", 1)
	c.Set("PgNotifier.maxOpenConnections", 100)
	c.Set("PgNotifier.trackBatchIntervalInS", "1s")
	c.Set("PgNotifier.maxPollSleep", "1s")
	c.Set("PgNotifier.jobOrphanTimeout", "1s")

	n, err := notifier.New(ctx, c, logger.NOP, statsStore, workspaceIdentifier,
		pgResource.DBDsn,
	)
	require.NoError(t, err)
	require.NoError(t, n.ClearJobs(ctx))

	const (
		jobs              = 120
		subscribers       = 8
		subscriberWorkers = 4
	)

	groupCtx, groupCancel := context.WithCancel(ctx)
	g, gCtx := errgroup.WithContext(groupCtx)

	responses := make(chan *model.PublishResponse)

	for i := 0; i < jobs; i++ {
		g.Go(func() error {
			publishCh, err := n.Publish(gCtx, payload)
			require.NoError(t, err)

			response, ok := <-publishCh
			require.True(t, ok)
			require.NotNil(t, response)

			responses <- response

			response, ok = <-publishCh
			require.False(t, ok)
			require.Nil(t, response)

			return nil
		})
	}
	for i := 0; i < subscribers; i++ {
		g.Go(func() error {
			subscriberCh := n.Subscribe(gCtx, workerID, subscriberWorkers)
			for j := 0; j < subscriberWorkers; j++ {
				g.Go(func() error {
					for job := range subscriberCh {
						switch job.ID % 4 {
						case 0, 1, 2:
							n.UpdateClaim(gCtx, job, &model.ClaimResponse{
								Payload: model.Payload(`{"test": "payload"}`),
							})
						case 3:
							n.UpdateClaim(gCtx, job, &model.ClaimResponse{
								Err: errors.New("test error"),
							})
						}
					}
					return nil
				})
			}
			return nil
		})

		g.Go(func() error {
			return n.RunMaintenanceWorker(gCtx)
		})
	}
	g.Go(func() error {
		return n.Wait(gCtx)
	})
	g.Go(func() error {
		failedResponses := make([]*model.Job, 0)
		succeededResponses := make([]*model.Job, 0)

		for i := 0; i < jobs; i++ {
			job := <-responses
			require.NoError(t, job.Err)

			for _, n := range job.Notifiers {
				if n.Error != nil {
					failedResponses = append(failedResponses, &n)
				} else {
					succeededResponses = append(succeededResponses, &n)
				}
			}
		}

		failedResCount := jobs * len(payload.Payloads) / 4
		succeededResCount := (3 * jobs * len(payload.Payloads)) / 4

		require.Equal(t, failedResCount, len(failedResponses))
		require.Equal(t, succeededResCount, len(succeededResponses))

		close(responses)
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
	}).LastValue(), jobs*len(payload.Payloads))
}
