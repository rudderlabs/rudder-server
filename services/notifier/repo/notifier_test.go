package repo_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/services/notifier/model"
	"github.com/rudderlabs/rudder-server/services/notifier/repo"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
)

func TestNotifierRepo(t *testing.T) {
	const (
		workspaceIdentifier = "test_workspace_identifier"
		workerID            = "test_worker"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := resource.SetupPostgres(pool, t)
	require.NoError(t, err)

	err = (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "pg_notifier_queue_migrations",
	}).Migrate("pg_notifier_queue")
	require.NoError(t, err)

	t.Log("db:", pgResource.DBDsn)

	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()

	db := sqlmiddleware.New(pgResource.DB)

	r := repo.NewNotifier(db, repo.WithNow(func() time.Time {
		return now
	}))

	request := model.PublishRequest{
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

	t.Run("Insert and get", func(t *testing.T) {
		t.Run("create notifier", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.NoError(t, err)

			notifiers, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))

			for i, notifier := range notifiers {
				require.EqualValues(t, notifier.Payload, model.Payload(fmt.Sprintf(`{"id": "%d", "sid": "1"}`, i+1)))
				require.EqualValues(t, notifier.WorkspaceIdentifier, workspaceIdentifier)
				require.EqualValues(t, notifier.BatchID, batchID)
				require.EqualValues(t, notifier.Type, request.JobType)
				require.EqualValues(t, notifier.Priority, request.Priority)
				require.EqualValues(t, notifier.Status, model.Waiting)
				require.EqualValues(t, notifier.WorkerID, "")
				require.EqualValues(t, notifier.Attempt, 0)
				require.EqualValues(t, notifier.CreatedAt.UTC(), now.UTC())
				require.EqualValues(t, notifier.UpdatedAt.UTC(), now.UTC())
				require.Nil(t, notifier.Error)
			}
		})

		t.Run("missing batch id", func(t *testing.T) {
			notifiers, err := r.GetByBatchID(ctx, "missing_batch_id")
			require.EqualError(t, err, "getting by batchID: no notifiers found")
			require.Nil(t, notifiers)
		})

		t.Run("context cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			cancel()

			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			notifiers, err := r.GetByBatchID(ctx, batchID)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, notifiers)
		})
	})

	t.Run("delete by batch id", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.NoError(t, err)

			notifiers, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))

			deletedCount, err := r.DeleteByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.EqualValues(t, deletedCount, len(request.Payloads))

			notifiers, err = r.GetByBatchID(ctx, batchID)
			require.EqualError(t, err, "getting by batchID: no notifiers found")
			require.Nil(t, notifiers)
		})

		t.Run("context cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			cancel()

			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			deletedCount, err := r.DeleteByBatchID(ctx, batchID)
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, deletedCount)
		})

		t.Run("missing batch id", func(t *testing.T) {
			deletedCount, err := r.DeleteByBatchID(ctx, "missing_batch_id")
			require.EqualError(t, err, "deleting by batchID: no rows affected")
			require.Zero(t, deletedCount)
		})
	})

	t.Run("reset workspace", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			batchID1, batchID2 := uuid.New().String(), uuid.New().String()
			workspaceIdentifier1, workspaceIdentifier2 := "test_workspace_1", "test_workspace_2"

			err := r.Insert(ctx, &request, workspaceIdentifier1, batchID1)
			require.NoError(t, err)
			err = r.Insert(ctx, &request, workspaceIdentifier2, batchID2)
			require.NoError(t, err)

			notifiers, err := r.GetByBatchID(ctx, batchID1)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))
			notifiers, err = r.GetByBatchID(ctx, batchID2)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))

			err = r.ResetForWorkspace(ctx, workspaceIdentifier1)
			require.NoError(t, err)

			notifiers, err = r.GetByBatchID(ctx, batchID1)
			require.EqualError(t, err, "getting by batchID: no notifiers found")
			require.Zero(t, notifiers, 0)
			notifiers, err = r.GetByBatchID(ctx, batchID2)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))
		})

		t.Run("context cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			cancel()

			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			err = r.ResetForWorkspace(ctx, batchID)
			require.ErrorIs(t, err, context.Canceled)
		})

		t.Run("empty", func(t *testing.T) {
			err := r.ResetForWorkspace(ctx, "missing_batch_id")
			require.NoError(t, err)
		})
	})

	t.Run("pending by batch id", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.NoError(t, err)

			notifiers, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))

			err = r.OnSuccess(ctx, &notifiers[0], model.Payload(`{"test": "payload"}`))
			require.NoError(t, err)
			err = r.OnFailed(ctx, &notifiers[1], errors.New("test error"), 100)
			require.NoError(t, err)
			err = r.OnFailed(ctx, &notifiers[2], errors.New("test error"), -1)
			require.NoError(t, err)

			pendingCount, err := r.PendingByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.EqualValues(t, pendingCount, 3)
		})

		t.Run("context cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			cancel()

			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			pendingCount, err := r.PendingByBatchID(ctx, batchID)
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, pendingCount)
		})

		t.Run("no pending", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.NoError(t, err)

			notifiers, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))

			for _, n := range notifiers {
				require.NoError(t, r.OnSuccess(ctx, &n, model.Payload(`{"test": "payload"}`)))
			}

			pendingCount, err := r.PendingByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Zero(t, pendingCount)
		})
	})

	t.Run("orphan job ids", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			batchID := uuid.New().String()
			orphanInterval := 5

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.NoError(t, err)

			notifiers, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))

			t.Run("no orphans", func(t *testing.T) {
				jobIDs, err := r.OrphanJobIDs(ctx, orphanInterval)
				require.NoError(t, err)
				require.Len(t, jobIDs, 0)
			})

			t.Run("few orphans", func(t *testing.T) {
				for _, notifier := range notifiers[:3] {
					_, err := db.ExecContext(ctx, `UPDATE pg_notifier_queue SET status = 'executing', last_exec_time = NOW() - $1 * INTERVAL '1 SECOND' WHERE id = $2;`, 2*orphanInterval, notifier.ID)
					require.NoError(t, err)
				}

				jobIDs, err := r.OrphanJobIDs(ctx, orphanInterval)
				require.NoError(t, err)
				require.Len(t, jobIDs, len(notifiers[:3]))
				for _, notifier := range notifiers[:3] {
					require.Contains(t, jobIDs, notifier.ID)
				}
			})
		})

		t.Run("context cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			cancel()

			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			jobIDs, err := r.OrphanJobIDs(ctx, 0)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, jobIDs)
		})
	})

	t.Run("claim", func(t *testing.T) {
		uNow := now.Add(time.Second * 10).Truncate(time.Second).UTC()
		ur := repo.NewNotifier(db, repo.WithNow(func() time.Time {
			return uNow
		}))

		t.Run("success", func(t *testing.T) {
			_, err := db.ExecContext(ctx, "TRUNCATE TABLE pg_notifier_queue;")
			require.NoError(t, err)

			batchID := uuid.New().String()

			err = r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.NoError(t, err)

			notifiers, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))

			for i, notifier := range notifiers {
				claimedNotifier, err := ur.Claim(ctx, workerID+strconv.Itoa(i))
				require.NoError(t, err)
				require.EqualValues(t, claimedNotifier.ID, notifier.ID)
				require.EqualValues(t, claimedNotifier.Status, model.Executing)
				require.EqualValues(t, claimedNotifier.WorkerID, workerID+strconv.Itoa(i))
				require.EqualValues(t, claimedNotifier.LastExecTime.UTC(), uNow.UTC())
			}

			claimedNotifier, err := ur.Claim(ctx, workerID)
			require.EqualError(t, err, "claiming job: scanning: sql: no rows in result set")
			require.Nil(t, claimedNotifier)
		})

		t.Run("context cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			cancel()

			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			_, err = ur.Claim(ctx, workerID)
			require.ErrorIs(t, err, context.Canceled)
		})
	})

	t.Run("claim success", func(t *testing.T) {
		uNow := now.Add(time.Second * 10).Truncate(time.Second).UTC()
		ur := repo.NewNotifier(db, repo.WithNow(func() time.Time {
			return uNow
		}))

		t.Run("success", func(t *testing.T) {
			batchID := uuid.New().String()
			payload := model.Payload(`{"test": "payload"}`)

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.NoError(t, err)

			notifiers, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))

			for _, notifier := range notifiers {
				notifier.Payload = payload
				require.NoError(t, ur.OnSuccess(ctx, &notifier, model.Payload(`{"test": "payload"}`)))
			}

			successClaims, err := ur.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))
			for _, notifier := range successClaims {
				require.EqualValues(t, notifier.UpdatedAt.UTC(), uNow.UTC())
				require.EqualValues(t, notifier.Status, model.Succeeded)
				require.EqualValues(t, notifier.Payload, payload)
				require.Nil(t, notifier.Error)
			}
		})

		t.Run("context cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			cancel()

			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			err = ur.OnSuccess(ctx, &model.Job{
				ID: 1,
			}, nil)
			require.ErrorIs(t, err, context.Canceled)
		})

		t.Run("no claims", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.NoError(t, err)

			notifiers, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))

			_, err = r.DeleteByBatchID(ctx, batchID)
			require.NoError(t, err)

			for _, n := range notifiers {
				err = ur.OnSuccess(ctx, &n, model.Payload(`{"test": "payload"}`))
				require.EqualError(t, err, "on claim success: no rows affected")
			}
		})
	})

	t.Run("claim failure", func(t *testing.T) {
		uNow := now.Add(time.Second * 10).Truncate(time.Second).UTC()
		ur := repo.NewNotifier(db, repo.WithNow(func() time.Time {
			return uNow
		}))

		t.Run("success", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.NoError(t, err)

			notifiers, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))

			for i, notifier := range notifiers {
				for j := 0; j < i+1; j++ {
					require.NoError(t, ur.OnFailed(ctx, &notifier, errors.New("test_error"), 2))
				}
			}

			failedClaims, err := ur.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))
			require.Equal(t, []string{model.Failed, model.Failed, model.Failed, model.Aborted, model.Aborted}, lo.Map(failedClaims, func(item model.Job, index int) string {
				return item.Status
			}))
			for i, notifier := range failedClaims {
				require.EqualValues(t, notifier.Error, errors.New("'test_error'"))
				require.EqualValues(t, notifier.Attempt, i+1)
				require.EqualValues(t, notifier.UpdatedAt.UTC(), uNow.UTC())
			}

			for _, notifier := range failedClaims {
				require.NoError(t, ur.OnSuccess(ctx, &notifier, model.Payload(`{"test": "payload"}`)))
			}

			successClaims, err := ur.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))
			for i, notifier := range successClaims {
				require.EqualValues(t, notifier.UpdatedAt.UTC(), uNow.UTC())
				require.EqualValues(t, notifier.Status, model.Succeeded)
				require.EqualValues(t, notifier.Attempt, i+1)
				require.EqualValues(t, notifier.Error, errors.New("'test_error'"))
			}
		})

		t.Run("context cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			cancel()

			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			err = ur.OnFailed(ctx, &model.Job{ID: 1}, errors.New("test_error"), 0)
			require.ErrorIs(t, err, context.Canceled)
		})

		t.Run("no claims", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(ctx, &request, workspaceIdentifier, batchID)
			require.NoError(t, err)

			notifiers, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, notifiers, len(request.Payloads))

			_, err = ur.DeleteByBatchID(ctx, batchID)
			require.NoError(t, err)

			for _, notifier := range notifiers {
				err := ur.OnFailed(ctx, &notifier, errors.New("test_error"), 0)
				require.EqualError(t, err, "on claim failed: no rows affected")
			}
		})
	})
}
