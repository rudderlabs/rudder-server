package repo_test

import (
	"context"
	"database/sql"
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

	publishRequest := model.PublishRequest{
		Payloads: []json.RawMessage{
			json.RawMessage(`{"id":"1"}`),
			json.RawMessage(`{"id":"2"}`),
			json.RawMessage(`{"id":"3"}`),
			json.RawMessage(`{"id":"4"}`),
			json.RawMessage(`{"id":"5"}`),
		},
		JobType:         model.JobTypeUpload,
		PayloadMetadata: json.RawMessage(`{"mid":"1"}`),
		Priority:        50,
	}

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()

	t.Run("Insert and get", func(t *testing.T) {
		t.Run("create notifier", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, jobMetadata, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, jobs, len(publishRequest.Payloads))
			require.EqualValues(t, jobMetadata, json.RawMessage(`{"mid": "1"}`))

			for i, job := range jobs {
				require.EqualValues(t, job.Payload, json.RawMessage(fmt.Sprintf(`{"id": "%d"}`, i+1)))
				require.EqualValues(t, job.WorkspaceIdentifier, workspaceIdentifier)
				require.EqualValues(t, job.BatchID, batchID)
				require.EqualValues(t, job.Type, publishRequest.JobType)
				require.EqualValues(t, job.Priority, publishRequest.Priority)
				require.EqualValues(t, job.Status, model.Waiting)
				require.EqualValues(t, job.WorkerID, "")
				require.EqualValues(t, job.Attempt, 0)
				require.EqualValues(t, job.CreatedAt.UTC(), now.UTC())
				require.EqualValues(t, job.UpdatedAt.UTC(), now.UTC())
				require.Nil(t, job.Error)
			}
		})

		t.Run("missing batch id", func(t *testing.T) {
			jobs, jobMetadata, err := r.GetByBatchID(ctx, "missing_batch_id")
			require.EqualError(t, err, "getting by batchID: no jobs found")
			require.Nil(t, jobs)
			require.Nil(t, jobMetadata)
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			jobs, jobMetadata, err := r.GetByBatchID(cancelledCtx, batchID)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, jobs)
			require.Nil(t, jobMetadata)
		})
	})

	t.Run("delete by batch id", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, jobMetadata, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, jobs, len(publishRequest.Payloads))
			require.EqualValues(t, jobMetadata, json.RawMessage(`{"mid": "1"}`))

			err = r.DeleteByBatchID(ctx, batchID)
			require.NoError(t, err)

			jobs, jobMetadata, err = r.GetByBatchID(ctx, batchID)
			require.EqualError(t, err, "getting by batchID: no jobs found")
			require.Nil(t, jobs)
			require.Nil(t, jobMetadata)
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			err = r.DeleteByBatchID(cancelledCtx, batchID)
			require.ErrorIs(t, err, context.Canceled)
		})
	})

	t.Run("reset workspace", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			var workspaceIdentifiers []string
			var batchIDs []string

			for i := 0; i < 10; i++ {
				batchID := uuid.New().String()
				workspaceIdentifier := workspaceIdentifier + "_" + uuid.New().String()
				workspaceIdentifiers = append(workspaceIdentifiers, workspaceIdentifier)

				err := r.Insert(ctx, &publishRequest, workspaceIdentifier, batchID)
				require.NoError(t, err)
			}
			for _, batchID := range batchIDs {
				jobs, jobMetadata, err := r.GetByBatchID(ctx, batchID)
				require.NoError(t, err)
				require.Len(t, jobs, len(publishRequest.Payloads))
				require.EqualValues(t, jobMetadata, json.RawMessage(`{"mid": "1"}`))
			}

			for i, workspaceIdentifier := range workspaceIdentifiers {
				if i%4 == 0 {
					continue
				}

				err = r.ResetForWorkspace(ctx, workspaceIdentifier)
				require.NoError(t, err)
			}

			for i, batchID := range batchIDs {
				if i%4 == 0 {
					jobs, jobMetadata, err := r.GetByBatchID(ctx, batchID)
					require.EqualError(t, err, "getting by batchID: no jobs found")
					require.Nil(t, jobs)
					require.Nil(t, jobMetadata)
					continue
				}

				jobs, jobMetadata, err := r.GetByBatchID(ctx, batchID)
				require.NoError(t, err)
				require.Len(t, jobs, len(publishRequest.Payloads))
				require.EqualValues(t, jobMetadata, json.RawMessage(`{"mid": "1"}`))
			}
		})

		t.Run("without metadata", func(t *testing.T) {
			publishRequest := model.PublishRequest{
				Payloads: []json.RawMessage{
					json.RawMessage(`{"id":"11"}`),
				},
				JobType:  model.JobTypeUpload,
				Priority: 75,
			}

			batchID := uuid.New().String()
			err := r.Insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			err = r.ResetForWorkspace(ctx, workspaceIdentifier)
			require.NoError(t, err)

			jobs, jobMetadata, err := r.GetByBatchID(ctx, batchID)
			require.EqualError(t, err, "getting by batchID: no jobs found")
			require.Nil(t, jobs)
			require.Nil(t, jobMetadata)
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			err = r.ResetForWorkspace(cancelledCtx, batchID)
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

			err := r.Insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, _, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, jobs, len(publishRequest.Payloads))

			err = r.OnClaimSuccess(ctx, &jobs[0], json.RawMessage(`{"test": "payload"}`))
			require.NoError(t, err)
			err = r.OnClaimFailed(ctx, &jobs[1], errors.New("test error"), 100)
			require.NoError(t, err)
			err = r.OnClaimFailed(ctx, &jobs[2], errors.New("test error"), -1)
			require.NoError(t, err)

			pendingCount, err := r.PendingByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.EqualValues(t, pendingCount, 3)
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			pendingCount, err := r.PendingByBatchID(cancelledCtx, batchID)
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, pendingCount)
		})

		t.Run("no pending", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, _, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)

			for _, job := range jobs {
				require.NoError(t, r.OnClaimSuccess(ctx, &job, json.RawMessage(`{"test": "payload"}`)))
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

			err := r.Insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, _, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)

			t.Run("no orphans", func(t *testing.T) {
				jobIDs, err := r.OrphanJobIDs(ctx, orphanInterval)
				require.NoError(t, err)
				require.Len(t, jobIDs, 0)
			})

			t.Run("few orphans", func(t *testing.T) {
				for _, notifier := range jobs[:3] {
					_, err := db.ExecContext(ctx, `UPDATE pg_notifier_queue SET status = 'executing', last_exec_time = NOW() - $1 * INTERVAL '1 SECOND' WHERE id = $2;`, 2*orphanInterval, notifier.ID)
					require.NoError(t, err)
				}

				jobIDs, err := r.OrphanJobIDs(ctx, orphanInterval)
				require.NoError(t, err)
				require.Len(t, jobIDs, len(jobs[:3]))
				for _, notifier := range jobs[:3] {
					require.Contains(t, jobIDs, notifier.ID)
				}
			})
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			jobIDs, err := r.OrphanJobIDs(cancelledCtx, 0)
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
			_, err := db.ExecContext(ctx, "TRUNCATE TABLE pg_notifier_queue; TRUNCATE TABLE pg_notifier_queue_metadata;")
			require.NoError(t, err)

			batchID := uuid.New().String()

			err = r.Insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			t.Run("with jobs", func(t *testing.T) {
				jobs, _, err := r.GetByBatchID(ctx, batchID)
				require.NoError(t, err)

				for i, job := range jobs {
					claimedNotifier, metadata, err := ur.Claim(ctx, workerID+strconv.Itoa(i))
					require.NoError(t, err)
					require.EqualValues(t, metadata, json.RawMessage(`{"mid": "1"}`))
					require.EqualValues(t, claimedNotifier.ID, job.ID)
					require.EqualValues(t, claimedNotifier.Status, model.Executing)
					require.EqualValues(t, claimedNotifier.WorkerID, workerID+strconv.Itoa(i))
					require.EqualValues(t, claimedNotifier.LastExecTime.UTC(), uNow.UTC())
				}
			})

			t.Run("no jobs", func(t *testing.T) {
				claimedNotifier, metadata, err := ur.Claim(ctx, workerID)
				require.ErrorIs(t, err, sql.ErrNoRows)
				require.Nil(t, claimedNotifier)
				require.Nil(t, metadata)
			})
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			claimedNotifier, metadata, err := ur.Claim(cancelledCtx, workerID)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, claimedNotifier)
			require.Nil(t, metadata)
		})
	})

	t.Run("claim success", func(t *testing.T) {
		uNow := now.Add(time.Second * 10).Truncate(time.Second).UTC()
		ur := repo.NewNotifier(db, repo.WithNow(func() time.Time {
			return uNow
		}))

		t.Run("success", func(t *testing.T) {
			batchID := uuid.New().String()
			payload := json.RawMessage(`{"test": "payload"}`)

			err := r.Insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, _, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)

			for _, job := range jobs {
				require.NoError(t, ur.OnClaimSuccess(ctx, &job, json.RawMessage(`{"test": "payload"}`)))
			}

			successClaims, _, err := ur.GetByBatchID(ctx, batchID)
			require.NoError(t, err)
			for _, notifier := range successClaims {
				require.EqualValues(t, notifier.UpdatedAt.UTC(), uNow.UTC())
				require.EqualValues(t, notifier.Status, model.Succeeded)
				require.EqualValues(t, notifier.Payload, payload)
				require.Nil(t, notifier.Error)
			}
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			err = ur.OnClaimSuccess(cancelledCtx, &model.Job{ID: 1}, nil)
			require.ErrorIs(t, err, context.Canceled)
		})
	})

	t.Run("claim failure", func(t *testing.T) {
		uNow := now.Add(time.Second * 10).Truncate(time.Second).UTC()
		ur := repo.NewNotifier(db, repo.WithNow(func() time.Time {
			return uNow
		}))

		t.Run("first failed and then succeeded", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, _, err := r.GetByBatchID(ctx, batchID)
			require.NoError(t, err)

			t.Run("marking failed", func(t *testing.T) {
				for i, job := range jobs {
					for j := 0; j < i+1; j++ {
						require.NoError(t, ur.OnClaimFailed(ctx, &job, errors.New("test_error"), 2))
					}
				}

				failedClaims, _, err := ur.GetByBatchID(ctx, batchID)
				require.NoError(t, err)
				require.Equal(t, []model.JobStatus{model.Failed, model.Failed, model.Failed, model.Aborted, model.Aborted}, lo.Map(failedClaims, func(item model.Job, index int) model.JobStatus {
					return item.Status
				}))

				for i, notifier := range failedClaims {
					require.EqualValues(t, notifier.Error, errors.New("'test_error'"))
					require.EqualValues(t, notifier.Attempt, i+1)
					require.EqualValues(t, notifier.UpdatedAt.UTC(), uNow.UTC())
				}
			})

			t.Run("marking succeeded", func(t *testing.T) {
				failedClaims, _, err := ur.GetByBatchID(ctx, batchID)
				require.NoError(t, err)

				for _, notifier := range failedClaims {
					require.NoError(t, ur.OnClaimSuccess(ctx, &notifier, json.RawMessage(`{"test": "payload"}`)))
				}

				successClaims, _, err := ur.GetByBatchID(ctx, batchID)
				require.NoError(t, err)
				for i, notifier := range successClaims {
					require.EqualValues(t, notifier.UpdatedAt.UTC(), uNow.UTC())
					require.EqualValues(t, notifier.Status, model.Succeeded)
					require.EqualValues(t, notifier.Attempt, i+1)
					require.EqualValues(t, notifier.Error, errors.New("'test_error'"))
				}
			})
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.Insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			err = ur.OnClaimFailed(cancelledCtx, &model.Job{ID: 1}, errors.New("test_error"), 0)
			require.ErrorIs(t, err, context.Canceled)
		})
	})
}
