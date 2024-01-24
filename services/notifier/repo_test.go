package notifier

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

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
)

func TestRepo(t *testing.T) {
	const (
		workspaceIdentifier = "test_workspace_identifier"
		workerID            = "test_worker"
	)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := postgres.Setup(pool, t)
	require.NoError(t, err)

	err = (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "pg_notifier_queue_migrations",
	}).Migrate("pg_notifier_queue")
	require.NoError(t, err)

	t.Log("db:", pgResource.DBDsn)

	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()

	db := sqlmw.New(pgResource.DB)

	r := newRepo(db, WithNow(func() time.Time {
		return now
	}))

	publishRequest := PublishRequest{
		Payloads: []json.RawMessage{
			json.RawMessage(`{"id":"1"}`),
			json.RawMessage(`{"id":"2"}`),
			json.RawMessage(`{"id":"3"}`),
			json.RawMessage(`{"id":"4"}`),
			json.RawMessage(`{"id":"5"}`),
		},
		JobType:      JobTypeUpload,
		UploadSchema: json.RawMessage(`{"UploadSchema":"1"}`),
		Priority:     50,
	}

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()

	t.Run("Insert and get", func(t *testing.T) {
		t.Run("create jobs", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, err := r.getByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, jobs, len(publishRequest.Payloads))

			for i, job := range jobs {
				require.EqualValues(t, job.Payload, json.RawMessage(fmt.Sprintf(`{"id": "%d"}`, i+1)))
				require.EqualValues(t, job.WorkspaceIdentifier, workspaceIdentifier)
				require.EqualValues(t, job.BatchID, batchID)
				require.EqualValues(t, job.Type, publishRequest.JobType)
				require.EqualValues(t, job.Priority, publishRequest.Priority)
				require.EqualValues(t, job.Status, Waiting)
				require.EqualValues(t, job.WorkerID, "")
				require.EqualValues(t, job.Attempt, 0)
				require.EqualValues(t, job.CreatedAt.UTC(), now.UTC())
				require.EqualValues(t, job.UpdatedAt.UTC(), now.UTC())
				require.Nil(t, job.Error)
			}
		})

		t.Run("missing batch id", func(t *testing.T) {
			jobs, err := r.getByBatchID(ctx, "missing_batch_id")
			require.EqualError(t, err, "getting by batchID: no jobs found")
			require.Nil(t, jobs)
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			jobs, err := r.getByBatchID(cancelledCtx, batchID)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, jobs)
		})
	})

	t.Run("delete by batch id", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, err := r.getByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, jobs, len(publishRequest.Payloads))

			err = r.deleteByBatchID(ctx, batchID)
			require.NoError(t, err)

			jobs, err = r.getByBatchID(ctx, batchID)
			require.EqualError(t, err, "getting by batchID: no jobs found")
			require.Nil(t, jobs)
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			err = r.deleteByBatchID(cancelledCtx, batchID)
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

				err := r.insert(ctx, &publishRequest, workspaceIdentifier, batchID)
				require.NoError(t, err)
			}
			for _, batchID := range batchIDs {
				jobs, err := r.getByBatchID(ctx, batchID)
				require.NoError(t, err)
				require.Len(t, jobs, len(publishRequest.Payloads))
			}

			for i, workspaceIdentifier := range workspaceIdentifiers {
				if i%4 == 0 {
					continue
				}

				err = r.resetForWorkspace(ctx, workspaceIdentifier)
				require.NoError(t, err)
			}

			for i, batchID := range batchIDs {
				if i%4 == 0 {
					jobs, err := r.getByBatchID(ctx, batchID)
					require.EqualError(t, err, "getting by batchID: no jobs found")
					require.Nil(t, jobs)
					continue
				}

				jobs, err := r.getByBatchID(ctx, batchID)
				require.NoError(t, err)
				require.Len(t, jobs, len(publishRequest.Payloads))
			}
		})

		t.Run("without upload schema", func(t *testing.T) {
			publishRequest := PublishRequest{
				Payloads: []json.RawMessage{
					json.RawMessage(`{"id":"11"}`),
				},
				JobType:  JobTypeUpload,
				Priority: 75,
			}

			batchID := uuid.New().String()
			err := r.insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			err = r.resetForWorkspace(ctx, workspaceIdentifier)
			require.NoError(t, err)

			jobs, err := r.getByBatchID(ctx, batchID)
			require.EqualError(t, err, "getting by batchID: no jobs found")
			require.Nil(t, jobs)
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			err = r.resetForWorkspace(cancelledCtx, batchID)
			require.ErrorIs(t, err, context.Canceled)
		})

		t.Run("empty", func(t *testing.T) {
			err := r.resetForWorkspace(ctx, "missing_batch_id")
			require.NoError(t, err)
		})
	})

	t.Run("pending by batch id", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, err := r.getByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Len(t, jobs, len(publishRequest.Payloads))

			err = r.onClaimSuccess(ctx, &jobs[0], json.RawMessage(`{"test": "payload"}`))
			require.NoError(t, err)
			err = r.onClaimFailed(ctx, &jobs[1], errors.New("test error"), 100)
			require.NoError(t, err)
			err = r.onClaimFailed(ctx, &jobs[2], errors.New("test error"), -1)
			require.NoError(t, err)

			pendingCount, err := r.pendingByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.EqualValues(t, pendingCount, 3)
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			pendingCount, err := r.pendingByBatchID(cancelledCtx, batchID)
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, pendingCount)
		})

		t.Run("no pending", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, err := r.getByBatchID(ctx, batchID)
			require.NoError(t, err)

			for _, job := range jobs {
				require.NoError(t, r.onClaimSuccess(ctx, &job, json.RawMessage(`{"test": "payload"}`)))
			}

			pendingCount, err := r.pendingByBatchID(ctx, batchID)
			require.NoError(t, err)
			require.Zero(t, pendingCount)
		})
	})

	t.Run("orphan job ids", func(t *testing.T) {
		t.Run("success", func(t *testing.T) {
			batchID := uuid.New().String()
			orphanInterval := 5

			err := r.insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, err := r.getByBatchID(ctx, batchID)
			require.NoError(t, err)

			t.Run("no orphans", func(t *testing.T) {
				jobIDs, err := r.orphanJobIDs(ctx, orphanInterval)
				require.NoError(t, err)
				require.Len(t, jobIDs, 0)
			})

			t.Run("few orphans", func(t *testing.T) {
				for _, job := range jobs[:3] {
					_, err := db.ExecContext(ctx, `
						UPDATE
						  pg_notifier_queue
						SET
						  status = 'executing',
						  last_exec_time = NOW() - $1 * INTERVAL '1 SECOND'
						WHERE
						  id = $2;`,
						2*orphanInterval,
						job.ID,
					)
					require.NoError(t, err)
				}

				jobIDs, err := r.orphanJobIDs(ctx, orphanInterval)
				require.NoError(t, err)
				require.Len(t, jobIDs, len(jobs[:3]))
				for _, job := range jobs[:3] {
					require.Contains(t, jobIDs, job.ID)
				}
			})
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			jobIDs, err := r.orphanJobIDs(cancelledCtx, 0)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, jobIDs)
		})
	})

	t.Run("claim", func(t *testing.T) {
		uNow := now.Add(time.Second * 10).Truncate(time.Second).UTC()
		ur := newRepo(db, WithNow(func() time.Time {
			return uNow
		}))

		t.Run("success", func(t *testing.T) {
			_, err := db.ExecContext(ctx, "TRUNCATE TABLE pg_notifier_queue;")
			require.NoError(t, err)

			batchID := uuid.New().String()

			err = r.insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			t.Run("with jobs", func(t *testing.T) {
				jobs, err := r.getByBatchID(ctx, batchID)
				require.NoError(t, err)

				for i, job := range jobs {
					claimedJob, err := ur.claim(ctx, workerID+strconv.Itoa(i))
					require.NoError(t, err)
					require.EqualValues(t, claimedJob.ID, job.ID)
					require.EqualValues(t, claimedJob.BatchID, job.BatchID)
					require.EqualValues(t, claimedJob.WorkerID, workerID+strconv.Itoa(i))
					require.EqualValues(t, claimedJob.WorkspaceIdentifier, job.WorkspaceIdentifier)
					require.EqualValues(t, claimedJob.Status, Executing)
					require.EqualValues(t, claimedJob.Type, job.Type)
					require.EqualValues(t, claimedJob.Priority, job.Priority)
					require.EqualValues(t, claimedJob.Attempt, job.Attempt)
					require.EqualValues(t, claimedJob.Error, job.Error)
					require.EqualValues(t, claimedJob.Payload, json.RawMessage(fmt.Sprintf(`{"id": "%d", "UploadSchema": "1"}`, i+1)))
					require.EqualValues(t, claimedJob.CreatedAt.UTC(), job.CreatedAt.UTC())
					require.EqualValues(t, claimedJob.UpdatedAt.UTC(), uNow.UTC())
					require.EqualValues(t, claimedJob.LastExecTime.UTC(), uNow.UTC())
				}
			})

			t.Run("no jobs", func(t *testing.T) {
				claimedJob, err := ur.claim(ctx, workerID)
				require.ErrorIs(t, err, sql.ErrNoRows)
				require.Nil(t, claimedJob)
			})
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			claimedJob, err := ur.claim(cancelledCtx, workerID)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, claimedJob)
		})
	})

	t.Run("claim success", func(t *testing.T) {
		uNow := now.Add(time.Second * 10).Truncate(time.Second).UTC()
		ur := newRepo(db, WithNow(func() time.Time {
			return uNow
		}))

		t.Run("success", func(t *testing.T) {
			batchID := uuid.New().String()
			payload := json.RawMessage(`{"test": "payload"}`)

			err := r.insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, err := r.getByBatchID(ctx, batchID)
			require.NoError(t, err)

			for _, job := range jobs {
				require.NoError(t, ur.onClaimSuccess(ctx, &job, json.RawMessage(`{"test": "payload"}`)))
			}

			successClaims, err := ur.getByBatchID(ctx, batchID)
			require.NoError(t, err)
			for _, job := range successClaims {
				require.EqualValues(t, job.UpdatedAt.UTC(), uNow.UTC())
				require.EqualValues(t, job.Status, Succeeded)
				require.EqualValues(t, job.Payload, payload)
				require.Nil(t, job.Error)
			}
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			err = ur.onClaimSuccess(cancelledCtx, &Job{ID: 1}, nil)
			require.ErrorIs(t, err, context.Canceled)
		})
	})

	t.Run("claim failure", func(t *testing.T) {
		uNow := now.Add(time.Second * 10).Truncate(time.Second).UTC()
		ur := newRepo(db, WithNow(func() time.Time {
			return uNow
		}))

		t.Run("first failed and then succeeded", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.insert(ctx, &publishRequest, workspaceIdentifier, batchID)
			require.NoError(t, err)

			jobs, err := r.getByBatchID(ctx, batchID)
			require.NoError(t, err)

			t.Run("marking failed", func(t *testing.T) {
				for i, job := range jobs {
					for j := 0; j < i+1; j++ {
						require.NoError(t, ur.onClaimFailed(ctx, &job, errors.New("test_error"), 2))
					}
				}

				failedClaims, err := ur.getByBatchID(ctx, batchID)
				require.NoError(t, err)
				require.Equal(t, []JobStatus{
					Failed,
					Failed,
					Failed,
					Aborted,
					Aborted,
				},
					lo.Map(failedClaims, func(item Job, index int) JobStatus {
						return item.Status
					}),
				)

				for i, job := range failedClaims {
					require.EqualValues(t, job.Error, errors.New("test_error"))
					require.EqualValues(t, job.Attempt, i+1)
					require.EqualValues(t, job.UpdatedAt.UTC(), uNow.UTC())
				}
			})

			t.Run("marking succeeded", func(t *testing.T) {
				failedClaims, err := ur.getByBatchID(ctx, batchID)
				require.NoError(t, err)

				for _, job := range failedClaims {
					require.NoError(t, ur.onClaimSuccess(ctx, &job, json.RawMessage(`{"test": "payload"}`)))
				}

				successClaims, err := ur.getByBatchID(ctx, batchID)
				require.NoError(t, err)
				for i, job := range successClaims {
					require.EqualValues(t, job.UpdatedAt.UTC(), uNow.UTC())
					require.EqualValues(t, job.Status, Succeeded)
					require.EqualValues(t, job.Attempt, i+1)
					require.EqualValues(t, job.Error, errors.New("test_error"))
				}
			})
		})

		t.Run("context cancelled", func(t *testing.T) {
			batchID := uuid.New().String()

			err := r.insert(cancelledCtx, &publishRequest, workspaceIdentifier, batchID)
			require.ErrorIs(t, err, context.Canceled)

			err = ur.onClaimFailed(cancelledCtx, &Job{ID: 1}, errors.New("test_error"), 0)
			require.ErrorIs(t, err, context.Canceled)
		})
	})
}
