package repo_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/samber/lo"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

func TestSource_Insert(t *testing.T) {
	const (
		sourceId      = "test_source_id"
		destinationId = "test_destination_id"
		workspaceId   = "test_workspace_id"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoSource := repo.NewSource(db, repo.WithNow(func() time.Time {
		return now
	}))

	t.Run("success", func(t *testing.T) {
		ids, err := repoSource.Insert(ctx, lo.RepeatBy(10, func(i int) model.SourceJob {
			return model.SourceJob{
				SourceID:      sourceId,
				DestinationID: destinationId,
				TableName:     "table-" + strconv.Itoa(i),
				WorkspaceID:   workspaceId,
				Metadata:      json.RawMessage(`{"key": "value"}`),
				JobType:       model.SourceJobTypeDeleteByJobRunID,
			}
		}))
		require.NoError(t, err)
		require.Len(t, ids, 10)
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		ids, err := repoSource.Insert(ctx, lo.RepeatBy(1, func(i int) model.SourceJob {
			return model.SourceJob{
				SourceID:      sourceId,
				DestinationID: destinationId,
				TableName:     "table-" + strconv.Itoa(i),
				WorkspaceID:   workspaceId,
				Metadata:      json.RawMessage(`{"key": "value"}`),
				JobType:       model.SourceJobTypeDeleteByJobRunID,
			}
		}))
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, ids)
	})
}

func TestSource_Reset(t *testing.T) {
	const (
		sourceId      = "test_source_id"
		destinationId = "test_destination_id"
		workspaceId   = "test_workspace_id"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoSource := repo.NewSource(db, repo.WithNow(func() time.Time {
		return now
	}))

	t.Run("success", func(t *testing.T) {
		ids, err := repoSource.Insert(ctx, lo.RepeatBy(10, func(i int) model.SourceJob {
			return model.SourceJob{
				SourceID:      sourceId,
				DestinationID: destinationId,
				TableName:     "table-" + strconv.Itoa(i),
				WorkspaceID:   workspaceId,
				Metadata:      json.RawMessage(`{"key": "value"}`),
				JobType:       model.SourceJobTypeDeleteByJobRunID,
			}
		}))
		require.NoError(t, err)
		require.Len(t, ids, 10)

		for _, id := range ids[0:3] {
			_, err = db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseAsyncJobTable+` SET status = $1 WHERE id = $2;`, model.SourceJobStatusSucceeded, id)
			require.NoError(t, err)
		}
		for _, id := range ids[3:6] {
			_, err = db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseAsyncJobTable+` SET status = $1 WHERE id = $2;`, model.SourceJobStatusExecuting, id)
			require.NoError(t, err)
		}
		for _, id := range ids[6:10] {
			_, err = db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseAsyncJobTable+` SET status = $1 WHERE id = $2;`, model.SourceJobStatusFailed, id)
			require.NoError(t, err)
		}

		err = repoSource.Reset(ctx)
		require.NoError(t, err)

		for _, id := range ids[0:3] {
			var status string
			err = db.QueryRowContext(ctx, `SELECT status FROM `+warehouseutils.WarehouseAsyncJobTable+` WHERE id = $1`, id).Scan(&status)
			require.NoError(t, err)
			require.Equal(t, model.SourceJobStatusSucceeded.String(), status)
		}

		for _, id := range ids[3:10] {
			var status string
			err = db.QueryRowContext(ctx, `SELECT status FROM `+warehouseutils.WarehouseAsyncJobTable+` WHERE id = $1`, id).Scan(&status)
			require.NoError(t, err)
			require.Equal(t, model.SourceJobStatusWaiting.String(), status)
		}
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		err := repoSource.Reset(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestSource_GetToProcess(t *testing.T) {
	const (
		sourceId      = "test_source_id"
		destinationId = "test_destination_id"
		workspaceId   = "test_workspace_id"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoSource := repo.NewSource(db, repo.WithNow(func() time.Time {
		return now
	}))

	t.Run("nothing to process", func(t *testing.T) {
		jobs, err := repoSource.GetToProcess(ctx, 20)
		require.NoError(t, err)
		require.Zero(t, jobs)
	})
	t.Run("few to process", func(t *testing.T) {
		ids, err := repoSource.Insert(ctx, lo.RepeatBy(25, func(i int) model.SourceJob {
			return model.SourceJob{
				SourceID:      sourceId,
				DestinationID: destinationId,
				TableName:     "table-" + strconv.Itoa(i),
				WorkspaceID:   workspaceId,
				Metadata:      json.RawMessage(`{"key": "value"}`),
				JobType:       model.SourceJobTypeDeleteByJobRunID,
			}
		}))
		require.NoError(t, err)
		require.Len(t, ids, 25)

		for _, id := range ids[6:10] {
			_, err = db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseAsyncJobTable+` SET status = $1 WHERE id = $2;`, model.SourceJobStatusExecuting, id)
			require.NoError(t, err)
		}
		for _, id := range ids[10:14] {
			_, err = db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseAsyncJobTable+` SET status = $1 WHERE id = $2;`, model.SourceJobStatusSucceeded, id)
			require.NoError(t, err)
		}
		for _, id := range ids[14:18] {
			_, err = db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseAsyncJobTable+` SET status = $1 WHERE id = $2;`, model.SourceJobStatusFailed, id)
			require.NoError(t, err)
		}
		for _, id := range ids[18:22] {
			_, err = db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseAsyncJobTable+` SET status = $1 WHERE id = $2;`, model.SourceJobStatusAborted, id)
			require.NoError(t, err)
		}

		jobs, err := repoSource.GetToProcess(ctx, 20)
		require.NoError(t, err)
		require.Len(t, jobs, 13)

		lo.ForEach(jobs, func(job model.SourceJob, index int) {
			require.Equal(t, sourceId, job.SourceID)
			require.Equal(t, destinationId, job.DestinationID)
			require.Equal(t, workspaceId, job.WorkspaceID)
			require.Contains(t, []model.SourceJobStatus{model.SourceJobStatusWaiting, model.SourceJobStatusFailed}, job.Status)
			require.Equal(t, model.SourceJobTypeDeleteByJobRunID, job.JobType)
			require.Equal(t, json.RawMessage(`{"key": "value"}`), job.Metadata)
			require.EqualValues(t, now.UTC(), job.CreatedAt.UTC())
			require.EqualValues(t, now.UTC(), job.UpdatedAt.UTC())
		})
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		jobs, err := repoSource.GetToProcess(ctx, -1)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, jobs)
	})
}

func TestSource_GetByJobRunTaskRun(t *testing.T) {
	const (
		sourceId      = "test_source_id"
		destinationId = "test_destination_id"
		workspaceId   = "test_workspace_id"
		jobRun        = "test-job-run"
		taskRun       = "test-task-run"
		otherJobRun   = "other-job-run"
		otherTaskRun  = "other-task-run"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoSource := repo.NewSource(db, repo.WithNow(func() time.Time {
		return now
	}))

	t.Run("source job available", func(t *testing.T) {
		ids, err := repoSource.Insert(ctx, lo.RepeatBy(1, func(i int) model.SourceJob {
			return model.SourceJob{
				SourceID:      sourceId,
				DestinationID: destinationId,
				TableName:     "table-" + strconv.Itoa(i),
				WorkspaceID:   workspaceId,
				Metadata:      json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`),
				JobType:       model.SourceJobTypeDeleteByJobRunID,
			}
		}))
		require.NoError(t, err)
		require.Len(t, ids, 1)

		job, err := repoSource.GetByJobRunTaskRun(ctx, jobRun, taskRun)
		require.NoError(t, err)
		require.Equal(t, job, &model.SourceJob{
			ID:            1,
			SourceID:      sourceId,
			DestinationID: destinationId,
			WorkspaceID:   workspaceId,
			TableName:     "table-0",
			Status:        model.SourceJobStatusWaiting,
			Error:         nil,
			JobType:       model.SourceJobTypeDeleteByJobRunID,
			Metadata:      json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`),
			CreatedAt:     now.UTC(),
			UpdatedAt:     now.UTC(),
			Attempts:      0,
		})
	})
	t.Run("source job not available", func(t *testing.T) {
		job, err := repoSource.GetByJobRunTaskRun(ctx, otherJobRun, otherTaskRun)
		require.ErrorIs(t, err, model.ErrSourcesJobNotFound)
		require.Nil(t, job)
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		job, err := repoSource.GetByJobRunTaskRun(ctx, jobRun, taskRun)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, job)
	})
}

func TestSource_OnUpdateSuccess(t *testing.T) {
	const (
		sourceId      = "test_source_id"
		destinationId = "test_destination_id"
		workspaceId   = "test_workspace_id"
		jobRun        = "test-job-run"
		taskRun       = "test-task-run"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoSource := repo.NewSource(db, repo.WithNow(func() time.Time {
		return now
	}))

	t.Run("source job found", func(t *testing.T) {
		ids, err := repoSource.Insert(ctx, lo.RepeatBy(1, func(i int) model.SourceJob {
			return model.SourceJob{
				SourceID:      sourceId,
				DestinationID: destinationId,
				TableName:     "table-" + strconv.Itoa(i),
				WorkspaceID:   workspaceId,
				Metadata:      json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`),
				JobType:       model.SourceJobTypeDeleteByJobRunID,
			}
		}))
		require.NoError(t, err)
		require.Len(t, ids, 1)

		err = repoSource.OnUpdateSuccess(ctx, int64(1))
		require.NoError(t, err)

		job, err := repoSource.GetByJobRunTaskRun(ctx, jobRun, taskRun)
		require.NoError(t, err)

		require.Equal(t, job, &model.SourceJob{
			ID:            1,
			SourceID:      sourceId,
			DestinationID: destinationId,
			WorkspaceID:   workspaceId,
			TableName:     "table-0",
			Status:        model.SourceJobStatusSucceeded,
			Error:         nil,
			JobType:       model.SourceJobTypeDeleteByJobRunID,
			Metadata:      json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`),
			CreatedAt:     now.UTC(),
			UpdatedAt:     now.UTC(),
			Attempts:      0,
		})
	})
	t.Run("source job not found", func(t *testing.T) {
		err := repoSource.OnUpdateSuccess(ctx, int64(-1))
		require.ErrorIs(t, err, model.ErrSourcesJobNotFound)
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		err := repoSource.OnUpdateSuccess(ctx, int64(1))
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestSource_OnUpdateFailure(t *testing.T) {
	const (
		sourceId      = "test_source_id"
		destinationId = "test_destination_id"
		workspaceId   = "test_workspace_id"
		jobRun        = "test-job-run"
		taskRun       = "test-task-run"
		testError     = "test-error"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoSource := repo.NewSource(db, repo.WithNow(func() time.Time {
		return now
	}))

	t.Run("source job found", func(t *testing.T) {
		ids, err := repoSource.Insert(ctx, lo.RepeatBy(1, func(i int) model.SourceJob {
			return model.SourceJob{
				SourceID:      sourceId,
				DestinationID: destinationId,
				TableName:     "table-" + strconv.Itoa(i),
				WorkspaceID:   workspaceId,
				Metadata:      json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`),
				JobType:       model.SourceJobTypeDeleteByJobRunID,
			}
		}))
		require.NoError(t, err)
		require.Len(t, ids, 1)

		t.Run("not crossed max attempt", func(t *testing.T) {
			err = repoSource.OnUpdateFailure(ctx, int64(1), errors.New(testError), 1)
			require.NoError(t, err)

			job, err := repoSource.GetByJobRunTaskRun(ctx, jobRun, taskRun)
			require.NoError(t, err)

			require.Equal(t, job, &model.SourceJob{
				ID:            1,
				SourceID:      sourceId,
				DestinationID: destinationId,
				WorkspaceID:   workspaceId,
				TableName:     "table-0",
				Status:        model.SourceJobStatusFailed,
				Error:         errors.New(testError),
				JobType:       model.SourceJobTypeDeleteByJobRunID,
				Metadata:      json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`),
				CreatedAt:     now.UTC(),
				UpdatedAt:     now.UTC(),
				Attempts:      1,
			})
		})
		t.Run("crossed max attempt", func(t *testing.T) {
			err = repoSource.OnUpdateFailure(ctx, int64(1), errors.New(testError), -1)
			require.NoError(t, err)

			job, err := repoSource.GetByJobRunTaskRun(ctx, jobRun, taskRun)
			require.NoError(t, err)

			require.Equal(t, job, &model.SourceJob{
				ID:            1,
				SourceID:      sourceId,
				DestinationID: destinationId,
				WorkspaceID:   workspaceId,
				TableName:     "table-0",
				Status:        model.SourceJobStatusAborted,
				Error:         errors.New(testError),
				JobType:       model.SourceJobTypeDeleteByJobRunID,
				Metadata:      json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`),
				CreatedAt:     now.UTC(),
				UpdatedAt:     now.UTC(),
				Attempts:      2,
			})
		})
	})
	t.Run("source job not found", func(t *testing.T) {
		err := repoSource.OnUpdateFailure(ctx, int64(-1), errors.New(testError), 1)
		require.ErrorIs(t, err, model.ErrSourcesJobNotFound)
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		err := repoSource.OnUpdateFailure(ctx, int64(1), errors.New(testError), 1)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestSource_MarkExecuting(t *testing.T) {
	const (
		sourceId      = "test_source_id"
		destinationId = "test_destination_id"
		workspaceId   = "test_workspace_id"
		jobRun        = "test-job-run"
		taskRun       = "test-task-run"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoSource := repo.NewSource(db, repo.WithNow(func() time.Time {
		return now
	}))

	t.Run("success", func(t *testing.T) {
		ids, err := repoSource.Insert(ctx, lo.RepeatBy(1, func(i int) model.SourceJob {
			return model.SourceJob{
				SourceID:      sourceId,
				DestinationID: destinationId,
				TableName:     "table-" + strconv.Itoa(i),
				WorkspaceID:   workspaceId,
				Metadata:      json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`),
				JobType:       model.SourceJobTypeDeleteByJobRunID,
			}
		}))
		require.NoError(t, err)
		require.Len(t, ids, 1)

		err = repoSource.MarkExecuting(ctx, []int64{1})
		require.NoError(t, err)

		job, err := repoSource.GetByJobRunTaskRun(ctx, jobRun, taskRun)
		require.NoError(t, err)

		require.Equal(t, job, &model.SourceJob{
			ID:            1,
			SourceID:      sourceId,
			DestinationID: destinationId,
			WorkspaceID:   workspaceId,
			TableName:     "table-0",
			Status:        model.SourceJobStatusExecuting,
			Error:         nil,
			JobType:       model.SourceJobTypeDeleteByJobRunID,
			Metadata:      json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`),
			CreatedAt:     now.UTC(),
			UpdatedAt:     now.UTC(),
			Attempts:      0,
		})
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		err := repoSource.MarkExecuting(ctx, []int64{1})
		require.ErrorIs(t, err, context.Canceled)
	})
}
