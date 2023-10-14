package repo_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

//nolint:unparam
func sourcesJobs(
	sourceID, destinationID, workspaceID string,
	jobType string, metadata json.RawMessage,
	count int,
) []model.SourceJob {
	sourcesJobs := make([]model.SourceJob, 0, count)
	for i := 0; i < count; i++ {
		sourcesJobs = append(sourcesJobs, model.SourceJob{
			SourceID:      sourceID,
			DestinationID: destinationID,
			TableName:     "table" + strconv.Itoa(i),
			WorkspaceID:   workspaceID,
			Metadata:      metadata,
			JobType:       jobType,
		})
	}
	return sourcesJobs
}

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
		ids, err := repoSource.Insert(
			ctx,
			sourcesJobs(sourceId, destinationId, workspaceId, model.DeleteByJobRunID, json.RawMessage(`{"key": "value"}`), 10),
		)
		require.NoError(t, err)
		require.Len(t, ids, 10)
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		ids, err := repoSource.Insert(ctx,
			sourcesJobs(sourceId, destinationId, workspaceId, model.DeleteByJobRunID, json.RawMessage(`{"key": "value"}`), 1),
		)
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
		ids, err := repoSource.Insert(ctx,
			sourcesJobs(sourceId, destinationId, workspaceId, model.DeleteByJobRunID, json.RawMessage(`{"key": "value"}`), 10),
		)
		require.NoError(t, err)
		require.Len(t, ids, 10)

		for _, id := range ids[0:3] {
			_, err = db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseAsyncJobTable+` SET status = $1 WHERE id = $2`, model.SourceJobStatusSucceeded, id)
			require.NoError(t, err)
		}
		for _, id := range ids[3:6] {
			_, err = db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseAsyncJobTable+` SET status = $1 WHERE id = $2`, model.SourceJobStatusExecuting, id)
			require.NoError(t, err)
		}
		for _, id := range ids[6:10] {
			_, err = db.ExecContext(ctx, `UPDATE `+warehouseutils.WarehouseAsyncJobTable+` SET status = $1 WHERE id = $2`, model.SourceJobStatusFailed, id)
			require.NoError(t, err)
		}

		err = repoSource.Reset(ctx)
		require.NoError(t, err)

		for _, id := range ids[0:3] {
			var status string
			err = db.QueryRowContext(ctx, `SELECT status FROM `+warehouseutils.WarehouseAsyncJobTable+` WHERE id = $1`, id).Scan(&status)
			require.NoError(t, err)
			require.Equal(t, model.SourceJobStatusSucceeded, status)
		}

		for _, id := range ids[3:10] {
			var status string
			err = db.QueryRowContext(ctx, `SELECT status FROM `+warehouseutils.WarehouseAsyncJobTable+` WHERE id = $1`, id).Scan(&status)
			require.NoError(t, err)
			require.Equal(t, model.SourceJobStatusWaiting, status)
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

	t.Run("success", func(t *testing.T) {
		ids, err := repoSource.Insert(
			ctx,
			sourcesJobs(sourceId, destinationId, workspaceId, model.DeleteByJobRunID, json.RawMessage(`{"key": "value"}`), 25),
		)
		require.NoError(t, err)
		require.Len(t, ids, 25)

		t.Run("less jobs", func(t *testing.T) {
			jobs, err := repoSource.GetToProcess(ctx, 25)
			require.NoError(t, err)
			require.Len(t, jobs, 25)
		})
		t.Run("more jobs", func(t *testing.T) {
			jobs, err := repoSource.GetToProcess(ctx, 20)
			require.NoError(t, err)
			require.Len(t, jobs, 20)
		})
		t.Run("equal jobs", func(t *testing.T) {
			jobs, err := repoSource.GetToProcess(ctx, 50)
			require.NoError(t, err)
			require.Len(t, jobs, 25)
		})
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		jobs, err := repoSource.GetToProcess(ctx, 10)
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

	t.Run("job is available", func(t *testing.T) {
		ids, err := repoSource.Insert(
			ctx,
			sourcesJobs(sourceId, destinationId, workspaceId, model.DeleteByJobRunID, json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`), 1),
		)
		require.NoError(t, err)
		require.Len(t, ids, 1)

		job, err := repoSource.GetByJobRunTaskRun(ctx, jobRun, taskRun)
		require.NoError(t, err)
		require.Equal(t, job, &model.SourceJob{
			ID:            1,
			SourceID:      sourceId,
			DestinationID: destinationId,
			WorkspaceID:   workspaceId,
			TableName:     "table0",
			Status:        model.SourceJobStatusWaiting,
			Error:         nil,
			JobType:       model.DeleteByJobRunID,
			Metadata:      json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`),
			CreatedAt:     now.UTC(),
			UpdatedAt:     now.UTC(),
			Attempts:      0,
		})
	})
	t.Run("job is not available", func(t *testing.T) {
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

	t.Run("success", func(t *testing.T) {
		ids, err := repoSource.Insert(
			ctx,
			sourcesJobs(sourceId, destinationId, workspaceId, model.DeleteByJobRunID, json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`), 1),
		)
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
			TableName:     "table0",
			Status:        model.SourceJobStatusSucceeded,
			Error:         nil,
			JobType:       model.DeleteByJobRunID,
			Metadata:      json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`),
			CreatedAt:     now.UTC(),
			UpdatedAt:     now.UTC(),
			Attempts:      0,
		})
	})
	t.Run("job not found", func(t *testing.T) {
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

	t.Run("success", func(t *testing.T) {
		ids, err := repoSource.Insert(
			ctx,
			sourcesJobs(sourceId, destinationId, workspaceId, model.DeleteByJobRunID, json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`), 1),
		)
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
				TableName:     "table0",
				Status:        model.Failed,
				Error:         errors.New(testError),
				JobType:       model.DeleteByJobRunID,
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
				TableName:     "table0",
				Status:        model.Aborted,
				Error:         errors.New(testError),
				JobType:       model.DeleteByJobRunID,
				Metadata:      json.RawMessage(`{"job_run_id": "test-job-run", "task_run_id": "test-task-run"}`),
				CreatedAt:     now.UTC(),
				UpdatedAt:     now.UTC(),
				Attempts:      2,
			})
		})
	})
	t.Run("job not found", func(t *testing.T) {
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
