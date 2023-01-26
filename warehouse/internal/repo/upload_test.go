package repo_test

import (
	"context"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/stretchr/testify/require"
)

func TestUploads_CRUD(t *testing.T) {
	ctx := context.Background()

	db := setupDB(t)

	now := time.Date(2021, 1, 1, 0, 0, 3, 0, time.UTC)
	r := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))

	destType := "RS"

	ogUpload := model.Upload{
		WorkspaceID:     "workspace_id",
		Namespace:       "namespace",
		SourceID:        "source_id",
		DestinationID:   "destination_id",
		DestinationType: destType,
		Status:          model.Waiting,
		Error:           []byte("{}"),
		FirstEventAt:    now.Add(-2 * time.Hour),
		LastEventAt:     now.Add(-time.Hour),
		LoadFileType:    "csv",
		Priority:        60,
		NextRetryTime:   now,
		Retried:         true,

		StagingFileStartID: 0,
		StagingFileEndID:   0,
		LoadFileStartID:    0,
		LoadFileEndID:      0,
		Timings:            nil,
		FirstAttemptAt:     time.Time{},
		LastAttemptAt:      time.Time{},
		Attempts:           0,
		UploadSchema:       model.Schema{},
		MergedSchema:       model.Schema{},
	}

	id, err := r.CreateWithStagingFiles(ctx, ogUpload, []model.StagingFile{
		{
			ID:           1,
			FirstEventAt: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),

			UseRudderStorage: true,
			SourceBatchID:    "source_batch_id",
			SourceTaskID:     "source_task_id",
			SourceTaskRunID:  "source_task_run_id",
			SourceJobID:      "source_job_id",
			SourceJobRunID:   "source_job_run_id",
		},
		{
			ID:          2,
			LastEventAt: time.Date(2021, 1, 1, 0, 0, 1, 0, time.UTC),
		},
	})
	require.NoError(t, err)
	ogUpload.ID = id
	ogUpload.Error = []byte("{}")
	ogUpload.StagingFileStartID = 1
	ogUpload.StagingFileEndID = 2
	ogUpload.FirstEventAt = time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	ogUpload.LastEventAt = time.Date(2021, 1, 1, 0, 0, 1, 0, time.UTC)
	ogUpload.LoadFileType = "csv"

	ogUpload.UseRudderStorage = true
	ogUpload.SourceBatchID = "source_batch_id"
	ogUpload.SourceTaskID = "source_task_id"
	ogUpload.SourceTaskRunID = "source_task_run_id"
	ogUpload.SourceJobID = "source_job_id"
	ogUpload.SourceJobRunID = "source_job_run_id"

	t.Run("Get", func(t *testing.T) {
		upload, err := r.Get(ctx, id)
		require.NoError(t, err)

		require.Equal(t, ogUpload, upload)
	})
	t.Run("GetToProcess", func(t *testing.T) {
		uploads, err := r.GetToProcess(ctx, destType, 10, repo.ProcessOptions{})
		require.NoError(t, err)

		require.Equal(t, []model.Upload{ogUpload}, uploads)
	})

	t.Run("UploadJobsStats", func(t *testing.T) {
		uploadStats, err := r.UploadJobsStats(ctx, destType, repo.ProcessOptions{})
		require.NoError(t, err)

		require.Equal(t, model.UploadJobsStats{
			PendingJobs:    1,
			PickupLag:      0,
			PickupWaitTime: 0,
		}, uploadStats)
	})
}

func TestUploads_Processing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db := setupDB(t)

	now := time.Date(2021, 1, 1, 0, 0, 3, 0, time.UTC)
	r := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))

	destType := "RS"

	uploads := []model.Upload{
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_id",
			DestinationID:   "destination_id",
			DestinationType: destType,
		},
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_id_1",
			DestinationID:   "destination_id",
			DestinationType: destType,
		},
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_id_2",
			DestinationID:   "destination_id",
			DestinationType: destType,
		},
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_id_2",
			DestinationID:   "destination_id",
			DestinationType: "OTHER",
		},
		{
			WorkspaceID:     "workspace_id_2",
			Namespace:       "namespace",
			SourceID:        "source_id_3",
			DestinationID:   "destination_id_4",
			DestinationType: destType,
		},
	}

	for i := range uploads {
		id, err := r.CreateWithStagingFiles(ctx, uploads[i], []model.StagingFile{{
			ID: int64(i + 1),
		}})
		uploads[i].ID = id
		uploads[i].Error = []byte("{}")
		uploads[i].UploadSchema = model.Schema{}
		uploads[i].MergedSchema = model.Schema{}
		uploads[i].LoadFileType = "csv"
		uploads[i].StagingFileStartID = int64(i + 1)
		uploads[i].StagingFileEndID = int64(i + 1)
		require.NoError(t, err)
	}

	t.Run("select destination type", func(t *testing.T) {
		t.Parallel()

		s, err := r.GetToProcess(ctx, destType, 10, repo.ProcessOptions{})
		require.NoError(t, err)

		t.Log("should get all uploads for destType")
		require.Equal(t, []model.Upload{uploads[0], uploads[4]}, s)

		s, err = r.GetToProcess(ctx, "OTHER", 10, repo.ProcessOptions{})
		require.NoError(t, err)
		t.Log("should get all uploads for OTHER")
		require.Equal(t, []model.Upload{uploads[3]}, s)
	})

	t.Run("skip workspaces", func(t *testing.T) {
		t.Parallel()

		s, err := r.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			SkipWorkspaces: []string{"workspace_id", "workspace_id_2"},
		})
		require.NoError(t, err)
		require.Empty(t, s)

		s, err = r.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			SkipWorkspaces: []string{"workspace_id"},
		})
		require.NoError(t, err)
		require.Equal(t, []model.Upload{uploads[4]}, s)
	})

	t.Run("multiple sources", func(t *testing.T) {
		t.Parallel()

		s, err := r.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			AllowMultipleSourcesForJobsPickup: true,
		})
		require.NoError(t, err)

		t.Log("should get all uploads for destType")
		require.Equal(t, []model.Upload{uploads[0], uploads[1], uploads[2], uploads[4]}, s)

		s, err = r.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			AllowMultipleSourcesForJobsPickup: false,
		})
		require.NoError(t, err)
		t.Log("should get only one upload per source")
		require.Equal(t, []model.Upload{uploads[0], uploads[4]}, s)
	})

	t.Run("skip identifiers", func(t *testing.T) {
		s, err := r.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			AllowMultipleSourcesForJobsPickup: false,
			SkipIdentifiers:                   []string{"destination_id_4_namespace"},
		})
		require.NoError(t, err)
		require.Equal(t, []model.Upload{uploads[0]}, s)

		s, err = r.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			AllowMultipleSourcesForJobsPickup: true,
			SkipIdentifiers:                   []string{"source_id_3_destination_id_4_namespace"},
		})
		require.NoError(t, err)
		require.Equal(t, []model.Upload{uploads[0], uploads[1], uploads[2]}, s)
	})
}

func TestUploads_UploadMetadata(t *testing.T) {
	upload := model.Upload{
		ID:                 1,
		WorkspaceID:        "workspace_id",
		Namespace:          "namespace",
		SourceID:           "source_id",
		DestinationID:      "destination_id",
		DestinationType:    "destination_type",
		Status:             model.ExportedData,
		Error:              []byte("{}"),
		FirstEventAt:       time.Time{},
		LastEventAt:        time.Time{},
		UseRudderStorage:   true,
		SourceBatchID:      "source_batch_id",
		SourceTaskID:       "source_task_id",
		SourceTaskRunID:    "source_task_run_id",
		SourceJobID:        "source_job_id",
		SourceJobRunID:     "source_job_run_id",
		LoadFileType:       "load_file_type",
		NextRetryTime:      time.Date(2021, 1, 1, 0, 0, 1, 0, time.UTC),
		Priority:           40,
		Retried:            true,
		StagingFileStartID: 0,
		StagingFileEndID:   0,
		LoadFileStartID:    0,
		LoadFileEndID:      0,
		Timings:            []map[string]time.Time{},
		FirstAttemptAt:     time.Time{},
		LastAttemptAt:      time.Time{},
		Attempts:           0,
		UploadSchema:       nil,
		MergedSchema:       nil,
	}
	metadata := repo.ExtractUploadMetadata(upload)

	require.Equal(t, repo.UploadMetadata{
		UseRudderStorage: true,
		SourceBatchID:    "source_batch_id",
		SourceTaskID:     "source_task_id",
		SourceTaskRunID:  "source_task_run_id",
		SourceJobID:      "source_job_id",
		SourceJobRunID:   "source_job_run_id",
		LoadFileType:     "load_file_type",
		Retried:          true,
		Priority:         40,
		NextRetryTime:    time.Date(2021, 1, 1, 0, 0, 1, 0, time.UTC),
	}, metadata)
}