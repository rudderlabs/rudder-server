package repo_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestUploads_Get(t *testing.T) {
	ctx := context.Background()

	db := setupDB(t)

	now := time.Date(2021, 1, 1, 0, 0, 3, 0, time.UTC)
	repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))
	repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
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

	files := []model.StagingFile{
		{
			ID:           1,
			FirstEventAt: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),

			UseRudderStorage: true,
			SourceTaskRunID:  "source_task_run_id",
			SourceJobID:      "source_job_id",
			SourceJobRunID:   "source_job_run_id",
		},
		{
			ID:          2,
			LastEventAt: time.Date(2021, 1, 1, 0, 0, 1, 0, time.UTC),
		},
	}
	for _, file := range files {
		s := file.WithSchema(nil)
		_, err := repoStaging.Insert(ctx, &s)
		require.NoError(t, err)
	}

	id, err := repoUpload.CreateWithStagingFiles(ctx, ogUpload, files)
	require.NoError(t, err)
	ogUpload.ID = id
	ogUpload.Error = []byte("{}")
	ogUpload.StagingFileStartID = 1
	ogUpload.StagingFileEndID = 2
	ogUpload.FirstEventAt = time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	ogUpload.LastEventAt = time.Date(2021, 1, 1, 0, 0, 1, 0, time.UTC)
	ogUpload.LoadFileType = "csv"

	ogUpload.UseRudderStorage = true
	ogUpload.SourceTaskRunID = "source_task_run_id"
	ogUpload.SourceJobID = "source_job_id"
	ogUpload.SourceJobRunID = "source_job_run_id"

	t.Run("Get", func(t *testing.T) {
		upload, err := repoUpload.Get(ctx, id)
		require.NoError(t, err)

		require.Equal(t, ogUpload, upload)
	})
	t.Run("GetToProcess", func(t *testing.T) {
		uploads, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{})
		require.NoError(t, err)

		require.Equal(t, []model.Upload{ogUpload}, uploads)
	})

	t.Run("UploadJobsStats", func(t *testing.T) {
		uploadStats, err := repoUpload.UploadJobsStats(ctx, destType, repo.ProcessOptions{})
		require.NoError(t, err)

		require.Equal(t, model.UploadJobsStats{
			PendingJobs:    1,
			PickupLag:      0,
			PickupWaitTime: 0,
		}, uploadStats)
	})
	t.Run("UploadTimings", func(t *testing.T) {
		timings, err := repoUpload.UploadTimings(ctx, id)
		require.NoError(t, err)
		require.Equal(t, model.Timings{}, timings)

		expected := model.Timings{{
			"download": time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		}, {
			"upload": time.Date(2021, 1, 1, 0, 0, 1, 0, time.UTC),
		}}

		r, err := json.Marshal(expected)
		require.NoError(t, err)

		// TODO: implement and use repo method
		_, err = db.Exec("UPDATE wh_uploads SET timings = $1 WHERE id = $2", r, id)
		require.NoError(t, err)

		timings, err = repoUpload.UploadTimings(ctx, id)
		require.NoError(t, err)
		require.Equal(t, expected, timings)

		_, err = repoUpload.UploadTimings(ctx, -1)
		require.Equal(t, err, model.ErrUploadNotFound)
	})
}

func TestUploads_Processing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db := setupDB(t)

	now := time.Date(2021, 1, 1, 0, 0, 3, 0, time.UTC)
	repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))
	repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
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

		stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
		require.NoError(t, err)

		id, err := repoUpload.CreateWithStagingFiles(ctx, uploads[i], []model.StagingFile{{
			ID:            stagingID,
			SourceID:      uploads[i].SourceID,
			DestinationID: uploads[i].DestinationID,
		}})
		require.NoError(t, err)

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

		s, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{})
		require.NoError(t, err)

		t.Log("should get all uploads for destType")
		require.Equal(t, []model.Upload{uploads[0], uploads[4]}, s)

		s, err = repoUpload.GetToProcess(ctx, "OTHER", 10, repo.ProcessOptions{})
		require.NoError(t, err)
		t.Log("should get all uploads for OTHER")
		require.Equal(t, []model.Upload{uploads[3]}, s)
	})

	t.Run("skip workspaces", func(t *testing.T) {
		t.Parallel()

		s, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			SkipWorkspaces: []string{"workspace_id", "workspace_id_2"},
		})
		require.NoError(t, err)
		require.Empty(t, s)

		s, err = repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			SkipWorkspaces: []string{"workspace_id"},
		})
		require.NoError(t, err)
		require.Equal(t, []model.Upload{uploads[4]}, s)
	})

	t.Run("multiple sources", func(t *testing.T) {
		t.Parallel()

		s, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			AllowMultipleSourcesForJobsPickup: true,
		})
		require.NoError(t, err)

		t.Log("should get all uploads for destType")
		require.Equal(t, []model.Upload{uploads[0], uploads[1], uploads[2], uploads[4]}, s)

		s, err = repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			AllowMultipleSourcesForJobsPickup: false,
		})
		require.NoError(t, err)
		t.Log("should get only one upload per source")
		require.Equal(t, []model.Upload{uploads[0], uploads[4]}, s)
	})

	t.Run("skip identifiers", func(t *testing.T) {
		s, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			AllowMultipleSourcesForJobsPickup: false,
			SkipIdentifiers:                   []string{"destination_id_4_namespace"},
		})
		require.NoError(t, err)
		require.Equal(t, []model.Upload{uploads[0]}, s)

		s, err = repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
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
		SourceTaskRunID:  "source_task_run_id",
		SourceJobID:      "source_job_id",
		SourceJobRunID:   "source_job_run_id",
		LoadFileType:     "load_file_type",
		Retried:          true,
		Priority:         40,
		NextRetryTime:    time.Date(2021, 1, 1, 0, 0, 1, 0, time.UTC),
	}, metadata)
}

func TestUploads_Delete(t *testing.T) {
	ctx := context.Background()
	db := setupDB(t)

	now := time.Date(2021, 1, 1, 0, 0, 3, 0, time.UTC)
	repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))
	repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
		return now
	}))

	file := model.StagingFile{
		WorkspaceID:   "workspace_id",
		Location:      "s3://bucket/path/to/file",
		SourceID:      "source_id",
		DestinationID: "destination_id",
		Status:        warehouseutils.StagingFileWaitingState,
		Error:         nil,
		FirstEventAt:  time.Now(),
		LastEventAt:   time.Now(),
	}.WithSchema([]byte(`{"type": "object"}`))

	stagingID, err := repoStaging.Insert(ctx, &file)
	require.NoError(t, err)

	uploadID, err := repoUpload.CreateWithStagingFiles(ctx, model.Upload{
		SourceID:      "source_id",
		DestinationID: "destination_id",
		Status:        model.Waiting,
	}, []model.StagingFile{
		{
			ID:            stagingID,
			SourceID:      "source_id",
			DestinationID: "destination_id",
		},
	})
	require.NoError(t, err)

	files, err := repoStaging.Pending(ctx, "source_id", "destination_id")
	require.NoError(t, err)
	require.Len(t, files, 0)

	err = repoUpload.DeleteWaiting(ctx, uploadID)
	require.NoError(t, err)

	files, err = repoStaging.Pending(ctx, "source_id", "destination_id")
	require.NoError(t, err)
	require.Len(t, files, 1)
}

func TestUploads_InterruptedDestinations(t *testing.T) {
	t.Parallel()
	db := setupDB(t)

	_, err := db.Exec(`INSERT INTO wh_uploads (destination_id, source_id, in_progress, destination_type, status, namespace, schema, created_at, updated_at)
		VALUES
		(1, 1, true, 'RS', 'exporting_data', '', '{}', NOW(), NOW()),
		(2, 1, true, 'RS', 'exporting_data_failed', '', '{}', NOW(), NOW()),
		(3, 1, true, 'RS', 'exporting_data_failed', '', '{}', NOW(), NOW()),

		(4, 1, true, 'RS', 'exported_data', '', '{}', NOW(), NOW()),
		(5, 1, true, 'RS', 'aborted', '', '{}', NOW(), NOW()),
		(6, 1, true, 'RS', 'failed', '', '{}', NOW(), NOW()),
		(7, 1, true, 'SNOWFLAKE', 'exporting_data', '', '{}', NOW(), NOW())
	`)
	require.NoError(t, err)

	repoUpload := repo.NewUploads(db)
	ids, err := repoUpload.InterruptedDestinations(context.Background(), "RS")
	require.NoError(t, err)

	require.Equal(t, []string{"1", "2", "3"}, ids)
}
