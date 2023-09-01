package repo_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samber/lo"

	"github.com/stretchr/testify/require"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestUploads_Count(t *testing.T) {
	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
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
			Status:          model.ExportedData,
			SourceTaskRunID: "task_run_id",
		},
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_id",
			DestinationID:   "destination_id",
			DestinationType: destType,
			Status:          model.Aborted,
			SourceTaskRunID: "task_run_id",
		},
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_id",
			DestinationID:   "destination_id",
			DestinationType: destType,
			Status:          model.ExportingData,
			SourceTaskRunID: "task_run_id",
		},
		{
			WorkspaceID:     "workspace_id_1",
			Namespace:       "namespace",
			SourceID:        "source_id_1",
			DestinationID:   "destination_id_1",
			DestinationType: destType,
			Status:          model.Aborted,
			SourceTaskRunID: "task_run_id_1",
		},
		{
			WorkspaceID:     "workspace_id_1",
			Namespace:       "namespace",
			SourceID:        "source_id_1",
			DestinationID:   "destination_id_1",
			DestinationType: destType,
			Status:          model.Aborted,
			SourceTaskRunID: "task_run_id_1",
		},
	}

	for i := range uploads {

		stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
		require.NoError(t, err)

		id, err := repoUpload.CreateWithStagingFiles(ctx, uploads[i], []*model.StagingFile{{
			ID:              stagingID,
			SourceID:        uploads[i].SourceID,
			DestinationID:   uploads[i].DestinationID,
			SourceTaskRunID: uploads[i].SourceTaskRunID,
		}})

		require.NoError(t, err)

		uploads[i].ID = id
		uploads[i].Error = []byte("{}")
		uploads[i].UploadSchema = model.Schema{}
		uploads[i].LoadFileType = "csv"
		uploads[i].StagingFileStartID = int64(i + 1)
		uploads[i].StagingFileEndID = int64(i + 1)
	}

	t.Run("query to count with not equal filters works correctly", func(t *testing.T) {
		t.Parallel()

		count, err := repoUpload.Count(ctx, []repo.FilterBy{
			{Key: "source_id", Value: "source_id"},
			{Key: "metadata->>'source_task_run_id'", Value: "task_run_id"},
			{Key: "status", NotEquals: true, Value: model.ExportedData},
			{Key: "status", NotEquals: true, Value: model.Aborted},
		}...)

		require.NoError(t, err)
		require.Equal(t, int64(1), count)
	})

	t.Run("query to count with equal filters works correctly", func(t *testing.T) {
		t.Parallel()
		count, err := repoUpload.Count(ctx, []repo.FilterBy{
			{Key: "source_id", Value: "source_id_1"},
			{Key: "metadata->>'source_task_run_id'", Value: "task_run_id_1"},
			{Key: "status", Value: model.Aborted},
		}...)

		require.NoError(t, err)
		require.Equal(t, int64(2), count)
	})
}

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
	}

	files := []*model.StagingFile{
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

func TestUploads_GetToProcess(t *testing.T) {
	var (
		workspaceID     = "workspace_id"
		namespace       = "namespace"
		sourceID        = "source_id"
		destID          = "dest_id"
		sourceTaskRunID = "source_task_run_id"
		sourceJobID     = "source_job_id"
		sourceJobRunID  = "source_job_run_id"
		loadFileType    = "csv"
		destType        = warehouseutils.RS
		ctx             = context.Background()
	)

	prepareUpload := func(db *sqlmiddleware.DB, sourceID string, status model.UploadStatus, priority int, now, nextRetryTime time.Time) model.Upload {
		stagingFileID := int64(0)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))
		repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
			return now
		}))

		var (
			startStagingFileID = atomic.AddInt64(&stagingFileID, 1)
			endStagingFileID   = atomic.AddInt64(&stagingFileID, 1)
			firstEventAt       = now.Add(-30 * time.Minute)
			lastEventAt        = now
		)

		ogUpload := model.Upload{
			WorkspaceID:     workspaceID,
			Namespace:       namespace,
			SourceID:        sourceID,
			DestinationID:   destID,
			DestinationType: destType,
			Status:          status,
			Error:           []byte("{}"),
			FirstEventAt:    firstEventAt,
			LastEventAt:     lastEventAt,
			LoadFileType:    loadFileType,
			Priority:        priority,
			NextRetryTime:   nextRetryTime,
			Retried:         true,

			StagingFileStartID: startStagingFileID,
			StagingFileEndID:   endStagingFileID,
			LoadFileStartID:    0,
			LoadFileEndID:      0,
			Timings:            nil,
			FirstAttemptAt:     time.Time{},
			LastAttemptAt:      time.Time{},
			Attempts:           0,
			UploadSchema:       model.Schema{},

			UseRudderStorage: true,
			SourceTaskRunID:  sourceTaskRunID,
			SourceJobID:      sourceJobID,
			SourceJobRunID:   sourceTaskRunID,
		}

		files := []*model.StagingFile{
			{
				ID:           startStagingFileID,
				FirstEventAt: firstEventAt,

				UseRudderStorage: true,
				SourceTaskRunID:  sourceTaskRunID,
				SourceJobID:      sourceJobID,
				SourceJobRunID:   sourceJobRunID,
			},
			{
				ID:          endStagingFileID,
				LastEventAt: lastEventAt,
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

		return ogUpload
	}

	t.Run("none present", func(t *testing.T) {
		t.Parallel()

		var (
			db         = setupDB(t)
			repoUpload = repo.NewUploads(db)
			priority   = 100

			uploads []model.Upload
		)

		uploads = append(uploads,
			prepareUpload(db, sourceID, model.Aborted, priority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
			prepareUpload(db, sourceID, model.ExportedData, priority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
		)
		require.Len(t, uploads, 2)

		toProcess, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{})
		require.NoError(t, err)
		require.Len(t, toProcess, 0)
	})

	t.Run("skip identifier", func(t *testing.T) {
		t.Parallel()

		var (
			db         = setupDB(t)
			repoUpload = repo.NewUploads(db)
			priority   = 100

			uploads []model.Upload
		)

		uploads = append(uploads,
			prepareUpload(db, sourceID, model.Waiting, priority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
			prepareUpload(db, sourceID, model.ExportingDataFailed, priority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
		)
		require.Len(t, uploads, 2)

		toProcess, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{})
		require.NoError(t, err)
		require.Len(t, toProcess, 1)
		require.Equal(t, uploads[0].ID, toProcess[0].ID)

		skipIdentifier := fmt.Sprintf("%s_%s", destID, namespace)
		toProcess, err = repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			SkipIdentifiers: []string{skipIdentifier},
		})
		require.NoError(t, err)
		require.Len(t, toProcess, 0)
	})

	t.Run("skip workspaces", func(t *testing.T) {
		t.Parallel()

		var (
			db         = setupDB(t)
			repoUpload = repo.NewUploads(db)
			priority   = 100

			uploads []model.Upload
		)

		uploads = append(uploads,
			prepareUpload(db, sourceID, model.Waiting, priority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
			prepareUpload(db, sourceID, model.ExportingDataFailed, priority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
		)
		require.Len(t, uploads, 2)

		toProcess, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{})
		require.NoError(t, err)
		require.Len(t, toProcess, 1)
		require.Equal(t, uploads[0].ID, toProcess[0].ID)

		toProcess, err = repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			SkipWorkspaces: []string{workspaceID},
		})
		require.NoError(t, err)
		require.Len(t, toProcess, 0)
	})

	t.Run("ordering by priority", func(t *testing.T) {
		t.Parallel()

		var (
			db                = setupDB(t)
			repoUpload        = repo.NewUploads(db)
			lowPriority       = 100
			highPriority      = 0
			differentSourceID = "source_id_2"

			uploads []model.Upload
		)

		uploads = append(uploads,
			prepareUpload(db, sourceID, model.Waiting, lowPriority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
			prepareUpload(db, sourceID, model.Waiting, highPriority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
			prepareUpload(db, differentSourceID, model.Waiting, lowPriority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
			prepareUpload(db, differentSourceID, model.Waiting, highPriority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
		)
		require.Len(t, uploads, 4)

		toProcess, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{})
		require.NoError(t, err)
		require.Len(t, toProcess, 1)
		require.Equal(t, uploads[1].ID, toProcess[0].ID)

		toProcess, err = repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			AllowMultipleSourcesForJobsPickup: true,
		})
		require.NoError(t, err)
		require.Len(t, toProcess, 2)
		require.Equal(t, uploads[1].ID, toProcess[0].ID)
		require.Equal(t, uploads[3].ID, toProcess[1].ID)
	})

	t.Run("ordering by first event at", func(t *testing.T) {
		t.Parallel()

		var (
			db                = setupDB(t)
			repoUpload        = repo.NewUploads(db)
			lowPriority       = 100
			highPriority      = 0
			differentSourceID = "source_id_2"

			uploads []model.Upload
		)

		uploads = append(uploads,
			prepareUpload(db, sourceID, model.ExportingDataFailed, lowPriority,
				time.Date(2021, 1, 1, 0, 40, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
			prepareUpload(db, sourceID, model.Waiting, highPriority,
				time.Date(2021, 1, 1, 0, 25, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
			prepareUpload(db, differentSourceID, model.GeneratedUploadSchema, lowPriority,
				time.Date(2021, 1, 1, 0, 30, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
			prepareUpload(db, differentSourceID, model.GeneratingLoadFiles, highPriority,
				time.Date(2021, 1, 1, 0, 15, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
		)
		require.Len(t, uploads, 4)

		toProcess, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{})
		require.NoError(t, err)
		require.Len(t, toProcess, 1)
		require.Equal(t, uploads[3].ID, toProcess[0].ID)

		toProcess, err = repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
			AllowMultipleSourcesForJobsPickup: true,
		})
		require.NoError(t, err)
		require.Len(t, toProcess, 2)
		require.Equal(t, uploads[3].ID, toProcess[0].ID)
		require.Equal(t, uploads[1].ID, toProcess[1].ID)
	})

	t.Run("allow multiple sources for jobs pickup", func(t *testing.T) {
		t.Parallel()

		var (
			db                = setupDB(t)
			repoUpload        = repo.NewUploads(db)
			priority          = 100
			differentSourceID = "source_id_2"

			uploads []model.Upload
		)

		uploads = append(uploads,
			prepareUpload(db, sourceID, model.Waiting, priority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
			prepareUpload(db, sourceID, model.ExportingDataFailed, priority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
			prepareUpload(db, differentSourceID, model.Waiting, priority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
			prepareUpload(db, differentSourceID, model.ExportingDataFailed, priority,
				time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC),
			),
		)
		require.Len(t, uploads, 4)

		t.Run("single source", func(t *testing.T) {
			toProcess, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{})
			require.NoError(t, err)
			require.Len(t, toProcess, 1)
			require.Equal(t, uploads[0].ID, toProcess[0].ID)
		})

		t.Run("multiple sources", func(t *testing.T) {
			toProcess, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
				AllowMultipleSourcesForJobsPickup: true,
			})
			require.NoError(t, err)
			require.Len(t, toProcess, 2)
			require.Equal(t, uploads[0].ID, toProcess[0].ID)
			require.Equal(t, uploads[2].ID, toProcess[1].ID)
		})

		t.Run("skip few identifiers", func(t *testing.T) {
			toProcess, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
				SkipIdentifiers: []string{
					fmt.Sprintf("%s_%s_%s", sourceID, destID, namespace),
				},
				AllowMultipleSourcesForJobsPickup: true,
			})
			require.NoError(t, err)
			require.Len(t, toProcess, 1)
			require.Equal(t, uploads[2].ID, toProcess[0].ID)
		})

		t.Run("skip all identifiers", func(t *testing.T) {
			toProcess, err := repoUpload.GetToProcess(ctx, destType, 10, repo.ProcessOptions{
				SkipIdentifiers: []string{
					fmt.Sprintf("%s_%s_%s", sourceID, destID, namespace),
					fmt.Sprintf("%s_%s_%s", differentSourceID, destID, namespace),
				},
				AllowMultipleSourcesForJobsPickup: true,
			})
			require.NoError(t, err)
			require.Len(t, toProcess, 0)
		})
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

		id, err := repoUpload.CreateWithStagingFiles(ctx, uploads[i], []*model.StagingFile{{
			ID:            stagingID,
			SourceID:      uploads[i].SourceID,
			DestinationID: uploads[i].DestinationID,
		}})
		require.NoError(t, err)

		uploads[i].ID = id
		uploads[i].Error = []byte("{}")
		uploads[i].UploadSchema = model.Schema{}
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
	}, []*model.StagingFile{
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

func TestUploads_PendingTableUploads(t *testing.T) {
	t.Parallel()

	const (
		uploadID    = 1
		namespace   = "namespace"
		destID      = "destination_id"
		sourceID    = "source_id"
		destType    = "RS"
		workspaceID = "workspace_id"
	)

	var (
		ctx             = context.Background()
		db              = setupDB(t)
		repoUpload      = repo.NewUploads(db)
		repoTableUpload = repo.NewTableUploads(db)
		repoStaging     = repo.NewStagingFiles(db)
	)

	for _, status := range []string{"exporting_data", "aborted"} {
		file := model.StagingFile{
			WorkspaceID:   workspaceID,
			Location:      "s3://bucket/path/to/file",
			SourceID:      sourceID,
			DestinationID: destID,
			Status:        warehouseutils.StagingFileWaitingState,
			Error:         nil,
			FirstEventAt:  time.Now(),
			LastEventAt:   time.Now(),
		}.WithSchema([]byte(`{"type": "object"}`))

		stagingID, err := repoStaging.Insert(ctx, &file)
		require.NoError(t, err)

		_, err = repoUpload.CreateWithStagingFiles(
			ctx,
			model.Upload{
				SourceID:        sourceID,
				DestinationID:   destID,
				Status:          status,
				Namespace:       namespace,
				DestinationType: destType,
			},
			[]*model.StagingFile{
				{
					ID:            stagingID,
					SourceID:      sourceID,
					DestinationID: destID,
				},
			},
		)
		require.NoError(t, err)
	}

	for i, tu := range []struct {
		status string
		err    string
	}{
		{
			status: "exporting_data",
			err:    "{}",
		},
		{
			status: "exporting_data_failed",
			err:    "error loading data",
		},
	} {
		tableName := fmt.Sprintf("test_table_%d", i+1)

		err := repoTableUpload.Insert(ctx, uploadID, []string{tableName})
		require.NoError(t, err)

		err = repoTableUpload.Set(ctx, uploadID, tableName, repo.TableUploadSetOptions{
			Status: &tu.status,
			Error:  &tu.err,
		})
		require.NoError(t, err)
	}

	t.Run("should return pending table uploads", func(t *testing.T) {
		t.Parallel()

		repoUpload := repo.NewUploads(db)
		pendingTableUploads, err := repoUpload.PendingTableUploads(context.Background(), namespace, uploadID, destID)
		require.NoError(t, err)
		require.NotEmpty(t, pendingTableUploads)

		expectedPendingTableUploads := []model.PendingTableUpload{
			{
				UploadID:      uploadID,
				DestinationID: destID,
				Namespace:     namespace,
				TableName:     "test_table_1",
				Status:        "exporting_data",
				Error:         "{}",
			},
			{
				UploadID:      uploadID,
				DestinationID: destID,
				Namespace:     namespace,
				TableName:     "test_table_2",
				Status:        "exporting_data_failed",
				Error:         "error loading data",
			},
		}
		require.Equal(t, expectedPendingTableUploads, pendingTableUploads)
	})

	t.Run("should return empty pending table uploads", func(t *testing.T) {
		t.Parallel()

		repoUpload := repo.NewUploads(db)
		pendingTableUploads, err := repoUpload.PendingTableUploads(context.Background(), namespace, int64(-1), destID)
		require.NoError(t, err)
		require.Empty(t, pendingTableUploads)
	})

	t.Run("cancelled context", func(t *testing.T) {
		t.Parallel()

		repoUpload := repo.NewUploads(db)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := repoUpload.PendingTableUploads(ctx, namespace, uploadID, destID)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestUploads_ResetInProgress(t *testing.T) {
	const (
		sourceID        = "source_id"
		destinationID   = "destination_id"
		destinationType = "destination_type"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))

	t.Run("success", func(t *testing.T) {
		repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
			return now
		}))

		stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
		require.NoError(t, err)

		uploadID, err := repoUpload.CreateWithStagingFiles(ctx, model.Upload{
			SourceID:        sourceID,
			DestinationID:   destinationID,
			DestinationType: destinationType,
			Status:          model.Waiting,
		}, []*model.StagingFile{
			{
				ID:            stagingID,
				SourceID:      sourceID,
				DestinationID: destinationID,
			},
		})
		require.NoError(t, err)

		_, err = db.ExecContext(ctx, `UPDATE wh_uploads SET in_progress = TRUE WHERE id = $1;`, uploadID)
		require.NoError(t, err)

		uploadsToProcess, err := repoUpload.GetToProcess(ctx, destinationType, 1, repo.ProcessOptions{})
		require.NoError(t, err)
		require.Len(t, uploadsToProcess, 0)

		err = repoUpload.ResetInProgress(ctx, destinationType)
		require.NoError(t, err)

		uploadsToProcess, err = repoUpload.GetToProcess(ctx, destinationType, 1, repo.ProcessOptions{})
		require.NoError(t, err)
		require.Len(t, uploadsToProcess, 1)
	})

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		err := repoUpload.ResetInProgress(ctx, destinationType)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestUploads_LastCreatedAt(t *testing.T) {
	const (
		sourceID        = "source_id"
		destinationID   = "destination_id"
		destinationType = "destination_type"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))

	t.Run("many uploads", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
				return now
			}))
			stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
			require.NoError(t, err)

			repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
				return now.Add(time.Second * time.Duration(i+1))
			}))

			_, err = repoUpload.CreateWithStagingFiles(ctx, model.Upload{
				SourceID:        sourceID,
				DestinationID:   destinationID,
				DestinationType: destinationType,
				Status:          model.Waiting,
			}, []*model.StagingFile{
				{
					ID:            stagingID,
					SourceID:      sourceID,
					DestinationID: destinationID,
				},
			})
			require.NoError(t, err)
		}

		lastCreatedAt, err := repoUpload.LastCreatedAt(ctx, sourceID, destinationID)
		require.NoError(t, err)
		require.Equal(t, lastCreatedAt.UTC(), now.Add(time.Second*5).UTC())
	})

	t.Run("no uploads", func(t *testing.T) {
		lastCreatedAt, err := repoUpload.LastCreatedAt(ctx, "unknown_source", "unknown_destination")
		require.NoError(t, err)
		require.Equal(t, lastCreatedAt, time.Time{})
	})

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		lastCreatedAt, err := repoUpload.LastCreatedAt(ctx, sourceID, destinationID)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, lastCreatedAt, time.Time{})
	})
}

func TestUploads_TriggerUpload(t *testing.T) {
	const (
		sourceID        = "source_id"
		destinationID   = "destination_id"
		destinationType = "destination_type"
		workspaceID     = "workspace_id"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))
	repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
		return now
	}))

	stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
	require.NoError(t, err)

	uploadID, err := repoUpload.CreateWithStagingFiles(ctx, model.Upload{
		SourceID:        sourceID,
		DestinationID:   destinationID,
		DestinationType: destinationType,
		WorkspaceID:     workspaceID,
		Status:          model.Waiting,
	}, []*model.StagingFile{
		{
			ID:            stagingID,
			SourceID:      sourceID,
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
		},
	})
	require.NoError(t, err)

	t.Run("success", func(t *testing.T) {
		err := repoUpload.TriggerUpload(ctx, uploadID)
		require.NoError(t, err)

		upload, err := repoUpload.Get(ctx, uploadID)
		require.NoError(t, err)
		require.Equal(t, upload.Status, model.Waiting)
		require.True(t, upload.Retried)
		require.Equal(t, upload.Priority, 50)
	})

	t.Run("unknown id", func(t *testing.T) {
		err := repoUpload.TriggerUpload(ctx, -1)
		require.EqualError(t, err, "trigger uploads: no rows affected")
	})

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		err := repoUpload.TriggerUpload(ctx, uploadID)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestUploads_Retry(t *testing.T) {
	const (
		sourceID        = "source_id"
		destinationID   = "destination_id"
		destinationType = "destination_type"
		workspaceID     = "workspace_id"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))
	repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
		return now
	}))

	stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
	require.NoError(t, err)

	uploadID, err := repoUpload.CreateWithStagingFiles(ctx, model.Upload{
		SourceID:        sourceID,
		DestinationID:   destinationID,
		DestinationType: destinationType,
		WorkspaceID:     workspaceID,
		Status:          model.Waiting,
	}, []*model.StagingFile{
		{
			ID:            stagingID,
			SourceID:      sourceID,
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
		},
	})
	require.NoError(t, err)

	intervalInHours := time.Since(now.Add(-12*time.Hour)) / time.Hour

	t.Run("filters", func(t *testing.T) {
		testCases := []struct {
			name       string
			filters    model.RetryOptions
			retryCount int
			wantError  error
		}{
			{
				name: "all filters",
				filters: model.RetryOptions{
					WorkspaceID:     workspaceID,
					SourceIDs:       []string{sourceID},
					DestinationID:   destinationID,
					DestinationType: destinationType,
					ForceRetry:      true,
					UploadIds:       []int64{uploadID},
					IntervalInHours: int64(intervalInHours),
				},
				retryCount: 1,
			},
			{
				name: "no aborted jobs",
				filters: model.RetryOptions{
					WorkspaceID:     workspaceID,
					SourceIDs:       []string{sourceID},
					DestinationID:   destinationID,
					DestinationType: destinationType,
					ForceRetry:      false,
					UploadIds:       []int64{uploadID},
					IntervalInHours: int64(intervalInHours),
				},
				retryCount: 0,
			},
			{
				name: "few filters",
				filters: model.RetryOptions{
					WorkspaceID:     workspaceID,
					DestinationID:   destinationID,
					ForceRetry:      true,
					IntervalInHours: int64(intervalInHours),
				},
				retryCount: 1,
			},
			{
				name: "unknown filters",
				filters: model.RetryOptions{
					WorkspaceID:     "unknown_workspace_id",
					DestinationID:   "unknown_destination_id",
					IntervalInHours: int64(intervalInHours),
				},
				retryCount: 0,
			},
			{
				name:       "no filters",
				filters:    model.RetryOptions{},
				retryCount: 0,
			},
		}

		for _, tc := range testCases {
			t.Run("count to retry "+tc.name, func(t *testing.T) {
				retryCount, err := repoUpload.RetryCount(ctx, tc.filters)
				if tc.wantError != nil {
					require.ErrorIs(t, err, tc.wantError)
					return
				}
				require.NoError(t, err)
				require.EqualValues(t, retryCount, tc.retryCount)
			})

			t.Run("retry "+tc.name, func(t *testing.T) {
				retryCount, err := repoUpload.Retry(ctx, tc.filters)
				if tc.wantError != nil {
					require.ErrorIs(t, err, tc.wantError)
					return
				}
				require.NoError(t, err)
				require.EqualValues(t, retryCount, tc.retryCount)

				if tc.retryCount > 0 {
					upload, err := repoUpload.Get(ctx, uploadID)
					require.NoError(t, err)
					require.Equal(t, upload.Status, model.Waiting)
					require.True(t, upload.Retried)
					require.Equal(t, upload.Priority, 50)
				}
			})
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		retryCount, err := repoUpload.Retry(ctx, model.RetryOptions{})
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, retryCount)

		retryCount, err = repoUpload.RetryCount(ctx, model.RetryOptions{})
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, retryCount)
	})
}

func TestUploads_SyncsInfo(t *testing.T) {
	const (
		sourceID        = "source_id"
		destinationID   = "destination_id"
		destinationType = "destination_type"
		workspaceID     = "workspace_id"

		totalUploads = 100
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 1, 0, 0, 0, time.UTC)
	repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))
	repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
		return now
	}))

	firstEventAt, lastEventAt := now.Add(-2*time.Hour), now.Add(-1*time.Hour)
	lastExecAt := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	var uploadIDs []int64
	for i := 0; i < totalUploads; i++ {
		fid, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
		require.NoError(t, err)
		sid, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
		require.NoError(t, err)

		uploadID, err := repoUpload.CreateWithStagingFiles(ctx, model.Upload{
			SourceID:        sourceID,
			DestinationID:   destinationID,
			DestinationType: destinationType,
			WorkspaceID:     workspaceID,
			Status:          model.Waiting,
			NextRetryTime:   now,
		}, []*model.StagingFile{
			{
				ID:            fid,
				SourceID:      sourceID,
				DestinationID: destinationID,
				WorkspaceID:   workspaceID,
				FirstEventAt:  firstEventAt,
			},
			{
				ID:            sid,
				SourceID:      sourceID,
				DestinationID: destinationID,
				WorkspaceID:   workspaceID,
				LastEventAt:   lastEventAt,
			},
		})
		require.NoError(t, err)

		uploadIDs = append(uploadIDs, uploadID)
	}

	reverseUploadIDs := lo.Reverse(uploadIDs)

	for _, uploadID := range uploadIDs[26:51] {
		_, err := db.ExecContext(ctx, "UPDATE wh_uploads SET status = $3, error = $1, last_exec_at = $4, timings = $2 WHERE id = $5;", `
			{
			  "exporting_data_failed": {
				"errors": [
				  "test_exporting_error"
				],
				"attempt": 7
			  },
			  "generating_load_files_failed": {
				"errors": [
				  "test_generating_load_files_error"
				],
				"attempt": 3
			  }
			}
`, `
			[
			  {
				"exporting_data_failed": "2023-08-15T09:30:35.829895191Z"
			  }
			]
`,
			model.Failed,
			lastExecAt,
			uploadID,
		)
		require.NoError(t, err)
	}
	for _, uploadID := range uploadIDs[51:76] {
		_, err := db.ExecContext(ctx, "UPDATE wh_uploads SET status = $3, error = $1, last_exec_at = $4, timings = $2 WHERE id = $5;", `
			{
			  "exporting_data_failed": {
				"errors": [
				  "test_exporting_error"
				],
				"attempt": 25
			  },
			  "generating_load_files_failed": {
				"errors": [
				  "test_generating_load_files_error"
				],
				"attempt": 3
			  }
			}

`, `
			[
			  {
				"exporting_data_failed": "2023-08-15T09:30:35.829895191Z"
			  }
			]
`,
			model.Aborted,
			lastExecAt,
			uploadID,
		)
		require.NoError(t, err)
	}
	for _, uploadID := range uploadIDs[76:] {
		_, err := db.ExecContext(ctx, "UPDATE wh_uploads SET status = $2, metadata = $1 WHERE id = $3;", `
			{
			  "archivedStagingAndLoadFiles": true
			}
`,
			model.ExportedData,
			uploadID,
		)
		require.NoError(t, err)
	}

	t.Run("limit and offset", func(t *testing.T) {
		testCases := []struct {
			name        string
			limit       int
			offset      int
			expectedIDs []int64
		}{
			{
				name:        "limit 10 offset 0",
				limit:       10,
				offset:      0,
				expectedIDs: reverseUploadIDs[:10],
			},
			{
				name:        "limit 35 offset 10",
				limit:       35,
				offset:      10,
				expectedIDs: reverseUploadIDs[10:45],
			},
			{
				name:        "limit 10 offset 100",
				limit:       10,
				offset:      100,
				expectedIDs: []int64{},
			},
			{
				name:        "limit 10 offset 95",
				limit:       10,
				offset:      95,
				expectedIDs: reverseUploadIDs[95:],
			},
		}
		for _, tc := range testCases {
			options := model.SyncUploadOptions{
				SourceIDs:       []string{sourceID},
				DestinationID:   destinationID,
				DestinationType: destinationType,
			}

			t.Run("multi-tenant"+tc.name, func(t *testing.T) {
				uploadInfos, uploadsCount, err := repoUpload.SyncsInfoForMultiTenant(ctx, tc.limit, tc.offset, options)
				require.NoError(t, err)
				require.EqualValues(t, uploadsCount, totalUploads)
				require.Equal(t, len(uploadInfos), len(tc.expectedIDs))
				require.Equal(t, tc.expectedIDs, lo.Map(uploadInfos, func(info model.UploadInfo, index int) int64 {
					return info.ID
				}))
			})

			t.Run("non multi-tenant"+tc.name, func(t *testing.T) {
				uploadInfos, uploadsCount, err := repoUpload.SyncsInfoForNonMultiTenant(ctx, tc.limit, tc.offset, options)
				require.NoError(t, err)
				require.EqualValues(t, uploadsCount, totalUploads)
				require.Equal(t, len(uploadInfos), len(tc.expectedIDs))
				require.Equal(t, tc.expectedIDs, lo.Map(uploadInfos, func(info model.UploadInfo, index int) int64 {
					return info.ID
				}))
			})
		}
	})

	t.Run("all filters", func(t *testing.T) {
		filters := model.SyncUploadOptions{
			SourceIDs:       []string{sourceID},
			DestinationID:   destinationID,
			DestinationType: destinationType,
		}

		t.Log("syncs info for multi-tenant")
		multiTenantUploadInfos, multiTenantTotalUploads, err := repoUpload.SyncsInfoForMultiTenant(ctx, 1000, 0, filters)
		require.NoError(t, err)
		require.EqualValues(t, multiTenantTotalUploads, totalUploads)
		require.Len(t, multiTenantUploadInfos, totalUploads)

		t.Log("syncs info for non-multi-tenant")
		nonMultiTenantUploadInfos, nonMultiTenantTotalUploads, err := repoUpload.SyncsInfoForNonMultiTenant(ctx, 1000, 0, filters)
		require.NoError(t, err)
		require.EqualValues(t, nonMultiTenantTotalUploads, totalUploads)
		require.Len(t, nonMultiTenantUploadInfos, totalUploads)

		t.Log("compare multi-tenant and non-multi-tenant")
		require.EqualValues(t, multiTenantUploadInfos, nonMultiTenantUploadInfos)

		for id, uploadInfo := range nonMultiTenantUploadInfos {
			require.Equal(t, uploadInfo.ID, reverseUploadIDs[id])
			require.Equal(t, uploadInfo.SourceID, sourceID)
			require.Equal(t, uploadInfo.DestinationID, destinationID)
			require.Equal(t, uploadInfo.DestinationType, destinationType)
			require.Equal(t, uploadInfo.CreatedAt.UTC(), now.UTC())
			require.Equal(t, uploadInfo.UpdatedAt.UTC(), now.UTC())
			require.Equal(t, uploadInfo.FirstEventAt.UTC(), firstEventAt.UTC())
			require.Equal(t, uploadInfo.LastEventAt.UTC(), lastEventAt.UTC())
		}
		for _, uploadInfo := range nonMultiTenantUploadInfos[:26] {
			require.Equal(t, uploadInfo.Status, model.Waiting)
			require.Equal(t, uploadInfo.Error, "{}")
			require.Zero(t, uploadInfo.LastExecAt)
			require.Equal(t, uploadInfo.NextRetryTime.UTC(), now.UTC())
			require.Equal(t, uploadInfo.Duration, now.Sub(time.Time{})/time.Second)
			require.Zero(t, uploadInfo.Attempt)
			require.False(t, uploadInfo.IsArchivedUpload)
		}
		for _, uploadInfo := range nonMultiTenantUploadInfos[26:51] {
			require.Equal(t, uploadInfo.Status, model.Failed)
			require.Equal(t, uploadInfo.Error, "test_exporting_error")
			require.Equal(t, uploadInfo.LastExecAt.UTC(), lastExecAt.UTC())
			require.Equal(t, uploadInfo.NextRetryTime.UTC(), now.UTC())
			require.Equal(t, uploadInfo.Duration, time.Hour/time.Second)
			require.Equal(t, uploadInfo.Attempt, int64(10))
			require.False(t, uploadInfo.IsArchivedUpload)
		}
		for _, uploadInfo := range nonMultiTenantUploadInfos[51:76] {
			require.Equal(t, uploadInfo.Status, model.Aborted)
			require.Equal(t, uploadInfo.Error, "test_exporting_error")
			require.Equal(t, uploadInfo.LastExecAt.UTC(), lastExecAt.UTC())
			require.Zero(t, uploadInfo.NextRetryTime)
			require.Equal(t, uploadInfo.Duration, time.Hour/time.Second)
			require.Equal(t, uploadInfo.Attempt, int64(28))
			require.False(t, uploadInfo.IsArchivedUpload)
		}
		for _, uploadInfo := range nonMultiTenantUploadInfos[76:] {
			require.Equal(t, uploadInfo.Status, model.ExportedData)
			require.Equal(t, uploadInfo.Error, "{}")
			require.Zero(t, uploadInfo.LastExecAt)
			require.Zero(t, uploadInfo.NextRetryTime)
			require.Equal(t, uploadInfo.Duration, now.Sub(time.Time{})/time.Second)
			require.Zero(t, uploadInfo.Attempt)
			require.True(t, uploadInfo.IsArchivedUpload)
		}
	})

	t.Run("few filters", func(t *testing.T) {
		testCases := []struct {
			name        string
			options     model.SyncUploadOptions
			expectedIDs []int64
		}{
			{
				name: "only upload id",
				options: model.SyncUploadOptions{
					UploadID: 55,
				},
				expectedIDs: []int64{55},
			},
			{
				name: "status is aborted",
				options: model.SyncUploadOptions{
					Status: model.Aborted,
				},
				expectedIDs: reverseUploadIDs[51:76],
			},
		}

		for _, tc := range testCases {
			t.Run("multi-tenant"+tc.name, func(t *testing.T) {
				uploadInfos, totalUploads, err := repoUpload.SyncsInfoForMultiTenant(ctx, 1000, 0, tc.options)
				require.NoError(t, err)
				require.EqualValues(t, totalUploads, len(tc.expectedIDs))
				require.Equal(t, tc.expectedIDs, lo.Map(uploadInfos, func(info model.UploadInfo, index int) int64 {
					return info.ID
				}))
			})
			t.Run("non-multi-tenant"+tc.name, func(t *testing.T) {
				uploadInfos, totalUploads, err := repoUpload.SyncsInfoForNonMultiTenant(ctx, 1000, 0, tc.options)
				require.NoError(t, err)
				require.EqualValues(t, totalUploads, len(tc.expectedIDs))
				require.Equal(t, tc.expectedIDs, lo.Map(uploadInfos, func(info model.UploadInfo, index int) int64 {
					return info.ID
				}))
			})
		}
	})

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		filters := model.SyncUploadOptions{
			SourceIDs:       []string{sourceID},
			DestinationID:   destinationID,
			DestinationType: destinationType,
		}

		multiTenantUploadInfos, multiTenantTotalUploads, err := repoUpload.SyncsInfoForMultiTenant(ctx, 1000, 0, filters)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, multiTenantUploadInfos)
		require.Zero(t, multiTenantTotalUploads)

		nonMultiTenantUploadInfos, nonMultiTenantTotalUploads, err := repoUpload.SyncsInfoForNonMultiTenant(ctx, 1000, 0, filters)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, nonMultiTenantUploadInfos)
		require.Zero(t, nonMultiTenantTotalUploads)
	})
}

func TestUploads_GetLatestUploadInfo(t *testing.T) {
	const (
		sourceID        = "source_id"
		destinationID   = "destination_id"
		destinationType = "destination_type"
	)

	db, ctx := setupDB(t), context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))
	repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
		return now
	}))

	var uploadIDs []int64
	for i := 0; i < 10; i++ {
		stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
		require.NoError(t, err)

		uploadID, err := repoUpload.CreateWithStagingFiles(ctx, model.Upload{
			SourceID:        sourceID,
			DestinationID:   destinationID,
			DestinationType: destinationType,
			Status:          model.GeneratingLoadFiles,
			Priority:        (i + 1),
		}, []*model.StagingFile{
			{
				ID:            stagingID,
				SourceID:      sourceID,
				DestinationID: destinationID,
			},
		})
		require.NoError(t, err)

		uploadIDs = append(uploadIDs, uploadID)
	}

	t.Run("known pipeline", func(t *testing.T) {
		latestInfo, err := repoUpload.GetLatestUploadInfo(ctx, sourceID, destinationID)
		require.NoError(t, err)

		require.EqualValues(t, latestInfo.ID, len(uploadIDs))
		require.EqualValues(t, latestInfo.Priority, uploadIDs[len(uploadIDs)-1])
		require.EqualValues(t, latestInfo.Status, model.GeneratingLoadFiles)
	})

	t.Run("unknown pipeline", func(t *testing.T) {
		latestInfo, err := repoUpload.GetLatestUploadInfo(ctx, "unknown_source_id", "unknown_destination_id")
		require.ErrorIs(t, err, sql.ErrNoRows)
		require.Nil(t, latestInfo)
	})

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		latestInfo, err := repoUpload.GetLatestUploadInfo(ctx, sourceID, destinationID)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, latestInfo)
	})
}
