package repo_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/samber/lo/mutable"

	"github.com/stretchr/testify/require"

	sqlmiddleware "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/testhelper"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestUploads_Count(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

	reverseUploadIDs := uploadIDs
	mutable.Reverse(reverseUploadIDs)
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
			require.Zero(t, uploadInfo.Duration)
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
			require.Zero(t, uploadInfo.Duration)
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
			Priority:        i + 1,
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
		require.ErrorIs(t, err, model.ErrNoUploadsFound)
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

func TestUploads_FailedBatchOperations(t *testing.T) {
	const (
		sourceID      = "source_id"
		destinationID = "destination_id"
		workspaceID   = "workspace_id"
	)

	ctx := context.Background()

	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	start := now.Add(-24 * time.Hour)
	end := now.Add(24 * time.Hour)

	prepareData := func(
		db *sqlmiddleware.DB,
		status string,
		error json.RawMessage,
		errorCategory string,
		generateTableUploads bool,
		timings model.Timings,
		now time.Time,
	) {
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))
		repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
			return now
		}))
		repoTableUpload := repo.NewTableUploads(db, repo.WithNow(func() time.Time {
			return now
		}))

		var stagingFiles []*model.StagingFile
		for i := 0; i < 10; i++ {
			stagingFile := &model.StagingFile{
				WorkspaceID:   workspaceID,
				Location:      "s3://bucket/path/to/file",
				SourceID:      sourceID,
				DestinationID: destinationID,
				TotalEvents:   60,
				FirstEventAt:  now,
				LastEventAt:   now,
			}
			stagingFileWithSchema := stagingFile.WithSchema(json.RawMessage(`{"type": "object"}`))

			stagingID, err := repoStaging.Insert(ctx, &stagingFileWithSchema)
			require.NoError(t, err)

			stagingFile.ID = stagingID
			stagingFiles = append(stagingFiles, stagingFile)
		}

		uploadID, err := repoUpload.CreateWithStagingFiles(ctx, model.Upload{
			SourceID:        sourceID,
			DestinationID:   destinationID,
			DestinationType: warehouseutils.POSTGRES,
			Status:          status,
			UploadSchema:    model.Schema{},
			WorkspaceID:     workspaceID,
		}, stagingFiles)
		require.NoError(t, err)

		if len(error) > 0 {
			errorJson, err := json.Marshal(error)
			require.NoError(t, err)

			_, err = db.ExecContext(ctx, `UPDATE wh_uploads SET error = $1, error_category = $2 WHERE id = $3`,
				errorJson,
				errorCategory,
				uploadID,
			)
			require.NoError(t, err)
		}
		if len(timings) > 0 {
			timingsJson, err := json.Marshal(timings)
			require.NoError(t, err)

			_, err = db.ExecContext(ctx, `UPDATE wh_uploads SET timings = $1 WHERE id = $2`,
				timingsJson,
				uploadID,
			)
			require.NoError(t, err)
		}

		if generateTableUploads {
			tables := []string{
				"table_1",
				"table_2",
				"table_3",
				"table_4",
				"table_5",
				warehouseutils.DiscardsTable,
				warehouseutils.ToProviderCase(warehouseutils.SNOWFLAKE, warehouseutils.DiscardsTable),
			}
			err = repoTableUpload.Insert(ctx, uploadID, tables)
			require.NoError(t, err)

			totalEvents := int64(100)
			for _, table := range tables {
				err = repoTableUpload.Set(ctx, uploadID, table, repo.TableUploadSetOptions{
					TotalEvents: &totalEvents,
					Status:      &status,
				})
				require.NoError(t, err)
			}
		}
	}

	t.Run("empty", func(t *testing.T) {
		entries := []struct {
			status              string
			prepareTableUploads bool
		}{
			{
				status:              model.Waiting,
				prepareTableUploads: false,
			},
			{
				status:              model.ExportedData,
				prepareTableUploads: true,
			},
		}

		db := setupDB(t)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))

		for i, entry := range entries {
			prepareData(
				db, entry.status, nil, "",
				entry.prepareTableUploads, model.Timings{}, now.Add(-time.Duration(i+1)*time.Hour),
			)
		}

		failedBatches, err := repoUpload.RetrieveFailedBatches(ctx, model.RetrieveFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
			End:           end,
		})
		require.NoError(t, err)
		require.Empty(t, failedBatches)

		failedBatches, err = repoUpload.RetrieveFailedBatches(ctx, model.RetrieveFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         now.Add(time.Hour),
			End:           now.Add(time.Hour),
		})
		require.NoError(t, err)
		require.Empty(t, failedBatches)

		retries, err := repoUpload.RetryFailedBatches(ctx, model.RetryFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
			End:           end,
		})
		require.NoError(t, err)
		require.Zero(t, retries)
	})
	t.Run("pick from table uploads", func(t *testing.T) {
		entries := []struct {
			status        string
			error         json.RawMessage
			errorCategory string
			timings       model.Timings
		}{
			{
				status:        "created_remote_schema_failed",
				error:         json.RawMessage(`{"created_remote_schema_failed":{"errors":["some error 5","some error 6"],"attempt":2}}`),
				errorCategory: model.UncategorizedError,
				timings: model.Timings{
					{
						"created_remote_schema_failed": now,
					},
				},
			},
			{
				status:        "aborted",
				error:         json.RawMessage(`{"exporting_data_failed":{"errors":["some error 7","some error 8"],"attempt":2}}`),
				errorCategory: model.PermissionError,
				timings: model.Timings{
					{
						"exporting_data_failed": now,
					},
				},
			},
			{
				status: "exported_data",
			},
		}

		db := setupDB(t)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))

		for i, entry := range entries {
			prepareData(
				db, entry.status, entry.error, entry.errorCategory,
				true, entry.timings, now.Add(-time.Duration(i+1)*time.Hour),
			)
		}

		failedBatches, err := repoUpload.RetrieveFailedBatches(ctx, model.RetrieveFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
			End:           end,
		})
		require.NoError(t, err)
		require.EqualValues(t, failedBatches, []model.RetrieveFailedBatchesResponse{
			{
				Error:           `some error 8`,
				ErrorCategory:   model.PermissionError,
				SourceID:        sourceID,
				TotalEvents:     500,
				TotalSyncs:      1,
				FirstHappenedAt: now.Add(-2 * time.Hour).UTC(),
				LastHappenedAt:  now.Add(-2 * time.Hour).UTC(),
				Status:          model.Aborted,
			},
			{
				Error:           `some error 6`,
				ErrorCategory:   model.UncategorizedError,
				SourceID:        sourceID,
				TotalEvents:     500,
				TotalSyncs:      1,
				FirstHappenedAt: now.Add(-time.Hour).UTC(),
				LastHappenedAt:  now.Add(-time.Hour).UTC(),
				Status:          model.Failed,
			},
		})

		retries, err := repoUpload.RetryFailedBatches(ctx, model.RetryFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
			End:           end,
		})
		require.NoError(t, err)
		require.Equal(t, retries, int64(2))

		failedBatches, err = repoUpload.RetrieveFailedBatches(ctx, model.RetrieveFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
			End:           end,
		})
		require.NoError(t, err)
		require.EqualValues(t, failedBatches, []model.RetrieveFailedBatchesResponse{
			{
				Error:           `some error 8`,
				ErrorCategory:   model.PermissionError,
				SourceID:        sourceID,
				TotalEvents:     500,
				TotalSyncs:      1,
				FirstHappenedAt: now.UTC(),
				LastHappenedAt:  now.UTC(),
				Status:          "syncing",
			},
			{
				Error:           `some error 6`,
				ErrorCategory:   model.UncategorizedError,
				SourceID:        sourceID,
				TotalEvents:     500,
				TotalSyncs:      1,
				FirstHappenedAt: now.UTC(),
				LastHappenedAt:  now.UTC(),
				Status:          "syncing",
			},
		})
	})
	t.Run("optional end", func(t *testing.T) {
		entries := []struct {
			status        string
			error         json.RawMessage
			errorCategory string
			timings       model.Timings
		}{
			{
				status:        "created_remote_schema_failed",
				error:         json.RawMessage(`{"created_remote_schema_failed":{"errors":["some error 5","some error 6"],"attempt":2}}`),
				errorCategory: model.UncategorizedError,
				timings: model.Timings{
					{
						"created_remote_schema_failed": now,
					},
				},
			},
			{
				status:        "aborted",
				error:         json.RawMessage(`{"exporting_data_failed":{"errors":["some error 7","some error 8"],"attempt":2}}`),
				errorCategory: model.PermissionError,
				timings: model.Timings{
					{
						"exporting_data_failed": now,
					},
				},
			},
			{
				status: "exported_data",
			},
		}

		db := setupDB(t)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))

		for i, entry := range entries {
			prepareData(
				db, entry.status, entry.error, entry.errorCategory,
				true, entry.timings, now.Add(-time.Duration(i+1)*time.Hour),
			)
		}

		failedBatches, err := repoUpload.RetrieveFailedBatches(ctx, model.RetrieveFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
		})
		require.NoError(t, err)
		require.EqualValues(t, failedBatches, []model.RetrieveFailedBatchesResponse{
			{
				Error:           `some error 8`,
				ErrorCategory:   model.PermissionError,
				SourceID:        sourceID,
				TotalEvents:     500,
				TotalSyncs:      1,
				FirstHappenedAt: now.Add(-2 * time.Hour).UTC(),
				LastHappenedAt:  now.Add(-2 * time.Hour).UTC(),
				Status:          model.Aborted,
			},
			{
				Error:           `some error 6`,
				ErrorCategory:   model.UncategorizedError,
				SourceID:        sourceID,
				TotalEvents:     500,
				TotalSyncs:      1,
				FirstHappenedAt: now.Add(-time.Hour).UTC(),
				LastHappenedAt:  now.Add(-time.Hour).UTC(),
				Status:          model.Failed,
			},
		})

		retries, err := repoUpload.RetryFailedBatches(ctx, model.RetryFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
		})
		require.NoError(t, err)
		require.Equal(t, retries, int64(2))
	})
	t.Run("pick from staging files", func(t *testing.T) {
		entries := []struct {
			status              string
			error               json.RawMessage
			errorCategory       string
			prepareTableUploads bool
			timings             model.Timings
		}{
			{
				status:              "internal_processing_failed",
				error:               json.RawMessage(`{"internal_processing_failed":{"errors":["some error 1","some error 2"],"attempt":2}}`),
				errorCategory:       model.UncategorizedError,
				prepareTableUploads: false,
				timings: model.Timings{
					{
						"internal_processing_failed": now,
					},
				},
			},
			{
				status:              "generating_load_files_failed",
				error:               json.RawMessage(`{"generating_load_files_failed":{"errors":["some error 3","some error 4"],"attempt":2}}`),
				errorCategory:       model.PermissionError,
				prepareTableUploads: false,
				timings: model.Timings{
					{
						"generating_load_files_failed": now,
					},
				},
			},
			{
				status:              "exported_data",
				prepareTableUploads: true,
			},
		}

		db := setupDB(t)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))

		for i, entry := range entries {
			prepareData(
				db, entry.status, entry.error, entry.errorCategory,
				entry.prepareTableUploads, entry.timings, now.Add(-time.Duration(i+1)*time.Hour),
			)
		}

		failedBatches, err := repoUpload.RetrieveFailedBatches(ctx, model.RetrieveFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
			End:           end,
		})
		require.NoError(t, err)
		require.EqualValues(t, failedBatches, []model.RetrieveFailedBatchesResponse{
			{
				Error:           `some error 4`,
				ErrorCategory:   model.PermissionError,
				SourceID:        sourceID,
				TotalEvents:     600,
				TotalSyncs:      1,
				FirstHappenedAt: now.Add(-2 * time.Hour).UTC(),
				LastHappenedAt:  now.Add(-2 * time.Hour).UTC(),
				Status:          model.Failed,
			},
			{
				Error:           `some error 2`,
				ErrorCategory:   model.UncategorizedError,
				SourceID:        sourceID,
				TotalEvents:     600,
				TotalSyncs:      1,
				FirstHappenedAt: now.Add(-time.Hour).UTC(),
				LastHappenedAt:  now.Add(-time.Hour).UTC(),
				Status:          model.Failed,
			},
		})

		retries, err := repoUpload.RetryFailedBatches(ctx, model.RetryFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
			End:           end,
		})
		require.NoError(t, err)
		require.Equal(t, retries, int64(2))
	})
	t.Run("overlapping batches between table uploads and staging files", func(t *testing.T) {
		entries := []struct {
			status              string
			error               json.RawMessage
			errorCategory       string
			prepareTableUploads bool
			timings             model.Timings
		}{
			{
				status:              "internal_processing_failed",
				error:               json.RawMessage(`{"internal_processing_failed":{"errors":["some error 1","some error 2"],"attempt":2}}`),
				errorCategory:       model.UncategorizedError,
				prepareTableUploads: false,
				timings: model.Timings{
					{
						"internal_processing_failed": now,
					},
				},
			},
			{
				status:              "generating_load_files_failed",
				error:               json.RawMessage(`{"generating_load_files_failed":{"errors":["some error 3","some error 4"],"attempt":2}}`),
				errorCategory:       model.UncategorizedError,
				prepareTableUploads: false,
				timings: model.Timings{
					{
						"generating_load_files_failed": now,
					},
				},
			},
			{
				status:              "exporting_data_failed",
				error:               json.RawMessage(`{"exporting_data_failed":{"errors":["some error 5","some error 6"],"attempt":2}}`),
				errorCategory:       model.PermissionError,
				prepareTableUploads: true,
				timings: model.Timings{
					{
						"exporting_data_failed": now,
					},
				},
			},
			{
				status:              "aborted",
				error:               json.RawMessage(`{"exporting_data_failed":{"errors":["some error 7","some error 8"],"attempt":2}}`),
				prepareTableUploads: true,
				errorCategory:       model.ResourceNotFoundError,
				timings: model.Timings{
					{
						"exporting_data_failed": now,
					},
				},
			},
			{
				status:              "exported_data",
				prepareTableUploads: true,
			},
		}

		db := setupDB(t)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))

		for i, entry := range entries {
			prepareData(
				db, entry.status, entry.error, entry.errorCategory,
				entry.prepareTableUploads, entry.timings, now.Add(-time.Duration(i+1)*time.Hour),
			)
		}

		failedBatches, err := repoUpload.RetrieveFailedBatches(ctx, model.RetrieveFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
			End:           end,
		})
		require.NoError(t, err)
		require.EqualValues(t, failedBatches, []model.RetrieveFailedBatchesResponse{
			{
				Error:           `some error 6`,
				ErrorCategory:   model.PermissionError,
				SourceID:        sourceID,
				TotalEvents:     500,
				TotalSyncs:      1,
				FirstHappenedAt: now.Add(-3 * time.Hour).UTC(),
				LastHappenedAt:  now.Add(-3 * time.Hour).UTC(),
				Status:          model.Failed,
			},
			{
				Error:           `some error 8`,
				ErrorCategory:   model.ResourceNotFoundError,
				SourceID:        sourceID,
				TotalEvents:     500,
				TotalSyncs:      1,
				FirstHappenedAt: now.Add(-4 * time.Hour).UTC(),
				LastHappenedAt:  now.Add(-4 * time.Hour).UTC(),
				Status:          model.Aborted,
			},
			{
				Error:           `some error 2`,
				ErrorCategory:   model.UncategorizedError,
				SourceID:        sourceID,
				TotalEvents:     1200,
				TotalSyncs:      2,
				FirstHappenedAt: now.Add(-2 * time.Hour).UTC(),
				LastHappenedAt:  now.Add(-1 * time.Hour).UTC(),
				Status:          model.Failed,
			},
		})

		retries, err := repoUpload.RetryFailedBatches(ctx, model.RetryFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
			End:           end,
		})
		require.NoError(t, err)
		require.Equal(t, retries, int64(4))
	})
	t.Run("non-overlapping batches between table uploads and staging files", func(t *testing.T) {
		entries := []struct {
			status              string
			error               json.RawMessage
			errorCategory       string
			prepareTableUploads bool
			timings             model.Timings
		}{
			{
				status:              "generating_load_files_failed",
				error:               json.RawMessage(`{"generating_load_files_failed":{"errors":["some error 3","some error 4"],"attempt":2}}`),
				errorCategory:       model.PermissionError,
				prepareTableUploads: false,
				timings: model.Timings{
					{
						"generating_load_files_failed": now,
					},
				},
			},
			{
				status:              "exporting_data_failed",
				error:               json.RawMessage(`{"exporting_data_failed":{"errors":["some error 5","some error 6"],"attempt":2}}`),
				errorCategory:       model.PermissionError,
				prepareTableUploads: true,
				timings: model.Timings{
					{
						"exporting_data_failed": now,
					},
				},
			},
		}

		db := setupDB(t)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))

		for i, entry := range entries {
			prepareData(
				db, entry.status, entry.error, entry.errorCategory,
				entry.prepareTableUploads, entry.timings, now.Add(-time.Duration(i+1)*time.Hour),
			)
		}

		failedBatches, err := repoUpload.RetrieveFailedBatches(ctx, model.RetrieveFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
			End:           end,
		})
		require.NoError(t, err)
		require.EqualValues(t, failedBatches, []model.RetrieveFailedBatchesResponse{
			{
				Error:           `some error 4`,
				ErrorCategory:   model.PermissionError,
				SourceID:        sourceID,
				TotalEvents:     1100,
				TotalSyncs:      2,
				FirstHappenedAt: now.Add(-2 * time.Hour).UTC(),
				LastHappenedAt:  now.Add(-1 * time.Hour).UTC(),
				Status:          model.Failed,
			},
		})

		retries, err := repoUpload.RetryFailedBatches(ctx, model.RetryFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
			End:           end,
		})
		require.NoError(t, err)
		require.Equal(t, retries, int64(2))
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		db := setupDB(t)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))

		failedBatches, err := repoUpload.RetrieveFailedBatches(ctx, model.RetrieveFailedBatchesRequest{})
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, failedBatches)

		retries, err := repoUpload.RetryFailedBatches(ctx, model.RetryFailedBatchesRequest{
			DestinationID: destinationID,
			WorkspaceID:   workspaceID,
			Start:         start,
			End:           end,
		})
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, retries)
	})
	t.Run("Retries filter", func(t *testing.T) {
		entries := []struct {
			status              string
			error               json.RawMessage
			errorCategory       string
			prepareTableUploads bool
			timings             model.Timings
		}{
			{
				status:        "internal_processing_failed",
				error:         json.RawMessage(`{"internal_processing_failed":{"errors":["some error","some error"],"attempt":2}}`),
				errorCategory: model.UncategorizedError,
				timings: model.Timings{
					{
						"internal_processing_failed": now,
					},
				},
			},
			{
				status:        "generating_load_files_failed",
				error:         json.RawMessage(`{"generating_load_files_failed":{"errors":["some error","some error"],"attempt":2}}`),
				errorCategory: model.UncategorizedError,
				timings: model.Timings{
					{
						"generating_load_files_failed": now,
					},
				},
			},
			{
				status:              "exporting_data_failed",
				error:               json.RawMessage(`{"exporting_data_failed":{"errors":["some error","some error"],"attempt":2}}`),
				errorCategory:       model.PermissionError,
				prepareTableUploads: true,
				timings: model.Timings{
					{
						"exporting_data_failed": now,
					},
				},
			},
			{
				status:              "aborted",
				error:               json.RawMessage(`{"exporting_data_failed":{"errors":["some error","some error"],"attempt":2}}`),
				prepareTableUploads: true,
				errorCategory:       model.ResourceNotFoundError,
				timings: model.Timings{
					{
						"exporting_data_failed": now,
					},
				},
			},
			{
				status:              "exported_data",
				prepareTableUploads: true,
			},
		}

		db := setupDB(t)
		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return now
		}))

		for i, entry := range entries {
			prepareData(
				db, entry.status, entry.error, entry.errorCategory,
				entry.prepareTableUploads, entry.timings, now.Add(-time.Duration(i+1)*time.Hour),
			)
		}

		t.Run("errorCategory=default,status=failed", func(t *testing.T) {
			retries, err := repoUpload.RetryFailedBatches(ctx, model.RetryFailedBatchesRequest{
				DestinationID: destinationID,
				WorkspaceID:   workspaceID,
				SourceID:      sourceID,
				Start:         start,
				End:           end,
				ErrorCategory: model.UncategorizedError,
				Status:        model.Failed,
			})
			require.NoError(t, err)
			require.Equal(t, retries, int64(2))
		})
		t.Run("errorCategory=resourceNotFound,status=aborted", func(t *testing.T) {
			retries, err := repoUpload.RetryFailedBatches(ctx, model.RetryFailedBatchesRequest{
				DestinationID: destinationID,
				WorkspaceID:   workspaceID,
				SourceID:      sourceID,
				Start:         start,
				End:           end,
				ErrorCategory: model.ResourceNotFoundError,
				Status:        model.Aborted,
			})
			require.NoError(t, err)
			require.Equal(t, retries, int64(1))
		})
		t.Run("errorCategory=permissionError,status=failed", func(t *testing.T) {
			retries, err := repoUpload.RetryFailedBatches(ctx, model.RetryFailedBatchesRequest{
				DestinationID: destinationID,
				WorkspaceID:   workspaceID,
				SourceID:      sourceID,
				Start:         start,
				End:           end,
				ErrorCategory: model.PermissionError,
				Status:        model.Failed,
			})
			require.NoError(t, err)
			require.Equal(t, retries, int64(1))
		})
		t.Run("errorCategory=invalid,status=invalid", func(t *testing.T) {
			retries, err := repoUpload.RetryFailedBatches(ctx, model.RetryFailedBatchesRequest{
				DestinationID: destinationID,
				SourceID:      sourceID,
				Start:         start,
				End:           end,
				ErrorCategory: "invalid",
				Status:        "invalid",
			})
			require.NoError(t, err)
			require.Zero(t, retries)
		})
	})
}

func TestUploads_Update(t *testing.T) {
	db, ctx := setupDB(t), context.Background()

	now := time.Date(2021, 1, 1, 0, 0, 3, 0, time.UTC)
	repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return now
	}))
	repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
		return now
	}))

	const (
		sourceID        = "source_id"
		destinationID   = "destination_id"
		destinationType = "destination_type"
		workspaceID     = "workspace_id"
	)
	var (
		updatedStatus          = "updated_status"
		updatedStartLoadFileID = int64(101)
		updatedEndLoadFileID   = int64(1001)
		updatedUpdatedAt       = now.Add(time.Hour)
		updatedTimings         = json.RawMessage(`[{"generating_upload_schema":"2023-10-29T20:06:45.146042463Z"},{"generated_upload_schema":"2023-10-29T20:06:45.156733475Z"}]`)
		updatedSchema          = json.RawMessage(`{"tracks":{"id":"string","received_at":"datetime"}}`)
		updatedlastExecAt      = now.Add(time.Hour)
		updatedMetadata        = json.RawMessage(`{"retried":true,"priority":50,"nextRetryTime":"2023-10-29T20:06:25.492432247Z","load_file_type":"csv"}`)
		updatedError           = json.RawMessage(`{"exporting_data_failed":{"errors":["some error","some error"],"attempt":2}}`)
		updatedErrorCategory   = model.PermissionError
	)

	prepareUpload := func(t *testing.T) int64 {
		t.Helper()

		stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
		require.NoError(t, err)

		stagingFiles := []*model.StagingFile{
			{
				ID: stagingID,
			},
		}

		uploadID, err := repoUpload.CreateWithStagingFiles(ctx, model.Upload{
			SourceID:        sourceID,
			DestinationID:   destinationID,
			DestinationType: destinationType,
			UploadSchema:    model.Schema{},
			WorkspaceID:     workspaceID,
			Status:          model.Waiting,
			Priority:        100,
		}, stagingFiles)
		require.NoError(t, err)

		return uploadID
	}

	fieldsToUpdate := []repo.UpdateKeyValue{
		repo.UploadFieldStatus(updatedStatus),
		repo.UploadFieldStartLoadFileID(updatedStartLoadFileID),
		repo.UploadFieldEndLoadFileID(updatedEndLoadFileID),
		repo.UploadFieldUpdatedAt(updatedUpdatedAt),
		repo.UploadFieldTimings(updatedTimings),
		repo.UploadFieldSchema(updatedSchema),
		repo.UploadFieldLastExecAt(updatedlastExecAt),
		repo.UploadFieldInProgress(true),
		repo.UploadFieldMetadata(updatedMetadata),
		repo.UploadFieldError(updatedError),
		repo.UploadFieldErrorCategory(updatedErrorCategory),
	}

	compare := func(t *testing.T, id int64) {
		t.Helper()

		op, err := repoUpload.Get(ctx, id)
		require.NoError(t, err)
		require.Equal(t, updatedStatus, op.Status)
		require.Equal(t, updatedStartLoadFileID, op.LoadFileStartID)
		require.Equal(t, updatedEndLoadFileID, op.LoadFileEndID)
		require.JSONEq(t, string(updatedError), string(op.Error))
		require.True(t, op.Retried)
		require.Equal(t, 50, op.Priority)

		var timings model.Timings
		err = json.Unmarshal(updatedTimings, &timings)
		require.NoError(t, err)
		require.Equal(t, timings, op.Timings)

		var schema model.Schema
		err = json.Unmarshal(updatedSchema, &schema)
		require.NoError(t, err)
		require.Equal(t, schema, op.UploadSchema)

		var metadata repo.UploadMetadata
		err = json.Unmarshal(updatedMetadata, &metadata)
		require.NoError(t, err)
		require.Equal(t, metadata.Retried, op.Retried)
		require.Equal(t, metadata.Priority, op.Priority)
		require.Equal(t, metadata.NextRetryTime, op.NextRetryTime)
		require.Equal(t, metadata.LoadFileType, op.LoadFileType)
	}

	t.Run("empty fields", func(t *testing.T) {
		id := prepareUpload(t)

		err := repoUpload.Update(ctx, id, nil)
		require.Error(t, err)
	})
	t.Run("succeeded", func(t *testing.T) {
		id := prepareUpload(t)

		err := repoUpload.Update(ctx, id, fieldsToUpdate)
		require.NoError(t, err)

		compare(t, id)
	})
	t.Run("withTx succeeded", func(t *testing.T) {
		id := prepareUpload(t)

		err := repoUpload.WithTx(ctx, func(tx *sqlmiddleware.Tx) error {
			return repoUpload.UpdateWithTx(ctx, tx, id, fieldsToUpdate)
		})
		require.NoError(t, err)

		compare(t, id)
	})
	t.Run("withTx failed", func(t *testing.T) {
		id := prepareUpload(t)

		err := repoUpload.WithTx(ctx, func(tx *sqlmiddleware.Tx) error {
			require.NoError(t, repoUpload.UpdateWithTx(ctx, tx, id, fieldsToUpdate))
			return errors.New("test error")
		})
		require.Error(t, err)

		op, err := repoUpload.Get(ctx, id)
		require.NoError(t, err)
		require.Equal(t, model.Waiting, op.Status)
		require.False(t, op.Retried)
		require.Equal(t, 100, op.Priority)
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		err := repoUpload.Update(ctx, -1, fieldsToUpdate)
		require.ErrorIs(t, err, context.Canceled)

		err = repoUpload.WithTx(ctx, func(tx *sqlmiddleware.Tx) error {
			return repoUpload.UpdateWithTx(ctx, tx, -1, fieldsToUpdate)
		})
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestUploads_GetSyncLatencies(t *testing.T) {
	const (
		sourceID        = "source_id"
		destinationID   = "destination_id"
		destinationType = "destination_type"
		workspaceID     = "workspace_id"
	)

	db, ctx := setupDB(t), context.Background()
	now := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
		return now
	}))

	stagingID, err := repoStaging.Insert(ctx, &model.StagingFileWithSchema{})
	require.NoError(t, err)

	stagingFile := model.StagingFile{
		ID:            stagingID,
		SourceID:      sourceID,
		DestinationID: destinationID,
		WorkspaceID:   workspaceID,
	}

	latencies := make([]time.Duration, 2000)
	for i := 0; i < 2000; i++ {
		createdAt := now.Add(time.Duration(i*30) * time.Minute)
		lastExecAt := createdAt.Add(time.Duration(i%10) * time.Minute)
		exportedUpdatedAt := lastExecAt.Add(time.Duration(i%5) * time.Minute)
		abortedUpdatedAt := lastExecAt.Add(time.Duration(i%5) * time.Minute)
		inProgressUpdatedAt := lastExecAt.Add(time.Duration(i%5) * time.Minute)

		repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
			return createdAt
		}))

		exportedUploadID, err := repoUpload.CreateWithStagingFiles(ctx, model.Upload{
			SourceID:        sourceID,
			DestinationID:   destinationID,
			DestinationType: destinationType,
			WorkspaceID:     workspaceID,
			Status:          model.Waiting,
		}, []*model.StagingFile{lo.ToPtr(stagingFile)})
		require.NoError(t, err)
		abortedUploadID, err := repoUpload.CreateWithStagingFiles(ctx, model.Upload{
			SourceID:        sourceID,
			DestinationID:   destinationID,
			DestinationType: destinationType,
			WorkspaceID:     workspaceID,
			Status:          model.Waiting,
		}, []*model.StagingFile{lo.ToPtr(stagingFile)})
		require.NoError(t, err)
		inProgressUploadID, err := repoUpload.CreateWithStagingFiles(ctx, model.Upload{
			SourceID:        sourceID,
			DestinationID:   destinationID,
			DestinationType: destinationType,
			WorkspaceID:     workspaceID,
			Status:          model.Waiting,
		}, []*model.StagingFile{lo.ToPtr(stagingFile)})
		require.NoError(t, err)
		_, err = repoUpload.CreateWithStagingFiles(ctx, model.Upload{
			SourceID:        sourceID,
			DestinationID:   destinationID,
			DestinationType: destinationType,
			WorkspaceID:     workspaceID,
			Status:          model.Waiting,
		}, []*model.StagingFile{lo.ToPtr(stagingFile)})
		require.NoError(t, err)

		require.NoError(t, repoUpload.Update(ctx, exportedUploadID, []repo.UpdateKeyValue{
			repo.UploadFieldStatus(model.ExportedData),
			repo.UploadFieldLastExecAt(lastExecAt),
			repo.UploadFieldUpdatedAt(exportedUpdatedAt),
		}))
		require.NoError(t, repoUpload.Update(ctx, abortedUploadID, []repo.UpdateKeyValue{
			repo.UploadFieldStatus(model.Aborted),
			repo.UploadFieldLastExecAt(lastExecAt),
			repo.UploadFieldUpdatedAt(abortedUpdatedAt),
		}))
		require.NoError(t, repoUpload.Update(ctx, inProgressUploadID, []repo.UpdateKeyValue{
			repo.UploadFieldStatus(model.ExportingData),
			repo.UploadFieldInProgress(true),
			repo.UploadFieldLastExecAt(lastExecAt),
			repo.UploadFieldUpdatedAt(inProgressUpdatedAt),
		}))

		latencies[i] = exportedUpdatedAt.Sub(createdAt) / time.Second
	}

	testCases := []struct {
		name                    string
		aggregationMinutes      int
		expectedAggregationType model.LatencyAggregationType
		expectedLatencies       []time.Duration
	}{
		{
			name:                    "5 min interval (max)",
			aggregationMinutes:      5,
			expectedAggregationType: model.MaxLatency,
			expectedLatencies:       latencies,
		},
		{
			name:                    "1 hour interval (max)",
			aggregationMinutes:      60,
			expectedAggregationType: model.MaxLatency,
			expectedLatencies:       testhelper.MaxDurationInWindow(latencies, 2),
		},
		{
			name:                    "1 day interval (max)",
			aggregationMinutes:      1440,
			expectedAggregationType: model.MaxLatency,
			expectedLatencies:       testhelper.MaxDurationInWindow(latencies, 48),
		},
		{
			name:                    "1 day interval (p90)",
			aggregationMinutes:      1440,
			expectedAggregationType: model.P90Latency,
			expectedLatencies:       testhelper.PercentileDurationInWindow(latencies, 48, 0.9),
		},
		{
			name:                    "1 day interval (p95)",
			aggregationMinutes:      1440,
			expectedAggregationType: model.P95Latency,
			expectedLatencies:       testhelper.PercentileDurationInWindow(latencies, 48, 0.95),
		},
		{
			name:                    "1 day interval (avg)",
			aggregationMinutes:      1440,
			expectedAggregationType: model.AvgLatency,
			expectedLatencies:       testhelper.AverageDurationInWindow(latencies, 48),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
				return now
			}))
			syncLatencies, err := repoUpload.GetSyncLatencies(ctx, model.SyncLatencyRequest{
				SourceID:           sourceID,
				DestinationID:      destinationID,
				WorkspaceID:        workspaceID,
				StartTime:          now,
				AggregationMinutes: int64(tc.aggregationMinutes),
				AggregationType:    tc.expectedAggregationType,
			})
			require.NoError(t, err)
			require.Equal(t, tc.expectedLatencies, lo.Map(syncLatencies, func(item model.LatencyTimeSeriesDataPoint, index int) time.Duration {
				return time.Duration(int(item.LatencySeconds))
			}))
		})
	}
}

func TestGetFirstAbortedUploadsInContinuousAborts(t *testing.T) {
	t.Parallel()

	db, ctx := setupDB(t), context.Background()

	now := time.Now().UTC()
	repoUpload := repo.NewUploads(db, repo.WithNow(func() time.Time {
		return time.Now().UTC()
	}))
	repoStaging := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
		return time.Now().UTC()
	}))

	destType := "RS"
	uploads := []model.Upload{
		// uploads with last upload success
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_with_last_upload_success",
			DestinationID:   "destination_with_last_upload_success",
			DestinationType: destType,
			Status:          model.Aborted,
			SourceTaskRunID: "task_run_id_with_last_upload_success",
			LastEventAt:     now.Add(-3 * time.Hour),
			FirstEventAt:    now.Add(-4 * time.Hour),
		},
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_with_last_upload_success",
			DestinationID:   "destination_with_last_upload_success",
			DestinationType: destType,
			Status:          model.Aborted,
			SourceTaskRunID: "task_run_id_with_last_upload_success",
			LastEventAt:     now.Add(-2 * time.Hour),
			FirstEventAt:    now.Add(-3 * time.Hour),
		},
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_with_last_upload_success",
			DestinationID:   "destination_with_last_upload_success",
			DestinationType: destType,
			Status:          model.ExportedData,
			SourceTaskRunID: "task_run_id_with_last_upload_success",
			LastEventAt:     now.Add(-1 * time.Hour),
			FirstEventAt:    now.Add(-2 * time.Hour),
		},
		// uploads with no success
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_with_no_successful_upload",
			DestinationID:   "destination_with_no_successful_upload",
			DestinationType: destType,
			Status:          model.Aborted,
			SourceTaskRunID: "task_run_id_with_no_successful_upload",
			LastEventAt:     now.Add(-4 * time.Hour),
			FirstEventAt:    now.Add(-5 * time.Hour),
		},
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_with_no_successful_upload",
			DestinationID:   "destination_with_no_successful_upload",
			DestinationType: destType,
			Status:          model.Aborted,
			SourceTaskRunID: "task_run_id_with_no_successful_upload",
			LastEventAt:     now.Add(-3 * time.Hour),
			FirstEventAt:    now.Add(-4 * time.Hour),
		},
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_with_no_successful_upload",
			DestinationID:   "destination_with_no_successful_upload",
			DestinationType: destType,
			Status:          model.Aborted,
			SourceTaskRunID: "task_run_id_with_no_successful_upload",
			LastEventAt:     now.Add(-2 * time.Hour),
			FirstEventAt:    now.Add(-3 * time.Hour),
		},
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_with_no_successful_upload",
			DestinationID:   "destination_with_no_successful_upload",
			DestinationType: destType,
			Status:          model.Aborted,
			SourceTaskRunID: "task_run_id_with_no_successful_upload",
			LastEventAt:     now.Add(-1 * time.Hour),
			FirstEventAt:    now.Add(-2 * time.Hour),
		},
		// uploads with last upload aborted

		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_with_last_upload_aborted",
			DestinationID:   "destination_with_last_upload_aborted",
			DestinationType: destType,
			Status:          model.Aborted,
			SourceTaskRunID: "task_run_id_last_upload_aborted",
			LastEventAt:     now.Add(-4 * time.Hour),
			FirstEventAt:    now.Add(-5 * time.Hour),
		},
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_with_last_upload_aborted",
			DestinationID:   "destination_with_last_upload_aborted",
			DestinationType: destType,
			Status:          model.ExportedData,
			SourceTaskRunID: "task_run_id_last_upload_aborted",
			LastEventAt:     now.Add(-3 * time.Hour),
			FirstEventAt:    now.Add(-4 * time.Hour),
		},
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_with_last_upload_aborted",
			DestinationID:   "destination_with_last_upload_aborted",
			DestinationType: destType,
			Status:          model.Aborted,
			SourceTaskRunID: "task_run_id_last_upload_aborted",
			LastEventAt:     now.Add(-2 * time.Hour),
			FirstEventAt:    now.Add(-3 * time.Hour),
		},
		{
			WorkspaceID:     "workspace_id",
			Namespace:       "namespace",
			SourceID:        "source_with_last_upload_aborted",
			DestinationID:   "destination_with_last_upload_aborted",
			DestinationType: destType,
			Status:          model.Aborted,
			SourceTaskRunID: "task_run_id_last_upload_aborted",
			LastEventAt:     now.Add(-1 * time.Hour),
			FirstEventAt:    now.Add(-2 * time.Hour),
		},
		// uploads with last upload running
		{
			WorkspaceID:     "workspace_id_1",
			Namespace:       "namespace_1",
			SourceID:        "source_id_1",
			DestinationID:   "destination_id_1",
			DestinationType: destType,
			Status:          model.ExportingData,
			SourceTaskRunID: "task_run_id_1",
			LastEventAt:     now.Add(-time.Hour),
			FirstEventAt:    now.Add(-2 * time.Hour),
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
			FirstEventAt:    uploads[i].FirstEventAt,
			LastEventAt:     uploads[i].LastEventAt,
			Status:          uploads[i].Status,
			WorkspaceID:     uploads[i].WorkspaceID,
		}})
		require.NoError(t, err)

		uploads[i].ID = id
		uploads[i].Error = []byte("{}")
		uploads[i].UploadSchema = model.Schema{}
		uploads[i].LoadFileType = "csv"
		uploads[i].StagingFileStartID = int64(i + 1)
		uploads[i].StagingFileEndID = int64(i + 1)
	}

	t.Run("query to get the first aborted upload in series of continuous aborts", func(t *testing.T) {
		t.Parallel()

		uploads, err := repoUpload.GetFirstAbortedUploadInContinuousAbortsByDestination(ctx, "workspace_id", time.Now().AddDate(0, 0, -30))
		require.NoError(t, err)
		require.Len(t, uploads, 2)

		if uploads[0].ID > uploads[1].ID {
			uploads[0], uploads[1] = uploads[1], uploads[0]
		}

		require.Equal(t, uploads[0].ID, int64(4))
		require.Equal(t, uploads[0].SourceID, "source_with_no_successful_upload")
		require.Equal(t, uploads[0].DestinationID, "destination_with_no_successful_upload")
		require.Equal(t, uploads[0].LastEventAt.Unix(), now.Add(-4*time.Hour).Unix())
		require.Equal(t, uploads[0].FirstEventAt.Unix(), now.Add(-5*time.Hour).Unix())

		require.Equal(t, uploads[1].ID, int64(10))
		require.Equal(t, uploads[1].SourceID, "source_with_last_upload_aborted")
		require.Equal(t, uploads[1].DestinationID, "destination_with_last_upload_aborted")
		require.Equal(t, uploads[1].LastEventAt.Unix(), now.Add(-2*time.Hour).Unix())
		require.Equal(t, uploads[1].FirstEventAt.Unix(), now.Add(-3*time.Hour).Unix())
	})

	t.Run("query to get empty list of first aborted upload in series of continuous aborts", func(t *testing.T) {
		t.Parallel()

		uploads, err := repoUpload.GetFirstAbortedUploadInContinuousAbortsByDestination(ctx, "workspace_id_1", time.Now().AddDate(0, 0, -30))
		require.NoError(t, err)
		require.Len(t, uploads, 0)
	})
}
