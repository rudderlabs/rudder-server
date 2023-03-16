//go:build !warehouse_integration

package repo_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func setupDB(t testing.TB) *sql.DB {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pgResource, err := destination.SetupPostgres(pool, t)
	require.NoError(t, err)

	err = (&migrator.Migrator{
		Handle:          pgResource.DB,
		MigrationsTable: "wh_schema_migrations",
	}).Migrate("warehouse")
	require.NoError(t, err)

	t.Log("db:", pgResource.DBDsn)

	return pgResource.DB
}

func TestStagingFileRepo(t *testing.T) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Second).UTC()

	r := repo.NewStagingFiles(setupDB(t),
		repo.WithNow(func() time.Time {
			return now
		}),
	)

	testcases := []struct {
		name        string
		stagingFile model.StagingFileWithSchema
	}{
		{
			name: "create staging file",
			stagingFile: model.StagingFile{
				WorkspaceID:           "workspace_id",
				Location:              "s3://bucket/path/to/file",
				SourceID:              "source_id",
				DestinationID:         "destination_id",
				Status:                warehouseutils.StagingFileWaitingState,
				Error:                 fmt.Errorf("dummy error"),
				FirstEventAt:          now.Add(time.Second),
				LastEventAt:           now,
				UseRudderStorage:      true,
				DestinationRevisionID: "destination_revision_id",
				TotalEvents:           100,
				SourceTaskRunID:       "source_task_run_id",
				SourceJobID:           "source_job_id",
				SourceJobRunID:        "source_job_run_id",
				TimeWindow:            time.Date(1993, 8, 1, 3, 0, 0, 0, time.UTC),
			}.WithSchema([]byte(`{"type": "object"}`)),
		},
		{
			name: "missing FirstEventAt",
			stagingFile: model.StagingFile{
				WorkspaceID:           "workspace_id",
				Location:              "s3://bucket/path/to/file",
				SourceID:              "source_id",
				DestinationID:         "destination_id",
				Status:                warehouseutils.StagingFileWaitingState,
				Error:                 fmt.Errorf("dummy error"),
				LastEventAt:           now,
				UseRudderStorage:      true,
				DestinationRevisionID: "destination_revision_id",
				TotalEvents:           100,
				SourceTaskRunID:       "source_task_run_id",
				SourceJobID:           "source_job_id",
				SourceJobRunID:        "source_job_run_id",
				TimeWindow:            time.Date(1993, 8, 1, 3, 0, 0, 0, time.UTC),
			}.WithSchema([]byte(`{"type": "object"}`)),
		},
		{
			name: "missing LastEventAt",
			stagingFile: model.StagingFile{
				WorkspaceID:           "workspace_id",
				Location:              "s3://bucket/path/to/file",
				SourceID:              "source_id",
				DestinationID:         "destination_id",
				Status:                warehouseutils.StagingFileWaitingState,
				Error:                 fmt.Errorf("dummy error"),
				FirstEventAt:          now.Add(time.Second),
				UseRudderStorage:      true,
				DestinationRevisionID: "destination_revision_id",
				TotalEvents:           100,
				SourceTaskRunID:       "source_task_run_id",
				SourceJobID:           "source_job_id",
				SourceJobRunID:        "source_job_run_id",
				TimeWindow:            time.Date(1993, 8, 1, 3, 0, 0, 0, time.UTC),
			}.WithSchema([]byte(`{"type": "object"}`)),
		},
	}

	for _, tc := range testcases {
		t.Run("insert and get: "+tc.name, func(t *testing.T) {
			id, err := r.Insert(ctx, &tc.stagingFile)
			require.NoError(t, err)
			require.NotZero(t, id)

			retrieved, err := r.GetByID(ctx, id)
			require.NoError(t, err)

			expected := tc.stagingFile
			expected.ID = id
			expected.Error = nil
			expected.CreatedAt = now
			expected.UpdatedAt = now

			require.Equal(t, expected.StagingFile, retrieved)

			schema, err := r.GetSchemaByID(ctx, id)
			require.NoError(t, err)

			require.Equal(t, expected.Schema, schema)
		})
	}

	t.Run("get missing id", func(t *testing.T) {
		_, err := r.GetByID(ctx, -1)
		require.EqualError(t, err, "no staging file found with id: -1")
	})
}

func manyStagingFiles(size int, now time.Time) []*model.StagingFile {
	files := make([]*model.StagingFile, size)
	for i := range files {
		files[i] = &model.StagingFile{
			WorkspaceID:           "workspace_id",
			Location:              fmt.Sprintf("s3://bucket/path/to/file-%d", i),
			SourceID:              "source_id",
			DestinationID:         "destination_id",
			Status:                warehouseutils.StagingFileWaitingState,
			Error:                 fmt.Errorf("dummy error"),
			FirstEventAt:          now.Add(time.Second),
			LastEventAt:           now,
			UseRudderStorage:      true,
			DestinationRevisionID: "destination_revision_id",
			TotalEvents:           100,
			SourceTaskRunID:       "source_task_run_id",
			SourceJobID:           "source_job_id",
			SourceJobRunID:        "source_job_run_id",
			TimeWindow:            time.Date(1993, 8, 1, 3, 0, 0, 0, time.UTC),
		}
	}
	return files
}

func TestStagingFileRepo_Many(t *testing.T) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Second).UTC()
	db := setupDB(t)
	r := repo.NewStagingFiles(db,
		repo.WithNow(func() time.Time {
			return now
		}),
	)

	stagingFiles := manyStagingFiles(10, now)
	for i := range stagingFiles {
		file := stagingFiles[i].WithSchema([]byte(`{"type": "object"}`))
		id, err := r.Insert(ctx, &file)
		require.NoError(t, err)
		stagingFiles[i].ID = id
		stagingFiles[i].Error = nil
		stagingFiles[i].CreatedAt = now
		stagingFiles[i].UpdatedAt = now
	}

	t.Run("GetForUploadID", func(t *testing.T) {
		t.Parallel()
		u := repo.NewUploads(db)
		uploadId, err := u.CreateWithStagingFiles(context.TODO(), model.Upload{}, stagingFiles)
		require.NoError(t, err)
		testcases := []struct {
			name          string
			sourceID      string
			destinationID string
			uploadId      int64
			expected      []*model.StagingFile
		}{
			{
				name:          "get all",
				sourceID:      "source_id",
				destinationID: "destination_id",
				uploadId:      uploadId,
				expected:      stagingFiles,
			},
			{
				name:          "missing source id",
				sourceID:      "bad_source_id",
				destinationID: "destination_id",
				uploadId:      uploadId,
				expected:      []*model.StagingFile(nil),
			},
			{
				name:          "missing destination id",
				sourceID:      "source_id",
				destinationID: "bad_destination_id",
				uploadId:      uploadId,
				expected:      []*model.StagingFile(nil),
			},
		}
		for _, tc := range testcases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				retrieved, err := r.GetForUploadID(ctx, tc.sourceID, tc.destinationID, tc.uploadId)
				require.NoError(t, err)
				require.Equal(t, tc.expected, retrieved)
			})
		}
	})
}

func TestStagingFileRepo_Pending(t *testing.T) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Second).UTC()

	db := setupDB(t)
	r := repo.NewStagingFiles(db,
		repo.WithNow(func() time.Time {
			return now
		}),
	)
	uploadRepo := repo.NewUploads(db)

	inputData := []struct {
		SourceID      string
		DestinationID string
		Files         int
	}{
		{
			SourceID:      "source_id_1",
			DestinationID: "destination_id_1",
			Files:         10,
		},
		{
			SourceID:      "source_id_2",
			DestinationID: "destination_id_2",
			Files:         20,
		},
		{
			SourceID:      "source_id_2",
			DestinationID: "destination_id_3",
			Files:         16,
		},
	}

	for _, input := range inputData {
		stagingFiles := manyStagingFiles(input.Files, now)
		for i := range stagingFiles {
			stagingFiles[i].DestinationID = input.DestinationID
			stagingFiles[i].SourceID = input.SourceID

			file := stagingFiles[i].WithSchema([]byte(`{"type": "object"}`))

			id, err := r.Insert(ctx, &file)
			require.NoError(t, err)

			stagingFiles[i].ID = id
			stagingFiles[i].Error = nil
			stagingFiles[i].CreatedAt = now
			stagingFiles[i].UpdatedAt = now
		}
		pending, err := r.Pending(ctx, input.SourceID, input.DestinationID)
		require.NoError(t, err)
		require.Equal(t, stagingFiles, pending)

		countByDestID, err := r.CountPendingForDestination(ctx, input.DestinationID)
		require.NoError(t, err)
		require.Equal(t, int64(input.Files), countByDestID)

		countBySrcID, err := r.CountPendingForSource(ctx, input.SourceID)
		require.NoError(t, err)
		require.Equal(t, int64(input.Files), countBySrcID)

		uploadID, err := uploadRepo.CreateWithStagingFiles(ctx, model.Upload{
			SourceID:      input.SourceID,
			DestinationID: input.DestinationID,
		}, pending)
		require.NoError(t, err)

		pending, err = r.Pending(ctx, input.SourceID, input.DestinationID)
		require.NoError(t, err)
		require.Empty(t, pending)

		countByDestID, err = r.CountPendingForDestination(ctx, input.DestinationID)
		require.NoError(t, err)
		require.Zero(t, countByDestID)

		countBySrcID, err = r.CountPendingForSource(ctx, input.SourceID)
		require.NoError(t, err)
		require.Zero(t, countBySrcID)

		t.Run("Uploads", func(t *testing.T) {
			upload, err := uploadRepo.Get(ctx, uploadID)
			require.NoError(t, err)

			events, err := r.TotalEventsForUpload(ctx, upload)
			require.NoError(t, err)
			require.Equal(t, int64(input.Files)*100, events)

			firstEvent, err := r.FirstEventForUpload(ctx, upload)
			require.NoError(t, err)
			require.Equal(t, stagingFiles[0].FirstEventAt, firstEvent.UTC())

			revisionIDs, err := r.DestinationRevisionIDs(ctx, upload)
			require.NoError(t, err)
			require.Equal(t, []string{"destination_revision_id"}, revisionIDs)
		})
	}
}

func TestStagingFileRepo_Status(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Second).UTC()
	db := setupDB(t)
	r := repo.NewStagingFiles(db, repo.WithNow(func() time.Time {
		return now
	}))

	n := 10
	for i := 0; i < n; i++ {
		file := model.StagingFile{
			WorkspaceID:   "workspace_id",
			Location:      fmt.Sprintf("s3://bucket/path/to/file-%d", i),
			SourceID:      "source_id",
			DestinationID: "destination_id",
			Status:        warehouseutils.StagingFileWaitingState,
			Error:         nil,
			FirstEventAt:  now.Add(time.Second),
			LastEventAt:   now,
		}.WithSchema([]byte(`{"type": "object"}`))

		id, err := r.Insert(ctx, &file)
		require.NoError(t, err)

		file.ID = id
		file.Error = nil
		file.CreatedAt = now
		file.UpdatedAt = now
	}

	t.Run("SetStatuses", func(t *testing.T) {
		statuses := []string{
			warehouseutils.StagingFileSucceededState,
			warehouseutils.StagingFileFailedState,
			warehouseutils.StagingFileExecutingState,
			warehouseutils.StagingFileWaitingState,
			warehouseutils.StagingFileAbortedState,
		}

		for _, status := range statuses {
			status := status
			t.Run(status, func(t *testing.T) {
				now = now.Add(time.Second)

				err := r.SetStatuses(ctx,
					[]int64{1, 2, 3},
					status,
				)
				require.NoError(t, err)

				files, err := r.GetForUploadID(ctx, "source_id", "destination_id", 1)
				require.NoError(t, err)

				for _, file := range files {
					require.Equal(t, status, file.Status)
					require.Equal(t, now, file.UpdatedAt)
				}
			})
		}

		err := r.SetStatuses(ctx,
			[]int64{-1, 2, 3}, warehouseutils.StagingFileExecutingState)
		require.EqualError(t, err, "not all rows were updated: 2 != 3")

		err = r.SetStatuses(ctx,
			[]int64{}, warehouseutils.StagingFileExecutingState)
		require.EqualError(t, err, "no staging files to update")
	})

	t.Run("SetErrorStatus", func(t *testing.T) {
		now = now.Add(time.Second)

		err := r.SetErrorStatus(ctx,
			4,
			fmt.Errorf("the error"),
		)
		require.NoError(t, err)

		file, err := r.GetByID(ctx, 4)
		require.NoError(t, err)

		require.Equal(t, warehouseutils.StagingFileFailedState, file.Status)
		require.Equal(t, "the error", file.Error.Error())
		require.Equal(t, now, file.UpdatedAt)

		err = r.SetErrorStatus(ctx,
			-1,
			fmt.Errorf("the error"),
		)
		require.EqualError(t, err, "no rows affected")
	})
}

func TestStagingFileIDs(t *testing.T) {
	sfs := []*model.StagingFile{
		{
			ID: 1,
		},
		{
			ID: 2,
		},
		{
			ID: 3,
		},
	}
	ids := repo.StagingFileIDs(sfs)
	require.Equal(t, []int64{1, 2, 3}, ids)
}

func BenchmarkFiles(b *testing.B) {
	ctx := context.Background()
	db := setupDB(b)
	stagingRepo := repo.NewStagingFiles(db)
	uploadRepo := repo.NewUploads(db)

	size := 100000
	pending := 2

	for i := 0; i < size; i++ {
		file := model.StagingFile{
			WorkspaceID:   "workspace_id",
			Location:      fmt.Sprintf("s3://bucket/path/to/file-%d", i),
			SourceID:      "source_id",
			DestinationID: "destination_id",
			Status:        warehouseutils.StagingFileWaitingState,
			Error:         nil,
			FirstEventAt:  time.Now(),
			LastEventAt:   time.Now(),
		}.WithSchema([]byte(`{"type": "object"}`))

		id, err := stagingRepo.Insert(ctx, &file)
		require.NoError(b, err)

		if i >= (size - pending) {
			continue
		}

		_, err = uploadRepo.CreateWithStagingFiles(ctx, model.Upload{
			SourceID:      "source_id",
			DestinationID: "destination_id",
		}, []*model.StagingFile{
			{
				ID:            id,
				SourceID:      "source_id",
				DestinationID: "destination_id",
			},
		})
		require.NoError(b, err)
	}

	b.ResetTimer()

	b.Run("GetStagingFiles", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			ff, err := stagingRepo.Pending(ctx, "source_id", "destination_id")
			require.NoError(b, err)
			require.Equal(b, pending, len(ff))
		}
	})
}
