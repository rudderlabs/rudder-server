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

func setupDB(t *testing.T) *sql.DB {
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
				SourceBatchID:         "source_batch_id",
				SourceTaskID:          "source_task_id",
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
				SourceBatchID:         "source_batch_id",
				SourceTaskID:          "source_task_id",
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
				SourceBatchID:         "source_batch_id",
				SourceTaskID:          "source_task_id",
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

func manyStagingFiles(size int, now time.Time) []model.StagingFile {
	files := make([]model.StagingFile, size)
	for i := range files {
		files[i] = model.StagingFile{
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
			SourceBatchID:         "source_batch_id",
			SourceTaskID:          "source_task_id",
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

	r := repo.NewStagingFiles(setupDB(t),
		repo.WithNow(func() time.Time {
			return now
		}),
	)

	stagingFiles := manyStagingFiles(10, now)
	for i, _ := range stagingFiles {
		file := stagingFiles[i].WithSchema([]byte(`{"type": "object"}`))
		id, err := r.Insert(ctx, &file)
		require.NoError(t, err)

		stagingFiles[i].ID = id
		stagingFiles[i].Error = nil
		stagingFiles[i].CreatedAt = now
		stagingFiles[i].UpdatedAt = now
	}

	t.Run("GetInRange", func(t *testing.T) {
		t.Parallel()

		testcases := []struct {
			name          string
			sourceID      string
			destinationID string
			startID       int64
			endID         int64

			expected []model.StagingFile
		}{
			{
				name:          "get all",
				sourceID:      "source_id",
				destinationID: "destination_id",
				startID:       0,
				endID:         10,

				expected: stagingFiles,
			},
			{
				name:          "get all with start id",
				sourceID:      "source_id",
				destinationID: "destination_id",
				startID:       5,
				endID:         10,

				expected: stagingFiles[4:],
			},
			{
				name:          "get all with end id",
				sourceID:      "source_id",
				destinationID: "destination_id",
				startID:       0,
				endID:         5,

				expected: stagingFiles[:5],
			},
			{
				name:          "missing source id",
				sourceID:      "bad_source_id",
				destinationID: "destination_id",
				startID:       0,
				endID:         10,

				expected: []model.StagingFile(nil),
			},
			{
				name:          "missing destination id",
				sourceID:      "source_id",
				destinationID: "bad_destination_id",
				startID:       0,
				endID:         10,

				expected: []model.StagingFile(nil),
			},
		}

		for _, tc := range testcases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				retrieved, err := r.GetInRange(ctx, tc.sourceID, tc.destinationID, tc.startID, tc.endID)
				require.NoError(t, err)

				require.Equal(t, tc.expected, retrieved)
			})
		}
	})

	t.Run("GetAfterID", func(t *testing.T) {
		t.Parallel()

		testcases := []struct {
			name          string
			sourceID      string
			destinationID string
			startID       int64

			expected []model.StagingFile
		}{
			{
				name:          "get all",
				sourceID:      "source_id",
				destinationID: "destination_id",
				startID:       0,

				expected: stagingFiles,
			},
			{
				name:          "get all with start id",
				sourceID:      "source_id",
				destinationID: "destination_id",
				startID:       5,

				expected: stagingFiles[5:],
			},
			{
				name:          "missing source id",
				sourceID:      "bad_source_id",
				destinationID: "destination_id",
				startID:       0,

				expected: []model.StagingFile(nil),
			},
			{
				name:          "missing destination id",
				sourceID:      "source_id",
				destinationID: "bad_destination_id",
				startID:       0,

				expected: []model.StagingFile(nil),
			},
		}

		for _, tc := range testcases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				retrieved, err := r.GetAfterID(ctx, tc.sourceID, tc.destinationID, tc.startID)
				require.NoError(t, err)
				t.Log(retrieved, tc.expected)
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

		err = uploadRepo.CreateWithStagingFiles(ctx, model.Upload{
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

	}
}
