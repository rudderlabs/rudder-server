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

	return pgResource.DB
}

func TestStagingFileRepo(t *testing.T) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Second).UTC()

	r := repo.StagingFiles{
		DB: setupDB(t),
		Now: func() time.Time {
			return now
		},
	}

	testcases := []struct {
		name        string
		stagingFile model.StagingFile
	}{
		{
			name: "create staging file",
			stagingFile: model.StagingFile{
				WorkspaceID:           "workspace_id",
				Location:              "s3://bucket/path/to/file",
				SourceID:              "source_id",
				DestinationID:         "destination_id",
				Schema:                []byte(`{"type": "object"}`),
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
			},
		},
		{
			name: "missing FirstEventAt",
			stagingFile: model.StagingFile{
				WorkspaceID:           "workspace_id",
				Location:              "s3://bucket/path/to/file",
				SourceID:              "source_id",
				DestinationID:         "destination_id",
				Schema:                []byte(`{"type": "object"}`),
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
			},
		},
		{
			name: "missing LastEventAt",
			stagingFile: model.StagingFile{
				WorkspaceID:           "workspace_id",
				Location:              "s3://bucket/path/to/file",
				SourceID:              "source_id",
				DestinationID:         "destination_id",
				Schema:                []byte(`{"type": "object"}`),
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
			},
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

			require.Equal(t, expected, retrieved)
		})
	}

	t.Run("get missing id", func(t *testing.T) {
		_, err := r.GetByID(ctx, -1)
		require.EqualError(t, err, "no staging file found with id: -1")
	})
}

func TestStagingFileRepo_Many(t *testing.T) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Second).UTC()

	r := repo.StagingFiles{
		DB: setupDB(t),
		Now: func() time.Time {
			return now
		},
	}

	var stagingFiles []model.StagingFile
	n := 10
	for i := 0; i < n; i++ {
		file := model.StagingFile{
			WorkspaceID:           "workspace_id",
			Location:              fmt.Sprintf("s3://bucket/path/to/file-%d", i),
			SourceID:              "source_id",
			DestinationID:         "destination_id",
			Schema:                []byte(`{"type": "object"}`),
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

		id, err := r.Insert(ctx, &file)
		require.NoError(t, err)

		file.ID = id
		file.Error = nil
		file.CreatedAt = now
		file.UpdatedAt = now

		stagingFiles = append(stagingFiles, file)
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

func TestStagingFileRepo_Status(t *testing.T) {
	ctx := context.Background()

	now := time.Now().Truncate(time.Second).UTC()

	r := repo.StagingFiles{
		DB: setupDB(t),
		Now: func() time.Time {
			return now
		},
	}

	n := 10
	for i := 0; i < n; i++ {
		file := model.StagingFile{
			WorkspaceID:   "workspace_id",
			Location:      fmt.Sprintf("s3://bucket/path/to/file-%d", i),
			SourceID:      "source_id",
			DestinationID: "destination_id",
			Schema:        []byte(`{"type": "object"}`),
			Status:        warehouseutils.StagingFileWaitingState,
			Error:         fmt.Errorf(""),
			FirstEventAt:  now.Add(time.Second),
			LastEventAt:   now,
		}

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

				files, err := r.GetInRange(ctx, "source_id", "destination_id", 0, 3)
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
