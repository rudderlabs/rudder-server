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
	"github.com/rudderlabs/rudder-server/warehouse"
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
		stagingFile warehouse.StagingFileT
	}{
		{
			name: "create staging file",
			stagingFile: warehouse.StagingFileT{
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
			stagingFile: warehouse.StagingFileT{
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
			stagingFile: warehouse.StagingFileT{
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
			id, err := r.Insert(ctx, tc.stagingFile)
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

	var stagingFiles []warehouse.StagingFileT
	n := 10
	for i := 0; i < n; i++ {
		file := warehouse.StagingFileT{
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

		id, err := r.Insert(ctx, file)
		require.NoError(t, err)

		file.ID = id
		file.Error = nil
		file.CreatedAt = now
		file.UpdatedAt = now

		stagingFiles = append(stagingFiles, file)
	}

	testcases := []struct {
		name          string
		sourceID      string
		destinationID string
		startID       int64
		endID         int64

		expected []warehouse.StagingFileT
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

			expected: []warehouse.StagingFileT(nil),
		},
		{
			name:          "missing destination id",
			sourceID:      "source_id",
			destinationID: "bad_destination_id",
			startID:       0,
			endID:         10,

			expected: []warehouse.StagingFileT(nil),
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			retrieved, err := r.GetInRange(ctx, tc.sourceID, tc.destinationID, tc.startID, tc.endID)
			require.NoError(t, err)

			require.Equal(t, tc.expected, retrieved)
		})
	}
}
