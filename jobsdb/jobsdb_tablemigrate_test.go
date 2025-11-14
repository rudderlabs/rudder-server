package jobsdb

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestJobsDBTableMigrations(t *testing.T) {
	t.Run("13_partition_id_column", func(t *testing.T) {
		postgres := startPostgres(t)

		// Start JobsDB with DB version 12 (without partition_id column) to create initial tables
		jd := NewForWrite("test", WithDBHandle(postgres.DB), withDatabaseTablesVersion(12))
		require.NoError(t, jd.Start(), "it should be able to start JobsDB")
		jd.TearDown()

		// Add some data to ensure migration works fine with existing data
		_, err := postgres.DB.Exec(`
			INSERT INTO test_jobs_1 
				(uuid, workspace_id, user_id, custom_val, parameters, event_payload, event_count) 
			VALUES 
				(gen_random_uuid(), 'worskpace-1', 'user-1', 'gw', '{"source_id": "src1"}', '{}', 1)`)
		require.NoError(t, err, "it should be able to insert initial data")

		// Start JobsDB again to trigger migration to add partition_id column
		jd = NewForReadWrite("test", WithDBHandle(postgres.DB))
		require.NoError(t, jd.Start(), "it should be able to start JobsDB with latest table versions")
		defer jd.TearDown()

		// Verify that partition_id column exists, but is empty for existing rows
		unprocessed, err := jd.GetUnprocessed(context.Background(), GetQueryParams{JobsLimit: 1})
		require.NoError(t, err, "it should be able to get unprocessed jobs")
		require.Len(t, unprocessed.Jobs, 1)
		require.Empty(t, unprocessed.Jobs[0].PartitionID)

		// Store a new job and verify partition_id is set
		require.NoError(t, jd.Store(context.Background(), []*JobT{{
			UUID:         uuid.New(),
			WorkspaceId:  "workspace-1",
			UserID:       "user-1",
			PartitionID:  "partition-1",
			CustomVal:    "gw",
			Parameters:   []byte(`{"source_id": "src2"}`),
			EventPayload: []byte(`{}`),
			EventCount:   1,
		}}))

		unprocessed, err = jd.GetUnprocessed(context.Background(), GetQueryParams{JobsLimit: 2})
		require.NoError(t, err, "it should be able to get unprocessed jobs")
		require.Len(t, unprocessed.Jobs, 2)
		require.Equal(t, "partition-1", unprocessed.Jobs[1].PartitionID)
	})
}
