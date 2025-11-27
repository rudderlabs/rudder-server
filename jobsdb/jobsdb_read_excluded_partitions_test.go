package jobsdb

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestReadExcludedPartitionsManagement(t *testing.T) {
	t.Run("with partitioning enabled", func(t *testing.T) {
		postgres := startPostgres(t)
		db := NewForReadWrite("read_excluded", WithNumPartitions(64), WithDBHandle(postgres.DB))
		require.NoError(t, db.Start(), "should start the jobs db handle")
		defer db.TearDown()

		ctx := context.Background()

		t.Run("exclude and include partitions", func(t *testing.T) {
			// Store jobs for different workspaces which will go to different partitions
			jobs := []*JobT{
				{
					UUID:         uuid.New(),
					WorkspaceId:  "workspace-1",
					UserID:       "user-1",
					CustomVal:    "custom-val-1",
					Parameters:   []byte(`{}`),
					EventPayload: []byte(`{}`),
					EventCount:   1,
				},
				{
					UUID:         uuid.New(),
					WorkspaceId:  "workspace-2",
					UserID:       "user-2",
					CustomVal:    "custom-val-2",
					Parameters:   []byte(`{}`),
					EventPayload: []byte(`{}`),
					EventCount:   1,
				},
				{
					UUID:         uuid.New(),
					WorkspaceId:  "workspace-3",
					UserID:       "user-3",
					CustomVal:    "custom-val-3",
					Parameters:   []byte(`{}`),
					EventPayload: []byte(`{}`),
					EventCount:   1,
				},
			}

			err := db.Store(ctx, jobs)
			require.NoError(t, err, "should store jobs successfully")

			// Get all unprocessed jobs initially
			unprocessed, err := db.GetUnprocessed(ctx, GetQueryParams{JobsLimit: 10})
			require.NoError(t, err, "should get unprocessed jobs")
			require.Equal(t, 3, len(unprocessed.Jobs), "should return all 3 stored jobs")

			// Extract partition IDs from the jobs
			partitionIDs := make([]string, 0)
			for _, job := range unprocessed.Jobs {
				require.NotEmpty(t, job.PartitionID, "job should have a partition ID")
				partitionIDs = append(partitionIDs, job.PartitionID)
			}

			t.Run("add excluded partitions", func(t *testing.T) {
				// Exclude first two partitions
				partitionsToExclude := partitionIDs[:2]
				err := db.AddReadExcludedPartitionIDs(ctx, partitionsToExclude)
				require.NoError(t, err, "should add excluded partitions")

				// Get unprocessed jobs after exclusion
				unprocessed, err := db.GetUnprocessed(ctx, GetQueryParams{JobsLimit: 10})
				require.NoError(t, err, "should get unprocessed jobs")
				require.Equal(t, 1, len(unprocessed.Jobs), "should return only 1 job from non-excluded partition")
				require.Equal(t, partitionIDs[2], unprocessed.Jobs[0].PartitionID, "should return job from non-excluded partition")
			})

			t.Run("remove excluded partitions", func(t *testing.T) {
				// Remove one excluded partition
				partitionsToInclude := []string{partitionIDs[0]}
				err := db.RemoveReadExcludedPartitionIDs(ctx, partitionsToInclude)
				require.NoError(t, err, "should remove excluded partitions")

				// Get unprocessed jobs after removing exclusion
				unprocessed, err := db.GetUnprocessed(ctx, GetQueryParams{JobsLimit: 10})
				require.NoError(t, err, "should get unprocessed jobs")
				require.Equal(t, 2, len(unprocessed.Jobs), "should return 2 jobs from non-excluded partitions")

				// Verify we're getting the right partitions
				returnedPartitions := make(map[string]bool)
				for _, job := range unprocessed.Jobs {
					returnedPartitions[job.PartitionID] = true
				}
				require.True(t, returnedPartitions[partitionIDs[0]], "should include previously excluded partition")
				require.True(t, returnedPartitions[partitionIDs[2]], "should include never excluded partition")
				require.False(t, returnedPartitions[partitionIDs[1]], "should not include still-excluded partition")
			})
		})

		t.Run("add duplicate partitions", func(t *testing.T) {
			// Store a job
			job := &JobT{
				UUID:         uuid.New(),
				WorkspaceId:  "workspace-dup",
				UserID:       "user-dup",
				CustomVal:    "custom-val-dup",
				Parameters:   []byte(`{}`),
				EventPayload: []byte(`{}`),
				EventCount:   1,
			}
			err := db.Store(ctx, []*JobT{job})
			require.NoError(t, err, "should store job")

			// Get the partition ID
			unprocessed, err := db.GetUnprocessed(ctx, GetQueryParams{JobsLimit: 1, CustomValFilters: []string{"custom-val-dup"}})
			require.NoError(t, err, "should get unprocessed jobs")
			require.Equal(t, 1, len(unprocessed.Jobs), "should return the stored job")
			partitionID := unprocessed.Jobs[0].PartitionID

			// Add same partition multiple times
			err = db.AddReadExcludedPartitionIDs(ctx, []string{partitionID, partitionID, partitionID})
			require.NoError(t, err, "should handle duplicate partition IDs")

			// Verify exclusion works
			unprocessed, err = db.GetUnprocessed(ctx, GetQueryParams{JobsLimit: 10, CustomValFilters: []string{"custom-val-dup"}})
			require.NoError(t, err, "should get unprocessed jobs")
			require.Equal(t, 0, len(unprocessed.Jobs), "should exclude the partition")
		})

		t.Run("add empty partition list", func(t *testing.T) {
			err := db.AddReadExcludedPartitionIDs(ctx, []string{})
			require.NoError(t, err, "should handle empty partition list")
		})

		t.Run("remove empty partition list", func(t *testing.T) {
			err := db.RemoveReadExcludedPartitionIDs(ctx, []string{})
			require.NoError(t, err, "should handle empty partition list")
		})

		t.Run("remove non-existent partitions", func(t *testing.T) {
			err := db.RemoveReadExcludedPartitionIDs(ctx, []string{"non-existent-partition"})
			require.NoError(t, err, "should handle non-existent partitions gracefully")
		})
	})

	t.Run("without partitioning enabled", func(t *testing.T) {
		postgres := startPostgres(t)
		db := NewForReadWrite("no_partitions", WithDBHandle(postgres.DB))
		require.NoError(t, db.Start(), "should start the jobs db handle")
		defer db.TearDown()

		ctx := context.Background()

		t.Run("add excluded partitions fails", func(t *testing.T) {
			err := db.AddReadExcludedPartitionIDs(ctx, []string{"partition-1"})
			require.Error(t, err, "should fail when partitioning is not enabled")
			require.Contains(t, err.Error(), "partitioning is not enabled", "error should mention partitioning not enabled")
		})

		t.Run("remove excluded partitions fails", func(t *testing.T) {
			err := db.RemoveReadExcludedPartitionIDs(ctx, []string{"partition-1"})
			require.Error(t, err, "should fail when partitioning is not enabled")
			require.Contains(t, err.Error(), "partitioning is not enabled", "error should mention partitioning not enabled")
		})
	})

	t.Run("persistence across restarts", func(t *testing.T) {
		// Use a dedicated postgres container for this test to ensure clean state
		postgres := startPostgres(t)
		ctx := context.Background()
		tablePrefix := "persist_test"

		// Create first instance and add excluded partitions
		db1 := NewForReadWrite(tablePrefix, WithNumPartitions(64), WithDBHandle(postgres.DB))
		require.NoError(t, db1.Start(), "should start first db instance")

		// Store a job to get a partition ID
		job := &JobT{
			UUID:         uuid.New(),
			WorkspaceId:  "workspace-persist",
			UserID:       "user-persist",
			CustomVal:    "custom-val-persist",
			Parameters:   []byte(`{}`),
			EventPayload: []byte(`{}`),
			EventCount:   1,
		}
		err := db1.Store(ctx, []*JobT{job})
		require.NoError(t, err, "should store job")

		// Get partition ID and exclude it
		unprocessed, err := db1.GetUnprocessed(ctx, GetQueryParams{JobsLimit: 1})
		require.NoError(t, err, "should get unprocessed jobs")
		require.Equal(t, 1, len(unprocessed.Jobs), "should return the stored job")
		partitionID := unprocessed.Jobs[0].PartitionID

		err = db1.AddReadExcludedPartitionIDs(ctx, []string{partitionID})
		require.NoError(t, err, "should add excluded partition")

		// Verify exclusion works
		unprocessed, err = db1.GetUnprocessed(ctx, GetQueryParams{JobsLimit: 10})
		require.NoError(t, err, "should get unprocessed jobs")
		require.Equal(t, 0, len(unprocessed.Jobs), "should not return excluded job")

		// Tear down first instance but keep the database
		db1.Stop()

		// Create second instance with same prefix and database
		db2 := NewForReadWrite(tablePrefix, WithNumPartitions(64), WithDBHandle(postgres.DB))
		require.NoError(t, db2.Start(), "should start second db instance")
		defer db2.TearDown()

		// Verify exclusion persists
		unprocessed, err = db2.GetUnprocessed(ctx, GetQueryParams{JobsLimit: 10})
		require.NoError(t, err, "should get unprocessed jobs")
		require.Equal(t, 0, len(unprocessed.Jobs), "excluded partition should persist across restarts")
	})
}
