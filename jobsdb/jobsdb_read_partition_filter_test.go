package jobsdb

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestReadPartitionFilter(t *testing.T) {
	postgres := startPostgres(t)
	db := NewForReadWrite("read_excluded", WithNumPartitions(64), WithDBHandle(postgres.DB))
	require.NoError(t, db.Start(), "should start the jobs db handle")
	defer db.TearDown()

	ctx := context.Background()

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

	t.Run("get one partition id", func(t *testing.T) {
		// Get jobs from the first partition only
		result, err := db.GetUnprocessed(ctx, GetQueryParams{
			JobsLimit:        10,
			PartitionFilters: []string{partitionIDs[0]},
		})
		require.NoError(t, err, "should get unprocessed jobs with partition filter")
		require.Equal(t, 1, len(result.Jobs), "should return only 1 job for the specified partition")
		require.Equal(t, partitionIDs[0], result.Jobs[0].PartitionID, "job should be from the requested partition")
	})

	t.Run("get two partition ids", func(t *testing.T) {
		// Get jobs from the first two partitions
		result, err := db.GetUnprocessed(ctx, GetQueryParams{
			JobsLimit:        10,
			PartitionFilters: []string{partitionIDs[0], partitionIDs[1]},
		})
		require.NoError(t, err, "should get unprocessed jobs with partition filters")
		require.Equal(t, 2, len(result.Jobs), "should return 2 jobs for the specified partitions")

		returnedPartitions := map[string]bool{}
		for _, job := range result.Jobs {
			returnedPartitions[job.PartitionID] = true
		}
		require.True(t, returnedPartitions[partitionIDs[0]], "should include job from first partition")
		require.True(t, returnedPartitions[partitionIDs[1]], "should include job from second partition")
	})

	t.Run("get by partition id with same partition id in excluded list", func(t *testing.T) {
		// Add one partition to the excluded list
		err := db.AddReadExcludedPartitionIDs(ctx, []string{partitionIDs[0]})
		require.NoError(t, err, "should add partition to excluded list")

		// Get jobs by explicitly specifying that same partition - it should return the job
		result, err := db.GetUnprocessed(ctx, GetQueryParams{
			JobsLimit:        10,
			PartitionFilters: []string{partitionIDs[0]},
		})
		require.NoError(t, err, "should get unprocessed jobs with partition filter")
		require.Equal(t, 1, len(result.Jobs), "should return the job even though partition is in excluded list")
		require.Equal(t, partitionIDs[0], result.Jobs[0].PartitionID, "job should be from the requested partition")

		// Without partition filter, the excluded partition should not be returned
		result, err = db.GetUnprocessed(ctx, GetQueryParams{
			JobsLimit: 10,
		})
		require.NoError(t, err, "should get unprocessed jobs without partition filter")
		require.Equal(t, 2, len(result.Jobs), "should return only 2 jobs (excluding the excluded partition)")
		for _, job := range result.Jobs {
			require.NotEqual(t, partitionIDs[0], job.PartitionID, "should not include job from excluded partition")
		}

		// Clean up - remove from excluded list
		err = db.RemoveReadExcludedPartitionIDs(ctx, []string{partitionIDs[0]})
		require.NoError(t, err, "should remove partition from excluded list")
	})

	t.Run("get executing", func(t *testing.T) {
		// Get jobs in executing state using GetJobs with partition filter
		result, err := db.GetJobs(ctx, []string{Executing.State}, GetQueryParams{
			JobsLimit:        10,
			PartitionFilters: []string{unprocessed.Jobs[0].PartitionID},
		})
		require.NoError(t, err, "should get jobs in executing state")
		require.Equal(t, 0, len(result.Jobs), "should return 0 jobs since none are executing yet")

		// Mark one job as executing
		statuses := []*JobStatusT{
			{
				JobID:         unprocessed.Jobs[0].JobID,
				JobState:      Executing.State,
				AttemptNum:    1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "",
				ErrorResponse: []byte(`{}`),
				Parameters:    []byte(`{}`),
				WorkspaceId:   unprocessed.Jobs[0].WorkspaceId,
				PartitionID:   unprocessed.Jobs[0].PartitionID,
			},
		}
		err = db.UpdateJobStatus(ctx, statuses, nil, nil)
		require.NoError(t, err, "should update job status to executing")

		// Verify that GetUnprocessed with partition filter now returns 0 jobs for that partition
		// since the job is now in executing state
		result, err = db.GetUnprocessed(ctx, GetQueryParams{
			JobsLimit:        10,
			PartitionFilters: []string{unprocessed.Jobs[0].PartitionID},
		})
		require.NoError(t, err, "should get unprocessed jobs")
		require.Equal(t, 0, len(result.Jobs), "should return 0 jobs since the job is now executing")

		// Get jobs in executing state using GetJobs with partition filter
		result, err = db.GetJobs(ctx, []string{Executing.State}, GetQueryParams{
			JobsLimit:        10,
			PartitionFilters: []string{unprocessed.Jobs[0].PartitionID},
		})
		require.NoError(t, err, "should get jobs in executing state")
		require.Equal(t, 1, len(result.Jobs), "should return the executing job")
		require.Equal(t, unprocessed.Jobs[0].JobID, result.Jobs[0].JobID, "should return the correct job")
		require.Equal(t, unprocessed.Jobs[0].PartitionID, result.Jobs[0].PartitionID, "job should be from the requested partition")
	})

	t.Run("updating job status without filling in partition id", func(t *testing.T) {
		// when a job status is missing partition id, this will impact no jobs cache invalidation, leading to getjobs returning no results incorrectly
		// this is the reason why we'll monitoring warning logs for this case in production, to catch any missing partition ids during status updates

		// Get jobs in aborted state using GetJobs with partition filter
		result, err := db.GetJobs(ctx, []string{Aborted.State}, GetQueryParams{
			JobsLimit:        10,
			PartitionFilters: []string{unprocessed.Jobs[1].PartitionID},
		})
		require.NoError(t, err, "should get jobs in aborted state")
		require.Equal(t, 0, len(result.Jobs), "should return 0 jobs since none are executing yet")

		// Mark one job as aborted
		statuses := []*JobStatusT{
			{
				JobID:         unprocessed.Jobs[1].JobID,
				JobState:      Executing.State,
				AttemptNum:    1,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				ErrorCode:     "",
				ErrorResponse: []byte(`{}`),
				Parameters:    []byte(`{}`),
				WorkspaceId:   unprocessed.Jobs[1].WorkspaceId,
			},
		}
		err = db.UpdateJobStatus(ctx, statuses, nil, nil)
		require.NoError(t, err, "should update job status to executing")

		// Verify that GetUnprocessed with partition filter now returns 0 jobs for that partition
		// since the job is now in executing state
		result, err = db.GetUnprocessed(ctx, GetQueryParams{
			JobsLimit:        10,
			PartitionFilters: []string{unprocessed.Jobs[1].PartitionID},
		})
		require.NoError(t, err, "should get unprocessed jobs")
		require.Equal(t, 0, len(result.Jobs), "should return 0 jobs since the job is now executing")

		// Get jobs in aborted state using GetJobs with partition filter
		result, err = db.GetJobs(ctx, []string{Aborted.State}, GetQueryParams{
			JobsLimit:        10,
			PartitionFilters: []string{unprocessed.Jobs[1].PartitionID},
		})
		require.NoError(t, err, "should get jobs in aborted state")
		require.Equal(t, 0, len(result.Jobs), "should not return the aborted job, because no jobs cache should not have been invalidated")
	})
}
