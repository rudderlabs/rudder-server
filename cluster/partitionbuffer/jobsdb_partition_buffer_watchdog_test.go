package partitionbuffer

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

func TestPartitionBufferWatchdog(t *testing.T) {
	const (
		numPartitions = 64
	)

	t.Run("watchdog flushes unbuffered partitions", func(t *testing.T) {
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		pg, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		runNodeMigration(t, pg.DB)

		primary := jobsdb.NewForReadWrite("primary",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(numPartitions),
			jobsdb.WithSkipMaintenanceErr(true),
		)
		buffer := jobsdb.NewForReadWrite("buf",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithSkipMaintenanceErr(true),
		)

		// start buffer JobsDB, add jobs for partition1, partition2 and partition3 and stop it
		// Note: Jobs are stored in order partition1, partition3, partition2 so that unbuffered partitions
		// (partition1, partition3) come before the buffered partition (partition2) in the queue.
		// The watchdog processes jobs in order and gets blocked by buffered partitions,
		// so this ordering ensures partition1 and partition3 are flushed before hitting partition2.
		require.NoError(t, buffer.Start())
		now := time.Now()
		jobs := []*jobsdb.JobT{
			{UUID: uuid.New(), UserID: "user-1", CreatedAt: now, ExpireAt: now, CustomVal: "cv1", EventCount: 1, EventPayload: []byte(`{}`), Parameters: []byte(`{}`), WorkspaceId: "workspace-1", PartitionID: "partition1"},
			{UUID: uuid.New(), UserID: "user-1", CreatedAt: now, ExpireAt: now, CustomVal: "cv1", EventCount: 1, EventPayload: []byte(`{}`), Parameters: []byte(`{}`), WorkspaceId: "workspace-1", PartitionID: "partition3"},
			{UUID: uuid.New(), UserID: "user-1", CreatedAt: now, ExpireAt: now, CustomVal: "cv1", EventCount: 1, EventPayload: []byte(`{}`), Parameters: []byte(`{}`), WorkspaceId: "workspace-1", PartitionID: "partition2"},
		}
		require.NoError(t, buffer.Store(t.Context(), jobs))
		buffer.Stop()

		// create a partition buffer
		pb, err := NewJobsDBPartitionBuffer(t.Context(),
			WithReadWriteJobsDBs(primary, buffer),
			WithNumPartitions(numPartitions),
			WithFlushMoveTimeout(config.SingleValueLoader(10*time.Second)),
			WithWatchdogInterval(config.SingleValueLoader(100*time.Millisecond)), // short interval for faster test
		)
		require.NoError(t, err)

		// add partition2 to buffered partitions
		require.NoError(t, pb.BufferPartitions(t.Context(), []string{"partition2"}))

		// start the partition buffer
		require.NoError(t, pb.Start())
		t.Cleanup(func() {
			pb.Stop()
		})

		// verify that jobs for partition1 and partition3 are moved to primary jobsdb by the watchdog
		// verify that jobs for partition2 remain in the buffer jobsdb
		require.Eventually(t, func() bool {
			// Check primary JobsDB has jobs for partition1 and partition3
			primaryJobs, err := primary.GetUnprocessed(context.Background(), jobsdb.GetQueryParams{JobsLimit: 100})
			if err != nil {
				t.Logf("Error getting unprocessed jobs from primary: %v", err)
				return false
			}

			partition1Found := false
			partition3Found := false
			for _, job := range primaryJobs.Jobs {
				if job.PartitionID == "partition1" {
					partition1Found = true
				}
				if job.PartitionID == "partition3" {
					partition3Found = true
				}
				// partition2 should NOT be in primary
				if job.PartitionID == "partition2" {
					t.Logf("Unexpected: partition2 job found in primary JobsDB")
					return false
				}
			}

			if !partition1Found || !partition3Found {
				t.Logf("Waiting for partition1 and partition3 jobs to be moved to primary. Found: partition1=%v, partition3=%v", partition1Found, partition3Found)
				return false
			}

			// Check buffer JobsDB only has jobs for partition2
			bufferJobs, err := buffer.GetUnprocessed(context.Background(), jobsdb.GetQueryParams{JobsLimit: 100})
			if err != nil {
				t.Logf("Error getting unprocessed jobs from buffer: %v", err)
				return false
			}

			for _, job := range bufferJobs.Jobs {
				if job.PartitionID != "partition2" {
					t.Logf("Unexpected: %s job found in buffer JobsDB", job.PartitionID)
					return false
				}
			}
			// Ensure partition2 job is still in buffer
			partition2InBuffer := false
			for _, job := range bufferJobs.Jobs {
				if job.PartitionID == "partition2" {
					partition2InBuffer = true
					break
				}
			}
			if !partition2InBuffer {
				t.Logf("partition2 job not found in buffer JobsDB")
				return false
			}

			return true
		}, 30*time.Second, 200*time.Millisecond, "watchdog should move jobs for unbuffered partitions to primary JobsDB")
	})

	t.Run("watchdog gets interrupted while flushing unbuffered partitions", func(t *testing.T) {
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		pg, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		runNodeMigration(t, pg.DB)

		conf := config.New()

		conf.Set("JobsDB.maxDSSize", "200")
		conf.Set("JobsDB.dsLimit", "2")
		conf.Set("JobsDB.addNewDSLoopSleepDuration", "100ms")

		primary := jobsdb.NewForReadWrite("primary",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(numPartitions),
			jobsdb.WithSkipMaintenanceErr(true),
			jobsdb.WithConfig(conf),
		)
		buffer := jobsdb.NewForReadWrite("buf",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithSkipMaintenanceErr(true),
			jobsdb.WithConfig(conf),
		)

		// start buffer JobsDB, add 1000 jobs for partition1 in batches of 10 at a time and stop it
		require.NoError(t, buffer.Start())
		now := time.Now()
		for range 100 {
			batch := make([]*jobsdb.JobT, 10)
			for j := range 10 {
				batch[j] = &jobsdb.JobT{
					UUID: uuid.New(), UserID: "user-1", CreatedAt: now, ExpireAt: now,
					CustomVal: "cv1", EventCount: 1, EventPayload: []byte(`{}`),
					Parameters: []byte(`{}`), WorkspaceId: "workspace-1", PartitionID: "partition1",
				}
			}
			require.NoError(t, buffer.Store(t.Context(), batch))
			time.Sleep(10 * time.Millisecond)
		}
		buffer.Stop()

		// create a partition buffer with a small flush batch size so the watchdog
		// makes many iterations, giving us opportunities to acquire the write lock
		pb, err := NewJobsDBPartitionBuffer(t.Context(),
			WithReadWriteJobsDBs(primary, buffer),
			WithNumPartitions(numPartitions),
			WithFlushMoveTimeout(config.SingleValueLoader(10*time.Second)),
			WithWatchdogInterval(config.SingleValueLoader(100*time.Millisecond)), // short interval for faster test
			WithFlushBatchSize(config.SingleValueLoader(10)),                     // small batch for more lock check points
		)
		require.NoError(t, err)

		jpb := pb.(*jobsDBPartitionBuffer)

		// start the partition buffer
		require.NoError(t, pb.Start())
		t.Cleanup(func() {
			pb.Stop()
		})

		// The buffer will now start trying to flush partition1,
		// but we will mark it as buffered as soon as we find the
		// first job for partition1 in the primary JobsDB.
		// This should cause the watchdog to stop flushing partition1
		// and leave remaining jobs for partition1 in the buffer JobsDB.
		//
		// The watchdog will be fast in flushing jobs, but on each loop
		// it acquires a read lock on the buffered partitions to check.
		// We can acquire a write lock and release it only after checking
		// primary jobsdb in a loop until we see the first job for partition1.
		// Then we add partition1 to buffered partitions and release the lock,
		// which should cause the watchdog to stop flushing partition1.

		// Wait for the watchdog to start flushing by polling primary for partition1 jobs
		require.Eventually(t, func() bool {
			jpb.bufferedPartitionsMu.Lock()
			defer jpb.bufferedPartitionsMu.Unlock()
			primaryJobs, err := primary.GetUnprocessed(t.Context(), jobsdb.GetQueryParams{JobsLimit: 100})
			require.NoError(t, err)
			// wait for at least 50 jobs to be moved to primary
			return len(primaryJobs.Jobs) > 50
		}, 30*time.Second, 10*time.Millisecond, "watchdog should start moving partition1 jobs to primary")

		// Add partition1 to buffered partitions, in-memory should be enough
		jpb.bufferedPartitionsMu.Lock()
		jpb.bufferedPartitions = jpb.bufferedPartitions.Append(map[string]struct{}{"partition1": {}})
		jpb.bufferedPartitionsMu.Unlock()

		time.Sleep(1 * time.Second) // wait a moment for watchdog to pick up the change (move its last batch)
		bufferedJobs, err := buffer.GetUnprocessed(t.Context(), jobsdb.GetQueryParams{JobsLimit: 10000})
		require.NoError(t, err)
		bufferedCount := len(bufferedJobs.Jobs)
		require.Greater(t, bufferedCount, 0, "there should be some jobs left in the buffer after watchdog is interrupted")
		t.Logf("Buffer has %d jobs left", bufferedCount)
		// Verify that the same number of jobs for partition1 remain in the buffer JobsDB after interruption,
		// meaning watchdog stopped flushing partition1
		time.Sleep(4 * time.Second)
		bufferJobs, err := buffer.GetUnprocessed(context.Background(), jobsdb.GetQueryParams{JobsLimit: 10000})
		require.NoError(t, err)
		require.EqualValues(t, bufferedCount, len(bufferJobs.Jobs), "number of jobs left in buffer should remain the same after watchdog interruption")
		pb.Stop()
	})
}
