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

func TestPartitionBufferWatchDog(t *testing.T) {
	const (
		numPartitions = 64
	)

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
}
