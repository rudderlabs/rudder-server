package partitionbuffer

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

func TestJobsDBPartitionBufferStore(t *testing.T) {
	const numPartitions = 64
	jobs := func() []*jobsdb.JobT {
		return []*jobsdb.JobT{
			{
				UUID:         uuid.New(),
				CustomVal:    "test",
				EventCount:   1,
				Parameters:   []byte("{}"),
				WorkspaceId:  "workspace-1",
				UserID:       "user-1",
				EventPayload: []byte("{}"),
				PartitionID:  "partition-1",
			},
			{
				UUID:         uuid.New(),
				CustomVal:    "test",
				EventCount:   1,
				Parameters:   []byte("{}"),
				WorkspaceId:  "workspace-1",
				UserID:       "user-2",
				EventPayload: []byte("{}"),
				PartitionID:  "partition-2",
			},
		}
	}

	type testSetup struct {
		rwPB    JobsDBPartitionBuffer
		roPB    JobsDBPartitionBuffer
		woPB    JobsDBPartitionBuffer
		primary jobsdb.JobsDB
		buffer  jobsdb.JobsDB
		sqlDB   *sql.DB
	}
	setupBuffers := func(t *testing.T) testSetup {
		ts := testSetup{}
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		pg, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		runNodeMigration(t, pg.DB)
		ts.primary = jobsdb.NewForReadWrite("primary",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(numPartitions),
			jobsdb.WithSkipMaintenanceErr(true),
		)
		require.NoError(t, ts.primary.Start(), "it should be able to start JobsDB")
		t.Cleanup(func() {
			ts.primary.Stop()
		})
		ts.buffer = jobsdb.NewForReadWrite("buf",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(numPartitions),
			jobsdb.WithSkipMaintenanceErr(true),
		)
		require.NoError(t, ts.buffer.Start(), "it should be able to start JobsDB Buffer")
		t.Cleanup(func() {
			ts.buffer.Stop()
		})
		ts.rwPB, err = NewJobsDBPartitionBuffer(t.Context(), WithReadWriteJobsDBs(ts.primary, ts.buffer), WithNumPartitions(numPartitions))
		require.NoError(t, err)
		ts.roPB, err = NewJobsDBPartitionBuffer(t.Context(), WithReaderOnlyAndFlushJobsDBs(ts.primary, ts.buffer, ts.primary), WithNumPartitions(numPartitions))
		require.NoError(t, err)
		ts.woPB, err = NewJobsDBPartitionBuffer(t.Context(), WithWithWriterOnlyJobsDBs(ts.primary, ts.buffer), WithNumPartitions(numPartitions))
		require.NoError(t, err)
		ts.sqlDB = pg.DB
		return ts
	}

	t.Run("read write buffer", func(t *testing.T) {
		t.Run("no buffered partitions", func(t *testing.T) {
			ts := setupBuffers(t)
			pb := ts.rwPB
			err := pb.Store(t.Context(), jobs())
			require.NoError(t, err)
			storedJobs, err := ts.primary.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, storedJobs.Jobs, 2)
		})
		t.Run("with buffered partitions", func(t *testing.T) {
			ts := setupBuffers(t)
			pb := ts.rwPB

			// buffer partition-1
			err := pb.BufferPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)
			err = pb.Store(t.Context(), jobs())
			require.NoError(t, err)

			primaryJobs, err := ts.primary.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, primaryJobs.Jobs, 1)
			require.Equal(t, "partition-2", primaryJobs.Jobs[0].PartitionID)

			bufferedJobs, err := ts.buffer.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferedJobs.Jobs, 1)
			require.Equal(t, "partition-1", bufferedJobs.Jobs[0].PartitionID)

			// buffer partition-2 as well
			err = pb.BufferPartitions(t.Context(), []string{"partition-2"})
			require.NoError(t, err)
			err = pb.Store(t.Context(), jobs())
			require.NoError(t, err)

			primaryJobs, err = ts.primary.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, primaryJobs.Jobs, 1) // still only partition-2 from first store

			bufferedJobs, err = ts.buffer.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferedJobs.Jobs, 3) // partition-1 from first store + both from second store
		})
		t.Run("jobs missing partitionID", func(t *testing.T) {
			ts := setupBuffers(t)
			pb := ts.rwPB
			jobsList := jobs()
			var partitionIDs []string
			for _, job := range jobsList {
				job.PartitionID = ""
				partitionIDs = append(partitionIDs, jobsdb.DefaultParititionFunction(job, numPartitions))
			}

			// buffer the first partition
			err := pb.BufferPartitions(t.Context(), []string{partitionIDs[0]})
			require.NoError(t, err)
			err = pb.Store(t.Context(), jobsList)
			require.NoError(t, err)

			storedJobs, err := ts.primary.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, storedJobs.Jobs, 1)

			bufferedJobs, err := ts.buffer.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferedJobs.Jobs, 1)
		})

		t.Run("WithTx from another jobsdb", func(t *testing.T) {
			ts := setupBuffers(t)
			pb := ts.rwPB

			err := ts.primary.WithStoreSafeTx(t.Context(), func(tx jobsdb.StoreSafeTx) error {
				return pb.StoreInTx(t.Context(), tx, jobs())
			})
			require.NoError(t, err)
		})
	})

	t.Run("write only buffer", func(t *testing.T) {
		t.Run("no buffered partitions", func(t *testing.T) {
			ts := setupBuffers(t)
			pb := ts.woPB
			err := pb.Store(t.Context(), jobs())
			require.NoError(t, err)
			storedJobs, err := ts.primary.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, storedJobs.Jobs, 2)
		})
		t.Run("with buffered partitions", func(t *testing.T) {
			ts := setupBuffers(t)
			pb := ts.woPB

			// buffer partition-1 through read only buffer
			err := ts.roPB.BufferPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)
			// refresh the write only buffer's buffered partitions
			err = pb.RefreshBufferedPartitions(t.Context())
			require.NoError(t, err)

			// now store jobs
			err = pb.Store(t.Context(), jobs())
			require.NoError(t, err)

			primaryJobs, err := ts.primary.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, primaryJobs.Jobs, 1)
			require.Equal(t, "partition-2", primaryJobs.Jobs[0].PartitionID)

			bufferedJobs, err := ts.buffer.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferedJobs.Jobs, 1)
			require.Equal(t, "partition-1", bufferedJobs.Jobs[0].PartitionID)

			// store again without changing buffered partitions
			err = pb.Store(t.Context(), jobs())
			require.NoError(t, err)

			primaryJobs, err = ts.primary.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, primaryJobs.Jobs, 2)
			require.Equal(t, "partition-2", primaryJobs.Jobs[1].PartitionID)

			bufferedJobs, err = ts.buffer.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferedJobs.Jobs, 2)
			require.Equal(t, "partition-1", bufferedJobs.Jobs[1].PartitionID)

			// buffer partition-2 as well through read only buffer
			err = ts.roPB.BufferPartitions(t.Context(), []string{"partition-2"})
			require.NoError(t, err)
			// write only buffer will refresh its buffered partitions automatically before store
			err = pb.Store(t.Context(), jobs())
			require.NoError(t, err)

			primaryJobs, err = ts.primary.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, primaryJobs.Jobs, 2) // still only partition-2 from previous store

			bufferedJobs, err = ts.buffer.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferedJobs.Jobs, 4) // partition-1 from previous store + both from current store
		})
	})

	t.Run("read only buffer cannot store", func(t *testing.T) {
		ts := setupBuffers(t)
		pb := ts.roPB
		err := pb.Store(t.Context(), jobs())
		require.ErrorIs(t, err, ErrStoreNotSupported)
		err = ts.primary.WithStoreSafeTx(t.Context(), func(tx jobsdb.StoreSafeTx) error {
			return pb.StoreInTx(t.Context(), tx, jobs())
		})
		require.ErrorIs(t, err, ErrStoreNotSupported)
	})
}
