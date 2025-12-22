package partitionbuffer

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

func TestFlushBufferedPartitions(t *testing.T) {
	const (
		flushBatchSizeConfig   = "flushBatchSize"
		flushPayloadSizeConfig = "flushPayloadSize"
		flushMoveTimeoutConfig = "flushMoveTimeout"
	)
	t.Run("readwrite", func(t *testing.T) {
		setup := func(t *testing.T) (pb JobsDBPartitionBuffer, c *config.Config, sqlDB *sql.DB, bufferDB jobsdb.JobsDB) {
			c = config.New()
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)
			pg, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			runNodeMigration(t, pg.DB)
			primaryRW := jobsdb.NewForReadWrite("rt",
				jobsdb.WithDBHandle(pg.DB),
				jobsdb.WithNumPartitions(64),
				jobsdb.WithSkipMaintenanceErr(true),
				jobsdb.WithConfig(c),
			)
			require.NoError(t, primaryRW.Start(), "it should be able to start JobsDB")
			t.Cleanup(func() {
				primaryRW.TearDown()
			})
			bufferRW := jobsdb.NewForReadWrite("rt_buf",
				jobsdb.WithDBHandle(pg.DB),
				jobsdb.WithNumPartitions(64),
				jobsdb.WithSkipMaintenanceErr(true),
				jobsdb.WithConfig(c),
			)
			require.NoError(t, bufferRW.Start(), "it should be able to start JobsDB Buffer")
			t.Cleanup(func() {
				bufferRW.TearDown()
			})
			pb, err = NewJobsDBPartitionBuffer(t.Context(),
				WithReadWriteJobsDBs(primaryRW, bufferRW),
				WithFlushBatchSize(c.GetReloadableIntVar(3, 1, flushBatchSizeConfig)),
				WithFlushPayloadSize(c.GetReloadableInt64Var(100*bytesize.MB, 1, flushPayloadSizeConfig)),
				WithFlushMoveTimeout(c.GetReloadableDurationVar(60, time.Second, flushMoveTimeoutConfig)),
			)
			require.NoError(t, err)
			return pb, c, pg.DB, bufferRW
		}

		t.Run("move once then flush", func(t *testing.T) {
			pb, _, sqlDB, bufferDB := setup(t)

			// Mark partition-1 as buffered
			err := pb.BufferPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)

			// Add 2 jobs to partition-1
			jobs := []*jobsdb.JobT{
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
					PartitionID:  "partition-1",
				},
			}
			err = pb.Store(t.Context(), jobs)
			require.NoError(t, err)

			// Verify jobs are in the buffer
			bufferJobs, err := bufferDB.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferJobs.Jobs, 2, "expected 2 jobs in buffer before flush")

			// Flush buffered partitions
			err = pb.FlushBufferedPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)

			// Verify all jobs are now in the primary jobsdb
			var primaryCount int
			err = sqlDB.QueryRow("SELECT COUNT(*) FROM rt_jobs_1 WHERE partition_id = 'partition-1'").Scan(&primaryCount)
			require.NoError(t, err)
			require.Equal(t, 2, primaryCount, "expected 2 jobs in primary jobsdb after flush")

			// Verify buffer has no unprocessed jobs for partition-1
			bufferJobs, err = bufferDB.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferJobs.Jobs, 0, "expected no unprocessed jobs in buffer after flush")

			// Verify buffered partitions no longer has partition-1
			var count int
			err = sqlDB.QueryRow("select count(*) from rt_buffered_partitions where partition_id = 'partition-1'").Scan(&count)
			require.NoError(t, err, "it should be able to query buffered partitions")
			require.Equal(t, 0, count, "expected partition-1 to be removed from buffered partitions after flush")
		})

		t.Run("move twice then flush", func(t *testing.T) {
			pb, _, sqlDB, bufferDB := setup(t)

			// Mark partition-1 as buffered
			err := pb.BufferPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)

			// Add 4 jobs to partition-1 (flushBatchSize is 3, so this should trigger 2 moves)
			jobs := []*jobsdb.JobT{
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
					PartitionID:  "partition-1",
				},
				{
					UUID:         uuid.New(),
					CustomVal:    "test",
					EventCount:   1,
					Parameters:   []byte("{}"),
					WorkspaceId:  "workspace-1",
					UserID:       "user-3",
					EventPayload: []byte("{}"),
					PartitionID:  "partition-1",
				},
				{
					UUID:         uuid.New(),
					CustomVal:    "test",
					EventCount:   1,
					Parameters:   []byte("{}"),
					WorkspaceId:  "workspace-1",
					UserID:       "user-4",
					EventPayload: []byte("{}"),
					PartitionID:  "partition-1",
				},
			}
			err = pb.Store(t.Context(), jobs)
			require.NoError(t, err)

			// Verify jobs are in the buffer
			bufferJobs, err := bufferDB.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferJobs.Jobs, 4, "expected 4 jobs in buffer before flush")

			// Flush buffered partitions (should trigger 2 moves: first batch of 3, then batch of 1, then switchover)
			err = pb.FlushBufferedPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)

			// Verify all jobs are now in the primary jobsdb
			var primaryCount int
			err = sqlDB.QueryRow("SELECT COUNT(*) FROM rt_jobs_1 WHERE partition_id = 'partition-1'").Scan(&primaryCount)
			require.NoError(t, err)
			require.Equal(t, 4, primaryCount, "expected 4 jobs in primary jobsdb after flush")

			// Verify buffer has no unprocessed jobs for partition-1
			bufferJobs, err = bufferDB.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferJobs.Jobs, 0, "expected no unprocessed jobs in buffer after flush")

			// Verify buffered partitions no longer has partition-1
			var count int
			err = sqlDB.QueryRow("select count(*) from rt_buffered_partitions where partition_id = 'partition-1'").Scan(&count)
			require.NoError(t, err, "it should be able to query buffered partitions")
			require.Equal(t, 0, count, "expected partition-1 to be removed from buffered partitions after flush")
		})

		t.Run("flush immediately", func(t *testing.T) {
			pb, c, sqlDB, bufferDB := setup(t)

			// Mark partition-1 as buffered
			err := pb.BufferPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)

			// Add 2 jobs to partition-1
			jobs := []*jobsdb.JobT{
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
					PartitionID:  "partition-1",
				},
			}
			err = pb.Store(t.Context(), jobs)
			require.NoError(t, err)

			// Verify jobs are in the buffer
			bufferJobs, err := bufferDB.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferJobs.Jobs, 2, "expected 2 jobs in buffer before flush")

			// Set flushMoveTimeout to 0s to trigger immediate switchover
			c.Set(flushMoveTimeoutConfig, 0)

			// Flush buffered partitions (should skip move loop and go straight to switchover due to timeout)
			err = pb.FlushBufferedPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)

			// Verify all jobs are now in the primary jobsdb
			var primaryCount int
			err = sqlDB.QueryRow("SELECT COUNT(*) FROM rt_jobs_1 WHERE partition_id = 'partition-1'").Scan(&primaryCount)
			require.NoError(t, err)
			require.Equal(t, 2, primaryCount, "expected 2 jobs in primary jobsdb after flush")

			// Verify buffer has no unprocessed jobs for partition-1
			bufferJobs, err = bufferDB.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferJobs.Jobs, 0, "expected no unprocessed jobs in buffer after flush")

			// Verify buffered partitions no longer has partition-1
			var count int
			err = sqlDB.QueryRow("select count(*) from rt_buffered_partitions where partition_id = 'partition-1'").Scan(&count)
			require.NoError(t, err, "it should be able to query buffered partitions")
			require.Equal(t, 0, count, "expected partition-1 to be removed from buffered partitions after flush")
		})
	})

	t.Run("separate writer and reader", func(t *testing.T) {
		setup := func(t *testing.T) (pbw, pbr JobsDBPartitionBuffer, c *config.Config, sqlDB *sql.DB, bufferReaderDB jobsdb.JobsDB) {
			c = config.New()
			pool, err := dockertest.NewPool("")
			require.NoError(t, err)
			pg, err := postgres.Setup(pool, t)
			require.NoError(t, err)
			runNodeMigration(t, pg.DB)
			primaryWO := jobsdb.NewForWrite("gw",
				jobsdb.WithDBHandle(pg.DB),
				jobsdb.WithNumPartitions(64),
				jobsdb.WithSkipMaintenanceErr(true),
				jobsdb.WithConfig(c),
			)
			require.NoError(t, primaryWO.Start(), "it should be able to start writer JobsDB")
			t.Cleanup(func() {
				primaryWO.TearDown()
			})
			primaryRO := jobsdb.NewForRead("gw",
				jobsdb.WithDBHandle(pg.DB),
				jobsdb.WithNumPartitions(64),
				jobsdb.WithSkipMaintenanceErr(true),
				jobsdb.WithConfig(c),
			)
			require.NoError(t, primaryRO.Start(), "it should be able to start reader JobsDB")
			t.Cleanup(func() {
				primaryRO.TearDown()
			})

			bufferWO := jobsdb.NewForWrite("gw_buf",
				jobsdb.WithDBHandle(pg.DB),
				jobsdb.WithNumPartitions(64),
				jobsdb.WithSkipMaintenanceErr(true),
				jobsdb.WithConfig(c),
			)
			require.NoError(t, bufferWO.Start(), "it should be able to start writer Buffer")
			t.Cleanup(func() {
				bufferWO.TearDown()
			})

			bufferRO := jobsdb.NewForRead("gw_buf",
				jobsdb.WithDBHandle(pg.DB),
				jobsdb.WithNumPartitions(64),
				jobsdb.WithSkipMaintenanceErr(true),
				jobsdb.WithConfig(c),
			)
			require.NoError(t, bufferRO.Start(), "it should be able to start reader Buffer")
			t.Cleanup(func() {
				bufferRO.TearDown()
			})

			pbw, err = NewJobsDBPartitionBuffer(t.Context(),
				WithWriterOnlyJobsDBs(primaryWO, bufferWO),
			)
			require.NoError(t, err)

			pbr, err = NewJobsDBPartitionBuffer(t.Context(),
				WithReaderOnlyAndFlushJobsDBs(primaryRO, bufferRO, primaryWO),
				WithFlushBatchSize(c.GetReloadableIntVar(3, 1, flushBatchSizeConfig)),
				WithFlushPayloadSize(c.GetReloadableInt64Var(100*bytesize.MB, 1, flushPayloadSizeConfig)),
				WithFlushMoveTimeout(c.GetReloadableDurationVar(60, time.Second, flushMoveTimeoutConfig)),
			)
			require.NoError(t, err)
			return pbw, pbr, c, pg.DB, bufferRO
		}

		t.Run("move once then flush", func(t *testing.T) {
			pbw, pbr, _, sqlDB, bufferReaderDB := setup(t)

			// Mark partition-1 as buffered in pbr
			err := pbr.BufferPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)

			// Refresh partitions in pbw so it knows partition-1 is buffered
			err = pbw.RefreshBufferedPartitions(t.Context())
			require.NoError(t, err)

			// Add 2 jobs to partition-1 in pbw
			jobs := []*jobsdb.JobT{
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
					PartitionID:  "partition-1",
				},
			}
			err = pbw.Store(t.Context(), jobs)
			require.NoError(t, err)

			// Verify jobs are in the buffer
			bufferJobs, err := bufferReaderDB.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferJobs.Jobs, 2, "expected 2 jobs in buffer before flush")

			// Flush buffered partitions using pbr
			err = pbr.FlushBufferedPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)

			// Verify all jobs are now in the primary jobsdb
			var primaryCount int
			err = sqlDB.QueryRow("SELECT COUNT(*) FROM gw_jobs_1 WHERE partition_id = 'partition-1'").Scan(&primaryCount)
			require.NoError(t, err)
			require.Equal(t, 2, primaryCount, "expected 2 jobs in primary jobsdb after flush")

			// Verify buffer has no unprocessed jobs for partition-1
			bufferJobs, err = bufferReaderDB.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferJobs.Jobs, 0, "expected no unprocessed jobs in buffer after flush")

			// Verify buffered partitions no longer has partition-1
			var count int
			err = sqlDB.QueryRow("select count(*) from gw_buffered_partitions where partition_id = 'partition-1'").Scan(&count)
			require.NoError(t, err, "it should be able to query buffered partitions")
			require.Equal(t, 0, count, "expected partition-1 to be removed from buffered partitions after flush")
		})

		t.Run("move twice then flush", func(t *testing.T) {
			pbw, pbr, _, sqlDB, bufferReaderDB := setup(t)

			// Mark partition-1 as buffered in pbr
			err := pbr.BufferPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)

			// Refresh partitions in pbw so it knows partition-1 is buffered
			err = pbw.RefreshBufferedPartitions(t.Context())
			require.NoError(t, err)

			// Add 4 jobs to partition-1 in pbw (flushBatchSize is 3, so this should trigger 2 moves)
			jobs := []*jobsdb.JobT{
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
					PartitionID:  "partition-1",
				},
				{
					UUID:         uuid.New(),
					CustomVal:    "test",
					EventCount:   1,
					Parameters:   []byte("{}"),
					WorkspaceId:  "workspace-1",
					UserID:       "user-3",
					EventPayload: []byte("{}"),
					PartitionID:  "partition-1",
				},
				{
					UUID:         uuid.New(),
					CustomVal:    "test",
					EventCount:   1,
					Parameters:   []byte("{}"),
					WorkspaceId:  "workspace-1",
					UserID:       "user-4",
					EventPayload: []byte("{}"),
					PartitionID:  "partition-1",
				},
			}
			err = pbw.Store(t.Context(), jobs)
			require.NoError(t, err)

			// Verify jobs are in the buffer
			bufferJobs, err := bufferReaderDB.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferJobs.Jobs, 4, "expected 4 jobs in buffer before flush")

			// Flush buffered partitions using pbr (should trigger 2 moves: first batch of 3, then batch of 1, then switchover)
			err = pbr.FlushBufferedPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)

			// Verify all jobs are now in the primary jobsdb
			var primaryCount int
			err = sqlDB.QueryRow("SELECT COUNT(*) FROM gw_jobs_1 WHERE partition_id = 'partition-1'").Scan(&primaryCount)
			require.NoError(t, err)
			require.Equal(t, 4, primaryCount, "expected 4 jobs in primary jobsdb after flush")

			// Verify buffer has no unprocessed jobs for partition-1
			bufferJobs, err = bufferReaderDB.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferJobs.Jobs, 0, "expected no unprocessed jobs in buffer after flush")

			// Verify buffered partitions no longer has partition-1
			var count int
			err = sqlDB.QueryRow("select count(*) from gw_buffered_partitions where partition_id = 'partition-1'").Scan(&count)
			require.NoError(t, err, "it should be able to query buffered partitions")
			require.Equal(t, 0, count, "expected partition-1 to be removed from buffered partitions after flush")
		})

		t.Run("flush immediately", func(t *testing.T) {
			pbw, pbr, c, sqlDB, bufferReaderDB := setup(t)

			// Mark partition-1 as buffered in pbr
			err := pbr.BufferPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)

			// Refresh partitions in pbw so it knows partition-1 is buffered
			err = pbw.RefreshBufferedPartitions(t.Context())
			require.NoError(t, err)

			// Add 2 jobs to partition-1 in pbw
			jobs := []*jobsdb.JobT{
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
					PartitionID:  "partition-1",
				},
			}
			err = pbw.Store(t.Context(), jobs)
			require.NoError(t, err)

			// Verify jobs are in the buffer
			bufferJobs, err := bufferReaderDB.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferJobs.Jobs, 2, "expected 2 jobs in buffer before flush")

			// Set flushMoveTimeout to 0s to trigger immediate switchover
			c.Set(flushMoveTimeoutConfig, 0)

			// Flush buffered partitions using pbr (should skip move loop and go straight to switchover due to timeout)
			err = pbr.FlushBufferedPartitions(t.Context(), []string{"partition-1"})
			require.NoError(t, err)

			// Verify all jobs are now in the primary jobsdb
			var primaryCount int
			err = sqlDB.QueryRow("SELECT COUNT(*) FROM gw_jobs_1 WHERE partition_id = 'partition-1'").Scan(&primaryCount)
			require.NoError(t, err)
			require.Equal(t, 2, primaryCount, "expected 2 jobs in primary jobsdb after flush")

			// Verify buffer has no unprocessed jobs for partition-1
			bufferJobs, err = bufferReaderDB.GetJobs(t.Context(), []string{jobsdb.Unprocessed.State}, jobsdb.GetQueryParams{JobsLimit: 10})
			require.NoError(t, err)
			require.Len(t, bufferJobs.Jobs, 0, "expected no unprocessed jobs in buffer after flush")

			// Verify buffered partitions no longer has partition-1
			var count int
			err = sqlDB.QueryRow("select count(*) from gw_buffered_partitions where partition_id = 'partition-1'").Scan(&count)
			require.NoError(t, err, "it should be able to query buffered partitions")
			require.Equal(t, 0, count, "expected partition-1 to be removed from buffered partitions after flush")
		})
	})
}

func TestFlushErrors(t *testing.T) {
	setup := func(t *testing.T) (pb *jobsDBPartitionBuffer, primaryMock, bufferMock *mockJobsDB) {
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		pg, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		runNodeMigration(t, pg.DB)

		primaryRW := jobsdb.NewForReadWrite("rt",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
		)
		require.NoError(t, primaryRW.Start(), "it should be able to start JobsDB")
		t.Cleanup(func() {
			primaryRW.TearDown()
		})
		bufferRW := jobsdb.NewForReadWrite("rt_buf",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
		)
		require.NoError(t, bufferRW.Start(), "it should be able to start JobsDB Buffer")
		t.Cleanup(func() {
			bufferRW.TearDown()
		})
		primaryMock = &mockJobsDB{JobsDB: primaryRW}
		bufferMock = &mockJobsDB{JobsDB: bufferRW}
		pbuf, err := NewJobsDBPartitionBuffer(t.Context(), WithReadWriteJobsDBs(primaryMock, bufferMock))
		require.NoError(t, err)
		err = pbuf.BufferPartitions(t.Context(), []string{"partition-1"})
		require.NoError(t, err)
		jobs := []*jobsdb.JobT{
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
		}
		err = pbuf.Store(t.Context(), jobs)
		require.NoError(t, err)
		return pbuf.(*jobsDBPartitionBuffer), primaryMock, bufferMock
	}
	t.Run("moveBufferedPartitions", func(t *testing.T) {
		t.Run("with canceled context", func(t *testing.T) {
			pb, _, _ := setup(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := pb.moveBufferedPartitions(ctx, []string{"partition-1"}, 1, bytesize.MB)
			require.Error(t, err, "flush should fail with canceled context")
			require.ErrorIs(t, err, context.Canceled, "error should be context.Canceled")
		})

		t.Run("primaryWriteJobsDB.StoreInTx failing", func(t *testing.T) {
			pb, primaryMock, _ := setup(t)
			primaryMock.storeInTxErr = errors.New("failed")
			_, err := pb.moveBufferedPartitions(t.Context(), []string{"partition-1"}, 1, bytesize.MB)
			require.Error(t, err, "moveBufferedPartitions should fail when StoreInTx fails")
			require.EqualError(t, err, "moving buffered jobs to primary jobsdb: failed")
		})

		t.Run("bufferReadJobsDB.UpdateJobStatusInTx failing", func(t *testing.T) {
			pb, _, bufferMock := setup(t)
			bufferMock.updateJobStatusInTxErr = errors.New("failed")
			_, err := pb.moveBufferedPartitions(t.Context(), []string{"partition-1"}, 1, bytesize.MB)
			require.Error(t, err, "moveBufferedPartitions should fail when UpdateJobStatusInTx fails")
			require.EqualError(t, err, "updating job statuses for moved jobs: failed")
		})
	})

	t.Run("switchoverBufferedPartitions", func(t *testing.T) {
		t.Run("with canceled context", func(t *testing.T) {
			pb, _, _ := setup(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := pb.switchoverBufferedPartitions(ctx, []string{"partition-1"}, 1, bytesize.MB)
			require.Error(t, err, "flush should fail with canceled context")
			require.ErrorIs(t, err, context.Canceled, "error should be context.Canceled")
		})

		t.Run("bufferReadJobsDB.RefreshDSList failing", func(t *testing.T) {
			pb, _, bufferMock := setup(t)
			pb.differentReaderWriterDBs = true
			bufferMock.refreshDSListErr = errors.New("failed")
			err := pb.switchoverBufferedPartitions(t.Context(), []string{"partition-1"}, 1, bytesize.MB)
			require.Error(t, err, "switchoverBufferedPartitions should fail when RefreshDSList fails")
			require.EqualError(t, err, "refreshing buffer DS list during switchover: failed")
		})

		t.Run("moveBufferedPartitions failing during switchover", func(t *testing.T) {
			pb, _, bufferMock := setup(t)
			pb.differentReaderWriterDBs = true
			bufferMock.getUnprocessedErr = errors.New("failed")
			err := pb.switchoverBufferedPartitions(t.Context(), []string{"partition-1"}, 1, bytesize.MB)
			require.Error(t, err, "switchoverBufferedPartitions should fail when moveBufferedPartitions fails")
			require.EqualError(t, err, "moving buffered partitions during switchover: failed")
		})
	})

	t.Run("FlushBufferedPartitions", func(t *testing.T) {
		t.Run("partitions not buffered", func(t *testing.T) {
			pb, _, _ := setup(t)
			err := pb.FlushBufferedPartitions(t.Context(), []string{"partition-2"})
			require.Error(t, err, "flush should not succeed if partition is not buffered")
			require.EqualError(t, err, "partitions are not buffered, cannot flush: [partition-2]")
		})

		t.Run("trying to flush a partition that is already being flushed", func(t *testing.T) {
			pb, _, _ := setup(t)
			pb.flushingPartitions["partition-1"] = struct{}{}
			err := pb.FlushBufferedPartitions(t.Context(), []string{"partition-1"})
			require.Error(t, err, "flush should not succeed if partition is already being flushed")
			require.EqualError(t, err, "partitions are already being flushed: [partition-1]")
		})

		t.Run("moveBufferedPartitions failing", func(t *testing.T) {
			pb, _, _ := setup(t)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			err := pb.FlushBufferedPartitions(ctx, []string{"partition-1"})
			require.Error(t, err, "flush should fail with canceled context")
			require.EqualError(t, err, "acquiring a buffered partitions read lock during flush: context canceled")
		})

		t.Run("switchoverBufferedPartitions failing", func(t *testing.T) {
			pb, _, bufferMock := setup(t)
			pb.differentReaderWriterDBs = true
			bufferMock.refreshDSListErr = errors.New("failed")
			err := pb.FlushBufferedPartitions(t.Context(), []string{"partition-1"})
			require.Error(t, err, "flush should fail with canceled context")
			require.EqualError(t, err, "switchover of buffered partitions: refreshing buffer DS list during switchover: failed")
		})
	})
}

type mockJobsDB struct {
	jobsdb.JobsDB
	storeInTxErr           error
	updateJobStatusInTxErr error
	refreshDSListErr       error
	getUnprocessedErr      error
}

func (m *mockJobsDB) StoreInTx(ctx context.Context, sst jobsdb.StoreSafeTx, jobs []*jobsdb.JobT) error {
	if m.storeInTxErr != nil {
		return m.storeInTxErr
	}
	return m.JobsDB.StoreInTx(ctx, sst, jobs)
}

func (m *mockJobsDB) UpdateJobStatusInTx(ctx context.Context, tx jobsdb.UpdateSafeTx, statusList []*jobsdb.JobStatusT, customValFilters []string, parameterFilters []jobsdb.ParameterFilterT) error {
	if m.updateJobStatusInTxErr != nil {
		return m.updateJobStatusInTxErr
	}
	return m.JobsDB.UpdateJobStatusInTx(ctx, tx, statusList, customValFilters, parameterFilters)
}

func (m *mockJobsDB) RefreshDSList(ctx context.Context) error {
	if m.refreshDSListErr != nil {
		return m.refreshDSListErr
	}
	return m.JobsDB.RefreshDSList(ctx)
}

func (m *mockJobsDB) GetUnprocessed(ctx context.Context, params jobsdb.GetQueryParams) (jobsdb.JobsResult, error) {
	if m.getUnprocessedErr != nil {
		return jobsdb.JobsResult{}, m.getUnprocessedErr
	}
	return m.JobsDB.GetUnprocessed(ctx, params)
}
