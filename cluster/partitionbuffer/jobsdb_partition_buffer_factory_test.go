package partitionbuffer

import (
	"testing"

	"github.com/google/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

func TestNewJobsDBPartitionBuffer(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pg, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	runNodeMigration(t, pg.DB)

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
			},
			{
				UUID:         uuid.New(),
				CustomVal:    "test",
				EventCount:   1,
				Parameters:   []byte("{}"),
				WorkspaceId:  "workspace-1",
				UserID:       "user-2",
				EventPayload: []byte("{}"),
			},
		}
	}
	t.Run("WithReadWriteJobsDBs", func(t *testing.T) {
		ctx := t.Context()

		primary := jobsdb.NewForReadWrite("rw", jobsdb.WithDBHandle(pg.DB), jobsdb.WithNumPartitions(64))
		buffer := jobsdb.NewForReadWrite("rw_buf", jobsdb.WithDBHandle(pg.DB), jobsdb.WithNumPartitions(64))
		require.NoError(t, buffer.Start(), "it should be able to start JobsDB Buffer")
		pb, err := NewJobsDBPartitionBuffer(ctx, WithReadWriteJobsDBs(primary, buffer), WithLogger(logger.NOP))
		require.NoError(t, err, "it should be able to create JobsDBPartitionBuffer")
		require.NoError(t, pb.Start(), "it should be able to start pb buffer")
		t.Cleanup(func() {
			pb.Stop()
		})

		err = pb.Store(ctx, jobs())
		require.NoError(t, err, "it should be able to store jobs")

		err = pb.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
			return pb.StoreInTx(ctx, tx, jobs())
		})
		require.NoError(t, err, "it should be able to store jobs in tx")

		res := pb.StoreEachBatchRetry(ctx, [][]*jobsdb.JobT{jobs(), jobs()})
		require.Len(t, res, 0, "it should be able to store each batch retry")

		err = pb.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
			res, err := pb.StoreEachBatchRetryInTx(ctx, tx, [][]*jobsdb.JobT{jobs(), jobs()})
			require.Len(t, res, 0, "it should be able to store each batch retry in tx")
			return err
		})
		require.NoError(t, err, "it should be able to store each batch retry in tx")

		err = pb.BufferPartitions(ctx, []string{"partition-1"})
		require.NoError(t, err, "it should be able to buffer partitions")
		err = pb.FlushBufferedPartitions(ctx, []string{"partition-1"})
		require.NoError(t, err, "FlushBufferedPartitions should be supported in reader-only mode")
	})

	t.Run("WithWriterOnlyJobsDBs", func(t *testing.T) {
		ctx := t.Context()

		primaryWriter := jobsdb.NewForReadWrite("wo", jobsdb.WithDBHandle(pg.DB), jobsdb.WithNumPartitions(64))
		bufferWriter := jobsdb.NewForReadWrite("wo_buf", jobsdb.WithDBHandle(pg.DB), jobsdb.WithNumPartitions(64))
		pb, err := NewJobsDBPartitionBuffer(ctx, WithWriterOnlyJobsDBs(primaryWriter, bufferWriter), WithStats(stats.NOP))
		require.NoError(t, err)
		require.NoError(t, pb.Start(), "it should be able to start pb buffer")
		t.Cleanup(func() {
			pb.Stop()
		})

		err = pb.Store(ctx, jobs())
		require.NoError(t, err, "it should be able to store jobs")

		err = pb.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
			return pb.StoreInTx(ctx, tx, jobs())
		})
		require.NoError(t, err, "it should be able to store jobs in tx")

		res := pb.StoreEachBatchRetry(ctx, [][]*jobsdb.JobT{jobs(), jobs()})
		require.Len(t, res, 0, "it should be able to store each batch retry")

		err = pb.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
			res, err := pb.StoreEachBatchRetryInTx(ctx, tx, [][]*jobsdb.JobT{jobs(), jobs()})
			require.Len(t, res, 0, "it should be able to store each batch retry in tx")
			return err
		})
		require.NoError(t, err, "it should be able to store each batch retry in tx")

		err = pb.BufferPartitions(ctx, []string{"partition-1"})
		require.NoError(t, err, "it should be able to buffer partitions")

		err = pb.FlushBufferedPartitions(ctx, []string{"partition-1"})
		require.ErrorIs(t, err, ErrFlushNotSupported, "FlushBufferedPartitions should not be supported in writer-only mode")
	})

	t.Run("WithReaderOnlyAndFlushJobsDBs", func(t *testing.T) {
		ctx := t.Context()

		// need to create the writer before the reader so that journal tables are created
		primaryWriter := jobsdb.NewForReadWrite("ro", jobsdb.WithDBHandle(pg.DB), jobsdb.WithNumPartitions(64))
		primaryReader := jobsdb.NewForRead("ro", jobsdb.WithDBHandle(pg.DB), jobsdb.WithNumPartitions(64))

		// we need to first create a writer buffer DB to create journal tables, even though we won't use it
		bufferWriter := jobsdb.NewForWrite("ro_buf", jobsdb.WithDBHandle(pg.DB), jobsdb.WithNumPartitions(64))
		require.NoError(t, bufferWriter.Start(), "it should be able to start buffer reader JobsDB")
		t.Cleanup(func() {
			bufferWriter.TearDown()
		})

		bufferReader := jobsdb.NewForRead("ro_buf", jobsdb.WithDBHandle(pg.DB), jobsdb.WithNumPartitions(64))

		pb, err := NewJobsDBPartitionBuffer(ctx, WithReaderOnlyAndFlushJobsDBs(primaryReader, bufferReader, primaryWriter))
		require.NoError(t, err)
		require.NoError(t, pb.Start(), "it should be able to start pb buffer")
		t.Cleanup(func() {
			pb.Stop()
		})

		// This configuration is for reader-only mode, so Store operations should not be supported (canStore=false)
		err = pb.Store(ctx, jobs())
		require.ErrorIs(t, err, ErrStoreNotSupported, "Store should not be supported in reader-only mode")

		err = primaryWriter.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
			return pb.StoreInTx(ctx, tx, jobs())
		})
		require.ErrorIs(t, err, ErrStoreNotSupported, "StoreInTx should not be supported in reader-only mode")

		batch1 := jobs()
		batch2 := jobs()
		res := pb.StoreEachBatchRetry(ctx, [][]*jobsdb.JobT{batch1, batch2})
		require.Len(t, res, 2, "StoreEachBatchRetry should return errors for 2 batches")
		require.Contains(t, res[batch1[0].UUID], ErrStoreNotSupported.Error(), "Each job should have store not supported error")
		require.Contains(t, res[batch2[0].UUID], ErrStoreNotSupported.Error(), "Each job should have store not supported error")

		err = primaryWriter.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
			res, err := pb.StoreEachBatchRetryInTx(ctx, tx, [][]*jobsdb.JobT{batch1, batch2})
			require.Len(t, res, 2, "StoreEachBatchRetry should return errors for 2 batches")
			require.Contains(t, res[batch1[0].UUID], ErrStoreNotSupported.Error(), "Each job should have store not supported error")
			require.Contains(t, res[batch2[0].UUID], ErrStoreNotSupported.Error(), "Each job should have store not supported error")
			return err
		})
		require.NoError(t, err, "StoreEachBatchRetryInTx should complete without error")

		err = pb.BufferPartitions(ctx, []string{"partition-1"})
		require.NoError(t, err, "it should be able to buffer partitions")
		err = pb.FlushBufferedPartitions(ctx, []string{"partition-1"})
		require.NoError(t, err, "FlushBufferedPartitions should be supported in reader-only mode")
	})

	t.Run("With invalid config", func(t *testing.T) {
		ctx := t.Context()
		_, err := NewJobsDBPartitionBuffer(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidJobsDBPartitionBufferConfig)
	})

	t.Run("Failing to load buffered partitions from db", func(t *testing.T) {
		pg, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		runNodeMigration(t, pg.DB)
		ctx := t.Context()
		primary := jobsdb.NewForReadWrite("rw",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
		)
		require.NoError(t, primary.Start(), "it should be able to start JobsDB")
		t.Cleanup(func() {
			primary.TearDown()
		})
		buffer := jobsdb.NewForReadWrite("rw_buf",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
		)
		require.NoError(t, buffer.Start(), "it should be able to start JobsDB Buffer")
		t.Cleanup(func() {
			buffer.TearDown()
		})
		pg.DB.Close() // close the DB to simulate failure in refreshing buffered partitions
		_, err = NewJobsDBPartitionBuffer(ctx, WithReadWriteJobsDBs(primary, buffer))
		require.Error(t, err)
	})
}
