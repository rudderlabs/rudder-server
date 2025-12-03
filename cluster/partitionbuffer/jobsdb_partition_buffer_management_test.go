package partitionbuffer

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
	"github.com/rudderlabs/rudder-server/jobsdb"
	migrator "github.com/rudderlabs/rudder-server/services/sql-migrator"
)

func TestJobsDBPartitionBufferManagement(t *testing.T) {
	setup := func(t *testing.T) (pb JobsDBPartitionBuffer, sqlDB *sql.DB) {
		pool, err := dockertest.NewPool("")
		require.NoError(t, err)
		pg, err := postgres.Setup(pool, t)
		require.NoError(t, err)
		runNodeMigration(t, pg.DB)
		rtDB := jobsdb.NewForReadWrite("rt",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
		)
		require.NoError(t, rtDB.Start(), "it should be able to start JobsDB")
		t.Cleanup(func() {
			rtDB.TearDown()
		})
		rtBuffer := jobsdb.NewForReadWrite("rt_buf",
			jobsdb.WithDBHandle(pg.DB),
			jobsdb.WithNumPartitions(64),
			jobsdb.WithSkipMaintenanceErr(true),
		)
		require.NoError(t, rtBuffer.Start(), "it should be able to start JobsDB Buffer")
		t.Cleanup(func() {
			rtBuffer.TearDown()
		})
		pb, err = NewJobsDBPartitionBuffer(t.Context(), WithReadWriteJobsDBs(rtDB, rtBuffer))
		require.NoError(t, err)
		return pb, pg.DB
	}

	t.Run("BufferPartitions", func(t *testing.T) {
		pb, db := setup(t)
		jdbpb, ok := pb.(*jobsDBPartitionBuffer)
		require.True(t, ok, "it should be able to cast to jobsDBPartitionBuffer")
		currentVersion := jdbpb.bufferedPartitionsVersion

		err := pb.BufferPartitions(t.Context(), []string{"partition-1", "partition-2"})
		require.NoError(t, err, "it should be able to buffer partitions")

		newVersion := jdbpb.bufferedPartitionsVersion
		require.Greater(t, newVersion, currentVersion, "buffered partitions version should have increased")

		bufferedPartitions := jdbpb.bufferedPartitions
		_, exists1 := bufferedPartitions.Get("partition-1")
		_, exists2 := bufferedPartitions.Get("partition-2")
		require.True(t, exists1, "it should have partition-1 in buffered partitions")
		require.True(t, exists2, "it should have partition-2 in buffered partitions")
		count := 0
		err = db.QueryRowContext(t.Context(), "select count(*) from rt_buffered_partitions").Scan(&count)
		require.NoError(t, err, "it should be able to query buffered partitions")

		t.Run("with context cancellation", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			err := pb.BufferPartitions(ctx, []string{"partition-3", "partition-4"})
			require.Error(t, err, "it should return error when context is cancelled")
			require.ErrorIs(t, err, context.Canceled)
		})

		t.Run("with database error", func(t *testing.T) {
			db.Close()
			err := pb.BufferPartitions(t.Context(), []string{"partition-3", "partition-4"})
			require.Error(t, err, "it should return error when database is closed")
		})
	})

	t.Run("RefreshBufferedPartitions", func(t *testing.T) {
		pb, db := setup(t)
		jdbpb, ok := pb.(*jobsDBPartitionBuffer)
		require.True(t, ok, "it should be able to cast to jobsDBPartitionBuffer")
		currentVersion := jdbpb.bufferedPartitionsVersion

		// Another process inserts buffered partitions directly into the DB
		_, err := db.ExecContext(t.Context(), "insert into rt_buffered_partitions (partition_id) values ('partition-3'), ('partition-4')")
		require.NoError(t, err, "it should be able to insert buffered partitions directly")

		err = pb.RefreshBufferedPartitions(t.Context())
		require.NoError(t, err, "it should be able to refresh buffered partitions")
		newVersion := jdbpb.bufferedPartitionsVersion
		require.Greater(t, newVersion, currentVersion, "buffered partitions version should have increased")

		bufferedPartitions := jdbpb.bufferedPartitions
		_, exists3 := bufferedPartitions.Get("partition-3")
		_, exists4 := bufferedPartitions.Get("partition-4")
		require.True(t, exists3, "it should have partition-3 in buffered partitions")
		require.True(t, exists4, "it should have partition-4 in buffered partitions")

		t.Run("with context cancellation", func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			err := pb.RefreshBufferedPartitions(ctx)
			require.Error(t, err, "it should return error when context is cancelled")
			require.ErrorIs(t, err, context.Canceled)
		})

		t.Run("with database error", func(t *testing.T) {
			db.Close()
			err := pb.RefreshBufferedPartitions(t.Context())
			require.Error(t, err, "it should return error when database is closed")
		})
	})
}

func runNodeMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	m := &migrator.Migrator{
		Handle:          db,
		MigrationsTable: "node_migrations",
	}
	require.NoError(t, m.Migrate("node"))
}
