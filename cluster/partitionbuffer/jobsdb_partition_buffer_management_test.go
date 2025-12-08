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
	"github.com/rudderlabs/rudder-server/utils/tx"
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

		t.Run("with same partitions", func(t *testing.T) {
			err := pb.BufferPartitions(t.Context(), []string{"partition-1", "partition-2"})
			require.NoError(t, err, "it should be able to buffer partitions")
			sameVersion := jdbpb.bufferedPartitionsVersion
			require.Equal(t, newVersion, sameVersion, "buffered partitions version should remain the same when buffering same partitions")
		})
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

	t.Run("removeBufferPartitions", func(t *testing.T) {
		pb, db := setup(t)
		jdbpb, ok := pb.(*jobsDBPartitionBuffer)
		require.True(t, ok, "it should be able to cast to jobsDBPartitionBuffer")

		// First, buffer some partitions
		err := pb.BufferPartitions(t.Context(), []string{"partition-1", "partition-2", "partition-3"})
		require.NoError(t, err, "it should be able to buffer partitions")

		// Verify partitions were buffered
		count := 0
		err = db.QueryRowContext(t.Context(), "select count(*) from rt_buffered_partitions").Scan(&count)
		require.NoError(t, err, "it should be able to query buffered partitions")
		require.Equal(t, 3, count, "should have 3 buffered partitions")

		t.Run("successfully removes single partition", func(t *testing.T) {
			currentVersion := jdbpb.bufferedPartitionsVersion

			err := jdbpb.WithTx(func(tx *tx.Tx) error {
				jdbpb.bufferedPartitionsMu.Lock()
				defer jdbpb.bufferedPartitionsMu.Unlock()
				return jdbpb.removeBufferPartitions(t.Context(), tx, []string{"partition-1"})
			})
			require.NoError(t, err, "it should be able to remove buffered partition")

			// Verify version increased
			newVersion := jdbpb.bufferedPartitionsVersion
			require.Greater(t, newVersion, currentVersion, "buffered partitions version should have increased")

			// Verify in-memory state
			bufferedPartitions := jdbpb.bufferedPartitions
			_, exists1 := bufferedPartitions.Get("partition-1")
			_, exists2 := bufferedPartitions.Get("partition-2")
			_, exists3 := bufferedPartitions.Get("partition-3")
			require.False(t, exists1, "partition-1 should be removed from buffered partitions")
			require.True(t, exists2, "partition-2 should still be in buffered partitions")
			require.True(t, exists3, "partition-3 should still be in buffered partitions")

			// Verify database state
			count := 0
			err = db.QueryRowContext(t.Context(), "select count(*) from rt_buffered_partitions").Scan(&count)
			require.NoError(t, err, "it should be able to query buffered partitions")
			require.Equal(t, 2, count, "should have 2 buffered partitions remaining")
		})

		t.Run("successfully removes multiple partitions", func(t *testing.T) {
			currentVersion := jdbpb.bufferedPartitionsVersion

			err := jdbpb.WithTx(func(tx *tx.Tx) error {
				jdbpb.bufferedPartitionsMu.Lock()
				defer jdbpb.bufferedPartitionsMu.Unlock()
				return jdbpb.removeBufferPartitions(t.Context(), tx, []string{"partition-2", "partition-3"})
			})
			require.NoError(t, err, "it should be able to remove multiple buffered partitions")

			// Verify version increased
			newVersion := jdbpb.bufferedPartitionsVersion
			require.Greater(t, newVersion, currentVersion, "buffered partitions version should have increased")

			// Verify in-memory state
			bufferedPartitions := jdbpb.bufferedPartitions
			_, exists2 := bufferedPartitions.Get("partition-2")
			_, exists3 := bufferedPartitions.Get("partition-3")
			require.False(t, exists2, "partition-2 should be removed from buffered partitions")
			require.False(t, exists3, "partition-3 should be removed from buffered partitions")

			// Verify database state
			count := 0
			err = db.QueryRowContext(t.Context(), "select count(*) from rt_buffered_partitions").Scan(&count)
			require.NoError(t, err, "it should be able to query buffered partitions")
			require.Equal(t, 0, count, "should have 0 buffered partitions remaining")
		})

		t.Run("handles duplicate partition ids", func(t *testing.T) {
			// Add partitions back
			err := pb.BufferPartitions(t.Context(), []string{"partition-4", "partition-5"})
			require.NoError(t, err, "it should be able to buffer partitions")

			err = jdbpb.WithTx(func(tx *tx.Tx) error {
				jdbpb.bufferedPartitionsMu.Lock()
				defer jdbpb.bufferedPartitionsMu.Unlock()
				return jdbpb.removeBufferPartitions(t.Context(), tx, []string{"partition-4", "partition-4", "partition-5", "partition-5"})
			})
			require.NoError(t, err, "it should handle duplicate partition ids")

			// Verify all partitions were removed despite duplicates
			bufferedPartitions := jdbpb.bufferedPartitions
			_, exists4 := bufferedPartitions.Get("partition-4")
			_, exists5 := bufferedPartitions.Get("partition-5")
			require.False(t, exists4, "partition-4 should be removed")
			require.False(t, exists5, "partition-5 should be removed")
		})

		t.Run("handles non-existent partitions gracefully", func(t *testing.T) {
			// Add a partition
			err := pb.BufferPartitions(t.Context(), []string{"partition-6"})
			require.NoError(t, err, "it should be able to buffer partition")

			currentVersion := jdbpb.bufferedPartitionsVersion

			// Try to remove a mix of existing and non-existent partitions
			err = jdbpb.WithTx(func(tx *tx.Tx) error {
				jdbpb.bufferedPartitionsMu.Lock()
				defer jdbpb.bufferedPartitionsMu.Unlock()
				return jdbpb.removeBufferPartitions(t.Context(), tx, []string{"partition-6", "partition-non-existent"})
			})
			require.NoError(t, err, "it should handle non-existent partitions gracefully")

			// Verify version increased
			newVersion := jdbpb.bufferedPartitionsVersion
			require.Greater(t, newVersion, currentVersion, "buffered partitions version should have increased")

			// Verify existing partition was removed
			bufferedPartitions := jdbpb.bufferedPartitions
			_, exists6 := bufferedPartitions.Get("partition-6")
			require.False(t, exists6, "partition-6 should be removed")
		})

		t.Run("with context cancellation", func(t *testing.T) {
			// Add partitions
			err := pb.BufferPartitions(t.Context(), []string{"partition-7", "partition-8"})
			require.NoError(t, err, "it should be able to buffer partitions")

			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			err = jdbpb.WithTx(func(tx *tx.Tx) error {
				jdbpb.bufferedPartitionsMu.Lock()
				defer jdbpb.bufferedPartitionsMu.Unlock()
				return jdbpb.removeBufferPartitions(ctx, tx, []string{"partition-7"})
			})
			require.Error(t, err, "it should return error when context is cancelled")

			// Verify partitions were not removed
			bufferedPartitions := jdbpb.bufferedPartitions
			_, exists7 := bufferedPartitions.Get("partition-7")
			_, exists8 := bufferedPartitions.Get("partition-8")
			require.True(t, exists7, "partition-7 should still be buffered after failed removal")
			require.True(t, exists8, "partition-8 should still be buffered after failed removal")
		})

		t.Run("transaction rollback does not update in-memory state", func(t *testing.T) {
			// Add partition
			err := pb.BufferPartitions(t.Context(), []string{"partition-9"})
			require.NoError(t, err, "it should be able to buffer partition")

			currentVersion := jdbpb.bufferedPartitionsVersion

			// Intentionally cause transaction to rollback
			err = jdbpb.WithTx(func(tx *tx.Tx) error {
				jdbpb.bufferedPartitionsMu.Lock()
				defer jdbpb.bufferedPartitionsMu.Unlock()
				if err := jdbpb.removeBufferPartitions(t.Context(), tx, []string{"partition-9"}); err != nil {
					return err
				}
				// Force rollback by returning an error
				return context.Canceled
			})
			require.Error(t, err, "transaction should fail")

			// Verify version did not change (rollback)
			unchangedVersion := jdbpb.bufferedPartitionsVersion
			require.Equal(t, currentVersion, unchangedVersion, "version should not have changed on rollback")

			// Verify in-memory state was not updated (because transaction rolled back)
			bufferedPartitions := jdbpb.bufferedPartitions
			_, exists9 := bufferedPartitions.Get("partition-9")
			require.True(t, exists9, "partition-9 should still be buffered after rollback")

			// Verify database state was not updated
			var dbCount int
			err = db.QueryRowContext(t.Context(), "select count(*) from rt_buffered_partitions where partition_id = 'partition-9'").Scan(&dbCount)
			require.NoError(t, err, "it should be able to query buffered partitions")
			require.Equal(t, 1, dbCount, "partition-9 should still exist in database after rollback")
		})

		t.Run("empty partition list", func(t *testing.T) {
			currentVersion := jdbpb.bufferedPartitionsVersion

			// Try to remove empty partition list
			err := jdbpb.WithTx(func(tx *tx.Tx) error {
				jdbpb.bufferedPartitionsMu.Lock()
				defer jdbpb.bufferedPartitionsMu.Unlock()
				return jdbpb.removeBufferPartitions(t.Context(), tx, []string{})
			})
			require.NoError(t, err, "it should handle empty partition list gracefully")

			// Version should not change when no partitions are removed
			newVersion := jdbpb.bufferedPartitionsVersion
			require.Equal(t, currentVersion, newVersion, "buffered partitions version should not have changed")
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
