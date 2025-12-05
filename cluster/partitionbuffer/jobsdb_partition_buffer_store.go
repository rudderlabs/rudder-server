package partitionbuffer

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	utilstx "github.com/rudderlabs/rudder-server/utils/tx"
)

const releaseRLockKey = "jobsDBPartitionBuffer.releaseRLock"

// Store stores the provided jobs into the appropriate JobsDBs based on their partition buffering status
// It returns ErrStoreNotSupported if the JobsDBPartitionBuffer does not support Store operations
func (b *jobsDBPartitionBuffer) Store(ctx context.Context, jobList []*jobsdb.JobT) error {
	return b.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
		return b.StoreInTx(ctx, tx, jobList)
	})
}

// WithStoreSafeTx acquires a read lock on buffered partitions early and starts a StoreSafeTx on the primary write JobsDB.
// It returns ErrStoreNotSupported if the JobsDBPartitionBuffer does not support Store operations
func (b *jobsDBPartitionBuffer) WithStoreSafeTx(ctx context.Context, fn func(tx jobsdb.StoreSafeTx) error) error {
	if !b.canStore {
		return ErrStoreNotSupported
	}
	releaseRLock := b.readLockBufferedPartitions()
	var txStarted bool // if we are not able to start the tx, we need to release the lock at the end
	defer func() {
		if !txStarted {
			releaseRLock()
		}
	}()
	return b.primaryWriteJobsDB.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
		txStarted = true
		tx.Tx().SetValue(releaseRLockKey, releaseRLock)
		tx.Tx().AddCompletionListener(releaseRLock)
		return fn(tx)
	})
}

// StoreInTx stores the provided jobs into the appropriate JobsDBs based on their partition buffering status within the provided StoreSafeTx
func (b *jobsDBPartitionBuffer) StoreInTx(ctx context.Context, tx jobsdb.StoreSafeTx, jobList []*jobsdb.JobT) error {
	if !b.canStore {
		return ErrStoreNotSupported
	}
	releaseRLock, ok := utilstx.GetTxValue[func()](tx.Tx(), releaseRLockKey)
	if !ok { // only possible if WithStoreSafeTx of another unbuffered JobsDB was used
		b.logger.Warnn("Release lock key was not found in the transaction context, creating one.",
			logger.NewStringField("prefix", b.Identifier()),
			logger.NewStringField("method", "StoreInTx"),
		)
		releaseRLock = b.readLockBufferedPartitions()
		tx.Tx().AddCompletionListener(releaseRLock)
	}
	if b.bufferedPartitions.Len() == 0 { // no buffered partitions
		return b.primaryWriteJobsDB.StoreInTx(ctx, tx, jobList)
	}
	// split jobs and store accordingly
	// make sure we have the latest version of buffered partitions if that is necessary
	if err := b.withLatestBufferedPartitions(ctx, releaseRLock, tx.Tx(), func() error {
		primaryJobs, bufferedJobs := b.splitJobs(jobList)
		if len(primaryJobs) > 0 {
			if err := b.primaryWriteJobsDB.StoreInTx(ctx, tx, primaryJobs); err != nil {
				return fmt.Errorf("storing in primary jobsdb in tx: %w", err)
			}
		}
		if len(bufferedJobs) > 0 {
			if err := b.bufferWriteJobsDB.WithStoreSafeTxFromTx(ctx, tx.Tx(), func(tx jobsdb.StoreSafeTx) error {
				return b.bufferWriteJobsDB.StoreInTx(ctx, tx, bufferedJobs)
			}); err != nil {
				return fmt.Errorf("storing in buffer jobsdb in tx: %w", err)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("storing through partition buffer in tx: %w", err)
	}
	return nil
}

// withLatestBufferedPartitions refreshes the buffered partitions from the database if needed and then calls the provided function with the latest snapshot.
// If partitions need to be refreshed, the read lock is released and a write lock is acquired in its place to update the buffered partitions.
func (b *jobsDBPartitionBuffer) withLatestBufferedPartitions(ctx context.Context, releaseRLock func(), tx *utilstx.Tx, f func() error) error {
	releaseLock := releaseRLock
	tx.AddCompletionListener(func() {
		releaseLock()
	})
	if !b.differentReaderWriterDBs { // no need to refresh move on with current snapshot
		return f()
	}
	diff, err := b.versionDiff(ctx, tx) // get the version difference
	if err != nil {
		return err
	}
	if diff == 0 { // no difference  move on with current snapshot
		return f()
	}
	releaseRLock() // release the read lock before acquiring the write lock
	b.bufferedPartitionsMu.Lock()
	releaseLock = b.bufferedPartitionsMu.Unlock // update the release function to release the write lock
	// check again after acquiring the write lock
	diff, err = b.versionDiff(ctx, tx)
	if err != nil {
		return err
	}
	if diff == 0 { // no difference  move on with current snapshot
		return f()
	}
	bufferedPartitions, err := b.getBufferedPartitionsInTx(ctx, tx) // get the latest buffered partitions from the database
	if err != nil {
		return err
	}
	b.bufferedPartitionsVersion += diff
	b.bufferedPartitions = bufferedPartitions
	return f()
}

// splitJobs is a convenience wrapper around spitJobBatches for a single batch of jobs
func (b *jobsDBPartitionBuffer) splitJobs(jobList []*jobsdb.JobT) (primary, buffered []*jobsdb.JobT) {
	for _, job := range jobList {
		if job.PartitionID == "" {
			job.PartitionID = jobsdb.DefaultParititionFunction(job, b.numPartitions)
		}
		if b.bufferedPartitions.Has(job.PartitionID) {
			buffered = append(buffered, job)
		} else {
			primary = append(primary, job)
		}
	}
	return primary, buffered
}

// versionDiff returns the difference between the version of buffered partitions in the database and the current version in memory
func (b *jobsDBPartitionBuffer) versionDiff(ctx context.Context, tx *utilstx.Tx) (int, error) {
	dbVersion, err := b.getBufferedPartitionsVersionInTx(ctx, tx)
	if err != nil {
		return 0, err
	}
	return dbVersion - b.bufferedPartitionsVersion, nil
}

// readLockBufferedPartitions acquires a read lock on bufferedPartitions and returns a function to release it.
// The returned function is safe to call multiple times.
func (b *jobsDBPartitionBuffer) readLockBufferedPartitions() func() {
	b.bufferedPartitionsMu.RLock()
	return sync.OnceFunc(func() {
		b.bufferedPartitionsMu.RUnlock()
	})
}

// failAllBatches returns a map of job UUIDs to the provided error message for the first job of each batch
func failAllBatches(jobBatches [][]*jobsdb.JobT, err error) map[uuid.UUID]string {
	errorMessagesMap := make(map[uuid.UUID]string, len(jobBatches))
	for i := range jobBatches {
		errorMessagesMap[jobBatches[i][0].UUID] = err.Error()
	}
	return errorMessagesMap
}
