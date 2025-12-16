package partitionbuffer

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-server/jobsdb"
	utilstx "github.com/rudderlabs/rudder-server/utils/tx"
)

// sentinel error to indicate stale buffered partitions that need refreshing
var errStaleBufferedPartitions = errors.New("stale buffered partitions")

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
	for {
		if !b.bufferedPartitionsMu.RTryLockWithContext(ctx) {
			return fmt.Errorf("acquiring a buffered partitions read lock: %w", ctx.Err())
		}
		err := b.primaryWriteJobsDB.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) (err error) {
			if !b.differentReaderWriterDBs { // no need to check for stale version
				return fn(tx)
			}
			diff, err := b.versionDiff(ctx, tx.Tx()) // get the version difference
			if err != nil {
				return err
			}
			if diff != 0 { // stale version
				return errStaleBufferedPartitions
			}
			return fn(tx)
		})
		b.bufferedPartitionsMu.RUnlock()
		if !errors.Is(err, errStaleBufferedPartitions) {
			return err
		}
		// refresh buffered partitions and retry
		if err := b.RefreshBufferedPartitions(ctx); err != nil {
			return fmt.Errorf("refreshing buffered partitions after stale error: %w", err)
		}
	}
}

// StoreInTx stores the provided jobs into the appropriate JobsDBs based on their partition buffering status within the provided StoreSafeTx
func (b *jobsDBPartitionBuffer) StoreInTx(ctx context.Context, tx jobsdb.StoreSafeTx, jobList []*jobsdb.JobT) error {
	if !b.canStore {
		return ErrStoreNotSupported
	}
	if b.bufferedPartitions.Len() == 0 { // no buffered partitions
		return b.primaryWriteJobsDB.StoreInTx(ctx, tx, jobList)
	}
	// split jobs and store accordingly
	// make sure we have the latest version of buffered partitions if that is necessary
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

// failAllBatches returns a map of job UUIDs to the provided error message for the first job of each batch
func failAllBatches(jobBatches [][]*jobsdb.JobT, err error) map[uuid.UUID]string {
	errorMessagesMap := make(map[uuid.UUID]string, len(jobBatches))
	for i := range jobBatches {
		errorMessagesMap[jobBatches[i][0].UUID] = err.Error()
	}
	return errorMessagesMap
}
