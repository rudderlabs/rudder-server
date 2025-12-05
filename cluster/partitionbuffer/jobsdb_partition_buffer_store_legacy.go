package partitionbuffer

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	utilstx "github.com/rudderlabs/rudder-server/utils/tx"
)

// StoreEachBatchRetry stores the provided job batches into the appropriate JobsDBs based on their partition buffering status
// It fails all batches with ErrStoreNotSupported if the JobsDBPartitionBuffer does not support Store operations
func (b *jobsDBPartitionBuffer) StoreEachBatchRetry(ctx context.Context, jobBatches [][]*jobsdb.JobT) map[uuid.UUID]string {
	var m map[uuid.UUID]string
	var err error
	err = b.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
		m, err = b.StoreEachBatchRetryInTx(ctx, tx, jobBatches)
		return err
	})
	if err != nil {
		return failAllBatches(jobBatches, fmt.Errorf("storing each batch through partition buffer: %w", err))
	}
	return m
}

// StoreEachBatchRetryInTx stores the provided job batches into the appropriate JobsDBs based on their partition buffering status within the provided StoreSafeTx
// It fails all batches with ErrStoreNotSupported if the JobsDBPartitionBuffer does not support Store operations
func (b *jobsDBPartitionBuffer) StoreEachBatchRetryInTx(ctx context.Context, tx jobsdb.StoreSafeTx, jobBatches [][]*jobsdb.JobT) (map[uuid.UUID]string, error) {
	if !b.canStore {
		return failAllBatches(jobBatches, ErrStoreNotSupported), nil
	}
	releaseRLock, ok := utilstx.GetTxValue[func()](tx.Tx(), releaseRLockKey)
	if !ok {
		b.logger.Warnn("Release lock key was not found in the transaction context, creating one.",
			logger.NewStringField("prefix", b.Identifier()),
			logger.NewStringField("method", "StoreEachBatchRetryInTx"),
		)
		releaseRLock = b.readLockBufferedPartitions()
		tx.Tx().AddCompletionListener(releaseRLock)
	}
	if b.bufferedPartitions.Len() == 0 { // no buffered partitions
		return b.primaryWriteJobsDB.StoreEachBatchRetryInTx(ctx, tx, jobBatches)
	}
	m := make(map[uuid.UUID]string)
	if err := b.withLatestBufferedPartitions(ctx, releaseRLock, tx.Tx(), func() error {
		// we can only include the first job's UUID from each batch in the result map in case of a failure
		for _, batch := range jobBatches {
			primaryJobs, bufferedJobs := b.splitJobs(batch)
			if len(primaryJobs) > 0 {
				var err error
				bm, err := b.primaryWriteJobsDB.StoreEachBatchRetryInTx(ctx, tx, [][]*jobsdb.JobT{primaryJobs})
				if err != nil {
					return fmt.Errorf("storing each batch in primary jobsdb in tx: %w", err)
				}
				if len(bm) > 0 { // store failed, skip buffered jobs store
					m[batch[0].UUID] = slices.Collect(maps.Values(bm))[0]
					continue
				}
			}
			if len(bufferedJobs) > 0 {
				if err := b.bufferWriteJobsDB.WithStoreSafeTxFromTx(ctx, tx.Tx(), func(tx jobsdb.StoreSafeTx) error {
					bm, err := b.bufferWriteJobsDB.StoreEachBatchRetryInTx(ctx, tx, [][]*jobsdb.JobT{bufferedJobs})
					if err != nil {
						return err
					}
					if len(bm) > 0 { // store failed, skip buffered jobs store
						m[batch[0].UUID] = slices.Collect(maps.Values(bm))[0]
					}
					return nil
				}); err != nil {
					return fmt.Errorf("storing each batch in buffer jobsdb in tx: %w", err)
				}
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("storing each batch through partition buffer in tx: %w", err)
	}
	return m, nil
}
