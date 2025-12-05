package partitionbuffer

import (
	"context"

	"github.com/google/uuid"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

// Store stores the provided list of jobs into the appropriate JobsDBs based on buffered partitions
func (b *jobsDBPartitionBuffer) Store(ctx context.Context, jobList []*jobsdb.JobT) error {
	if !b.canStore {
		return ErrStoreNotSupported
	}
	// TODO: implement Store logic
	return nil
}

// StoreInTx stores the provided list of jobs into the appropriate JobsDBs within the provided transaction based on buffered partitions
func (b *jobsDBPartitionBuffer) StoreInTx(ctx context.Context, tx jobsdb.StoreSafeTx, jobList []*jobsdb.JobT) error {
	if !b.canStore {
		return ErrStoreNotSupported
	}
	// TODO: implement StoreInTx logic
	return nil
}

// StoreEachBatchRetry stores each batch of jobs into the appropriate JobsDBs based on buffered partitions, while keeping track of failed jobs
func (b *jobsDBPartitionBuffer) StoreEachBatchRetry(ctx context.Context, jobBatches [][]*jobsdb.JobT) map[uuid.UUID]string {
	if !b.canStore {
		res := make(map[uuid.UUID]string)
		for _, batch := range jobBatches {
			for _, job := range batch {
				res[job.UUID] = ErrStoreNotSupported.Error()
			}
		}
		return res
	}
	// TODO: implement StoreEachBatchRetry logic
	return nil
}

// StoreEachBatchRetryInTx stores each batch of jobs into the appropriate JobsDBs within the provided transaction based on buffered partitions, while keeping track of failed jobs
func (b *jobsDBPartitionBuffer) StoreEachBatchRetryInTx(ctx context.Context, tx jobsdb.StoreSafeTx, jobBatches [][]*jobsdb.JobT) (map[uuid.UUID]string, error) {
	if !b.canStore {
		res := make(map[uuid.UUID]string)
		for _, batch := range jobBatches {
			for _, job := range batch {
				res[job.UUID] = ErrStoreNotSupported.Error()
			}
		}
		return res, nil
	}
	// TODO: implement StoreEachBatchRetryInTx logic
	return nil, nil // nolint: nilnil
}
