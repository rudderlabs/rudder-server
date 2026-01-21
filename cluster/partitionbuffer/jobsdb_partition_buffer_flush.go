package partitionbuffer

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/tx"
)

// FlushBufferedPartitions flushes the buffered data for the provided partition ids to the database and unmarks them as buffered.
func (b *jobsDBPartitionBuffer) FlushBufferedPartitions(ctx context.Context, partitions []string) error {
	{ // block for validation and marking partitions as flushing
		if !b.canFlush {
			return ErrFlushNotSupported
		}
		b.flushingPartitionsMu.Lock()
		if alreadyFlushing := lo.Intersect(lo.Keys(b.flushingPartitions), partitions); len(alreadyFlushing) > 0 {
			b.flushingPartitionsMu.Unlock()
			return fmt.Errorf("partitions are already being flushed: %+v", partitions)
		}
		if !b.bufferedPartitionsMu.RTryLockWithContext(ctx) {
			b.flushingPartitionsMu.Unlock()
			return fmt.Errorf("acquiring a buffered partitions read lock during flush: %w", ctx.Err())
		}

		// only keep partitions that are actually buffered
		partitions = lo.Intersect(slices.Collect(b.bufferedPartitions.Keys()), partitions)
		b.bufferedPartitionsMu.RUnlock()

		if len(partitions) == 0 {
			b.flushingPartitionsMu.Unlock()
			return nil
		}

		for _, partitionID := range partitions {
			b.flushingPartitions[partitionID] = struct{}{}
		}
		// ensure we unmark partitions as flushing at the end regardless of success or failure
		defer func() {
			b.flushingPartitionsMu.Lock()
			for _, partitionID := range partitions {
				delete(b.flushingPartitions, partitionID)
			}
			b.flushingPartitionsMu.Unlock()
		}()
		b.flushingPartitionsMu.Unlock()
	}

	defer b.stats.NewTaggedStat("jobsdb_pbuffer_flush_time", stats.TimerType, stats.Tags{
		"prefix": b.Identifier(),
	}).RecordDuration()()

	start := time.Now()
	moveTimeout := time.After(b.flushMoveTimeout.Load())
	var totalCount int

	// move in batches until we stop reaching limits
	b.logger.Infon("Flushing jobs from buffer to primary jobsdb (move phase)",
		logger.NewStringField("partitions", strings.Join(partitions, ",")),
		logger.NewStringField("prefix", b.Identifier()),
	)
	for limitsReached := true; limitsReached; {
		var err error
		select {
		case <-moveTimeout:
			// timeout reached, break out to switchover
			b.logger.Warnn("Flush move timeout reached, proceeding to switchover",
				logger.NewStringField("partitions", fmt.Sprintf("%v", partitions)),
				logger.NewDurationField("duration", time.Since(start)),
			)
			limitsReached = false
		default:
			var movedCount int
			movedCount, limitsReached, err = b.moveBufferedPartitions(ctx, partitions, b.flushBatchSize.Load(), b.flushPayloadSize.Load())
			if err != nil {
				return fmt.Errorf("moving buffered partitions: %w", err)
			}
			totalCount += movedCount
		}
	}
	// switchover
	b.logger.Infon("Flushing jobs from buffer to primary jobsdb (switchover phase)",
		logger.NewStringField("partitions", strings.Join(partitions, ",")),
		logger.NewStringField("prefix", b.Identifier()),
	)
	switchoverCount, err := b.switchoverBufferedPartitions(ctx, partitions, b.flushBatchSize.Load(), b.flushPayloadSize.Load())
	if err != nil {
		return fmt.Errorf("switchover of buffered partitions: %w", err)
	}
	totalCount += switchoverCount
	b.logger.Infon("Flushing of buffered partitions completed successfully",
		logger.NewStringField("partitions", strings.Join(partitions, ",")),
		logger.NewStringField("prefix", b.Identifier()),
		logger.NewDurationField("duration", time.Since(start)),
		logger.NewIntField("totalCount", int64(totalCount)),
	)
	return nil
}

// moveBufferedPartitions moves a batch of buffered jobs to the primary JobsDB for the given partition IDs. It returns whether any limits were reached during the fetch.
// If limits were reached, the caller should call this method again to move more data.
func (b *jobsDBPartitionBuffer) moveBufferedPartitions(ctx context.Context, partitionIDs []string, batchSize int, payloadSize int64) (count int, limitsReached bool, err error) {
	defer b.stats.NewTaggedStat("jobsdb_pbuffer_move_time", stats.TimerType, stats.Tags{
		"prefix": b.Identifier(),
	}).RecordDuration()()

	bufferedJobs, err := b.bufferReadJobsDB.GetUnprocessed(ctx, jobsdb.GetQueryParams{
		PartitionFilters: partitionIDs,
		JobsLimit:        batchSize,
		PayloadSizeLimit: payloadSize,
	})
	if err != nil {
		return 0, false, err
	}
	if len(bufferedJobs.Jobs) > 0 {
		now := time.Now()
		statusList := lo.Map(bufferedJobs.Jobs, func(job *jobsdb.JobT, _ int) *jobsdb.JobStatusT {
			return &jobsdb.JobStatusT{
				JobID:         job.JobID,
				JobState:      jobsdb.Succeeded.State,
				AttemptNum:    1,
				ExecTime:      now,
				RetryTime:     now,
				ErrorCode:     "200",
				ErrorResponse: []byte("{}"),
				Parameters:    []byte("{}"),
				JobParameters: job.Parameters,
				WorkspaceId:   job.WorkspaceId,
				PartitionID:   job.PartitionID,
				CustomVal:     job.CustomVal,
			}
		})
		if err := b.primaryWriteJobsDB.WithStoreSafeTx(ctx, func(tx jobsdb.StoreSafeTx) error {
			if err := b.primaryWriteJobsDB.StoreInTx(ctx, tx, bufferedJobs.Jobs); err != nil {
				return fmt.Errorf("moving buffered jobs to primary jobsdb: %w", err)
			}
			// create job statuses
			if err := b.bufferReadJobsDB.WithUpdateSafeTxFromTx(ctx, tx.Tx(), func(tx jobsdb.UpdateSafeTx) error {
				return b.bufferReadJobsDB.UpdateJobStatusInTx(ctx, tx, statusList)
			}); err != nil {
				return fmt.Errorf("updating job statuses for moved jobs: %w", err)
			}
			return nil
		}); err != nil {
			return 0, false, err
		}
		b.stats.NewTaggedStat("jobsdb_pbuffer_move_jobs_count", stats.CountType, stats.Tags{
			"prefix": b.Identifier(),
		}).Count(len(bufferedJobs.Jobs))
	}
	return len(bufferedJobs.Jobs), bufferedJobs.DSLimitsReached || bufferedJobs.LimitsReached, nil
}

func (b *jobsDBPartitionBuffer) switchoverBufferedPartitions(ctx context.Context, partitionIDs []string, batchSize int, payloadSize int64) (count int, err error) {
	defer b.stats.NewTaggedStat("jobsdb_pbuffer_switchover_time", stats.TimerType, stats.Tags{
		"prefix": b.Identifier(),
	}).RecordDuration()()

	if !b.bufferedPartitionsMu.TryLockWithContext(ctx) {
		return 0, fmt.Errorf("acquiring a buffered partitions write lock during switchover: %w", ctx.Err())
	}
	b.logger.Infon("Buffered partitions write lock acquired (switchover phase)",
		logger.NewStringField("partitions", strings.Join(partitionIDs, ",")),
		logger.NewStringField("prefix", b.Identifier()),
	)
	defer func() {
		b.bufferedPartitionsMu.Unlock()
		b.logger.Infon("Buffered partitions write lock released (switchover phase)",
			logger.NewStringField("partitions", strings.Join(partitionIDs, ",")),
			logger.NewStringField("prefix", b.Identifier()),
		)
	}()
	totalMoved := 0
	err = b.WithTx(func(tx *tx.Tx) error {
		// disable idle_in_transaction_session_timeout for the duration of this transaction, since it may take long to move all remaining data
		if _, err := tx.ExecContext(ctx, "SET LOCAL idle_in_transaction_session_timeout = '0ms'"); err != nil {
			return fmt.Errorf("disabling idle_in_transaction_session_timeout during switchover: %w", err)
		}
		if b.differentBufferDBs {
			// mark partitions as unbuffered in the database early, for holding the global lock
			if err := b.removeBufferPartitions(ctx, tx, partitionIDs); err != nil {
				return fmt.Errorf("removing buffered partitions during switchover: %w", err)
			}
			// refresh DS list in the buffer read JobsDB so that we are confident that we are going to be moving all remaining data from the buffer
			if err := b.bufferReadJobsDB.RefreshDSList(ctx); err != nil {
				return fmt.Errorf("refreshing buffer DS list during switchover: %w", err)
			}
		}
		// move any remaining buffered data
		for limitsReached := true; limitsReached; {
			var movedCount int
			movedCount, limitsReached, err = b.moveBufferedPartitions(ctx, partitionIDs, batchSize, payloadSize)
			if err != nil {
				return fmt.Errorf("moving buffered partitions during switchover: %w", err)
			}
			totalMoved += movedCount
		}
		if !b.differentBufferDBs {
			// mark partitions as unbuffered in the database late
			if err := b.removeBufferPartitions(ctx, tx, partitionIDs); err != nil {
				return fmt.Errorf("removing buffered partitions during switchover: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return totalMoved, nil
}
