package partitionbuffer

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

// startBufferWatchdog starts a watchdog that periodically checks for unbuffered partitions that have buffered jobs in the buffer JobsDB and flushes them.
func (b *jobsDBPartitionBuffer) startBufferWatchdog() {
	if !b.canFlush {
		return
	}
	ctx := b.lifecycleCtx
	b.lifecycleWG.Go(func() {
		getBufferedPartitionsSnapshot := func() ([]string, error) {
			if !b.bufferedPartitionsMu.RTryLockWithContext(ctx) {
				return nil, fmt.Errorf("acquiring a buffered partitions read lock: %w", ctx.Err())
			}
			defer b.bufferedPartitionsMu.RUnlock()
			return slices.Collect(b.bufferedPartitions.Keys()), nil
		}

		// run performs a single iteration of the watchdog logic, returning true if a partition was flushed:
		// it gets the first unprocessed job from the buffer JobsDB, checks if its partition is buffered,
		// and if not, flushes the partition.
		// It returns true if a partition was flushed, false otherwise.
		run := func(ctx context.Context) (bool, error) {
			bufferedPartitions, err := getBufferedPartitionsSnapshot()
			if err != nil {
				return false, fmt.Errorf("getting buffered partitions snapshot: %w", err)
			}
			var firstJob *jobsdb.JobT
			// try to find a job in buffered JobsDB
			for dsLimitsReached := true; dsLimitsReached; {
				r, err := b.bufferReadJobsDB.GetUnprocessed(ctx, jobsdb.GetQueryParams{JobsLimit: 1})
				if err != nil {
					return false, fmt.Errorf("checking for unprocessed jobs in buffer JobsDB: %w", err)
				}
				if len(r.Jobs) > 0 {
					firstJob = r.Jobs[0]
					break
				}
				dsLimitsReached = r.DSLimitsReached
			}
			if firstJob == nil { // no jobs found
				return false, nil
			}
			partitionID := firstJob.PartitionID
			if slices.Contains(bufferedPartitions, partitionID) {
				// partition is buffered, skip flushing
				return false, nil
			}
			// make sure this partition is still not marked as buffered (race condition check)
			bufferedPartitions, err = getBufferedPartitionsSnapshot()
			if err != nil {
				return false, fmt.Errorf("getting buffered partitions snapshot: %w", err)
			}
			if slices.Contains(bufferedPartitions, partitionID) {
				// partition is buffered, skip flushing
				return false, nil
			}
			b.logger.Warnn("Flushing buffered jobs for unbuffered partition",
				logger.NewStringField("partitionId", partitionID),
				logger.NewStringField("prefix", b.bufferReadJobsDB.Identifier()),
			)
			if err := b.doFlushBufferedPartitions(ctx, []string{partitionID}, false); err != nil {
				return false, fmt.Errorf("flushing buffered partition %q: %w", partitionID, err)
			}
			return true, nil
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(b.watchdogInterval.Load()):
				// keep flushing partitions as long as run keeps finding unbuffered partitions with buffered jobs
				for flushedPartition := true; flushedPartition; {
					var err error
					flushedPartition, err = run(ctx)
					if err != nil && ctx.Err() == nil {
						b.logger.Errorn("Buffer watchdog encountered an error",
							logger.NewStringField("prefix", b.bufferReadJobsDB.Identifier()),
							obskit.Error(err),
						)
					}
				}
			}
		}
	})
}
