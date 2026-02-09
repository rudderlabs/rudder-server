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
			isPartitionBuffered := func(partitionID string) (bool, error) {
				bufferedPartitions, err := getBufferedPartitionsSnapshot()
				if err != nil {
					return false, fmt.Errorf("getting buffered partitions snapshot: %w", err)
				}
				return slices.Contains(bufferedPartitions, partitionID), nil
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
			partitionBuffered, err := isPartitionBuffered(partitionID)
			if err != nil {
				return false, fmt.Errorf("checking if partition %s is buffered: %w", partitionID, err)
			}
			if partitionBuffered {
				return false, nil
			}
			b.logger.Warnn("Moving buffered jobs for unbuffered partition",
				logger.NewStringField("partitionId", partitionID),
				logger.NewStringField("prefix", b.bufferReadJobsDB.Identifier()),
			)

			var jobCount int
			for limitsReached := true; limitsReached; {
				// before moving each batch, check if the partition got buffered in the meantime, and if so, stop moving to avoid conflicts with the buffering process
				partitionBuffered, err := isPartitionBuffered(partitionID)
				if err != nil {
					return false, fmt.Errorf("checking if partition %s is buffered: %w", partitionID, err)
				}
				if partitionBuffered {
					b.logger.Warnn("Moving buffered jobs for unbuffered partition was preempted by the partition being marked as buffered",
						logger.NewStringField("partitionId", partitionID),
						logger.NewStringField("prefix", b.bufferReadJobsDB.Identifier()),
						logger.NewIntField("jobCount", int64(jobCount)),
					)
					return false, nil
				}
				select {
				case <-ctx.Done():
					b.logger.Warnn("Moving buffered jobs for unbuffered partition was interrupted due to context cancellation",
						logger.NewStringField("partitionId", partitionID),
						logger.NewStringField("prefix", b.bufferReadJobsDB.Identifier()),
						logger.NewIntField("jobCount", int64(jobCount)),
					)
					return false, ctx.Err()
				default:
					var movedCount int
					movedCount, limitsReached, err = b.moveBufferedPartitions(ctx, []string{partitionID}, b.flushBatchSize.Load(), b.flushPayloadSize.Load())
					if err != nil {
						return false, fmt.Errorf("moving buffered partitions: %w", err)
					}
					jobCount += movedCount
				}
			}
			b.logger.Infon("Moving buffered jobs for unbuffered partition completed",
				logger.NewStringField("partitionId", partitionID),
				logger.NewStringField("prefix", b.bufferReadJobsDB.Identifier()),
				logger.NewIntField("jobCount", int64(jobCount)),
			)
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
