package partitionbuffer

import "context"

// FlushBufferedPartitions flushes the buffered data for the provided partition ids to the database and unmarks them as buffered.
func (b *jobsDBPartitionBuffer) FlushBufferedPartitions(ctx context.Context, partitions []string) error {
	if !b.canFlush {
		return ErrFlushNotSupported
	}
	// TODO: implement flush logic
	return nil
}
