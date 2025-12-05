package partitionbuffer

import (
	"context"
	"errors"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

type Opt func(*jobsDBPartitionBuffer)

// ErrInvalidJobsDBPartitionBufferConfig is returned when the configuration for JobsDBPartitionBuffer is invalid
var ErrInvalidJobsDBPartitionBufferConfig = errors.New("invalid jobsdb partition buffer configuration, need to use WithReadWriteJobsDBs, WithWithWriterOnlyJobsDBs or WithReaderOnlyAndFlushJobsDBs")

// WithReadWriteJobsDBs sets both read and write JobsDBs for primary and buffer
func WithReadWriteJobsDBs(primary, buffer jobsdb.JobsDB) Opt {
	return func(b *jobsDBPartitionBuffer) {
		b.JobsDB = primary
		b.primaryReadJobsDB = primary
		b.primaryWriteJobsDB = primary
		b.bufferReadJobsDB = buffer
		b.bufferWriteJobsDB = buffer
		b.canStore = true
		b.canFlush = true
		b.differentReaderWriterDBs = false
		b.lifecycleJobsDBs = []jobsdb.JobsDB{primary, buffer}
	}
}

// WithWithWriterOnlyJobsDBs sets only the writer JobsDBs for primary and buffer
func WithWithWriterOnlyJobsDBs(primaryWriter, bufferWriter jobsdb.JobsDB) Opt {
	return func(b *jobsDBPartitionBuffer) {
		b.JobsDB = primaryWriter
		b.primaryWriteJobsDB = primaryWriter
		b.bufferWriteJobsDB = bufferWriter
		b.canStore = true
		b.canFlush = false
		b.differentReaderWriterDBs = true
		b.lifecycleJobsDBs = []jobsdb.JobsDB{primaryWriter, bufferWriter}
	}
}

// WithReaderOnlyAndFlushJobsDBs sets only the reader JobsDBs for primary and buffer, and writer as primary
func WithReaderOnlyAndFlushJobsDBs(primaryReader, bufferReader, primaryWriter jobsdb.JobsDB) Opt {
	return func(b *jobsDBPartitionBuffer) {
		b.JobsDB = primaryReader
		b.primaryReadJobsDB = primaryReader
		b.bufferReadJobsDB = bufferReader
		b.primaryWriteJobsDB = primaryWriter
		b.canStore = false
		b.canFlush = true
		b.differentReaderWriterDBs = true
		b.lifecycleJobsDBs = []jobsdb.JobsDB{primaryReader, bufferReader, primaryWriter}
	}
}

// WithLogger sets the logger for the JobsDBPartitionBuffer
func WithLogger(logger logger.Logger) Opt {
	return func(b *jobsDBPartitionBuffer) {
		b.logger = logger
	}
}

func WithNumPartitions(numPartitions int) Opt {
	return func(b *jobsDBPartitionBuffer) {
		b.numPartitions = numPartitions
	}
}

// NewJobsDBPartitionBuffer creates a new JobsDBPartitionBuffer with the given options
func NewJobsDBPartitionBuffer(ctx context.Context, opts ...Opt) (JobsDBPartitionBuffer, error) {
	jb := &jobsDBPartitionBuffer{}
	for _, opt := range opts {
		opt(jb)
	}

	if jb.JobsDB == nil {
		return nil, ErrInvalidJobsDBPartitionBufferConfig
	}
	if err := jb.RefreshBufferedPartitions(ctx); err != nil {
		return nil, err
	}
	if jb.logger == nil {
		jb.logger = logger.NewLogger().Child("partitionbuffer")
	}
	return jb, nil
}
