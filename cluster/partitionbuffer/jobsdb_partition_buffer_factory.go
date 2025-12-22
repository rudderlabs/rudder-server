package partitionbuffer

import (
	"context"
	"errors"
	"time"

	golock "github.com/viney-shih/go-lock"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

type Opt func(*jobsDBPartitionBuffer)

// ErrInvalidJobsDBPartitionBufferConfig is returned when the configuration for JobsDBPartitionBuffer is invalid
var ErrInvalidJobsDBPartitionBufferConfig = errors.New("invalid jobsdb partition buffer configuration, need to use WithReadWriteJobsDBs, WithWriterOnlyJobsDBs or WithReaderOnlyAndFlushJobsDBs")

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

// WithWriterOnlyJobsDBs sets only the writer JobsDBs for primary and buffer
func WithWriterOnlyJobsDBs(primaryWriter, bufferWriter jobsdb.JobsDB) Opt {
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

// WithStats sets the stats collector for the JobsDBPartitionBuffer
func WithStats(stats stats.Stats) Opt {
	return func(b *jobsDBPartitionBuffer) {
		b.stats = stats
	}
}

// WithNumPartitions sets the number of partitions for the JobsDBPartitionBuffer
func WithNumPartitions(numPartitions int) Opt {
	return func(b *jobsDBPartitionBuffer) {
		b.numPartitions = numPartitions
	}
}

// WithFlushBatchSize sets the flush batch size for the JobsDBPartitionBuffer
func WithFlushBatchSize(flushBatchSize config.ValueLoader[int]) Opt {
	return func(b *jobsDBPartitionBuffer) {
		b.flushBatchSize = flushBatchSize
	}
}

// WithFlushPayloadSize sets the flush payload size for the JobsDBPartitionBuffer
func WithFlushPayloadSize(flushPayloadSize config.ValueLoader[int64]) Opt {
	return func(b *jobsDBPartitionBuffer) {
		b.flushPayloadSize = flushPayloadSize
	}
}

// WithFlushMoveTimeout sets the flush move timeout for the JobsDBPartitionBuffer
func WithFlushMoveTimeout(flushMoveTimeout config.ValueLoader[time.Duration]) Opt {
	return func(b *jobsDBPartitionBuffer) {
		b.flushMoveTimeout = flushMoveTimeout
	}
}

// NewJobsDBPartitionBuffer creates a new JobsDBPartitionBuffer with the given options
func NewJobsDBPartitionBuffer(ctx context.Context, opts ...Opt) (JobsDBPartitionBuffer, error) {
	jb := &jobsDBPartitionBuffer{
		numPartitions:        64,
		flushBatchSize:       config.SingleValueLoader(100000),
		flushPayloadSize:     config.SingleValueLoader(500 * bytesize.MB),
		flushMoveTimeout:     config.SingleValueLoader(30 * time.Minute),
		bufferedPartitionsMu: golock.NewCASMutex(),
		flushingPartitions:   make(map[string]struct{}),
	}
	for _, opt := range opts {
		opt(jb)
	}
	if jb.JobsDB == nil {
		return nil, ErrInvalidJobsDBPartitionBufferConfig
	}
	if jb.stats == nil {
		jb.stats = stats.Default
	}
	if jb.logger == nil {
		jb.logger = logger.NewLogger().Child("partitionbuffer")
	}
	if err := jb.RefreshBufferedPartitions(ctx); err != nil {
		return nil, err
	}
	return jb, nil
}
