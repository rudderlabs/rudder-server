package partitionbuffer

import (
	"context"
	"errors"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

// ErrStoreNotSupported is returned when Store operations are attempted on a JobsDBPartitionBuffer that does not support storing
var ErrStoreNotSupported = errors.New("store not supported")

// ErrFlushNotSupported is returned when Flush operations are attempted on a JobsDBPartitionBuffer that does not support flushing
var ErrFlushNotSupported = errors.New("flush not supported")

// JobsDBPartitionBuffer is an interface that extends jobsdb.JobsDB with partition buffering (and flushing) capabilities
type JobsDBPartitionBuffer interface {
	jobsdb.JobsDB
	// BufferPartitions marks the provided partition ids to be buffered
	BufferPartitions(ctx context.Context, partitionIds []string) error
	// RefreshBufferedPartitions refreshes the list of buffered partitions from the database
	RefreshBufferedPartitions(ctx context.Context) error
	// FlushBufferedPartitions flushes the buffered data for the provided partition ids to the database and unmarks them as buffered.
	FlushBufferedPartitions(ctx context.Context, partitionIds []string) error
}

type jobsDBPartitionBuffer struct {
	jobsdb.JobsDB

	logger logger.Logger

	primaryWriteJobsDB jobsdb.JobsDB // primary JobsDB for write operations
	primaryReadJobsDB  jobsdb.JobsDB // primary JobsDB for read operations

	bufferWriteJobsDB jobsdb.JobsDB // buffer JobsDB for write operations
	bufferReadJobsDB  jobsdb.JobsDB // buffer JobsDB for read operations

	lifecycleJobsDBs []jobsdb.JobsDB // JobsDBs involved in lifecycle operations (like Close)

	// nolint: unused // TODO: to be used in Store implementation
	differentReaderWriterDBs  bool                           // if having different reader/writer DBs, we need to refresh buffered partitions on every Store
	canStore                  bool                           // indicates if Store operations are supported
	canFlush                  bool                           // indicates if Flush operations are supported
	numPartitions             int                            // number of partitions used in partition function
	bufferedPartitionsMu      sync.RWMutex                   // mutex to protect bufferedPartitionsVersion & bufferedPartitions
	bufferedPartitionsVersion int                            // version of the buffered partitions
	bufferedPartitions        *readOnlyMap[string, struct{}] // buffered partitions
}
