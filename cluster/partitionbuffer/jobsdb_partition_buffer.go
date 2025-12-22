package partitionbuffer

import (
	"context"
	"errors"
	"sync"
	"time"

	golock "github.com/viney-shih/go-lock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/maputil"
	"github.com/rudderlabs/rudder-go-kit/stats"
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
	primaryWriteJobsDB jobsdb.JobsDB   // primary JobsDB for write operations
	primaryReadJobsDB  jobsdb.JobsDB   // primary JobsDB for read operations
	bufferWriteJobsDB  jobsdb.JobsDB   // buffer JobsDB for write operations
	bufferReadJobsDB   jobsdb.JobsDB   // buffer JobsDB for read operations
	lifecycleJobsDBs   []jobsdb.JobsDB // JobsDBs involved in lifecycle operations (like Close)
	logger             logger.Logger
	stats              stats.Stats

	// configuration
	differentReaderWriterDBs bool                              // if having different reader/writer DBs, we need to refresh buffered partitions on every Store
	canStore                 bool                              // indicates if Store operations are supported
	canFlush                 bool                              // indicates if Flush operations are supported
	numPartitions            int                               // number of partitions used in partition function
	flushBatchSize           config.ValueLoader[int]           // number of records to flush in a single batch
	flushPayloadSize         config.ValueLoader[int64]         // total payload size (in bytes) to flush in a single batch
	flushMoveTimeout         config.ValueLoader[time.Duration] // timeout for move operation, before forcing switchover

	// state
	bufferedPartitionsMu      *golock.CASMutex                       // mutex to protect bufferedPartitionsVersion & bufferedPartitions
	bufferedPartitionsVersion int                                    // version gets bumped whenever partitions change in the database, used for comparing cache validity
	bufferedPartitions        *maputil.ReadOnlyMap[string, struct{}] // buffered partitions

	flushingPartitionsMu sync.Mutex          // mutex to protect flushingPartitions
	flushingPartitions   map[string]struct{} // partitions that are currently being flushed
}
