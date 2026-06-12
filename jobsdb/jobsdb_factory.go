package jobsdb

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

// OptsFunc configures a JobsDB handle before initialization.
type OptsFunc func(jd *Handle)

// WithClearDB removes existing JobsDB tables during setup when clearDB is true.
func WithClearDB(clearDB bool) OptsFunc {
	return func(jd *Handle) {
		jd.conf.clearAll = clearDB
	}
}

// WithDSLimit limits how many datasets read paths scan per query.
func WithDSLimit(limit config.ValueLoader[int]) OptsFunc {
	return func(jd *Handle) {
		jd.conf.dsLimit = limit
	}
}

// WithDBHandle uses dbHandle instead of opening the default database handle.
func WithDBHandle(dbHandle *sql.DB) OptsFunc {
	return func(jd *Handle) {
		jd.dbHandle = dbHandle
	}
}

// WithConfig uses c for JobsDB configuration lookups.
func WithConfig(c *config.Config) OptsFunc {
	return func(jd *Handle) {
		jd.config = c
	}
}

// WithStats uses s for JobsDB metrics.
func WithStats(s stats.Stats) OptsFunc {
	return func(jd *Handle) {
		jd.stats = s
	}
}

// WithSkipMaintenanceErr lets setup continue when maintenance queries fail.
func WithSkipMaintenanceErr(ignore bool) OptsFunc {
	return func(jd *Handle) {
		jd.conf.skipMaintenanceError = ignore
	}
}

// WithJobMaxAge sets the maximum age used by old-job cleanup.
func WithJobMaxAge(jobMaxAge config.ValueLoader[time.Duration]) OptsFunc {
	return func(jd *Handle) {
		jd.conf.jobMaxAge = jobMaxAge
	}
}

// WithNumPartitions enables partition-aware reads and writes.
// numPartitions must be a power of two.
func WithNumPartitions(numPartitions int) OptsFunc {
	{
		return func(jd *Handle) {
			// numPartitions must be a power-of-two number
			if (numPartitions & (numPartitions - 1)) != 0 {
				panic(fmt.Errorf("invalid number of jobsdb partitions, needs to be power of two: %d", numPartitions))
			}
			jd.conf.numPartitions = numPartitions
			// default partition function using a 32-bit key space and Murmur3 hash
			if jd.conf.partitionFunction == nil {
				jd.conf.partitionFunction = func(job *JobT) string {
					return DefaultParititionFunction(job, jd.conf.numPartitions)
				}
			}
		}
	}
}

// WithPriorityPoolDB sets a dedicated connection pool for high-priority operations.
// Operations that use WithPriorityPool(ctx) context will use this pool and bypass
// the regular reader/writer queues.
func WithPriorityPoolDB(pool *sql.DB) OptsFunc {
	return func(jd *Handle) {
		jd.priorityPool = pool
	}
}

// WithMaintenancePoolDB sets a dedicated connection pool for jobsdb-internal
// maintenance operations (compaction setup queries, post-commit dsList refresh,
// status-table cleanup/vacuum). Isolating these from the main pool prevents a
// deadlock vector where main-pool waiters are queued on the dsListLock writer
// that compaction holds while it tries to grab a connection for
// doRefreshDSRangeList.
//
// If no maintenance pool is provided, maintenance operations fall back to the
// main dbHandle.
func WithMaintenancePoolDB(pool *sql.DB) OptsFunc {
	return func(jd *Handle) {
		jd.maintenancePool = pool
	}
}

// WithTriggerAddNewDS overrides the addNewDS loop trigger, allowing callers
// (tests and benchmarks) to deterministically control when new datasets are created.
func WithTriggerAddNewDS(trigger func() <-chan time.Time) OptsFunc {
	return func(jd *Handle) {
		jd.TriggerAddNewDS = trigger
	}
}

// WithMultiConsumer enables multi-consumer mode for this handle.
// This is a one-way option: once datasets exist with the multi-consumer schema,
// removing this option will cause startup to fail.
func WithMultiConsumer() OptsFunc {
	return func(jd *Handle) {
		jd.conf.multiConsumer = true
	}
}

// withDatabaseTablesVersion sets the schema version used by table migration tests.
func withDatabaseTablesVersion(dbVersion int) OptsFunc {
	return func(jd *Handle) {
		jd.conf.dbTablesVersion = dbVersion
	}
}

// NewForRead creates a JobsDB handle for read-only ownership.
func NewForRead(tablePrefix string, opts ...OptsFunc) *Handle {
	return newOwnerType(Read, tablePrefix, opts...)
}

// NewForWrite creates a JobsDB handle for write-only ownership.
func NewForWrite(tablePrefix string, opts ...OptsFunc) *Handle {
	return newOwnerType(Write, tablePrefix, opts...)
}

// NewForReadWrite creates a JobsDB handle that owns both reads and writes.
func NewForReadWrite(tablePrefix string, opts ...OptsFunc) *Handle {
	return newOwnerType(ReadWrite, tablePrefix, opts...)
}

func newOwnerType(ownerType OwnerType, tablePrefix string, opts ...OptsFunc) *Handle {
	j := &Handle{
		ownerType:   ownerType,
		tablePrefix: tablePrefix,
	}

	for _, fn := range opts {
		fn(j)
	}

	j.init()

	return j
}
