package jobsdb

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type OptsFunc func(jd *Handle)

// WithClearDB if set to true it will remove all existing tables
func WithClearDB(clearDB bool) OptsFunc {
	return func(jd *Handle) {
		jd.conf.clearAll = clearDB
	}
}

func WithDSLimit(limit config.ValueLoader[int]) OptsFunc {
	return func(jd *Handle) {
		jd.conf.dsLimit = limit
	}
}

func WithDBHandle(dbHandle *sql.DB) OptsFunc {
	return func(jd *Handle) {
		jd.dbHandle = dbHandle
	}
}

func WithConfig(c *config.Config) OptsFunc {
	return func(jd *Handle) {
		jd.config = c
	}
}

func WithStats(s stats.Stats) OptsFunc {
	return func(jd *Handle) {
		jd.stats = s
	}
}

func WithSkipMaintenanceErr(ignore bool) OptsFunc {
	return func(jd *Handle) {
		jd.conf.skipMaintenanceError = ignore
	}
}

func WithJobMaxAge(jobMaxAge config.ValueLoader[time.Duration]) OptsFunc {
	return func(jd *Handle) {
		jd.conf.jobMaxAge = jobMaxAge
	}
}

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

// withDatabaseTablesVersion sets the database tables version to use (internal use only for verifying database table migrations)
func withDatabaseTablesVersion(dbVersion int) OptsFunc {
	return func(jd *Handle) {
		jd.conf.dbTablesVersion = dbVersion
	}
}

func NewForRead(tablePrefix string, opts ...OptsFunc) *Handle {
	return newOwnerType(Read, tablePrefix, opts...)
}

func NewForWrite(tablePrefix string, opts ...OptsFunc) *Handle {
	return newOwnerType(Write, tablePrefix, opts...)
}

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
