/*
Implementation of JobsDB for keeping track of jobs (type JobT) and job status
(type JobStatusT). Jobs are stored in jobs_%d table while job status is stored
in job_status_%d table. Each such table pair (e.g. jobs_1, job_status_1) is called
a dataset (type dataSetT). After a dataset grows beyond a size, a new dataset is
created and jobs are written to a new dataset. When most of the jobs from a dataset
have been processed, we migrate the remaining jobs to a new intermediate
dataset and delete the old dataset. The range of job ids in a dataset are tracked
via the dataSetRangeT struct

The key reason for choosing this structure is to avoid costly DELETE and UPDATE
operations in DB. Instead, we just use WRITE (append) and DELETE TABLE (deleting a file)
operations which are fast.
Also, keeping each dataset small (enough to cache in memory) ensures that reads are
mostly serviced from memory cache.
*/

package jobsdb

//go:generate mockgen -destination=../mocks/jobsdb/mock_jobsdb.go -package=mocks_jobsdb github.com/rudderlabs/rudder-server/jobsdb JobsDB

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/partmap"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/collectors"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/cache"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/dsindex"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

var ErrStaleDsList = errors.New("stale dataset list")

const (
	pgReadonlyTableExceptionFuncName = "readonly_table_exception()"
	pgErrorCodeTableReadonly         = "RS001"
)

type payloadColumnType string

const (
	JSONB payloadColumnType = "jsonb"
	BYTEA payloadColumnType = "bytea"
	TEXT  payloadColumnType = "text"
)

// GetQueryParams is a struct to hold jobsdb query params.
type GetQueryParams struct {
	// query conditions

	WorkspaceID      string
	CustomValFilters []string
	ParameterFilters []ParameterFilterT
	PartitionFilters []string

	stateFilters                   []string
	afterJobID                     *int64
	ignoreReadPartitionsExclusions bool // if true, includes results from all partitions, ignoring any preconfigured excluded read partitions

	// query limits

	// Limit the total number of jobs.
	// A value less than or equal to zero will return no results
	JobsLimit int
	// Limit the total number of events, 1 job contains 1+ event(s).
	// A value less than or equal to zero will disable this limit (no limit),
	// only values greater than zero are considered as valid limits.
	EventsLimit int
	// Limit the total job payload size
	// A value less than or equal to zero will disable this limit (no limit),
	// only values greater than zero are considered as valid limits.
	PayloadSizeLimit int64
}

// HandleInspector is only intended to be used by tests for verifying the handle's internal state
type HandleInspector struct {
	*Handle
}

// MoreToken is a token that can be used to fetch more jobs
type MoreToken any

// MoreJobsResult is a JobsResult with a MoreToken
type MoreJobsResult struct {
	JobsResult
	More MoreToken
}

/*
JobsDB interface contains public methods to access JobsDB data
*/
type JobsDB interface {
	// Identifier returns the jobsdb's identifier, a.k.a. table prefix
	Identifier() string

	/* Commands */

	// WithTx begins a new transaction that can be used by the provided function.
	// If the function returns an error, the transaction will be rollbacked and return the error,
	// otherwise the transaction will be committed and a nil error will be returned.
	WithTx(context.Context, func(tx *Tx) error) error

	// WithStoreSafeTx prepares a store-safe environment and then starts a transaction
	// that can be used by the provided function.
	WithStoreSafeTx(context.Context, func(tx StoreSafeTx) error) error

	// WithStoreSafeTxFromTx prepares a store-safe environment for an existing transaction.
	WithStoreSafeTxFromTx(context.Context, *Tx, func(tx StoreSafeTx) error) error

	// Store stores the provided jobs to the database
	Store(ctx context.Context, jobList []*JobT) error

	// StoreInTx stores the provided jobs to the database using an existing transaction.
	// Please ensure that you are using an StoreSafeTx, e.g.
	//    jobsdb.WithStoreSafeTx(ctx, func(tx StoreSafeTx) error {
	//	      jobsdb.StoreInTx(ctx, tx, jobList)
	//    })
	StoreInTx(ctx context.Context, tx StoreSafeTx, jobList []*JobT) error

	// WithUpdateSafeTx prepares an update-safe environment and then starts a transaction
	// that can be used by the provided function. An update-safe transaction shall be used if the provided function
	// needs to call UpdateJobStatusInTx.
	WithUpdateSafeTx(context.Context, func(tx UpdateSafeTx) error) error

	// WithUpdateSafeTxFromTx prepares an update-safe environment for an existing transaction.
	WithUpdateSafeTxFromTx(ctx context.Context, tx *Tx, f func(tx UpdateSafeTx) error) error

	// UpdateJobStatus updates the provided job statuses
	UpdateJobStatus(ctx context.Context, statusList []*JobStatusT) error

	// UpdateJobStatusInTx updates the provided job statuses in an existing transaction.
	// Please ensure that you are using an UpdateSafeTx, e.g.
	//    jobsdb.WithUpdateSafeTx(ctx, func(tx UpdateSafeTx) error {
	//	      jobsdb.UpdateJobStatusInTx(ctx, tx, statusList)
	//    })
	UpdateJobStatusInTx(ctx context.Context, tx UpdateSafeTx, statusList []*JobStatusT) error

	/* Queries */

	// GetJobs finds jobs in any of the provided state(s)
	GetJobs(ctx context.Context, states []string, params GetQueryParams) (JobsResult, error)

	// GetUnprocessed finds unprocessed jobs, i.e. new jobs whose state hasn't been marked in the database yet
	GetUnprocessed(ctx context.Context, params GetQueryParams) (JobsResult, error)

	// GetImporting finds jobs in importing state
	GetImporting(ctx context.Context, params GetQueryParams, more MoreToken) (*MoreJobsResult, error)

	// GetAborted finds jobs in aborted state
	GetAborted(ctx context.Context, params GetQueryParams) (JobsResult, error)

	// GetWaiting finds jobs in waiting state
	GetWaiting(ctx context.Context, params GetQueryParams) (JobsResult, error)

	// GetSucceeded finds jobs in succeeded state
	GetSucceeded(ctx context.Context, params GetQueryParams) (JobsResult, error)

	// GetFailed finds jobs in failed state
	GetFailed(ctx context.Context, params GetQueryParams) (JobsResult, error)

	// GetToProcess finds jobs in any of the following states: failed, waiting, unprocessed.
	// It also returns a MoreToken that can be used to fetch more jobs, if available, with a subsequent call.
	GetToProcess(ctx context.Context, params GetQueryParams, more MoreToken) (*MoreJobsResult, error)

	// GetPileUpCounts returns statistics (counters) of incomplete jobs
	// grouped by workspaceId and destination type
	GetPileUpCounts(ctx context.Context, cutoffTime time.Time, increaseFunc rmetrics.IncreasePendingEventsFunc) (err error)

	// GetDistinctParameterValues returns the list of distinct parameter("source_id", "destination_id", "workspace_id") values inside the jobs tables filtering for the passed customVal
	GetDistinctParameterValues(ctx context.Context, parameter ParameterName, customValFilter string) (values []string, err error)

	/* Admin */

	Ping() error
	DeleteExecuting()
	FailExecuting()
	RefreshDSList(ctx context.Context) error

	/* Journal */

	GetJournalEntries(opType string) (entries []JournalEntryT)
	JournalDeleteEntry(opID int64)
	JournalMarkStart(opType string, opPayload json.RawMessage) (int64, error)
	JournalMarkDone(opID int64) error

	ReadExcludedPartitionsManager

	// lifecycle management
	Start() error
	Stop()
}

type ParameterName interface {
	string() string
}

type parameterName string

func (p parameterName) string() string {
	return string(p)
}

const (
	SourceID      parameterName = "parameters->>'source_id'"
	DestinationID parameterName = "parameters->>'destination_id'"
	WorkspaceID   parameterName = "workspace_id"
)

type ConnectionID struct {
	SourceID      string
	DestinationID string
}

/*
JobT is the basic type for creating jobs. The JobID is generated
by the system and LastJobStatus is populated when reading a processed
job  while rest should be set by the user.
*/
type JobT struct {
	UUID          uuid.UUID       `json:"UUID"`
	JobID         int64           `json:"JobID"`
	UserID        string          `json:"UserID"`
	CreatedAt     time.Time       `json:"CreatedAt"`
	ExpireAt      time.Time       `json:"ExpireAt"`
	CustomVal     string          `json:"CustomVal"`
	EventCount    int             `json:"EventCount"`
	EventPayload  json.RawMessage `json:"EventPayload"`
	LastJobStatus JobStatusT      `json:"LastJobStatus"`
	Parameters    json.RawMessage `json:"Parameters"`
	WorkspaceId   string          `json:"WorkspaceId"`
	PartitionID   string          `json:"PartitionId"`
}

func (job *JobT) String() string {
	var sb strings.Builder
	sb.WriteString("JobID=")
	sb.WriteString(strconv.FormatInt(job.JobID, 10))
	sb.WriteString(", UserID=")
	sb.WriteString(job.UserID)
	sb.WriteString(", CreatedAt=")
	sb.WriteString(job.CreatedAt.String())
	sb.WriteString(", ExpireAt=")
	sb.WriteString(job.ExpireAt.String())
	sb.WriteString(", CustomVal=")
	sb.WriteString(job.CustomVal)
	sb.WriteString(", Parameters=")
	sb.WriteString(string(job.Parameters))
	sb.WriteString(", EventPayload=")
	sb.WriteString(string(job.EventPayload))
	sb.WriteString(" EventCount=")
	sb.WriteString(strconv.Itoa(job.EventCount))
	return sb.String()
}

func (job *JobT) sanitizeJSON() error {
	job.UserID = string(bytes.ReplaceAll([]byte(job.UserID), []byte("\x00"), []byte("")))
	var err error
	job.EventPayload, err = misc.SanitizeJSON(job.EventPayload)
	if err != nil {
		return err
	}
	job.Parameters, err = misc.SanitizeJSON(job.Parameters)
	if err != nil {
		return err
	}
	return nil
}

/*
Handle is the main type implementing the database for implementing
jobs. The caller must call the SetUp function on a Handle object
*/
type Handle struct {
	dbHandle             *sql.DB
	priorityPool         *sql.DB // dedicated connection pool for high-priority operations (e.g., partition migration)
	maintenancePool      *sql.DB // dedicated connection pool for jobsdb-internal maintenance ops (compaction setup, refresh, vacuum), isolated from the main pool to avoid deadlock when main-pool waiters hold the dsListLock writer
	sharedConnectionPool bool
	ownerType            OwnerType
	tablePrefix          string
	logger               logger.Logger
	stats                stats.Stats

	dsList              *versionedDSList
	dropDSListLock      sync.RWMutex  // protects dropDSList
	dropDSList          []dropDSEntry // list of datasets to be dropped asynchronously in a non-blocking fashion
	dropNotify          chan struct{} // used to notify the dropDSLoop of new entries in dropDSList
	dsRangeFuncMap      map[string]func() (dsRangeMinMax, error)
	distinctValuesCache *distinctValuesCache
	dsListLock          *lock.Locker
	dsCompactionLock    *lock.Locker
	// lastCompactionProbeIndex stores the dsindex of the last dataset probed by
	// getCompactionList when no eligible datasets were found and scanning was
	// cut short by maxCompactionProbe. The next invocation resumes from here
	// instead of re-scanning from the beginning.
	// Only accessed from the single compactionLoop goroutine.
	lastCompactionProbeIndex *dsindex.Index
	noResultsCache           *cache.NoResultsCache[ParameterFilterT]

	excludedReadPartitionsLock sync.RWMutex
	excludedReadPartitions     map[string]struct{}

	// table count stats
	statTableCount                  stats.Measurement
	statReadExcludedPartitionsCount stats.Gauge

	// ds creation and drop period stats
	statNewDSPeriod   stats.Timer
	newDSCreationTime time.Time
	statDropDSPeriod  stats.Timer
	dsDropTime        time.Time

	backgroundCancel context.CancelFunc
	backgroundGroup  *errgroup.Group

	TriggerAddNewDS   func() <-chan time.Time
	compactionPaused  atomic.Bool
	TriggerCompaction func() <-chan time.Time
	TriggerRefreshDS  func() <-chan time.Time

	lifecycle struct {
		mu      sync.Mutex
		started bool
	}

	config *config.Config
	conf   struct {
		payloadColumnType               payloadColumnType
		maxTableSize                    config.ValueLoader[int64]
		cacheExpiration                 config.ValueLoader[time.Duration]
		addNewDSLoopSleepDuration       config.ValueLoader[time.Duration]
		addNewDSTimeout                 config.ValueLoader[time.Duration]
		refreshDSListLoopSleepDuration  config.ValueLoader[time.Duration]
		refreshDSTimeout                config.ValueLoader[time.Duration]
		minDSRetentionPeriod            config.ValueLoader[time.Duration]
		maxDSRetentionPeriod            config.ValueLoader[time.Duration]
		jobMaxAge                       config.ValueLoader[time.Duration]
		writeCapacity                   chan struct{}
		readCapacity                    chan struct{}
		enableWriterQueue               bool
		enableReaderQueue               bool
		clearAll                        bool
		skipMaintenanceError            bool
		dsLimit                         config.ValueLoader[int]
		maxReaders                      int
		maxWriters                      int
		maxOpenConnections              int
		analyzeThreshold                config.ValueLoader[int]
		MaxDSSize                       config.ValueLoader[int]
		numPartitions                   int // if zero or negative, no partitioning
		partitionFunction               func(job *JobT) string
		warnOnStatusMissingPartitionID  config.ValueLoader[bool]
		holdDSListLockDuringStore       config.ValueLoader[bool] // escape hatch: hold the dsList read lock for the entire store callback
		noResultsCacheStateOptimization config.ValueLoader[bool]
		// getJobsUseLateralJoin replaces the v_last_* view join in getJobsDS with a
		// correlated LATERAL subquery against the raw job_status table.
		getJobsUseLateralJoin config.ValueLoader[bool]
		dbTablesVersion       int // version of the database tables schema (0 means latest)

		compaction struct {
			maxCompactOnce, maxCompactionProbe config.ValueLoader[int]
			vacuumFullStatusTableThreshold     config.ValueLoader[int64]
			vacuumAnalyzeStatusTableThreshold  config.ValueLoader[int64]
			jobStatusCompactionThres           config.ValueLoader[float64]
			jobMinRowsLeftCompactionThreshold  config.ValueLoader[float64]
			loopSleepDuration                  config.ValueLoader[time.Duration]
			timeout                            config.ValueLoader[time.Duration]
			// nonBlockingCompletedDSDrop routes datasets with zero pending jobs
			// through the async dropDSLoop instead of the in-TX postCompactionHandleDS
			// path, so the drop is compaction-lock-free and does not block concurrent readers.
			nonBlockingCompletedDSDrop config.ValueLoader[bool]
			// nonBlockingCompaction gates the new compaction TX shape (EXCLUSIVE
			// lock + readonly trigger on the source status table, async source drop).
			// When disabled, falls back to the legacy doCompaction flow.
			nonBlockingCompaction config.ValueLoader[bool]
			// compactionDeferStatusLock further minimizes the status-table lock
			// window during non-blocking compaction by splitting the copy into two
			// phases: pending jobs are copied first WITHOUT any status-table lock,
			// then each source status table is locked EXCLUSIVE one after another only
			// to copy the latest statuses of the moved jobs. Requires
			// nonBlockingCompaction; has no effect on its own.
			compactionDeferStatusLock config.ValueLoader[bool]
			// getJobsRetryOnCompaction gates the snapshot revalidation in getJobs:
			// when set, getJobs detects that a queried dataset is no longer in the
			// published list (e.g. a non-blocking compaction completed mid-read) and
			// retries from scratch. Requires nonBlockingCompaction to also be set;
			// has no effect on its own.
			getJobsRetryOnCompaction config.ValueLoader[bool]
		}
	}
}

type ParameterFilterT struct {
	Name  string
	Value string
}

func (p ParameterFilterT) String() string {
	return p.Name + ":" + p.Value
}

func (p ParameterFilterT) GetName() string {
	return p.Name
}

func (p ParameterFilterT) GetValue() string {
	return p.Value
}

type ParameterFilterList []ParameterFilterT

func (l ParameterFilterList) String() string {
	sb := strings.Builder{}
	for i, p := range l {
		if i > 0 {
			sb.WriteString(";")
		}
		sb.WriteString(p.String())
	}
	return sb.String()
}

var dbInvalidJsonErrors = map[string]struct{}{
	"22P02": {},
	"22P05": {},
	"22025": {},
	"22019": {},
	"22021": {}, // invalid byte sequence for encoding "UTF8"
}

// Some helper functions
func (jd *Handle) assertError(err error) {
	if err != nil {
		jd.printLists(true)
		jd.logger.Fataln("assertError failure",
			obskit.Error(err),
			logger.NewStringField("tablePrefix", jd.tablePrefix),
			logger.NewStringField("ownerType", jd.ownerType.Identifier()),
			logger.NewStringField("noResultsCache", jd.noResultsCache.String()))
		panic(err)
	}
}

func (jd *Handle) assert(cond bool, errorString string) {
	if !cond {
		jd.printLists(true)
		jd.logger.Fataln("assert condition failed",
			obskit.Error(errors.New(errorString)),
			logger.NewStringField("tablePrefix", jd.tablePrefix),
			logger.NewStringField("ownerType", jd.ownerType.Identifier()),
			logger.NewStringField("noResultsCache", jd.noResultsCache.String()))
		panic(fmt.Errorf("[[ %s ]]: %s", jd.tablePrefix, errorString))
	}
}

type jobStateT struct {
	isValid    bool
	isTerminal bool
	State      string
}

// State definitions
var (
	// Not valid, Not terminal
	Unprocessed = jobStateT{isValid: false, isTerminal: false, State: "not_picked_yet"}

	// Valid, Not terminal
	Failed    = jobStateT{isValid: true, isTerminal: false, State: "failed"}
	Executing = jobStateT{isValid: true, isTerminal: false, State: "executing"}
	Waiting   = jobStateT{isValid: true, isTerminal: false, State: "waiting"}
	Importing = jobStateT{isValid: true, isTerminal: false, State: "importing"}

	// Valid, Terminal
	Succeeded = jobStateT{isValid: true, isTerminal: true, State: "succeeded"}
	Aborted   = jobStateT{isValid: true, isTerminal: true, State: "aborted"}
	Migrated  = jobStateT{isValid: true, isTerminal: true, State: "migrated"}
	Filtered  = jobStateT{isValid: true, isTerminal: true, State: "filtered"}

	terminalStates         map[string]struct{}
	validTerminalStates    []string
	validNonTerminalStates []string
)

// Adding a new state to this list, will require an enum change in postgres db.
var jobStates = []jobStateT{
	Unprocessed,
	Failed,
	Executing,
	Waiting,
	Succeeded,
	Aborted,
	Migrated,
	Importing,
	Filtered,
}

// OwnerType for this jobsdb instance
type OwnerType string

const (
	// Read : Only Reader of this jobsdb instance
	Read OwnerType = "READ"
	// Write : Only Writer of this jobsdb instance
	Write OwnerType = "WRITE"
	// ReadWrite : Reader and Writer of this jobsdb instance
	ReadWrite OwnerType = ""
)

func (ot OwnerType) Identifier() string {
	switch ot {
	case Read:
		return "r"
	case Write:
		return "w"
	case ReadWrite:
		return "rw"
	default:
		return ""

	}
}

func init() {
	terminalStates = make(map[string]struct{})
	for _, js := range jobStates {
		if !js.isValid {
			continue
		}
		if js.isTerminal {
			terminalStates[js.State] = struct{}{}
			validTerminalStates = append(validTerminalStates, js.State)
		} else {
			validNonTerminalStates = append(validNonTerminalStates, js.State)
		}
	}
}

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

/*
Setup is used to initialize the HandleT structure.
clearAll = True means it will remove all existing tables
tablePrefix must be unique and is used to separate
multiple users of JobsDB
*/
func (jd *Handle) Setup(
	ownerType OwnerType, clearAll bool, tablePrefix string,
) error {
	jd.ownerType = ownerType
	jd.conf.clearAll = clearAll
	jd.tablePrefix = tablePrefix
	jd.init()
	return jd.Start()
}

func (jd *Handle) init() {
	jd.dsList = newVersionedDSList(nil, nil)
	jd.dropNotify = make(chan struct{}, 1)
	if jd.logger == nil {
		jd.logger = logger.NewLogger().Child("jobsdb").Child(jd.tablePrefix)
	}
	jd.dsRangeFuncMap = make(map[string]func() (dsRangeMinMax, error))
	jd.distinctValuesCache = NewDistinctValuesCache()

	if jd.config == nil {
		jd.config = config.Default
	}

	if string(jd.conf.payloadColumnType) == "" {
		jd.conf.payloadColumnType = TEXT
	}

	if jd.stats == nil {
		jd.stats = stats.Default
	}
	jd.dsListLock = lock.NewLocker("dsListLock", jd.tablePrefix, jd.stats)
	jd.dsCompactionLock = lock.NewLocker("dsCompactionLock", jd.tablePrefix, jd.stats)

	jd.loadConfig()

	// Initialize dbHandle if not already set
	if jd.dbHandle != nil {
		jd.sharedConnectionPool = true
	} else {
		var err error
		psqlInfo := misc.GetConnectionString(jd.config, "jobsdb_"+jd.tablePrefix)
		jd.dbHandle, err = sql.Open("postgres", psqlInfo)
		jd.assertError(err)

		jd.assertError(
			jd.stats.RegisterCollector(
				collectors.NewDatabaseSQLStats(
					"jobsdb_"+jd.tablePrefix+"_"+jd.ownerType.Identifier(),
					jd.dbHandle,
				),
			),
		)

		var maxConns int
		if !jd.conf.enableReaderQueue || !jd.conf.enableWriterQueue {
			maxConns = jd.conf.maxOpenConnections
		} else {
			maxConns = 2 // buffer
			maxConns += jd.conf.maxReaders + jd.conf.maxWriters
			switch jd.ownerType {
			case Read:
				maxConns += 3 // migrate, refreshDsList, dropDS
			case Write:
				maxConns += 1 // addNewDS
			case ReadWrite:
				maxConns += 3 // migrate, addNewDS, dropDS
			}
			if maxConns >= jd.conf.maxOpenConnections {
				maxConns = jd.conf.maxOpenConnections
			}
		}
		jd.dbHandle.SetMaxOpenConns(maxConns)

		jd.assertError(jd.dbHandle.Ping())
	}

	jd.workersAndAuxSetup()

	err := jd.WithTx(context.Background(), func(tx *Tx) error {
		// only one migration should run at a time and block all other processes from adding or removing tables
		return jd.withDistributedLock(context.Background(), tx, "schema_migrate", func() error {
			// Database schema migration should happen early, even before jobsdb is started,
			// so that we can be sure that all the necessary tables are created and considered to be in
			// the latest schema version, before rudder-migrator starts introducing new tables.
			jd.dsListLock.WithLock(func(l lock.LockToken) {
				writer := jd.ownerType == Write || jd.ownerType == ReadWrite
				if writer && jd.conf.clearAll {
					jd.dropDatabaseTables(l)
				}
				templateData := func() map[string]any {
					// Important: if jobsdb type is acting as a writer then refreshDSList
					// doesn't return the full list of datasets, only the rightmost two.
					// But we need to run the schema migration against all datasets, no matter
					// whether jobsdb is a writer or not.
					datasets, err := getDSList(jd, tx, jd.tablePrefix)
					jd.assertError(err)

					datasetIndices := make([]string, 0)
					for _, dataset := range datasets {
						datasetIndices = append(datasetIndices, dataset.Index)
					}

					return map[string]any{
						"Prefix":              jd.tablePrefix,
						"Datasets":            datasetIndices,
						"PartitioningEnabled": jd.conf.numPartitions > 0,
					}
				}()

				if writer {
					jd.setupDatabaseTables(templateData)
				}

				// Run changesets that should always run for both writer and reader jobsdbs.
				//
				// When running separate gw and processor instances we cannot control the order of execution
				// and we cannot guarantee that after a gw migration completes, processor
				// will not create new tables using the old schema.
				//
				// Changesets that run always can help in such cases, by bringing non-migrated tables into a usable state.
				jd.runAlwaysChangesets(templateData)

				// finally refresh the dataset list to make sure [datasetList] field is populated
				err := jd.doRefreshDSRangeListWithDB(l, jd.dbHandle)
				jd.assertError(err)
			})
			return nil
		})
	})
	if err != nil {
		panic(fmt.Errorf("failed to run schema migration for %s: %w", jd.tablePrefix, err))
	}
}

func (jd *Handle) workersAndAuxSetup() {
	jd.assert(jd.tablePrefix != "", "tablePrefix received is empty")

	var defaultLogCacheBranchInvalidation bool
	switch jd.tablePrefix {
	case "gw", "rt", "batch_rt", "arc":
		defaultLogCacheBranchInvalidation = true
	}
	jd.noResultsCache = cache.NewNoResultsCache(
		cacheParameterFilters,
		func() time.Duration { return jd.conf.cacheExpiration.Load() },
		cache.WithWarnOnBranchInvalidation[ParameterFilterT](
			jd.config.GetReloadableBoolVar(defaultLogCacheBranchInvalidation, jd.configKeys("logCacheBranchInvalidation")...),
			jd.logger),
	)

	jd.logger.Infon("Connected to DB")
	jd.statTableCount = jd.stats.NewTaggedStat("jobsdb.tables_count", stats.GaugeType, stats.Tags{"customVal": jd.tablePrefix})
	jd.statNewDSPeriod = jd.stats.NewTaggedStat("jobsdb.new_ds_period", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	jd.statDropDSPeriod = jd.stats.NewTaggedStat("jobsdb.drop_ds_period", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	jd.statReadExcludedPartitionsCount = jd.stats.NewTaggedStat("jobsdb_read_excluded_partitions_count", stats.GaugeType, stats.Tags{"customVal": jd.tablePrefix})
}

func (jd *Handle) loadConfig() {
	// maxTableSizeInMB: Maximum Table size in MB
	jd.conf.maxTableSize = jd.config.GetReloadableInt64Var(300, 1000000, jd.configKeys("maxTableSizeInMB")...)
	jd.conf.cacheExpiration = jd.config.GetReloadableDurationVar(120, time.Minute, jd.configKeys("cacheExpiration")...)
	// addNewDSLoopSleepDuration: How often is the loop (which checks for adding new DS) run
	jd.conf.addNewDSLoopSleepDuration = jd.config.GetReloadableDurationVar(5, time.Second, jd.configKeys("addNewDSLoopSleepDuration")...)
	// refreshDSListLoopSleepDuration: How often is the loop (which refreshes DSList) run
	jd.conf.refreshDSListLoopSleepDuration = jd.config.GetReloadableDurationVar(10, time.Second, jd.configKeys("refreshDSListLoopSleepDuration")...)

	jd.conf.enableWriterQueue = jd.config.GetBoolVar(true, jd.configKeys("enableWriterQueue")...)
	jd.conf.enableReaderQueue = jd.config.GetBoolVar(true, jd.configKeys("enableReaderQueue")...)
	jd.conf.maxWriters = jd.config.GetIntVar(3, 1, jd.configKeys("maxWriters")...)
	jd.conf.maxReaders = jd.config.GetIntVar(6, 1, jd.configKeys("maxReaders")...)
	jd.conf.maxOpenConnections = jd.config.GetIntVar(20, 1, jd.configKeys("maxOpenConnections")...)
	jd.conf.analyzeThreshold = jd.config.GetReloadableIntVar(30000, 1, jd.configKeys("analyzeThreshold")...)
	jd.conf.minDSRetentionPeriod = jd.config.GetReloadableDurationVar(0, time.Minute, jd.configKeys("minDSRetention")...)
	jd.conf.maxDSRetentionPeriod = jd.config.GetReloadableDurationVar(90, time.Minute, jd.configKeys("maxDSRetention")...)
	jd.conf.refreshDSTimeout = jd.config.GetReloadableDurationVar(10, time.Minute, jd.configKeys("refreshDS.timeout")...)
	jd.conf.addNewDSTimeout = jd.config.GetReloadableDurationVar(5, time.Minute, jd.configKeys("addNewDS.timeout")...)

	// compactionConfig

	// loopSleepDuration: How often the compaction loop runs
	jd.conf.compaction.loopSleepDuration = jd.config.GetReloadableDurationVar(30, time.Second, jd.configKeys("compactionLoopSleepDuration", "migrateDSLoopSleepDuration", "migrateDSLoopSleepDurationInS")...)
	jd.conf.compaction.timeout = jd.config.GetReloadableDurationVar(10, time.Minute, jd.configKeys("compactionTimeout", "migrateDS.timeout")...)
	// jobStatusCompactionThres: A DS is compacted if the job_status exceeds this (* no_of_jobs)
	jd.conf.compaction.jobStatusCompactionThres = jd.config.GetReloadableFloat64Var(3, jd.configKeys("jobStatusCompactionThreshold", "jobStatusMigrateThreshold")...)
	// jobMinRowsLeftCompactionThreshold: A DS with a low number of pending rows should be eligible for compaction if the number of pending rows are
	// less than jobMinRowsLeftCompactionThreshold percent of maxDSSize (e.g. if jobMinRowsLeftCompactionThreshold is 0.5
	// then DSs that have less than 50% of maxDSSize pending rows are eligible for compaction)
	jd.conf.compaction.jobMinRowsLeftCompactionThreshold = jd.config.GetReloadableFloat64Var(0.6, jd.configKeys("jobMinRowsLeftCompactionThreshold", "jobMinRowsLeftMigrateThreshold")...)
	// maxCompactOnce: Maximum number of DSs that are compacted together into one destination
	jd.conf.compaction.maxCompactOnce = jd.config.GetReloadableIntVar(10, 1, jd.configKeys("maxCompactOnce", "maxMigrateOnce")...)
	// maxCompactionProbe: Maximum number of DSs that are checked from left to right if they are eligible for compaction
	jd.conf.compaction.maxCompactionProbe = jd.config.GetReloadableIntVar(10, 1, jd.configKeys("maxCompactProbe", "maxMigrateDSProbe")...)
	jd.conf.compaction.vacuumFullStatusTableThreshold = jd.config.GetReloadableInt64Var(500*bytesize.MB, 1, jd.configKeys("vacuumFullStatusTableThreshold")...)
	jd.conf.compaction.vacuumAnalyzeStatusTableThreshold = jd.config.GetReloadableInt64Var(30000, 1, jd.configKeys("vacuumAnalyzeStatusTableThreshold")...)
	jd.conf.compaction.nonBlockingCompletedDSDrop = jd.config.GetReloadableBoolVar(false, jd.configKeys("nonBlockingCompletedDSDrop")...)
	jd.conf.compaction.nonBlockingCompaction = jd.config.GetReloadableBoolVar(true, jd.configKeys("nonBlockingCompaction")...)
	jd.conf.compaction.compactionDeferStatusLock = jd.config.GetReloadableBoolVar(true, jd.configKeys("compactionDeferStatusLock")...)
	jd.conf.compaction.getJobsRetryOnCompaction = jd.config.GetReloadableBoolVar(true, jd.configKeys("getJobsRetryOnCompaction")...)

	// maxDSSize: Maximum size of a DS. The process which adds new DS runs in the background
	// (every few seconds) so a DS may go beyond this size
	// passing `maxDSSize` by reference, so it can be hot reloaded
	jd.conf.MaxDSSize = jd.config.GetReloadableIntVar(100000, 1, jd.configKeys("maxDSSize")...)

	// starting with false as default since initial set of migrated jobs will not have partitionID set
	jd.conf.warnOnStatusMissingPartitionID = jd.config.GetReloadableBoolVar(false, jd.configKeys("warnOnStatusMissingPartitionID")...)

	// Default false: snapshot lastDS and release the dsList read lock before running the store callback,
	// so long-running stores don't block dsList writers. Flip to true to revert to holding the lock for the whole callback.
	jd.conf.holdDSListLockDuringStore = jd.config.GetReloadableBoolVar(false, jd.configKeys("holdDSListLockDuringStore")...)

	// when true, the per-state noResultsCache optimization is enabled: stateFilters are narrowed
	// against the cache before querying, and (!ok && !limitsReached) is used as a commit predicate.
	jd.conf.noResultsCacheStateOptimization = jd.config.GetReloadableBoolVar(false, jd.configKeys("noResultsCacheStateOptimization")...)
	jd.conf.getJobsUseLateralJoin = jd.config.GetReloadableBoolVar(true, jd.configKeys("getJobsUseLateralJoin")...)

	if jd.TriggerAddNewDS == nil {
		jd.TriggerAddNewDS = func() <-chan time.Time {
			return time.After(jd.conf.addNewDSLoopSleepDuration.Load())
		}
	}

	if jd.TriggerCompaction == nil {
		jd.TriggerCompaction = func() <-chan time.Time {
			return time.After(jd.conf.compaction.loopSleepDuration.Load())
		}
	}

	if jd.TriggerRefreshDS == nil {
		jd.TriggerRefreshDS = func() <-chan time.Time {
			return time.After(jd.conf.refreshDSListLoopSleepDuration.Load())
		}
	}

	if jd.conf.jobMaxAge == nil {
		jd.conf.jobMaxAge = jd.config.GetReloadableDurationVar(720, time.Hour, jd.configKeys("jobMaxAge")...)
	}
}

func (jd *Handle) configKeys(key string, additionalKeys ...string) []string {
	all := append([]string{key}, additionalKeys...)
	res := make([]string, 0, 2*len(all))
	for _, k := range all {
		res = append(res, "JobsDB."+jd.tablePrefix+"."+k)
	}
	for _, k := range all {
		res = append(res, "JobsDB."+k)
	}
	return res
}

// Start starts the jobsdb worker and housekeeping (migration, archive) threads.
// Start should be called before any other jobsdb methods are called.
func (jd *Handle) Start() error {
	jd.lifecycle.mu.Lock()
	defer jd.lifecycle.mu.Unlock()
	if jd.lifecycle.started {
		return nil
	}
	defer func() { jd.lifecycle.started = true }()

	jd.conf.writeCapacity = make(chan struct{}, jd.conf.maxWriters)
	jd.conf.readCapacity = make(chan struct{}, jd.conf.maxReaders)

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	jd.backgroundCancel = cancel
	jd.backgroundGroup = g

	jd.setUpForOwnerType(ctx, jd.ownerType)
	return nil
}

func (jd *Handle) setUpForOwnerType(ctx context.Context, ownerType OwnerType) {
	jd.dsListLock.WithLock(func(l lock.LockToken) {
		switch ownerType {
		case Read:
			jd.readerSetup(ctx, l)
		case Write:
			jd.writerSetup(ctx, l)
		case ReadWrite:
			jd.readerWriterSetup(ctx, l)
		}
	})
}

func (jd *Handle) readerSetup(ctx context.Context, l lock.LockToken) {
	jd.recoverFromJournal(Read)

	// This is a thread-safe operation.
	// Even if two different services (gateway and processor) perform this operation, there should not be any problem.
	jd.recoverFromJournal(ReadWrite)
	jd.assertError(func() error {
		err := jd.cleanupPreDropTables(ctx)
		if err != nil && ctx.Err() == nil {
			return err
		}
		return nil
	}())
	jd.assertError(jd.doRefreshDSRangeList(l))
	jd.assertError(func() error {
		err := jd.doCleanup(ctx, l)
		if err != nil && ctx.Err() == nil {
			return err
		}
		return nil
	}())
	jd.assertError(jd.loadReadExcludedPartitions())

	g := jd.backgroundGroup
	g.Go(crash.Wrapper(func() error {
		jd.refreshDSListLoop(ctx)
		return nil
	}))

	jd.startCompactionLoop(ctx)
	jd.startDropDSLoop(ctx)
}

func (jd *Handle) writerSetup(ctx context.Context, l lock.LockToken) {
	jd.recoverFromJournal(Write)
	// This is a thread-safe operation.
	// Even if two different services (gateway and processor) perform this operation, there should not be any problem.
	jd.recoverFromJournal(ReadWrite)
	jd.assertError(jd.doRefreshDSRangeList(l))

	// If no DS present, add one
	dsList, _ := jd.dsList.snapshot()
	if len(dsList) == 0 {
		jd.addNewDS(ctx, l, newDataSet(jd.tablePrefix, jd.computeNewIdxForAppend(l)))
	}

	jd.backgroundGroup.Go(crash.Wrapper(func() error {
		jd.addNewDSLoop(ctx)
		return nil
	}))
}

func (jd *Handle) readerWriterSetup(ctx context.Context, l lock.LockToken) {
	jd.recoverFromJournal(Read)

	jd.writerSetup(ctx, l)
	jd.assertError(func() error {
		err := jd.cleanupPreDropTables(ctx)
		if err != nil && ctx.Err() == nil {
			return err
		}
		return nil
	}())
	jd.assertError(func() error {
		err := jd.doCleanup(ctx, l)
		if err != nil && ctx.Err() == nil {
			return err
		}
		return nil
	}())
	jd.assertError(jd.loadReadExcludedPartitions())

	jd.startCompactionLoop(ctx)
	jd.startDropDSLoop(ctx)
}

// Stop stops the background goroutines and waits until they finish.
// Stop should be called once only after Start.
// Only Start and Close can be called after Stop.
func (jd *Handle) Stop() {
	jd.lifecycle.mu.Lock()
	defer jd.lifecycle.mu.Unlock()
	if jd.lifecycle.started {
		defer func() { jd.lifecycle.started = false }()
		jd.backgroundCancel()
		_ = jd.backgroundGroup.Wait()
	}
}

// TearDown stops the background goroutines,
//
//	waits until they finish and closes the database.
func (jd *Handle) TearDown() {
	jd.Stop()
	jd.Close()
}

// Close closes the database connection.
//
//	Stop should be called before Close.
//
//	Noop if the connection pool is shared with the handle.
func (jd *Handle) Close() {
	if !jd.sharedConnectionPool {
		if err := jd.dbHandle.Close(); err != nil {
			jd.logger.Errorn("error closing db connection", obskit.Error(err))
		}
	}
}

type transactionHandler interface {
	Exec(string, ...any) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	// If required, add other definitions that are common between *sql.DB and *sql.Tx
	// Never include Commit and Rollback in this interface
	// That ensures that whoever is acting on a transactionHandler can't commit or rollback
	// Only the function that passes *sql.Tx should do the commit or rollback based on the error it receives
}

func (jd *Handle) prepareAndExecStmtInTx(tx *sql.Tx, sqlStatement string) {
	stmt, err := tx.Prepare(sqlStatement)
	jd.assertError(err)
	defer func() { _ = stmt.Close() }()

	_, err = stmt.Exec()
	jd.assertError(err)
}

func (jd *Handle) startDropDSLoop(ctx context.Context) {
	jd.backgroundGroup.Go(crash.Wrapper(func() error {
		err := jd.dropDSLoop(ctx)
		if err != nil && ctx.Err() == nil {
			panic(fmt.Errorf("dropDSLoop for prefix %q: %w", jd.tablePrefix, err))
		}
		return nil
	}))
}

func (jd *Handle) dropDSLoop(ctx context.Context) error {
	nextDropDSEntry := func(ctx context.Context) (dropDSEntry, bool) {
		for {
			jd.dropDSListLock.RLock()
			if len(jd.dropDSList) > 0 {
				entry := jd.dropDSList[0]
				jd.dropDSListLock.RUnlock()
				return entry, true
			}
			jd.dropDSListLock.RUnlock()

			select {
			case <-ctx.Done():
				return dropDSEntry{}, false
			case <-jd.dropNotify:
			}
		}
	}
	for {
		entry, ok := nextDropDSEntry(ctx)
		if !ok {
			return nil
		}
		// Wait until all operations using this dataset are done.
		// This ensures that we don't drop a dataset which is currently being read.
		drained, err := jd.dsList.wait(entry.version)
		if err != nil {
			return fmt.Errorf("wait for dsList drain: %w", err)
		}
		select {
		case <-drained:
		case <-ctx.Done():
			return nil
		}
		// drop the dataset
		if err := jd.dropDSWithCtx(ctx, entry.ds); err != nil {
			return fmt.Errorf("dropDSWithCtx: %w", err)
		}
		// update the lists and cache
		if err := jd.dsListLock.WithLockInCtx(ctx, func(l lock.LockToken) error {
			jd.dropDSListLock.Lock()
			defer jd.dropDSListLock.Unlock()
			// Remove the entry from dropDSList
			jd.dropDSList = lo.Filter(jd.dropDSList, func(e dropDSEntry, _ int) bool {
				return e.version != entry.version || e.ds.Index != entry.ds.Index
			})
			// delete the entry from dsRangeFuncMap
			delete(jd.dsRangeFuncMap, entry.ds.Index)
			// Invalidate the distinctValuesCache for the dropped dataset
			jd.distinctValuesCache.RemoveDataset(entry.ds.JobTable)
			// If there are more datasets to drop, notify the dropDSLoop to check the next one
			if len(jd.dropDSList) > 0 {
				jd.dropNotifyPing()
			}
			return nil
		}); err != nil {
			return fmt.Errorf("removeDropDSEntry: %w", err)
		}
	}
}

func (jd *Handle) WithTx(ctx context.Context, f func(tx *Tx) error) error {
	return jd.withTxOnDB(ctx, jd.getDB(ctx), f)
}

// withMaintenanceTx runs f inside a transaction opened on the maintenance pool
// (or dbHandle if no maintenance pool is configured). Used by jobsdb-internal
// maintenance flows to keep their BeginTx off the main pool.
func (jd *Handle) withMaintenanceTx(ctx context.Context, f func(tx *Tx) error) error {
	return jd.withTxOnDB(ctx, jd.maintenanceDB(), f)
}

func (jd *Handle) withTxOnDB(ctx context.Context, db *sql.DB, f func(tx *Tx) error) error {
	sqltx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	tx := &Tx{Tx: sqltx}
	err = f(tx)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("%w; rollback error: %s", err, rollbackErr)
		}
		return err
	}
	return tx.Commit()
}

type moreToken struct {
	afterJobID *int64
}

var cacheParameterFilters = []string{"source_id", "destination_id"}

func (jd *Handle) addNewDSLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-jd.TriggerAddNewDS():
		}
		var dsListLock lock.LockToken
		var releaseDsListLock chan<- lock.LockToken
		addNewDS := func() error {
			ctx, cancel := context.WithTimeout(ctx, jd.conf.addNewDSTimeout.Load())
			defer cancel()
			defer func() {
				if releaseDsListLock != nil && dsListLock != nil {
					releaseDsListLock <- dsListLock
				}
			}()
			// Adding a new DS only creates a new DS & updates the cache. It doesn't move any data so we only take the list lock.
			// start a transaction
			err := jd.withMaintenanceTx(ctx, func(tx *Tx) error {
				return jd.withDistributedSharedLock(ctx, tx, "schema_migrate", func() error { // cannot run while schema migration is running
					return jd.withDistributedLock(ctx, tx, "add_ds", func() error { // only one add_ds can run at a time
						var err error
						// refresh ds list
						var dsList []dataSetT
						var nextDSIdx string
						// make sure we are operating on the latest version of the list
						dsList, err = getDSList(jd, tx, jd.tablePrefix)
						if err != nil {
							return fmt.Errorf("getDSList: %w", err)
						}
						latestDS := dsList[len(dsList)-1]
						full, err := jd.checkIfFullDSInTx(tx, latestDS)
						if err != nil {
							return fmt.Errorf("checkIfFullDSInTx: %w", err)
						}
						// checkIfFullDS is true for last DS in the list
						if full {
							// We acquire the list lock only after we have acquired the advisory lock.
							// We will release the list lock after the transaction ends, that's why we need to use an async lock
							dsListLock, releaseDsListLock, err = jd.dsListLock.AsyncLockWithCtx(ctx)
							if err != nil {
								return fmt.Errorf("acquiring dsListLock: %w", err)
							}
							jd.logger.Infon("[[ addNewDSLoop ]]: Acquired lock",
								logger.NewStringField("ds", latestDS.String()),
								logger.NewStringField("jobsdb", jd.tablePrefix))
							if _, err = tx.ExecContext(ctx, fmt.Sprintf(`LOCK TABLE %q IN EXCLUSIVE MODE;`, latestDS.JobTable)); err != nil {
								return fmt.Errorf("error locking table %s: %w", latestDS.JobTable, err)
							}

							nextDSIdx = jd.doComputeNewIdxForAppend(dsList)
							jd.logger.Infon("[[ addNewDSLoop ]]: NewDS", logger.NewStringField("tablePrefix", jd.tablePrefix))
							if err = jd.addNewDSInTx(ctx, tx, dsListLock, dsList, newDataSet(jd.tablePrefix, nextDSIdx)); err != nil {
								return fmt.Errorf("error adding new DS: %w", err)
							}

							// previous DS should become read only
							if err = setReadonlyDsInTx(ctx, tx, latestDS); err != nil {
								return fmt.Errorf("error making dataset read only: %w", err)
							}
						} else {
							// maybe another node added a new DS that we need to make visible to us
							if err := jd.refreshDSListWithDB(ctx, tx); err != nil {
								return fmt.Errorf("refreshDSList: %w", err)
							}
						}
						return nil
					})
				})
			})
			if err != nil {
				return fmt.Errorf("addNewDSLoop: %w", err)
			}
			// to get the updated DS list in the cache after createDS transaction has been committed.
			if dsListLock != nil {
				if err = jd.doRefreshDSRangeList(dsListLock); err != nil {
					return fmt.Errorf("refreshDSRangeList: %w", err)
				}
			}
			return nil
		}
		if err := addNewDS(); err != nil {
			if !jd.conf.skipMaintenanceError && ctx.Err() == nil {
				panic(fmt.Errorf("adding new ds for %q: %w", jd.tablePrefix, err))
			}
			jd.logger.Errorn("addNewDSLoop error", obskit.Error(err))
		}
	}
}

func (jd *Handle) getAdvisoryLockForOperation(operation string) int64 {
	key := fmt.Sprintf("%s_%s", jd.tablePrefix, operation)
	h := sha256.New()
	h.Write([]byte(key))
	return int64(binary.BigEndian.Uint32(h.Sum(nil)))
}

func (jd *Handle) refreshDSListLoop(ctx context.Context) {
	for {
		select {
		case <-jd.TriggerRefreshDS():
		case <-ctx.Done():
			return
		}
		timeoutCtx, cancel := context.WithTimeout(ctx, jd.conf.refreshDSTimeout.Load())
		if err := jd.RefreshDSList(timeoutCtx); err != nil {
			cancel()
			if !jd.conf.skipMaintenanceError && ctx.Err() == nil {
				panic(err)
			}
			jd.logger.Errorn("refreshDSListLoop error", obskit.Error(err))
		}
		cancel()
	}
}

// Identifier returns the identifier of the jobsdb. Here it is tablePrefix.
func (jd *Handle) Identifier() string {
	return jd.tablePrefix
}

// getDB returns the appropriate database handle based on context.
// If the context requests priority pool usage and a priority pool is configured,
// it returns the priority pool. Otherwise, it returns the regular dbHandle.
func (jd *Handle) getDB(ctx context.Context) *sql.DB {
	if usePriorityPool(ctx) && jd.priorityPool != nil {
		return jd.priorityPool
	}
	return jd.dbHandle
}

// maintenanceDB returns the connection pool to use for jobsdb-internal
// maintenance operations (compaction, dsList refresh,
// status-table cleanup/vacuum). Falls back to dbHandle when no dedicated
// maintenance pool was injected via WithMaintenancePoolDB.
func (jd *Handle) maintenanceDB() *sql.DB {
	if jd.maintenancePool != nil {
		return jd.maintenancePool
	}
	return jd.dbHandle
}

/*
printLists is a debugging function used to print
the current in-memory copy of jobs and job ranges
*/
func (jd *Handle) printLists(console bool) {
	dsList, dsRangeList := jd.dsList.snapshot()
	// This being an internal function, we don't lock
	if jd.logger.IsDebugLevel() {
		jd.logger.Debugn("printLists",
			logger.NewStringField("list", dsList.String()),
			logger.NewStringField("ranges", dsRangeList.String()),
		)
	}
	if console {
		fmt.Println("List:", dsList)
		fmt.Println("Ranges:", dsRangeList)
	}
}

func (jd *Handle) withDistributedLock(ctx context.Context, tx *Tx, operation string, f func() error) error {
	advisoryLock := jd.getAdvisoryLockForOperation(operation)
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`SELECT pg_advisory_xact_lock(%d);`, advisoryLock))
	if err != nil {
		return fmt.Errorf("error while acquiring advisory lock %d for operation %s: %w", advisoryLock, operation, err)
	}
	return f()
}

func (jd *Handle) withDistributedSharedLock(ctx context.Context, tx *Tx, operation string, f func() error) error {
	advisoryLock := jd.getAdvisoryLockForOperation(operation)
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`SELECT pg_advisory_xact_lock_shared(%d);`, advisoryLock))
	if err != nil {
		return fmt.Errorf("error while acquiring a shared advisory lock %d for operation %s: %w", advisoryLock, operation, err)
	}
	return f()
}

// DefaultParititionFunction is the default function to compute partition key for a job
func DefaultParititionFunction(job *JobT, numPartitions int) string {
	var partitionIdx uint32
	if numPartitions > 0 {
		partitionIdx, _ = partmap.Murmur3Partition32(job.UserID, uint32(numPartitions))
	}
	return job.WorkspaceId + "-" + strconv.Itoa(int(partitionIdx))
}
