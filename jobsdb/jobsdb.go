/*
Package jobsdb implements a dataset-based job store backed by PostgreSQL.

Jobs and job statuses are stored in prefixed dataset table pairs, e.g.
<prefix>_jobs_1 and <prefix>_job_status_1. Each pair is a dataset (dataSetT).
New jobs are appended to the latest dataset. When a dataset becomes full, JobsDB
creates a new dataset and makes the previous one read-only. Compaction copies
unfinished jobs from older datasets into an intermediate dataset and drops, or
asynchronously queues for drop, the old datasets.

Dataset job-id ranges are tracked in memory with dataSetRangeT. Reads use those
ranges, the dataset list, and no-result/distinct-value caches to avoid unnecessary
queries.
*/

package jobsdb

//go:generate mockgen -destination=../mocks/jobsdb/mock_jobsdb.go -package=mocks_jobsdb github.com/rudderlabs/rudder-server/jobsdb JobsDB

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/partmap"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/cache"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/dsindex"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
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

// HandleInspector is only intended to be used by tests for verifying the handle's internal state
type HandleInspector struct {
	*Handle
}

/*
JobsDB interface contains public methods to access JobsDB data
*/
type JobsDB interface {
	// Identifier returns the jobsdb's identifier, a.k.a. table prefix
	Identifier() string

	/* Commands */

	// WithTx begins a new transaction that can be used by the provided function.
	// If the function returns an error, the transaction will be rolled back and return the error,
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

	// GetPileUpCounts returns statistics of incomplete jobs grouped by workspace ID,
	// custom value, and destination ID.
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

type asserter interface {
	assert(cond bool, errorString string)
	assertError(err error)
}

/*
JobStatusT is used for storing the status of a job. The caller is responsible
for setting an appropriate job state.
*/
type JobStatusT struct {
	JobID         int64           `json:"JobID"`
	JobState      string          `json:"JobState"` // one of the valid jobStates; Unprocessed is represented by the absence of a status row
	AttemptNum    int             `json:"AttemptNum"`
	ExecTime      time.Time       `json:"ExecTime"`
	RetryTime     time.Time       `json:"RetryTime"`
	ErrorCode     string          `json:"ErrorCode"`
	ErrorResponse json.RawMessage `json:"ErrorResponse"`
	Parameters    json.RawMessage `json:"Parameters"`
	Consumer      string          `json:"Consumer"`
	JobParameters json.RawMessage `json:"-"`           // not stored in DB
	WorkspaceId   string          `json:"WorkspaceId"` // not stored in DB
	PartitionID   string          `json:"-"`           // not stored in DB
	CustomVal     string          `json:"-"`           // not stored in DB
}

type ConnectionID struct {
	SourceID      string
	DestinationID string
}

func (r *JobStatusT) sanitizeJson() error {
	var err error
	r.ErrorResponse, err = misc.SanitizeJSON(r.ErrorResponse)
	if err != nil {
		return err
	}

	r.Parameters, err = misc.SanitizeJSON(r.Parameters)
	if err != nil {
		return err
	}
	return nil
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
	Consumers     []string        `json:"Consumers"`
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
Handle is the main JobsDB implementation. Use NewForRead, NewForWrite,
NewForReadWrite, or Setup to initialize it.
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
	// cut short by maxCompactDSProbe. The next invocation resumes from here
	// instead of re-scanning from the beginning.
	// Only accessed from the single compactionLoop goroutine.
	lastCompactionProbeIndex *dsindex.Index
	noResultsCache           *cache.NoResultsCache[ParameterFilterT]

	excludedReadPartitionsLock sync.RWMutex
	excludedReadPartitions     map[string]struct{}

	// table count stats
	statTableCount        stats.Measurement
	statPreDropTableCount stats.Measurement

	statReadExcludedPartitionsCount stats.Gauge

	// ds creation and drop period stats
	statNewDSPeriod               stats.Measurement
	newDSCreationTime             time.Time
	statDropDSPeriod              stats.Measurement
	dsDropTime                    time.Time
	isStatNewDSPeriodInitialized  bool
	isStatDropDSPeriodInitialized bool

	backgroundCancel context.CancelFunc
	backgroundGroup  *errgroup.Group

	// skipSetupDBSetup is useful for testing as we mock the database client
	// TriggerAddNewDS, TriggerCompaction is useful for triggering addNewDS to run from tests.
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
		multiConsumer                   bool // if true, enables per-consumer indexes, views, and query paths
		warnOnStatusMissingPartitionID  config.ValueLoader[bool]
		holdDSListLockDuringStore       config.ValueLoader[bool] // escape hatch: hold the dsList read lock for the entire store callback
		staleDSListMaxRetries           config.ValueLoader[int]
		noResultsCacheStateOptimization config.ValueLoader[bool]
		// getJobsUseLateralJoin replaces the v_last_* view join in getJobsDS with a
		// correlated LATERAL subquery against the raw job_status table.
		getJobsUseLateralJoin config.ValueLoader[bool]
		dbTablesVersion       int // version of the database tables schema (0 means latest)

		compaction struct {
			maxCompactOnce, maxCompactDSProbe config.ValueLoader[int]
			vacuumFullStatusTableThreshold    config.ValueLoader[int64]
			vacuumAnalyzeStatusTableThreshold config.ValueLoader[int64]
			jobStatusCompactionThres          config.ValueLoader[float64]
			jobMinRowsLeftCompactionThreshold config.ValueLoader[float64]
			compactionLoopSleepDuration       config.ValueLoader[time.Duration]
			compactionTimeout                 config.ValueLoader[time.Duration]
			// compactionMinDSAge is the minimum age a partially-processed dataset
			// (one that needs pairing) must reach before it becomes eligible for
			// compaction, preventing freshly-created datasets (typically compaction
			// destinations) from being compacted again right away. Datasets without
			// a recorded creation time are always considered old enough.
			compactionMinDSAge config.ValueLoader[time.Duration]
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

// Identifier returns the identifier of the jobsdb. Here it is tablePrefix.
func (jd *Handle) Identifier() string {
	return jd.tablePrefix
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

// The default partition function computes the partition key for a job.
// The exported function name is misspelled for API compatibility.
func DefaultParititionFunction(job *JobT, numPartitions int) string {
	var partitionIdx uint32
	if numPartitions > 0 {
		partitionIdx, _ = partmap.Murmur3Partition32(job.UserID, uint32(numPartitions))
	}
	return job.WorkspaceId + "-" + strconv.Itoa(int(partitionIdx))
}
