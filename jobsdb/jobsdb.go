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
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/partmap"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/collectors"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/cache"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
	"github.com/rudderlabs/rudder-server/utils/crash"
	"github.com/rudderlabs/rudder-server/utils/misc"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

var errStaleDsList = errors.New("stale dataset list")

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

// QueryConditions holds jobsdb query conditions
type QueryConditions struct {
	// if IgnoreCustomValFiltersInQuery is true, CustomValFilters is not going to be used
	IgnoreCustomValFiltersInQuery bool
	CustomValFilters              []string
	ParameterFilters              []ParameterFilterT
	StateFilters                  []string
	AfterJobID                    *int64
}

// GetQueryParams is a struct to hold jobsdb query params.
type GetQueryParams struct {
	// query conditions

	// if IgnoreCustomValFiltersInQuery is true, CustomValFilters is not going to be used
	IgnoreCustomValFiltersInQuery bool
	WorkspaceID                   string
	CustomValFilters              []string
	ParameterFilters              []ParameterFilterT
	stateFilters                  []string
	afterJobID                    *int64

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

// StoreSafeTx sealed interface
type StoreSafeTx interface {
	Tx() *Tx
	SqlTx() *sql.Tx
	storeSafeTxIdentifier() string
}

type storeSafeTx struct {
	tx       *Tx
	identity string
}

func (r *storeSafeTx) storeSafeTxIdentifier() string {
	return r.identity
}

func (r *storeSafeTx) Tx() *Tx {
	return r.tx
}

func (r *storeSafeTx) SqlTx() *sql.Tx {
	return r.tx.Tx
}

// EmptyStoreSafeTx returns an empty interface usable only for tests
func EmptyStoreSafeTx() StoreSafeTx {
	return &storeSafeTx{tx: &Tx{}}
}

// UpdateSafeTx sealed interface
type UpdateSafeTx interface {
	Tx() *Tx
	SqlTx() *sql.Tx
	getDSList() []dataSetT
	getDSRangeList() []dataSetRangeT
	updateSafeTxSealIdentifier() string
}
type updateSafeTx struct {
	tx          *Tx
	identity    string
	dsList      []dataSetT
	dsRangeList []dataSetRangeT
}

func (r *updateSafeTx) updateSafeTxSealIdentifier() string {
	return r.identity
}

func (r *updateSafeTx) getDSList() []dataSetT {
	return r.dsList
}

func (r *updateSafeTx) getDSRangeList() []dataSetRangeT {
	return r.dsRangeList
}

func (r *updateSafeTx) Tx() *Tx {
	return r.tx
}

func (r *updateSafeTx) SqlTx() *sql.Tx {
	return r.tx.Tx
}

// EmptyUpdateSafeTx returns an empty interface usable only for tests
func EmptyUpdateSafeTx() UpdateSafeTx {
	return &updateSafeTx{tx: &Tx{}}
}

// HandleInspector is only intended to be used by tests for verifying the handle's internal state
type HandleInspector struct {
	*Handle
}

// DSIndicesList returns the slice of current ds indices
func (h *HandleInspector) DSIndicesList() []string {
	h.dsListLock.RLock()
	defer h.dsListLock.RUnlock()
	var indicesList []string
	for _, ds := range h.getDSList() {
		indicesList = append(indicesList, ds.Index)
	}

	return indicesList
}

// MoreToken is a token that can be used to fetch more jobs
type MoreToken interface{}

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
	WithTx(func(tx *Tx) error) error

	// WithStoreSafeTx prepares a store-safe environment and then starts a transaction
	// that can be used by the provided function.
	WithStoreSafeTx(context.Context, func(tx StoreSafeTx) error) error

	// WithStoreSafeTxFromTx prepares a store-safe environment for an existing transaction.
	WithStoreSafeTxFromTx(ctx context.Context, tx *Tx, f func(tx StoreSafeTx) error) error

	// Store stores the provided jobs to the database
	Store(ctx context.Context, jobList []*JobT) error

	// StoreInTx stores the provided jobs to the database using an existing transaction.
	// Please ensure that you are using an StoreSafeTx, e.g.
	//    jobsdb.WithStoreSafeTx(ctx, func(tx StoreSafeTx) error {
	//	      jobsdb.StoreInTx(ctx, tx, jobList)
	//    })
	StoreInTx(ctx context.Context, tx StoreSafeTx, jobList []*JobT) error

	// StoreEachBatchRetry tries to store all the provided job batches to the database
	//
	// returns the uuids of first job of each failed batch
	StoreEachBatchRetry(ctx context.Context, jobBatches [][]*JobT) map[uuid.UUID]string

	// StoreEachBatchRetryInTx tries to store all the provided job batches to the database, using an existing transaction.
	//
	// returns the uuids of first job of each failed batch
	//
	// Please ensure that you are using an StoreSafeTx, e.g.
	//    jobsdb.WithStoreSafeTx(func(tx StoreSafeTx) error {
	//	      jobsdb.StoreEachBatchRetryInTx(ctx, tx, jobBatches)
	//    })
	StoreEachBatchRetryInTx(ctx context.Context, tx StoreSafeTx, jobBatches [][]*JobT) (map[uuid.UUID]string, error)

	// WithUpdateSafeTx prepares an update-safe environment and then starts a transaction
	// that can be used by the provided function. An update-safe transaction shall be used if the provided function
	// needs to call UpdateJobStatusInTx.
	WithUpdateSafeTx(context.Context, func(tx UpdateSafeTx) error) error

	// UpdateJobStatus updates the provided job statuses
	UpdateJobStatus(ctx context.Context, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error

	// UpdateJobStatusInTx updates the provided job statuses in an existing transaction.
	// Please ensure that you are using an UpdateSafeTx, e.g.
	//    jobsdb.WithUpdateSafeTx(ctx, func(tx UpdateSafeTx) error {
	//	      jobsdb.UpdateJobStatusInTx(ctx, tx, statusList, customValFilters, parameterFilters)
	//    })
	UpdateJobStatusInTx(ctx context.Context, tx UpdateSafeTx, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error

	/* Queries */

	// GetJobs finds jobs in any of the provided state(s)
	GetJobs(ctx context.Context, states []string, params GetQueryParams) (JobsResult, error)

	// GetUnprocessed finds unprocessed jobs, i.e. new jobs whose state hasn't been marked in the database yet
	GetUnprocessed(ctx context.Context, params GetQueryParams) (JobsResult, error)

	// GetImporting finds jobs in importing state
	GetImporting(ctx context.Context, params GetQueryParams) (JobsResult, error)

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

	/* Journal */

	GetJournalEntries(opType string) (entries []JournalEntryT)
	JournalDeleteEntry(opID int64)
	JournalMarkStart(opType string, opPayload json.RawMessage) (int64, error)
	JournalMarkDone(opID int64) error

	IsMasterBackupEnabled() bool
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

/*
assertInterface contains public assert methods
*/
type assertInterface interface {
	assert(cond bool, errorString string)
	assertError(err error)
}

/*
UpdateJobStatusInTx updates the status of a batch of jobs in the past transaction
customValFilters[] is passed, so we can efficiently mark empty cache
Later we can move this to query
IMP NOTE: AcquireUpdateJobStatusLocks Should be called before calling this function
*/
func (jd *Handle) UpdateJobStatusInTx(ctx context.Context, tx UpdateSafeTx, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error {
	updateCmd := func(dsList []dataSetT, dsRangeList []dataSetRangeT) error {
		if len(statusList) == 0 {
			return nil
		}
		tags := statTags{CustomValFilters: customValFilters, ParameterFilters: parameterFilters}
		command := func() error {
			return jd.internalUpdateJobStatusInTx(ctx, tx.Tx(), dsList, dsRangeList, statusList, customValFilters, parameterFilters)
		}
		err := executeDbRequest(jd, newWriteDbRequest("update_job_status", &tags, command))
		return err
	}

	if tx.updateSafeTxSealIdentifier() != jd.Identifier() {
		return jd.inUpdateSafeCtx(ctx, func(dsList []dataSetT, dsRangeList []dataSetRangeT) error {
			return updateCmd(dsList, dsRangeList)
		})
	}
	return updateCmd(tx.getDSList(), tx.getDSRangeList())
}

/*
JobStatusT is used for storing status of the job. It is
the responsibility of the user of this module to set appropriate
job status. State can be one of
ENUM waiting, executing, succeeded, waiting_retry,  failed, aborted
*/
type JobStatusT struct {
	JobID         int64           `json:"JobID"`
	JobState      string          `json:"JobState"` // ENUM waiting, executing, succeeded, waiting_retry, filtered, failed, aborted, migrating, migrated, wont_migrate
	AttemptNum    int             `json:"AttemptNum"`
	ExecTime      time.Time       `json:"ExecTime"`
	RetryTime     time.Time       `json:"RetryTime"`
	ErrorCode     string          `json:"ErrorCode"`
	ErrorResponse json.RawMessage `json:"ErrorResponse"`
	Parameters    json.RawMessage `json:"Parameters"`
	JobParameters json.RawMessage `json:"-"`
	WorkspaceId   string          `json:"WorkspaceId"`
}

type ConnectionDetails struct {
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

// The struct fields need to be exposed to JSON package
type dataSetT struct {
	JobTable       string `json:"job"`
	JobStatusTable string `json:"status"`
	Index          string `json:"index"`
}

func (ds dataSetT) String() string {
	return "JobTable=" + ds.JobTable + ",JobStatusTable=" + ds.JobStatusTable + ",Index=" + ds.Index
}

type dataSetTList []dataSetT

func (l dataSetTList) String() string {
	sb := strings.Builder{}
	for i, ds := range l {
		if i > 0 {
			sb.WriteString(";")
		}
		sb.WriteString(ds.String())
	}
	return sb.String()
}

type dataSetRangeT struct {
	minJobID int64
	maxJobID int64
	ds       dataSetT
}

func (ds dataSetRangeT) String() string {
	return "minJobID=" + strconv.FormatInt(ds.minJobID, 10) + ",maxJobID=" + strconv.FormatInt(ds.maxJobID, 10) + ",ds=" + ds.ds.String()
}

type dataSetRangeTList []dataSetRangeT

func (l dataSetRangeTList) String() string {
	sb := strings.Builder{}
	for i, ds := range l {
		if i > 0 {
			sb.WriteString(";")
		}
		sb.WriteString(ds.String())
	}
	return sb.String()
}

type dsRangeMinMax struct {
	minJobID sql.NullInt64
	maxJobID sql.NullInt64
}

/*
Handle is the main type implementing the database for implementing
jobs. The caller must call the SetUp function on a Handle object
*/
type Handle struct {
	dbHandle             *sql.DB
	sharedConnectionPool bool
	ownerType            OwnerType
	tablePrefix          string
	logger               logger.Logger
	stats                stats.Stats

	datasetList         dataSetTList
	datasetRangeList    dataSetRangeTList
	dsRangeFuncMap      map[string]func() (dsRangeMinMax, error)
	distinctValuesCache *distinctValuesCache
	dsListLock          *lock.Locker
	dsMigrationLock     *lock.Locker
	noResultsCache      *cache.NoResultsCache[ParameterFilterT]

	// table count stats
	statTableCount        stats.Measurement
	statPreDropTableCount stats.Measurement

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
	// TODO: Remove this flag once we have test setup that uses real database
	skipSetupDBSetup bool

	// TriggerAddNewDS, TriggerMigrateDS is useful for triggering addNewDS to run from tests.
	// TODO: Ideally we should refactor the code to not use this override.
	TriggerAddNewDS  func() <-chan time.Time
	migrateDSPaused  atomic.Bool
	TriggerMigrateDS func() <-chan time.Time
	TriggerRefreshDS func() <-chan time.Time

	lifecycle struct {
		mu      sync.Mutex
		started bool
	}

	config *config.Config
	conf   struct {
		payloadColumnType              payloadColumnType
		maxTableSize                   config.ValueLoader[int64]
		cacheExpiration                config.ValueLoader[time.Duration]
		addNewDSLoopSleepDuration      config.ValueLoader[time.Duration]
		addNewDSTimeout                config.ValueLoader[time.Duration]
		refreshDSListLoopSleepDuration config.ValueLoader[time.Duration]
		refreshDSTimeout               config.ValueLoader[time.Duration]
		minDSRetentionPeriod           config.ValueLoader[time.Duration]
		maxDSRetentionPeriod           config.ValueLoader[time.Duration]
		jobMaxAge                      config.ValueLoader[time.Duration]
		writeCapacity                  chan struct{}
		readCapacity                   chan struct{}
		enableWriterQueue              bool
		enableReaderQueue              bool
		clearAll                       bool
		skipMaintenanceError           bool
		dsLimit                        config.ValueLoader[int]
		maxReaders                     int
		maxWriters                     int
		maxOpenConnections             int
		analyzeThreshold               config.ValueLoader[int]
		MaxDSSize                      config.ValueLoader[int]
		numPartitions                  int // if zero or negative, no partitioning
		partitionFunction              func(job *JobT) string
		dbTablesVersion                int // version of the database tables schema (0 means latest)

		migration struct {
			maxMigrateOnce, maxMigrateDSProbe config.ValueLoader[int]
			vacuumFullStatusTableThreshold    config.ValueLoader[int64]
			vacuumAnalyzeStatusTableThreshold config.ValueLoader[int64]
			jobStatusMigrateThres             config.ValueLoader[float64]
			jobMinRowsLeftMigrateThreshold    config.ValueLoader[float64]
			migrateDSLoopSleepDuration        config.ValueLoader[time.Duration]
			migrateDSTimeout                  config.ValueLoader[time.Duration]
		}
		backup struct {
			masterBackupEnabled config.ValueLoader[bool]
		}
	}
}

func (jd *Handle) IsMasterBackupEnabled() bool {
	return jd.conf.backup.masterBackupEnabled.Load()
}

// The struct which is written to the journal
type journalOpPayloadT struct {
	From []dataSetT `json:"from"`
	To   dataSetT   `json:"to"`
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
	for _, js := range jobStates {
		if !js.isValid {
			continue
		}
		if js.isTerminal {
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
			// numPartitions must be greater than 0 and power-of-two
			if numPartitions < 1 || (numPartitions&(numPartitions-1)) != 0 {
				panic(fmt.Errorf("invalid number of jobsdb partitions, needs to be power of two: %d", numPartitions))
			}
			jd.conf.numPartitions = numPartitions
			// default partition function using a 32-bit key space and Murmur3 hash
			if jd.conf.partitionFunction == nil {
				jd.conf.partitionFunction = func(job *JobT) string {
					var partitionIdx uint32
					if jd.conf.numPartitions > 0 {
						partitionIdx, _ = partmap.Murmur3Partition32(job.UserID, uint32(jd.conf.numPartitions))
					}
					return job.WorkspaceId + "-" + strconv.Itoa(int(partitionIdx))
				}
			}
		}
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
	jd.dsListLock = lock.NewLocker()
	jd.dsMigrationLock = lock.NewLocker()
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
				maxConns += 2 // migrate, refreshDsList
			case Write:
				maxConns += 1 // addNewDS
			case ReadWrite:
				maxConns += 3 // migrate, addNewDS, archive
			}
			if maxConns >= jd.conf.maxOpenConnections {
				maxConns = jd.conf.maxOpenConnections
			}
		}
		jd.dbHandle.SetMaxOpenConns(maxConns)

		jd.assertError(jd.dbHandle.Ping())
	}

	jd.workersAndAuxSetup()

	err := jd.WithTx(func(tx *Tx) error {
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
				templateData := func() map[string]interface{} {
					// Important: if jobsdb type is acting as a writer then refreshDSList
					// doesn't return the full list of datasets, only the rightmost two.
					// But we need to run the schema migration against all datasets, no matter
					// whether jobsdb is a writer or not.
					datasets, err := getDSList(jd, jd.dbHandle, jd.tablePrefix)
					jd.assertError(err)

					datasetIndices := make([]string, 0)
					for _, dataset := range datasets {
						datasetIndices = append(datasetIndices, dataset.Index)
					}

					return map[string]interface{}{
						"Prefix":   jd.tablePrefix,
						"Datasets": datasetIndices,
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
				err := jd.doRefreshDSRangeList(l)
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
	jd.statPreDropTableCount = jd.stats.NewTaggedStat("jobsdb.pre_drop_tables_count", stats.GaugeType, stats.Tags{"customVal": jd.tablePrefix})
	jd.statTableCount = jd.stats.NewTaggedStat("jobsdb.tables_count", stats.GaugeType, stats.Tags{"customVal": jd.tablePrefix})
	jd.statNewDSPeriod = jd.stats.NewTaggedStat("jobsdb.new_ds_period", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	jd.statDropDSPeriod = jd.stats.NewTaggedStat("jobsdb.drop_ds_period", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
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

	// migrationConfig

	// migrateDSLoopSleepDuration: How often is the loop (which checks for migrating DS) run
	jd.conf.migration.migrateDSLoopSleepDuration = jd.config.GetReloadableDurationVar(30, time.Second, jd.configKeys("migrateDSLoopSleepDuration", "migrateDSLoopSleepDurationInS")...)
	jd.conf.migration.migrateDSTimeout = jd.config.GetReloadableDurationVar(10, time.Minute, jd.configKeys("migrateDS.timeout")...)
	// jobStatusMigrateThres: A DS is migrated if the job_status exceeds this (* no_of_jobs)
	jd.conf.migration.jobStatusMigrateThres = jd.config.GetReloadableFloat64Var(3, jd.configKeys("jobStatusMigrateThreshold")...)
	// jobMinRowsLeftMigrateThreshold: A DS with a low number of pending rows should be eligible for migration if the number of pending rows are
	// less than jobMinRowsLeftMigrateThreshold percent of maxDSSize (e.g. if jobMinRowsLeftMigrateThreshold is 0.5
	// then DSs that have less than 50% of maxDSSize pending rows are eligible for migration)
	jd.conf.migration.jobMinRowsLeftMigrateThreshold = jd.config.GetReloadableFloat64Var(0.4, jd.configKeys("jobMinRowsLeftMigrateThreshold")...)
	// maxMigrateOnce: Maximum number of DSs that are migrated together into one destination
	jd.conf.migration.maxMigrateOnce = jd.config.GetReloadableIntVar(10, 1, jd.configKeys("maxMigrateOnce")...)
	// maxMigrateDSProbe: Maximum number of DSs that are checked from left to right if they are eligible for migration
	jd.conf.migration.maxMigrateDSProbe = jd.config.GetReloadableIntVar(10, 1, jd.configKeys("maxMigrateDSProbe")...)
	jd.conf.migration.vacuumFullStatusTableThreshold = jd.config.GetReloadableInt64Var(500*bytesize.MB, 1, jd.configKeys("vacuumFullStatusTableThreshold")...)
	jd.conf.migration.vacuumAnalyzeStatusTableThreshold = jd.config.GetReloadableInt64Var(30000, 1, jd.configKeys("vacuumAnalyzeStatusTableThreshold")...)

	// masterBackupEnabled = true => all the jobsdb are eligible for backup
	jd.conf.backup.masterBackupEnabled = jd.config.GetReloadableBoolVar(true, jd.configKeys("backup.enabled")...)

	// maxDSSize: Maximum size of a DS. The process which adds new DS runs in the background
	// (every few seconds) so a DS may go beyond this size
	// passing `maxDSSize` by reference, so it can be hot reloaded
	jd.conf.MaxDSSize = jd.config.GetReloadableIntVar(100000, 1, jd.configKeys("maxDSSize")...)

	if jd.TriggerAddNewDS == nil {
		jd.TriggerAddNewDS = func() <-chan time.Time {
			return time.After(jd.conf.addNewDSLoopSleepDuration.Load())
		}
	}

	if jd.TriggerMigrateDS == nil {
		jd.TriggerMigrateDS = func() <-chan time.Time {
			return time.After(jd.conf.migration.migrateDSLoopSleepDuration.Load())
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
	res := []string{
		"JobsDB." + jd.tablePrefix + "." + key,
		"JobsDB." + key,
	}
	for _, additionalKey := range additionalKeys {
		res = append(res, "JobsDB."+additionalKey)
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

	if !jd.skipSetupDBSetup {
		jd.setUpForOwnerType(ctx, jd.ownerType)
	}
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
	jd.assertError(jd.doRefreshDSRangeList(l))
	jd.assertError(func() error {
		err := jd.doCleanup(ctx)
		if err != nil && ctx.Err() == nil {
			return err
		}
		return nil
	}())

	g := jd.backgroundGroup
	g.Go(crash.Wrapper(func() error {
		jd.refreshDSListLoop(ctx)
		return nil
	}))

	jd.startMigrateDSLoop(ctx)
}

func (jd *Handle) writerSetup(ctx context.Context, l lock.LockToken) {
	jd.recoverFromJournal(Write)
	// This is a thread-safe operation.
	// Even if two different services (gateway and processor) perform this operation, there should not be any problem.
	jd.recoverFromJournal(ReadWrite)
	jd.assertError(jd.doRefreshDSRangeList(l))

	// If no DS present, add one
	if len(jd.getDSList()) == 0 {
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
		err := jd.doCleanup(ctx)
		if err != nil && ctx.Err() == nil {
			return err
		}
		return nil
	}())

	jd.startMigrateDSLoop(ctx)
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

/*
Function to return an ordered list of datasets and datasetRanges
Most callers use the in-memory list of dataset and datasetRanges
Caller must have the dsListLock readlocked
*/
func (jd *Handle) getDSList() dataSetTList {
	return jd.datasetList
}

// doRefreshDSList refreshes the ds list from the database
func (jd *Handle) doRefreshDSList(l lock.LockToken) (dataSetTList, error) {
	if l == nil {
		return nil, fmt.Errorf("cannot refresh DS list without a valid lock token")
	}
	var err error
	// Reset the global list
	if jd.datasetList, err = getDSList(jd, jd.dbHandle, jd.tablePrefix); err != nil {
		return nil, fmt.Errorf("getDSList %w", err)
	}
	// report table count metrics before shrinking the datasetList
	jd.statTableCount.Gauge(len(jd.datasetList))

	// if the owner of this jobsdb is a writer, then shrinking datasetList to have only last two datasets
	// this shrank datasetList is used to compute DSRangeList
	// This is done because, writers don't care about the left datasets in the sorted datasetList
	if jd.ownerType == Write {
		if len(jd.datasetList) > 2 {
			jd.datasetList = jd.datasetList[len(jd.datasetList)-2 : len(jd.datasetList)]
		}
	}

	return jd.datasetList, nil
}

func (jd *Handle) getDSRangeList() dataSetRangeTList {
	return jd.datasetRangeList
}

// doRefreshDSRangeList first refreshes the DS list and then calculate the DS range list
func (jd *Handle) doRefreshDSRangeList(l lock.LockToken) error {
	var prevMax int64

	// At this point we must have write-locked dsListLock
	dsList, err := jd.doRefreshDSList(l)
	if err != nil {
		return fmt.Errorf("refreshDSList %w", err)
	}
	var datasetRangeList dataSetRangeTList

	for idx := 0; idx < len(dsList)-1; idx++ {
		ds := dsList[idx]
		jd.assert(ds.Index != "", "ds.Index is empty")

		if _, ok := jd.dsRangeFuncMap[ds.Index]; !ok {
			getIndex := func() (sql.NullInt64, sql.NullInt64, error) {
				var minID, maxID sql.NullInt64
				sqlStatement := fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %q`, ds.JobTable)
				row := jd.dbHandle.QueryRow(sqlStatement)
				if err := row.Scan(&minID, &maxID); err != nil {
					return sql.NullInt64{}, sql.NullInt64{}, fmt.Errorf("scanning min & max jobID %w", err)
				}
				jd.logger.Debugn(sqlStatement,
					logger.NewIntField("minID", minID.Int64), logger.NewIntField("maxID", maxID.Int64))
				return minID, maxID, nil
			}
			jd.dsRangeFuncMap[ds.Index] = sync.OnceValues(func() (dsRangeMinMax, error) {
				minID, maxID, err := getIndex()
				if err != nil {
					return dsRangeMinMax{}, fmt.Errorf("getIndex %w", err)
				}
				return dsRangeMinMax{
					minJobID: minID,
					maxJobID: maxID,
				}, nil
			})
		}
		minMax, err := jd.dsRangeFuncMap[ds.Index]()
		if err != nil {
			return err
		}
		minID, maxID := minMax.minJobID, minMax.maxJobID

		// We store ranges EXCEPT for
		// 1. the last element (which is being actively written to)
		// 2. Migration target ds

		// Skipping asserts and updating prevMax if a ds is found to be empty
		// Happens if this function is called between addNewDS and populating data in two scenarios
		// Scenario-1: During internal migrations
		// Scenario-2: During scaleup scaledown
		if !minID.Valid || !maxID.Valid {
			continue
		}

		// TODO: Cleanup - Remove the line below and jd.inProgressMigrationTargetDS
		jd.assert(minID.Valid && maxID.Valid, fmt.Sprintf("minID.Valid: %v, maxID.Valid: %v. Either of them is false for table: %s", minID.Valid, maxID.Valid, ds.JobTable))
		jd.assert(idx == 0 || prevMax < minID.Int64, fmt.Sprintf("idx: %d != 0 and prevMax: %d >= minID.Int64: %v of table: %s", idx, prevMax, minID.Int64, ds.JobTable))
		datasetRangeList = append(datasetRangeList,
			dataSetRangeT{
				minJobID: minID.Int64,
				maxJobID: maxID.Int64,
				ds:       ds,
			})
		prevMax = maxID.Int64
	}
	jd.datasetRangeList = datasetRangeList
	return nil
}

func (jd *Handle) checkIfFullDSInTx(tx *Tx, ds dataSetT) (bool, error) {
	var (
		minJobCreatedAt sql.NullTime
		tableSize       int64
		rowCount        int
	)

	sqlStatement := fmt.Sprintf(
		`with combinedResult as (
			SELECT
			(SELECT created_at FROM %[1]q ORDER BY job_id ASC LIMIT 1) AS minJobCreatedAt,
			(SELECT PG_TOTAL_RELATION_SIZE('%[1]s')) AS tableSize,
			(SELECT COUNT(*) FROM %[1]q) AS rowCount
		)
		SELECT minJobCreatedAt, tableSize, rowCount FROM combinedResult`,
		ds.JobTable,
	)
	row := tx.QueryRow(sqlStatement)
	err := row.Scan(&minJobCreatedAt, &tableSize, &rowCount)
	if err != nil {
		return false, err
	}
	if !minJobCreatedAt.Valid {
		return false, nil
	}

	if jd.conf.maxDSRetentionPeriod.Load() > 0 {
		if time.Since(minJobCreatedAt.Time) >= jd.conf.maxDSRetentionPeriod.Load() {
			return true, nil
		}
	}

	if tableSize >= jd.conf.maxTableSize.Load() {
		jd.logger.Infon(
			"[JobsDB] DS full in size",
			logger.NewStringField("ds", ds.String()),
			logger.NewIntField("rowCount", int64(rowCount)),
			logger.NewIntField("tableSize", tableSize),
		)
		return true, nil
	}

	if rowCount >= jd.conf.MaxDSSize.Load() {
		jd.logger.Infon(
			"[JobsDB] DS full by rows",
			logger.NewStringField("ds", ds.String()),
			logger.NewIntField("rowCount", int64(rowCount)),
			logger.NewIntField("tableSize", tableSize),
		)
		return true, nil
	}

	return false, nil
}

/*
Function to add a new dataset. DataSet can be added to the end (e.g when last
becomes full OR in between during migration. DataSets are assigned numbers
monotonically when added  to end. So, with just add to end, numbers would be
like 1,2,3,4, and so on. These are called level0 datasets. And the Index is
called level0 Index
During internal migration, we add datasets in between. In the example above, if we migrate
1 & 2, we would need to create a new DS between 2 & 3. This is assigned the number 2_1.
This is called a level1 dataset and the Index (2_1) is called level1
Index. We may migrate 2_1 into 2_2 and so on so there may be multiple level 1 datasets.

Immediately after creating a level_1 dataset (2_1 above), everything prior to it is
deleted.
Hence, there should NEVER be any requirement for having more than two levels.

There is an exception to this. In case of cross node migration during a scale up/down,
we continue to accept new events in level0 datasets. To maintain the ordering guarantee,
we write the imported jobs to the previous level1 datasets. Now if an internal migration
is to happen on one of the level1 dataset, we have to migrate them to level2 dataset

Eg. When the node has 1, 2, 3, 4 data sets and an import is triggered, new events start
going to 5, 6, 7... so on. And the imported data start going to 4_1, 4_2, 4_3... so on
Now if an internal migration is to happen and we migrate 1, 2, 3, 4, 4_1, we need to
create a newDS between 4_1 and 4_2. This is assigned to 4_1_1, 4_1_2 and so on.
*/

func mapDSToLevel(ds dataSetT) (levelInt int, levelVals []int, err error) {
	indexStr := strings.Split(ds.Index, "_")
	// Currently we don't have a scenario where we need more than 3 levels.
	if len(indexStr) > 3 {
		err = fmt.Errorf("len(indexStr): %d > 3", len(indexStr))
		return levelInt, levelVals, err
	}
	for _, str := range indexStr {
		levelInt, err = strconv.Atoi(str)
		if err != nil {
			return levelInt, levelVals, err
		}
		levelVals = append(levelVals, levelInt)
	}
	return len(levelVals), levelVals, nil
}

func newDataSet(tablePrefix, dsIdx string) dataSetT {
	jobTable := fmt.Sprintf("%s_jobs_%s", tablePrefix, dsIdx)
	jobStatusTable := fmt.Sprintf("%s_job_status_%s", tablePrefix, dsIdx)
	return dataSetT{
		JobTable:       jobTable,
		JobStatusTable: jobStatusTable,
		Index:          dsIdx,
	}
}

func (jd *Handle) addNewDS(ctx context.Context, l lock.LockToken, ds dataSetT) {
	err := jd.WithTx(func(tx *Tx) error {
		dsList, err := jd.doRefreshDSList(l)
		jd.assertError(err)
		return jd.addNewDSInTx(ctx, tx, l, dsList, ds)
	})
	jd.assertError(err)
	jd.assertError(jd.doRefreshDSRangeList(l))
}

// NOTE: If addNewDSInTx is directly called, make sure to explicitly call refreshDSRangeList(l) to update the DS list in cache, once transaction has completed.
func (jd *Handle) addNewDSInTx(ctx context.Context, tx *Tx, l lock.LockToken, dsList []dataSetT, ds dataSetT) error {
	defer jd.getTimerStat(
		"add_new_ds",
		&statTags{CustomValFilters: []string{jd.tablePrefix}},
	).RecordDuration()()
	if l == nil {
		return errors.New("nil ds list lock token provided")
	}
	jd.logger.Infon("Creating new DS", logger.NewStringField("ds", ds.String()))
	err := jd.createDSInTx(ctx, tx, ds)
	if err != nil {
		return err
	}
	err = jd.setSequenceNumberInTx(tx, l, dsList, ds.Index)
	if err != nil {
		return err
	}
	// Tracking time interval between new ds creations. Hence calling end before start
	if jd.isStatNewDSPeriodInitialized {
		jd.statNewDSPeriod.Since(jd.newDSCreationTime)
	}
	jd.newDSCreationTime = time.Now()
	jd.isStatNewDSPeriodInitialized = true

	return nil
}

func (jd *Handle) computeNewIdxForAppend(l lock.LockToken) string {
	dList, err := jd.doRefreshDSList(l)
	jd.assertError(err)
	return jd.doComputeNewIdxForAppend(dList)
}

func (jd *Handle) doComputeNewIdxForAppend(dList []dataSetT) string {
	var newDSIdx string
	if len(dList) == 0 {
		newDSIdx = "1"
	} else {
		levels, levelVals, err := mapDSToLevel(dList[len(dList)-1])
		jd.assertError(err)
		// Last one can only be Level0
		jd.assert(levels == 1, fmt.Sprintf("levels:%d != 1", levels))
		newDSIdx = fmt.Sprintf("%d", levelVals[0]+1)
	}
	return newDSIdx
}

type transactionHandler interface {
	Exec(string, ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	// If required, add other definitions that are common between *sql.DB and *sql.Tx
	// Never include Commit and Rollback in this interface
	// That ensures that whoever is acting on a transactionHandler can't commit or rollback
	// Only the function that passes *sql.Tx should do the commit or rollback based on the error it receives
}

func (jd *Handle) createDSInTx(ctx context.Context, tx *Tx, newDS dataSetT) error {
	// Mark the start of operation. If we crash somewhere here, we delete the
	// DS being added
	opPayload, err := jsonrs.Marshal(&journalOpPayloadT{To: newDS})
	if err != nil {
		return err
	}

	opID, err := jd.JournalMarkStartInTx(tx, addDSOperation, opPayload)
	if err != nil {
		return err
	}

	// Create the jobs and job_status tables
	if err = jd.createDSTablesInTx(ctx, tx, newDS); err != nil {
		return fmt.Errorf("creating DS tables %w", err)
	}
	if err = jd.createDSIndicesInTx(ctx, tx, newDS); err != nil {
		return fmt.Errorf("creating DS indices %w", err)
	}

	err = jd.journalMarkDoneInTx(tx, opID)
	if err != nil {
		return err
	}
	return nil
}

func (jd *Handle) createDSTablesInTx(ctx context.Context, tx *Tx, newDS dataSetT) error {
	var columnType payloadColumnType
	switch jd.conf.payloadColumnType {
	case JSONB:
		columnType = JSONB
	case BYTEA:
		columnType = BYTEA
	case TEXT:
		columnType = TEXT
	default:
		columnType = JSONB
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %q (
		job_id BIGSERIAL PRIMARY KEY,
		workspace_id TEXT NOT NULL DEFAULT '',
		uuid UUID NOT NULL,
		user_id TEXT NOT NULL,
		partition_id TEXT NOT NULL DEFAULT '',
		parameters JSONB NOT NULL,
		custom_val VARCHAR(64) NOT NULL,
		event_payload `+string(columnType)+` NOT NULL,
		event_count INTEGER NOT NULL DEFAULT 1,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		expire_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());`, newDS.JobTable)); err != nil {
		return fmt.Errorf("creating %s: %w", newDS.JobTable, err)
	}

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %q (
		id BIGSERIAL,
		job_id BIGINT,
		job_state VARCHAR(64),
		attempt SMALLINT,
		exec_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		retry_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		error_code VARCHAR(32),
		error_response JSONB DEFAULT '{}'::JSONB,
		parameters JSONB DEFAULT '{}'::JSONB);`, newDS.JobStatusTable)); err != nil {
		return fmt.Errorf("creating %s: %w", newDS.JobStatusTable, err)
	}
	return nil
}

func (jd *Handle) createDSIndicesInTx(ctx context.Context, tx *Tx, newDS dataSetT) error {
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_ws" ON %[1]q (workspace_id)`, newDS.JobTable)); err != nil {
		return fmt.Errorf("creating workspace_id index: %w", err)
	}
	if jd.conf.numPartitions > 0 {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_partid" ON %[1]q (partition_id)`, newDS.JobTable)); err != nil {
			return fmt.Errorf("creating partition_id index: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_cv" ON %[1]q (custom_val)`, newDS.JobTable)); err != nil {
		return fmt.Errorf("creating custom_val index: %w", err)
	}
	for _, param := range cacheParameterFilters {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_%[2]s" ON %[1]q USING BTREE ((parameters->>'%[2]s'))`, newDS.JobTable, param)); err != nil {
			return fmt.Errorf("creating %s index: %w", param, err)
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_jid_id_js" ON %[1]q(job_id asc,id desc,job_state)`, newDS.JobStatusTable)); err != nil {
		return fmt.Errorf("adding job_id_id index: %w", err)
	}
	// index used for maxDSRetention during migration
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_id_js" ON %[1]q(id ,job_state) INCLUDE (exec_time)`, newDS.JobStatusTable)); err != nil {
		return fmt.Errorf("adding job_id_js index: %w", err)
	}

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE VIEW "v_last_%[1]s" AS SELECT DISTINCT ON (job_id) * FROM %[1]q ORDER BY job_id ASC, id DESC`, newDS.JobStatusTable)); err != nil {
		return fmt.Errorf("create view: %w", err)
	}
	return nil
}

func (jd *Handle) setSequenceNumberInTx(tx *Tx, l lock.LockToken, dsList []dataSetT, newDSIdx string) error {
	if l == nil {
		return errors.New("nil ds list lock token provided")
	}

	var maxID sql.NullInt64

	// Now set the min JobID for the new DS just added to be 1 more than previous max
	if len(dsList) > 0 {
		sqlStatement := fmt.Sprintf(`SELECT MAX(job_id) FROM %q`, dsList[len(dsList)-1].JobTable)
		err := tx.QueryRowContext(context.TODO(), sqlStatement).Scan(&maxID)
		if err != nil {
			return err
		}

		newDSMin := maxID.Int64 + 1
		sqlStatement = fmt.Sprintf(`ALTER SEQUENCE "%[1]s_jobs_%[2]s_job_id_seq" MINVALUE %[3]d START %[3]d RESTART %[3]d`,
			jd.tablePrefix, newDSIdx, newDSMin)
		_, err = tx.ExecContext(context.TODO(), sqlStatement)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetMaxDSIndex returns max dataset index in the DB
func (jd *Handle) GetMaxDSIndex() (maxDSIndex int64) {
	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()

	// dList is already sorted.
	dList := jd.getDSList()
	ds := dList[len(dList)-1]
	maxDSIndex, err := strconv.ParseInt(ds.Index, 10, 64)
	if err != nil {
		panic(err)
	}

	return maxDSIndex
}

func (jd *Handle) prepareAndExecStmtInTx(tx *sql.Tx, sqlStatement string) {
	stmt, err := tx.Prepare(sqlStatement)
	jd.assertError(err)
	defer func() { _ = stmt.Close() }()

	_, err = stmt.Exec()
	jd.assertError(err)
}

func (jd *Handle) prepareAndExecStmtInTxAllowMissing(tx *sql.Tx, sqlStatement string) {
	const (
		savepointSql = "SAVEPOINT prepareAndExecStmtInTxAllowMissing"
		rollbackSql  = "ROLLBACK TO " + savepointSql
	)

	stmt, err := tx.Prepare(sqlStatement)
	jd.assertError(err)
	defer func() { _ = stmt.Close() }()

	_, err = tx.Exec(savepointSql)
	jd.assertError(err)

	_, err = stmt.Exec()
	if err != nil {
		var pqError *pq.Error
		ok := errors.As(err, &pqError)
		if ok && pqError.Code == ("42P01") {
			jd.logger.Infon("sql statement exec failed because table doesn't exist",
				logger.NewStringField("tablePrefix", jd.tablePrefix),
				logger.NewStringField("sqlStatement", sqlStatement),
			)
			_, err = tx.Exec(rollbackSql)
			jd.assertError(err)
		} else {
			jd.assertError(err)
		}
	}
}

func (jd *Handle) dropDS(ds dataSetT) error {
	return jd.WithTx(func(tx *Tx) error {
		return jd.dropDSInTx(tx, ds)
	})
}

// dropDS drops a dataset
func (jd *Handle) dropDSInTx(tx *Tx, ds dataSetT) error {
	var err error
	if _, err = tx.Exec(fmt.Sprintf(`LOCK TABLE %q IN ACCESS EXCLUSIVE MODE;`, ds.JobStatusTable)); err != nil {
		return err
	}
	if _, err = tx.Exec(fmt.Sprintf(`LOCK TABLE %q IN ACCESS EXCLUSIVE MODE;`, ds.JobTable)); err != nil {
		return err
	}
	if _, err = tx.Exec(fmt.Sprintf(`DROP TABLE %q CASCADE`, ds.JobStatusTable)); err != nil {
		return err
	}
	if _, err = tx.Exec(fmt.Sprintf(`DROP TABLE %q CASCADE`, ds.JobTable)); err != nil {
		return err
	}
	jd.postDropDs(ds)
	return nil
}

// Drop a dataset and ignore if a table is missing
func (jd *Handle) dropDSForRecovery(ds dataSetT) {
	var sqlStatement string
	var err error
	tx, err := jd.dbHandle.Begin()
	jd.assertError(err)
	sqlStatement = fmt.Sprintf(`LOCK TABLE %q IN ACCESS EXCLUSIVE MODE;`, ds.JobStatusTable)
	jd.prepareAndExecStmtInTxAllowMissing(tx, sqlStatement)

	sqlStatement = fmt.Sprintf(`LOCK TABLE %q IN ACCESS EXCLUSIVE MODE;`, ds.JobTable)
	jd.prepareAndExecStmtInTxAllowMissing(tx, sqlStatement)

	sqlStatement = fmt.Sprintf(`DROP TABLE IF EXISTS %q CASCADE`, ds.JobStatusTable)
	jd.prepareAndExecStmtInTx(tx, sqlStatement)
	sqlStatement = fmt.Sprintf(`DROP TABLE IF EXISTS %q CASCADE`, ds.JobTable)
	jd.prepareAndExecStmtInTx(tx, sqlStatement)
	err = tx.Commit()
	jd.assertError(err)
}

func (jd *Handle) postDropDs(ds dataSetT) {
	jd.noResultsCache.InvalidateDataset(ds.Index)

	// Tracking time interval between drop ds operations. Hence calling end before start
	if jd.isStatDropDSPeriodInitialized {
		jd.statDropDSPeriod.Since(jd.dsDropTime)
	}
	jd.dsDropTime = time.Now()
	jd.isStatDropDSPeriodInitialized = true
}

func (jd *Handle) dropAllDS(l lock.LockToken) error {
	var err error
	dList, err := getDSList(jd, jd.dbHandle, jd.tablePrefix)
	if err != nil {
		return fmt.Errorf("getDSList: %w", err)
	}
	for _, ds := range dList {
		if err = jd.dropDS(ds); err != nil {
			return fmt.Errorf("dropDS: %w", err)
		}
	}

	// Update the lists
	if err = jd.doRefreshDSRangeList(l); err != nil {
		return fmt.Errorf("refreshDSRangeList: %w", err)
	}
	return nil
}

func (jd *Handle) internalStoreJobsInTx(ctx context.Context, tx *Tx, ds dataSetT, jobList []*JobT) error {
	tags := &statTags{CustomValFilters: []string{jd.tablePrefix}}
	defer jd.getTimerStat(
		"store_jobs",
		tags,
	).RecordDuration()()

	tx.AddSuccessListener(func() {
		jd.invalidateCacheForJobs(ds, jobList)
	})
	tx.AddSuccessListener(func() {
		statTags := tags.getStatsTags(jd.tablePrefix)
		jd.stats.NewTaggedStat("jobsdb_stored_jobs", stats.CountType, statTags).Count(len(jobList))
		jd.stats.NewTaggedStat("jobsdb_stored_bytes", stats.CountType, statTags).Count(lo.SumBy(jobList, func(j *JobT) int { return len(j.EventPayload) }))
	})

	return jd.doStoreJobsInTx(ctx, tx, ds, jobList)
}

func (jd *Handle) WithStoreSafeTx(ctx context.Context, f func(tx StoreSafeTx) error) error {
	return jd.inStoreSafeCtx(ctx, func() error {
		return jd.WithTx(func(tx *Tx) error { return f(&storeSafeTx{tx: tx, identity: jd.tablePrefix}) })
	})
}

func (jd *Handle) WithStoreSafeTxFromTx(ctx context.Context, tx *Tx, f func(tx StoreSafeTx) error) error {
	return jd.inStoreSafeCtx(ctx, func() error {
		return f(&storeSafeTx{tx: tx, identity: jd.tablePrefix})
	})
}

func (jd *Handle) inStoreSafeCtx(ctx context.Context, f func() error) error {
	// Only locks the list
	op := func() error {
		if !jd.dsListLock.RTryLockWithCtx(ctx) {
			return fmt.Errorf("could not acquire a dslist read lock: %w", ctx.Err())
		}
		defer jd.dsListLock.RUnlock()
		return f()
	}
	for {
		err := op()
		if err != nil && errors.Is(err, errStaleDsList) {
			jd.logger.Errorn("[JobsDB] :: Store failed. Retrying after refreshing DS cache", obskit.Error(errStaleDsList))
			if err := jd.dsListLock.WithLockInCtx(ctx, func(l lock.LockToken) error {
				err = jd.doRefreshDSRangeList(l)
				if err != nil {
					return fmt.Errorf("refreshing ds list: %w", err)
				}
				return nil
			}); err != nil {
				return err
			}
		} else {
			return err
		}
	}
}

func (jd *Handle) WithUpdateSafeTx(ctx context.Context, f func(tx UpdateSafeTx) error) error {
	return jd.inUpdateSafeCtx(ctx, func(dsList []dataSetT, dsRangeList []dataSetRangeT) error {
		return jd.WithTx(func(tx *Tx) error {
			return f(&updateSafeTx{
				tx:          tx,
				identity:    jd.tablePrefix,
				dsList:      dsList,
				dsRangeList: dsRangeList,
			})
		})
	})
}

func (jd *Handle) inUpdateSafeCtx(ctx context.Context, f func(dsList []dataSetT, dsRangeList []dataSetRangeT) error) error {
	// The order of lock is very important. The migrateDSLoop
	// takes lock in this order so reversing this will cause
	// deadlocks
	if !jd.dsMigrationLock.RTryLockWithCtx(ctx) {
		return fmt.Errorf("could not acquire a migration read lock: %w", ctx.Err())
	}
	defer jd.dsMigrationLock.RUnlock()

	if !jd.dsListLock.RTryLockWithCtx(ctx) {
		return fmt.Errorf("could not acquire a dslist read lock: %w", ctx.Err())
	}
	dsList := jd.getDSList()
	dsRangeList := jd.getDSRangeList()
	jd.dsListLock.RUnlock()
	return f(dsList, dsRangeList)
}

func (jd *Handle) WithTx(f func(tx *Tx) error) error {
	sqltx, err := jd.dbHandle.Begin()
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

func (jd *Handle) invalidateCacheForJobs(ds dataSetT, jobList []*JobT) {
	cacheKeys := make(map[string]map[string]map[string]struct{})
	for _, job := range jobList {
		workspace := job.WorkspaceId
		customVal := job.CustomVal

		if _, ok := cacheKeys[workspace]; !ok {
			cacheKeys[workspace] = make(map[string]map[string]struct{})
		}
		if _, ok := cacheKeys[workspace][customVal]; !ok {
			cacheKeys[workspace][customVal] = make(map[string]struct{})
		}

		var params []string
		var parameterFilters []ParameterFilterT

		for _, key := range cacheParameterFilters {
			val := gjson.GetBytes(job.Parameters, key).String()
			params = append(params, key+":"+val)
			parameterFilters = append(parameterFilters, ParameterFilterT{Name: key, Value: val})
		}

		paramsKey := strings.Join(params, "#")
		if _, ok := cacheKeys[workspace][customVal][paramsKey]; !ok {
			cacheKeys[workspace][customVal][paramsKey] = struct{}{}
			jd.noResultsCache.Invalidate(ds.Index, workspace, []string{customVal}, []string{Unprocessed.State}, parameterFilters)
		}
	}
}

type moreToken struct {
	afterJobID *int64
}

func (jd *Handle) GetToProcess(ctx context.Context, params GetQueryParams, more MoreToken) (*MoreJobsResult, error) { // skipcq: CRT-P0003

	if params.JobsLimit == 0 {
		return &MoreJobsResult{More: more}, nil
	}
	params.stateFilters = []string{Failed.State, Waiting.State, Unprocessed.State}
	slices.Sort(params.stateFilters)
	tags := statTags{
		StateFilters:     params.stateFilters,
		CustomValFilters: params.CustomValFilters,
		WorkspaceID:      params.WorkspaceID,
		ParameterFilters: params.ParameterFilters,
		MoreToken:        more != nil,
	}
	command := func() moreQueryResult {
		return moreQueryResultWrapper(jd.getJobs(ctx, params, more))
	}
	res := executeDbRequest(jd, newReadDbRequest("get_jobs", &tags, command))
	return res.MoreJobsResult, res.err
}

var cacheParameterFilters = []string{"source_id", "destination_id"}

func (jd *Handle) GetPileUpCounts(ctx context.Context, cutoffTime time.Time, increaseFunc rmetrics.IncreasePendingEventsFunc) error {
	// pause migration to avoid any read locks being blocked during pileup count
	jd.migrateDSPaused.Store(true)
	defer jd.migrateDSPaused.Store(false)
	if !jd.dsMigrationLock.RTryLockWithCtx(ctx) {
		return fmt.Errorf("could not acquire a migration read lock: %w", ctx.Err())
	}
	defer jd.dsMigrationLock.RUnlock()
	if !jd.dsListLock.RTryLockWithCtx(ctx) {
		return fmt.Errorf("could not acquire a dslist read lock: %w", ctx.Err())
	}
	dsList := jd.getDSList()
	jd.dsListLock.RUnlock()

	queryString := `WITH pending AS (
	SELECT
		j.workspace_id AS workspace_id,
		j.custom_val AS custom_val
	FROM
		%[1]q j
		LEFT JOIN (SELECT DISTINCT ON (job_id) job_id, job_state FROM %[2]q WHERE exec_time < $1 ORDER BY job_id ASC, id DESC) s ON j.job_id = s.job_id
	WHERE
		s.job_id is null OR s.job_state = ANY($2)
)
SELECT
	workspace_id,
	custom_val,
	COUNT(*)
FROM pending GROUP BY workspace_id, custom_val`

	g, ctx := errgroup.WithContext(ctx)
	const defaultConcurrency = 4
	conc := jd.config.GetIntVar(defaultConcurrency, 1, jd.configKeys("pileupCountConcurrency")...)
	if conc < 1 || conc > defaultConcurrency {
		jd.logger.Warnn("GetPileUpCounts concurrency out of safe bounds, using default value",
			logger.NewIntField("concurrency", int64(conc)),
			logger.NewIntField("default", int64(defaultConcurrency)),
		)
		conc = defaultConcurrency
	}
	g.SetLimit(conc)
	for _, ds := range dsList {
		ds := ds
		g.Go(func() error {
			rows, err := jd.dbHandle.QueryContext(ctx, fmt.Sprintf(queryString, ds.JobTable, ds.JobStatusTable),
				cutoffTime,
				pq.Array([]string{Executing.State, Failed.State, Importing.State, Waiting.State}))
			if err != nil {
				return fmt.Errorf("getting pileup counts for %q: %w", ds.JobTable, err)
			}
			defer func() {
				_ = rows.Close()
			}()
			for rows.Next() {
				var (
					workspace, customVal sql.NullString
					count                sql.NullInt64
				)
				if err := rows.Scan(&workspace, &customVal, &count); err != nil {
					return fmt.Errorf("scanning pileup counts rows for %q: %w", ds.JobTable, err)
				}
				if count.Valid {
					increaseFunc(jd.tablePrefix, workspace.String, customVal.String, float64(count.Int64))
				}
			}
			if err = rows.Err(); err != nil {
				return fmt.Errorf("iterating pileup counts for %q: %w", ds.JobTable, err)
			}
			return nil
		})
	}
	return g.Wait()
}

func (jd *Handle) getDistinctValuesPerDataset(
	ctx context.Context,
	dsList []string,
	param ParameterName,
	customVal string,
) (map[string][]string, error) {
	var queries []string
	for _, ds := range dsList {
		if customVal == "" {
			queries = append(queries, fmt.Sprintf(`SELECT '%[2]s', * FROM (
				WITH RECURSIVE t AS (
					(SELECT %[1]s as parameter FROM %[2]q ORDER BY %[1]s LIMIT 1)
					UNION ALL
					(
						SELECT s.* FROM t, LATERAL(
							SELECT %[1]s as parameter FROM %[2]q f
							WHERE f.%[1]s > t.parameter
							ORDER BY %[1]s LIMIT 1
							)s
						)
					)
				SELECT * FROM t) a`, param.string(), ds))
		} else {
			queries = append(queries, fmt.Sprintf(`SELECT '%[2]s', * FROM (
				WITH RECURSIVE t AS (
					(SELECT %[1]s as parameter FROM %[2]q WHERE custom_val = '%[3]s' ORDER BY %[1]s LIMIT 1)
					UNION ALL
					(
						SELECT s.* FROM t, LATERAL(
							SELECT %[1]s as parameter FROM %[2]q f
							WHERE custom_val = '%[3]s' AND f.%[1]s > t.parameter
							ORDER BY %[1]s LIMIT 1
							)s
						)
					)
				SELECT * FROM t) a`, param.string(), ds, customVal))
		}
	}
	query := strings.Join(queries, " UNION ")
	rows, err := jd.dbHandle.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("couldn't query distinct parameter-%s: %w", param.string(), err)
	}
	defer func() { _ = rows.Close() }()
	result := make(map[string][]string)
	for rows.Next() {
		var (
			ds    string
			value string
		)
		err := rows.Scan(&ds, &value)
		if err != nil {
			return nil, fmt.Errorf("couldn't scan distinct parameter-%s: %w", param.string(), err)
		}
		result[ds] = append(result[ds], value)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows.Err() on distinct parameter-%s: %w", param.string(), err)
	}
	return result, nil
}

func (jd *Handle) GetDistinctParameterValues(ctx context.Context, parameter ParameterName, customValFilter string) ([]string, error) {
	if !jd.dsMigrationLock.RTryLockWithCtx(ctx) {
		return nil, fmt.Errorf("could not acquire a migration read lock: %w", ctx.Err())
	}
	defer jd.dsMigrationLock.RUnlock()
	if !jd.dsListLock.RTryLockWithCtx(ctx) {
		return nil, fmt.Errorf("could not acquire a dslist read lock: %w", ctx.Err())
	}
	dsList := jd.getDSList()
	jd.dsListLock.RUnlock()
	return jd.distinctValuesCache.GetDistinctValues(
		parameter.string()+customValFilter,
		lo.Map(dsList, func(ds dataSetT, _ int) string { return ds.JobTable }),
		func(datasets []string) (map[string][]string, error) {
			return jd.getDistinctValuesPerDataset(ctx, datasets, parameter, customValFilter)
		},
	)
}

func (jd *Handle) doStoreJobsInTx(ctx context.Context, tx *Tx, ds dataSetT, jobList []*JobT) error {
	store := func() error {
		var stmt *sql.Stmt
		var err error

		stmt, err = tx.PrepareContext(ctx, pq.CopyIn(ds.JobTable, "uuid", "user_id", "custom_val", "parameters", "event_payload", "event_count", "workspace_id", "partition_id"))
		if err != nil {
			return err
		}

		defer func() { _ = stmt.Close() }()
		for _, job := range jobList {
			eventCount := 1
			if job.EventCount > 1 {
				eventCount = job.EventCount
			}
			// Assign partition ID if not already assigned
			if job.PartitionID == "" && jd.conf.numPartitions > 0 {
				job.PartitionID = jd.conf.partitionFunction(job)
			}
			if _, err = stmt.ExecContext(ctx, job.UUID, job.UserID, job.CustomVal, string(job.Parameters), string(job.EventPayload), eventCount, job.WorkspaceId, job.PartitionID); err != nil {
				return err
			}
		}
		if _, err = stmt.ExecContext(ctx); err != nil {
			return err
		}
		if len(jobList) > jd.conf.analyzeThreshold.Load() {
			_, err = tx.ExecContext(ctx, fmt.Sprintf(`ANALYZE %q`, ds.JobTable))
		}

		return err
	}
	const (
		savepointSql = "SAVEPOINT doStoreJobsInTx"
		rollbackSql  = "ROLLBACK TO " + savepointSql
	)
	if _, err := tx.ExecContext(ctx, savepointSql); err != nil {
		return err
	}
	err := store()

	var e *pq.Error
	if err != nil && errors.As(err, &e) {
		if e.Code == pgErrorCodeTableReadonly {
			return errStaleDsList
		}
		if _, ok := dbInvalidJsonErrors[string(e.Code)]; ok {
			if _, err := tx.ExecContext(ctx, rollbackSql); err != nil {
				return err
			}
			for i := range jobList {
				err = jobList[i].sanitizeJSON()
				if err != nil {
					return fmt.Errorf("sanitizeJSON: %w", err)
				}
			}
			return store()
		}
	}
	return err
}

type JobsResult struct {
	Jobs          []*JobT
	LimitsReached bool
	EventsCount   int
	PayloadSize   int64
}

/*
stateFilters and customValFilters do a OR query on values passed in array
parameterFilters do a AND query on values included in the map.
A JobsLimit less than or equal to zero indicates no limit.
*/
func (jd *Handle) getJobsDS(ctx context.Context, ds dataSetT, lastDS bool, params GetQueryParams) (JobsResult, bool, error) { // skipcq: CRT-P0003
	stateFilters := params.stateFilters
	customValFilters := params.CustomValFilters
	var parameterFilters ParameterFilterList = params.ParameterFilters
	workspaceID := params.WorkspaceID
	checkValidJobState(jd, stateFilters)

	if jd.noResultsCache.Get(ds.Index, workspaceID, customValFilters, stateFilters, parameterFilters) {
		jd.logger.Debugn("[getJobsDS] Empty cache hit for ds: %v, stateFilters: %v, customValFilters: %v, parameterFilters: %v",
			logger.NewStringField("ds", ds.String()),
			logger.NewStringField("stateFilters", strings.Join(stateFilters, ",")),
			logger.NewStringField("customValFilters", strings.Join(customValFilters, ",")),
			logger.NewStringField("parameterFilters", parameterFilters.String()),
		)
		return JobsResult{}, false, nil
	}

	tags := statTags{
		StateFilters:     stateFilters,
		CustomValFilters: params.CustomValFilters,
		WorkspaceID:      workspaceID,
	}

	stateFilters = lo.Filter(stateFilters, func(state string, _ int) bool { // exclude states for which we already know that there are no jobs
		return !jd.noResultsCache.Get(ds.Index, workspaceID, customValFilters, []string{state}, parameterFilters)
	})

	defer jd.getTimerStat("jobsdb_get_jobs_ds_time", &tags).RecordDuration()()

	containsUnprocessed := lo.Contains(stateFilters, Unprocessed.State)
	skipCacheResult := params.afterJobID != nil
	cacheTx := map[string]*cache.NoResultTx[ParameterFilterT]{}
	if !skipCacheResult {
		for _, state := range stateFilters {
			// avoid setting result as noJobs if
			//  (1) state is unprocessed and
			//  (2) jobsdb owner is a reader and
			//  (3) ds is the right most one
			if state == Unprocessed.State && jd.ownerType == Read && lastDS {
				continue
			}
			cacheTx[state] = jd.noResultsCache.StartNoResultTx(ds.Index, workspaceID, customValFilters, []string{state}, parameterFilters)
		}
	}

	var filterConditions []string
	additionalPredicates := lo.FilterMap(stateFilters, func(s string, _ int) (string, bool) {
		return "(job_latest_state.job_id IS NULL)", s == Unprocessed.State
	})
	stateQuery := constructQueryOR("job_latest_state.job_state", lo.Filter(stateFilters, func(s string, _ int) bool {
		return s != Unprocessed.State
	}), additionalPredicates...)
	filterConditions = append(filterConditions, stateQuery)

	if params.afterJobID != nil {
		filterConditions = append(filterConditions, fmt.Sprintf("jobs.job_id > %d", *params.afterJobID))
	}

	if len(customValFilters) > 0 && !params.IgnoreCustomValFiltersInQuery {
		filterConditions = append(filterConditions, constructQueryOR("jobs.custom_val", customValFilters))
	}

	if len(parameterFilters) > 0 {
		filterConditions = append(filterConditions, constructParameterJSONQuery("jobs", parameterFilters))
	}

	if workspaceID != "" {
		filterConditions = append(filterConditions, fmt.Sprintf("jobs.workspace_id = '%s'", workspaceID))
	}

	var filterQuery string
	if len(filterConditions) > 0 {
		filterQuery = "WHERE " + strings.Join(filterConditions, " AND ")
	}

	var limitQuery string
	if params.JobsLimit > 0 {
		limitQuery = fmt.Sprintf(" LIMIT %d ", params.JobsLimit)
	}

	joinType := "LEFT"
	joinTable := "v_last_" + ds.JobStatusTable

	if !containsUnprocessed { // If we are not querying for unprocessed jobs, we can use an inner join
		joinType = "INNER"
	} else if slices.Equal(stateFilters, []string{Unprocessed.State}) {
		// If we are querying only for unprocessed jobs, we should join with the status table instead of the view (performance reasons)
		joinTable = ds.JobStatusTable
	}

	var rows *sql.Rows
	sqlStatement := fmt.Sprintf(`SELECT
									jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count,
									jobs.created_at, jobs.expire_at, jobs.workspace_id, jobs.partition_id,
									octet_length(jobs.event_payload::text) as payload_size,
									sum(jobs.event_count) over (order by jobs.job_id asc) as running_event_counts,
									sum(octet_length(jobs.event_payload::text)) over (order by jobs.job_id) as running_payload_size,
									job_latest_state.job_state, job_latest_state.attempt,
									job_latest_state.exec_time, job_latest_state.retry_time,
									job_latest_state.error_code, job_latest_state.error_response, job_latest_state.parameters
								FROM
									%[1]q AS jobs
									%[2]s JOIN %[3]q job_latest_state ON jobs.job_id=job_latest_state.job_id
								    %[4]s
									ORDER BY jobs.job_id %[5]s`,
		ds.JobTable, joinType, joinTable, filterQuery, limitQuery)

	var args []interface{}

	var wrapQuery []string
	if params.EventsLimit > 0 {
		// If there is a single job in the dataset containing more events than the EventsLimit, we should return it,
		// otherwise processing will halt.
		// Therefore, we always retrieve one more job from the database than our limit dictates.
		// This job will only be returned to the result in case of the aforementioned scenario, otherwise it gets filtered out
		// later, during row scanning
		wrapQuery = append(wrapQuery, fmt.Sprintf(`running_event_counts - t.event_count <= $%d`, len(args)+1))
		args = append(args, params.EventsLimit)
	}

	if params.PayloadSizeLimit > 0 {
		wrapQuery = append(wrapQuery, fmt.Sprintf(`running_payload_size - t.payload_size <= $%d`, len(args)+1))
		args = append(args, params.PayloadSizeLimit)
	}

	if len(wrapQuery) > 0 {
		sqlStatement = `SELECT * FROM (` + sqlStatement + `) t WHERE ` + strings.Join(wrapQuery, " AND ")
	}

	stmt, err := jd.dbHandle.PrepareContext(ctx, sqlStatement)
	if err != nil {
		return JobsResult{}, false, err
	}
	defer func() { _ = stmt.Close() }()
	rows, err = stmt.QueryContext(ctx, args...)
	if err != nil {
		return JobsResult{}, false, err
	}
	defer func() { _ = rows.Close() }()

	var runningEventCount int
	var runningPayloadSize int64

	var jobList []*JobT
	var limitsReached bool
	var eventCount int
	var payloadSize int64
	resultsetStates := map[string]struct{}{}
	for rows.Next() {
		var payloadSize int64
		var job JobT
		var payload []byte
		var jsState sql.NullString
		var jsAttemptNum sql.NullInt64
		var jsExecTime sql.NullTime
		var jsRetryTime sql.NullTime
		var jsErrorCode sql.NullString
		var jsErrorResponse []byte
		var jsParameters []byte
		err := rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&payload, &job.EventCount, &job.CreatedAt, &job.ExpireAt, &job.WorkspaceId, &job.PartitionID, &payloadSize, &runningEventCount, &runningPayloadSize,
			&jsState, &jsAttemptNum,
			&jsExecTime, &jsRetryTime,
			&jsErrorCode, &jsErrorResponse, &jsParameters)
		if err != nil {
			return JobsResult{}, false, err
		}
		job.EventPayload = payload
		if jsState.Valid {
			resultsetStates[jsState.String] = struct{}{}
			job.LastJobStatus.JobState = jsState.String
			job.LastJobStatus.AttemptNum = int(jsAttemptNum.Int64)
			job.LastJobStatus.ExecTime = jsExecTime.Time
			job.LastJobStatus.RetryTime = jsRetryTime.Time
			job.LastJobStatus.ErrorCode = jsErrorCode.String
			job.LastJobStatus.ErrorResponse = jsErrorResponse
			job.LastJobStatus.Parameters = jsParameters
		} else {
			resultsetStates[Unprocessed.State] = struct{}{}
		}
		job.LastJobStatus.JobParameters = job.Parameters

		if params.EventsLimit > 0 && runningEventCount > params.EventsLimit && len(jobList) > 0 {
			// events limit overflow is triggered as long as we have read at least one job
			limitsReached = true
			break
		}
		if params.PayloadSizeLimit > 0 && runningPayloadSize > params.PayloadSizeLimit && len(jobList) > 0 {
			// payload size limit overflow is triggered as long as we have read at least one job
			limitsReached = true
			break
		}
		// we are adding the job only after testing for limitsReached
		// so that we don't always overflow
		jobList = append(jobList, &job)
		payloadSize = runningPayloadSize
		eventCount = runningEventCount
	}
	if err := rows.Err(); err != nil {
		return JobsResult{}, false, err
	}
	if !limitsReached &&
		(params.JobsLimit > 0 && len(jobList) == params.JobsLimit) || // we reached the jobs limit
		(params.EventsLimit > 0 && eventCount >= params.EventsLimit) || // we reached the events limit
		(params.PayloadSizeLimit > 0 && payloadSize >= params.PayloadSizeLimit) { // we reached the payload limit
		limitsReached = true
	}

	if !skipCacheResult {
		for state, cacheTx := range cacheTx {
			// we are committing the cache Tx only if
			// (a) no jobs are returned by the query or
			// (b) the state is not present in the resultset and limits have not been reached
			if _, ok := resultsetStates[state]; len(jobList) == 0 || (!ok && !limitsReached) {
				if allEntriesCommitted := cacheTx.Commit(); !allEntriesCommitted {
					tags := &statTags{
						StateFilters:     []string{state},
						CustomValFilters: params.CustomValFilters,
						WorkspaceID:      params.WorkspaceID,
						ParameterFilters: params.ParameterFilters,
					}
					statTags := tags.getStatsTags(jd.tablePrefix)
					jd.stats.NewTaggedStat("jobsdb_cache_commit_misses", stats.CountType, statTags).Increment()
				}
			}
		}
	}

	return JobsResult{
		Jobs:          jobList,
		LimitsReached: limitsReached,
		PayloadSize:   payloadSize,
		EventsCount:   eventCount,
	}, true, nil
}

// updateJobStatusStats is a map containing statistics of job status updates grouped by: workspace -> state -> set of params (stringified) -> stats
type updateJobStatusStats map[workspaceIDKey]map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats

// workspaceIDKey represents workspace id as key
type workspaceIDKey string

// jobStateKey represents job state as key (failed, succeeded, etc)
type jobStateKey string

// parameterFiltersKey represents a list of job parameter filters (stringified) as key
type parameterFiltersKey string

// Merges metrics from two updateJobStatusStats together
func (ujss updateJobStatusStats) Merge(other updateJobStatusStats) {
	for ws, states := range other {
		if _, ok := ujss[ws]; !ok {
			ujss[ws] = make(map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats)
		}
		for state, paramsMetrics := range states {
			if _, ok := ujss[ws][state]; !ok {
				ujss[ws][state] = make(map[parameterFiltersKey]*UpdateJobStatusStats)
			}
			for params, metrics := range paramsMetrics {
				existingMetrics, ok := ujss[ws][state][params]
				if !ok {
					existingMetrics = &UpdateJobStatusStats{parameters: metrics.parameters}
					ujss[ws][state][params] = existingMetrics
				}
				existingMetrics.count += metrics.count
				existingMetrics.bytes += metrics.bytes
			}
		}
	}
}

// Aggregates metrics by state across all workspaces
func (ujss updateJobStatusStats) StatsByState() map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats {
	result := make(map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats)
	for _, states := range ujss {
		for state, paramsMetrics := range states {
			if _, ok := result[state]; !ok {
				result[state] = make(map[parameterFiltersKey]*UpdateJobStatusStats)
			}
			for params, metrics := range paramsMetrics {
				existingMetrics, ok := result[state][params]
				if !ok {
					existingMetrics = &UpdateJobStatusStats{parameters: metrics.parameters}
					result[state][params] = existingMetrics
				}
				existingMetrics.count += metrics.count
				existingMetrics.bytes += metrics.bytes
			}
		}
	}
	return result
}

// Stats for jobs grouped by status and parameters
type UpdateJobStatusStats struct {
	// job parameters
	parameters ParameterFilterList
	// number of jobs
	count int
	// total size of error responses in bytes
	bytes int
}

func (jd *Handle) updateJobStatusDSInTx(ctx context.Context, tx *Tx, ds dataSetT, statusList []*JobStatusT, tags statTags) (updatedStates updateJobStatusStats, err error) {
	if len(statusList) == 0 {
		return updatedStates, err
	}

	defer jd.getTimerStat(
		"update_job_status_ds_time",
		&tags,
	).RecordDuration()()
	updatedStates = updateJobStatusStats{}
	store := func() error {
		updatedStates = updateJobStatusStats{} // reset in case of retry
		stmt, err := tx.PrepareContext(ctx, pq.CopyIn(ds.JobStatusTable, "job_id", "job_state", "attempt", "exec_time",
			"retry_time", "error_code", "error_response", "parameters"))
		if err != nil {
			return err
		}

		defer func() { _ = stmt.Close() }()
		for _, status := range statusList {
			//  Handle the case when google analytics returns gif in response
			if _, ok := updatedStates[workspaceIDKey(status.WorkspaceId)]; !ok {
				updatedStates[workspaceIDKey(status.WorkspaceId)] = make(map[jobStateKey]map[parameterFiltersKey]*UpdateJobStatusStats)
			}
			if _, ok := updatedStates[workspaceIDKey(status.WorkspaceId)][jobStateKey(status.JobState)]; !ok {
				updatedStates[workspaceIDKey(status.WorkspaceId)][jobStateKey(status.JobState)] = make(map[parameterFiltersKey]*UpdateJobStatusStats)
			}
			var parameters ParameterFilterList
			var parametersKey parameterFiltersKey
			if status.JobParameters != nil {
				for _, param := range cacheParameterFilters {
					v := gjson.GetBytes(status.JobParameters, param).Str
					parameters = append(parameters, ParameterFilterT{Name: param, Value: v})
				}
				parametersKey = parameterFiltersKey(parameters.String())

			}
			pm, ok := updatedStates[workspaceIDKey(status.WorkspaceId)][jobStateKey(status.JobState)][parametersKey]
			if !ok {
				pm = &UpdateJobStatusStats{parameters: parameters}
				updatedStates[workspaceIDKey(status.WorkspaceId)][jobStateKey(status.JobState)][parametersKey] = pm
			}
			pm.count++
			pm.bytes += len(status.ErrorResponse)

			if !utf8.ValidString(string(status.ErrorResponse)) {
				status.ErrorResponse = []byte(`{}`)
			}
			_, err = stmt.ExecContext(ctx, status.JobID, status.JobState, status.AttemptNum, status.ExecTime,
				status.RetryTime, status.ErrorCode, string(status.ErrorResponse), string(status.Parameters))
			if err != nil {
				return err
			}
		}
		if _, err = stmt.ExecContext(ctx); err != nil {
			return err
		}

		if len(statusList) > jd.conf.analyzeThreshold.Load() {
			_, err = tx.ExecContext(ctx, fmt.Sprintf(`ANALYZE %q`, ds.JobStatusTable))
		}

		return err
	}
	const (
		savepointSql = "SAVEPOINT updateJobStatusDSInTx"
		rollbackSql  = "ROLLBACK TO " + savepointSql
	)
	if _, err = tx.ExecContext(ctx, savepointSql); err != nil {
		return updatedStates, err
	}
	err = store()
	var e *pq.Error
	if err != nil && errors.As(err, &e) {
		if _, ok := dbInvalidJsonErrors[string(e.Code)]; ok {
			if _, err = tx.ExecContext(ctx, rollbackSql); err != nil {
				return updatedStates, err
			}
			for i := range statusList {
				err = statusList[i].sanitizeJson()
				if err != nil {
					return updatedStates, err
				}
			}
			err = store()
		}
	}
	return updatedStates, err
}

/*
The next set of functions are the user visible functions to get/set job status.
For reading jobs, it scans from the oldest DS to the latest till it has found
enough jobs. For updating status, it finds the DS to which the job belongs
(using the in-memory range list) and adds the status to the appropriate DS.
These functions can race with the internal function to add new DS and create
new DS. Synchronization is handled by locks as described below.

In theory, we can keep just one lock. All operations which
change the DS structure (e.g. adding new dataset or moving records
from one DS to another thearby updating the DS range) can take a write lock
while functions which don't update the DS structure (as in list of DS or
ranges within DS can take the read lock) as they can run in paralle.

The drawback with this approach is that migrating a DS can take a long
time and can potentially block the jobs/job-batch store call. Blocking jobs store
is bad since user ACK won't be sent unless jobs store returns.

To handle this, we separate out the locks into dsListLock and dsMigrationLock.
Store() only needs to access the last element of dsList and is not
impacted by movement of data across ds so it only takes the dsListLock.
Other functions are impacted by movement of data across DS in background
so take both the list and data lock
*/
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
			err := jd.WithTx(func(tx *Tx) error {
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
							jd.logger.Infon("Acquired lock",
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
							if err := jd.refreshDSList(ctx); err != nil {
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
				panic(err)
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

func setReadonlyDsInTx(ctx context.Context, tx *Tx, latestDS dataSetT) error {
	sqlStatement := fmt.Sprintf(
		`CREATE TRIGGER readonlyTableTrg
		BEFORE INSERT
		ON %q
		FOR EACH STATEMENT
		EXECUTE PROCEDURE %s;`, latestDS.JobTable, pgReadonlyTableExceptionFuncName)
	_, err := tx.ExecContext(ctx, sqlStatement)
	return err
}

func (jd *Handle) refreshDSListLoop(ctx context.Context) {
	for {
		select {
		case <-jd.TriggerRefreshDS():
		case <-ctx.Done():
			return
		}
		timeoutCtx, cancel := context.WithTimeout(ctx, jd.conf.refreshDSTimeout.Load())
		if err := jd.refreshDSList(timeoutCtx); err != nil {
			cancel()
			if !jd.conf.skipMaintenanceError && ctx.Err() == nil {
				panic(err)
			}
			jd.logger.Errorn("refreshDSListLoop error", obskit.Error(err))
		}
		cancel()
	}
}

// refreshDSList refreshes the list of datasets in memory if the database view of the list has changed.
func (jd *Handle) refreshDSList(ctx context.Context) error {
	jd.logger.Debugn("Start", logger.NewStringField("operation", "refreshDSListLoop"))

	start := time.Now()
	var err error
	defer func() {
		jd.stats.NewTaggedStat("refresh_ds_loop", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix, "error": strconv.FormatBool(err != nil)}).Since(start)
	}()
	jd.dsListLock.RLock()
	previousDS := jd.datasetList
	jd.dsListLock.RUnlock()
	nextDS, err := getDSList(jd, jd.dbHandle, jd.tablePrefix)
	if err != nil {
		return fmt.Errorf("getDSList: %w", err)
	}
	previousLastDS, _ := lo.Last(previousDS)
	nextLastDS, _ := lo.Last(nextDS)

	if previousLastDS.Index == nextLastDS.Index {
		return nil
	}
	defer jd.stats.NewTaggedStat("refresh_ds_loop_lock", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix}).RecordDuration()()
	err = jd.dsListLock.WithLockInCtx(ctx, func(l lock.LockToken) error {
		return jd.doRefreshDSRangeList(l)
	})
	if err != nil {
		return fmt.Errorf("refreshDSRangeList: %w", err)
	}

	return nil
}

// Identifier returns the identifier of the jobsdb. Here it is tablePrefix.
func (jd *Handle) Identifier() string {
	return jd.tablePrefix
}

/*
We keep a journal of all the operations. The journal helps
*/
const (
	addDSOperation             = "ADD_DS"
	migrateCopyOperation       = "MIGRATE_COPY"
	postMigrateDSOperation     = "POST_MIGRATE_DS_OP"
	dropDSOperation            = "DROP_DS"
	RawDataDestUploadOperation = "S3_DEST_UPLOAD"
)

type JournalEntryT struct {
	OpID      int64
	OpType    string
	OpDone    bool
	OpPayload json.RawMessage
}

func (jd *Handle) dropJournal() {
	sqlStatement := fmt.Sprintf(`DROP TABLE IF EXISTS %s_journal`, jd.tablePrefix)
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
}

func (jd *Handle) JournalMarkStart(opType string, opPayload json.RawMessage) (int64, error) {
	var opID int64
	return opID, jd.WithTx(func(tx *Tx) error {
		var err error
		opID, err = jd.JournalMarkStartInTx(tx, opType, opPayload)
		return err
	})
}

func (jd *Handle) JournalMarkStartInTx(tx *Tx, opType string, opPayload json.RawMessage) (int64, error) {
	var opID int64
	jd.assert(opType == addDSOperation ||
		opType == migrateCopyOperation ||
		opType == postMigrateDSOperation ||
		opType == dropDSOperation ||
		opType == RawDataDestUploadOperation, fmt.Sprintf("opType: %s is not a supported op", opType))

	sqlStatement := fmt.Sprintf(`INSERT INTO %s_journal (operation, done, operation_payload, start_time, owner)
                                       VALUES ($1, $2, $3, $4, $5) RETURNING id`, jd.tablePrefix)
	err := tx.QueryRow(sqlStatement, opType, false, opPayload, time.Now(), jd.ownerType).Scan(&opID)
	return opID, err
}

// JournalMarkDone marks the end of a journal action
func (jd *Handle) JournalMarkDone(opID int64) error {
	return jd.WithTx(func(tx *Tx) error {
		return jd.journalMarkDoneInTx(tx, opID)
	})
}

// JournalMarkDoneInTx marks the end of a journal action in a transaction
func (jd *Handle) journalMarkDoneInTx(tx *Tx, opID int64) error {
	sqlStatement := fmt.Sprintf(`UPDATE %s_journal SET done=$2, end_time=$3 WHERE id=$1 AND owner=$4`, jd.tablePrefix)
	_, err := tx.Exec(sqlStatement, opID, true, time.Now(), jd.ownerType)
	return err
}

func (jd *Handle) JournalDeleteEntry(opID int64) {
	sqlStatement := fmt.Sprintf(`DELETE from "%s_journal" WHERE id=$1 AND owner=$2`, jd.tablePrefix)
	_, err := jd.dbHandle.Exec(sqlStatement, opID, jd.ownerType)
	jd.assertError(err)
}

func (jd *Handle) GetJournalEntries(opType string) (entries []JournalEntryT) {
	sqlStatement := fmt.Sprintf(`SELECT id, operation, done, operation_payload
                                	from "%s_journal"
                                	WHERE
									done=False
									AND
									operation = '%s'
									AND
									owner='%s'
									ORDER BY id`, jd.tablePrefix, opType, jd.ownerType)
	stmt, err := jd.dbHandle.Prepare(sqlStatement)
	jd.assertError(err)
	defer func() { _ = stmt.Close() }()

	rows, err := stmt.Query()
	jd.assertError(err)
	defer func() { _ = rows.Close() }()

	count := 0
	for rows.Next() {
		entries = append(entries, JournalEntryT{})
		err = rows.Scan(&entries[count].OpID, &entries[count].OpType, &entries[count].OpDone, &entries[count].OpPayload)
		jd.assertError(err)
		count++
	}
	jd.assertError(rows.Err())
	return entries
}

func (jd *Handle) recoverFromCrash(owner OwnerType, goRoutineType string) {
	var opTypes []string
	switch goRoutineType {
	case addDSGoRoutine:
		opTypes = []string{addDSOperation}
	case mainGoRoutine:
		opTypes = []string{migrateCopyOperation, postMigrateDSOperation, dropDSOperation}
	}

	sqlStatement := fmt.Sprintf(`SELECT id, operation, done, operation_payload
                                	from %s_journal
                                	WHERE
									done=False
									AND
									operation = ANY($1)
									AND
									owner = '%s'
                                	ORDER BY id`, jd.tablePrefix, owner)

	stmt, err := jd.dbHandle.Prepare(sqlStatement)
	jd.assertError(err)
	defer func() { _ = stmt.Close() }()

	rows, err := stmt.Query(pq.Array(opTypes))
	jd.assertError(err)
	defer func() { _ = rows.Close() }()

	var opID int64
	var opType string
	var opDone bool
	var opPayload json.RawMessage
	var opPayloadJSON journalOpPayloadT
	undoOp := false
	var count int

	for rows.Next() {
		err = rows.Scan(&opID, &opType, &opDone, &opPayload)
		jd.assertError(err)
		jd.assert(!opDone, "opDone is true")
		count++
	}
	jd.assertError(rows.Err())
	jd.assert(count <= 1, fmt.Sprintf("count:%d > 1", count))

	if count == 0 {
		// Nothing to recover
		return
	}

	// Need to recover the last failed operation
	// Get the payload and undo
	err = jsonrs.Unmarshal(opPayload, &opPayloadJSON)
	jd.assertError(err)

	switch opType {
	case addDSOperation:
		newDS := opPayloadJSON.To
		undoOp = true
		// Drop the table we were trying to create
		jd.logger.Infon("Recovering new DS operation", logger.NewStringField("newDS", newDS.String()))
		jd.dropDSForRecovery(newDS)
	case migrateCopyOperation:
		migrateDest := opPayloadJSON.To
		// Delete the destination of the interrupted
		// migration. After we start, code should
		// redo the migration
		jd.logger.Infon("Recovering migrateCopy operation", logger.NewStringField("migrateDest", migrateDest.String()))
		jd.dropDSForRecovery(migrateDest)
		undoOp = true
	case postMigrateDSOperation:
		var migrateSrc dataSetTList = opPayloadJSON.From
		for _, ds := range migrateSrc {
			jd.dropDSForRecovery(ds)
		}
		jd.logger.Infon("Recovering migrateDel operation", logger.NewStringField("migrateSrc", migrateSrc.String()))
		undoOp = false
	}

	if undoOp {
		sqlStatement = fmt.Sprintf(`DELETE from "%s_journal" WHERE id=$1`, jd.tablePrefix)
	} else {
		sqlStatement = fmt.Sprintf(`UPDATE "%s_journal" SET done=True WHERE id=$1`, jd.tablePrefix)
	}

	_, err = jd.dbHandle.Exec(sqlStatement, opID)
	jd.assertError(err)
}

const (
	addDSGoRoutine = "addDS"
	mainGoRoutine  = "main"
)

func (jd *Handle) recoverFromJournal(owner OwnerType) {
	jd.recoverFromCrash(owner, addDSGoRoutine)
	jd.recoverFromCrash(owner, mainGoRoutine)
}

func (jd *Handle) UpdateJobStatus(ctx context.Context, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error {
	return jd.WithUpdateSafeTx(ctx, func(tx UpdateSafeTx) error {
		return jd.UpdateJobStatusInTx(ctx, tx, statusList, customValFilters, parameterFilters)
	})
}

/*
internalUpdateJobStatusInTx updates the status of a batch of jobs
customValFilters[] is passed, so we can efficiently mark empty cache
Later we can move this to query
*/
func (jd *Handle) internalUpdateJobStatusInTx(ctx context.Context, tx *Tx, dsList []dataSetT, dsRangeList []dataSetRangeT, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error {
	// capture stats
	tags := statTags{
		CustomValFilters: customValFilters,
		ParameterFilters: parameterFilters,
	}
	defer jd.getTimerStat(
		"update_job_status_time",
		&tags,
	).RecordDuration()()

	// do update
	updatedStatesByDS, err := jd.doUpdateJobStatusInTx(ctx, tx, dsList, dsRangeList, statusList, tags)
	if err != nil {
		jd.logger.Infon("Error occurred while updating job statuses",
			logger.NewStringField("tablePrefix", jd.tablePrefix),
			obskit.Error(err),
		)
		return err
	}

	tx.AddSuccessListener(func() {
		// clear cache
		for ds, dsStats := range updatedStatesByDS {
			if len(dsStats) == 0 { // if no keys, we need to invalidate all keys
				jd.noResultsCache.Invalidate(ds.Index, "", nil, nil, nil)
			}
			for workspace, wsStats := range dsStats {
				if len(wsStats) == 0 { // if no keys, we need to invalidate all keys
					jd.noResultsCache.Invalidate(ds.Index, string(workspace), nil, nil, nil)
				}
				for state, parametersStats := range wsStats {
					stateList := []string{string(state)}
					parameterFilters := lo.UniqBy( // gather unique parameter filters
						lo.FlatMap(
							lo.Values(parametersStats), // from all JobStatusMetrics
							func(ujss *UpdateJobStatusStats, _ int) []ParameterFilterT {
								return ujss.parameters
							},
						),
						func(pf ParameterFilterT) string {
							return pf.String() // uniqueness by string representation
						},
					)
					// invalidate cache for this combination
					jd.noResultsCache.Invalidate(ds.Index, string(workspace), customValFilters, stateList, parameterFilters)
				}
			}
		}
	})

	// use the aggregated stats from updateJobStatusInTx
	tx.AddSuccessListener(func() {
		merged := updateJobStatusStats{}
		for _, dsStats := range updatedStatesByDS {
			merged.Merge(dsStats)
		}
		statsByState := merged.StatsByState()
		for state, parametersMap := range statsByState {
			for _, metrics := range parametersMap {
				statTags := tags.getStatsTags(jd.tablePrefix)
				statTags["jobState"] = string(state)
				if len(parameterFilters) == 0 { // only add parameter filters if not already set
					for _, pf := range metrics.parameters {
						statTags[pf.Name] = pf.Value
					}
				}
				jd.stats.NewTaggedStat("jobsdb_updated_jobs", stats.CountType, statTags).Count(metrics.count)
				jd.stats.NewTaggedStat("jobsdb_updated_bytes", stats.CountType, statTags).Count(metrics.bytes)
			}
		}
	})

	return nil
}

/*
doUpdateJobStatusInTx updates the status of a batch of jobs
customValFilters[] is passed, so we can efficiently mark empty cache
Later we can move this to query
*/
func (jd *Handle) doUpdateJobStatusInTx(ctx context.Context, tx *Tx, dsList []dataSetT, dsRangeList []dataSetRangeT, statusList []*JobStatusT, tags statTags) (updatedStatesByDS map[dataSetT]updateJobStatusStats, err error) {
	if len(statusList) == 0 {
		return updatedStatesByDS, err
	}

	// First we sort by JobID
	sort.Slice(statusList, func(i, j int) bool {
		return statusList[i].JobID < statusList[j].JobID
	})

	// We scan through the list of jobs and map them to DS
	var lastPos int
	updatedStatesByDS = make(map[dataSetT]updateJobStatusStats)
	for _, ds := range dsRangeList {
		minID := ds.minJobID
		maxID := ds.maxJobID
		// We have processed upto (but excluding) lastPos on statusList.
		// Hence, that element must lie in this or subsequent dataset's
		// range
		jd.assert(statusList[lastPos].JobID >= minID, fmt.Sprintf("statusList[lastPos].JobID: %d < minID:%d", statusList[lastPos].JobID, minID))
		var i int
		for i = lastPos; i < len(statusList); i++ {
			// The JobID is outside this DS's range
			if statusList[i].JobID > maxID {
				if i > lastPos && jd.logger.IsDebugLevel() {
					jd.logger.Debugn("Range",
						logger.NewStringField("ds", ds.String()),
						logger.NewIntField("lastPosJobID", statusList[lastPos].JobID),
						logger.NewIntField("prevJobID", statusList[i-1].JobID),
						logger.NewIntField("lastPos", int64(lastPos)),
						logger.NewIntField("prevPos", int64(i-1)),
					)
				}
				var updatedStates updateJobStatusStats
				updatedStates, err = jd.updateJobStatusDSInTx(ctx, tx, ds.ds, statusList[lastPos:i], tags)
				if err != nil {
					return updatedStatesByDS, err
				}
				// do not set for ds without any new state written as it would clear emptyCache
				if len(updatedStates) > 0 {
					updatedStatesByDS[ds.ds] = updatedStates
				}
				lastPos = i
				break
			}
		}
		// Reached the end. Need to process this range
		if i == len(statusList) && lastPos < i {
			jd.logger.Debugn("Range",
				logger.NewStringField("ds", ds.String()),
				logger.NewIntField("lastPosJobID", statusList[lastPos].JobID),
				logger.NewIntField("prevJobID", statusList[i-1].JobID),
				logger.NewIntField("index", int64(i)),
			)
			var updatedStates updateJobStatusStats
			updatedStates, err = jd.updateJobStatusDSInTx(ctx, tx, ds.ds, statusList[lastPos:i], tags)
			if err != nil {
				return updatedStatesByDS, err
			}
			// do not set for ds without any new state written as it would clear emptyCache
			if len(updatedStates) > 0 {
				updatedStatesByDS[ds.ds] = updatedStates
			}
			lastPos = i
			break
		}
	}

	// The last (most active DS) might not have range element as it is being written to
	if lastPos < len(statusList) {
		// Make sure range is missing for the last ds and migration ds (if at all present)
		jd.assert(len(dsRangeList) >= len(dsList)-2, fmt.Sprintf("len(dsRangeList):%d < len(dsList):%d-2", len(dsRangeList), len(dsList)))
		// Update status in the last element
		jd.logger.Debugn("RangeEnd",
			logger.NewIntField("jobID", statusList[lastPos].JobID),
			logger.NewIntField("lenStatusList", int64(len(statusList))))
		var updatedStates updateJobStatusStats
		updatedStates, err = jd.updateJobStatusDSInTx(ctx, tx, dsList[len(dsList)-1], statusList[lastPos:], tags)
		if err != nil {
			return updatedStatesByDS, err
		}
		// do not set for ds without any new state written as it would clear emptyCache
		if len(updatedStates) > 0 {
			updatedStatesByDS[dsList[len(dsList)-1]] = updatedStates
		}
	}
	return updatedStatesByDS, err
}

// Store stores new jobs to the jobsdb.
// If enableWriterQueue is true, this goes through writer worker pool.
func (jd *Handle) Store(ctx context.Context, jobList []*JobT) error {
	return jd.WithStoreSafeTx(ctx, func(tx StoreSafeTx) error {
		return jd.StoreInTx(ctx, tx, jobList)
	})
}

// StoreInTx stores new jobs to the jobsdb.
// If enableWriterQueue is true, this goes through writer worker pool.
func (jd *Handle) StoreInTx(ctx context.Context, tx StoreSafeTx, jobList []*JobT) error {
	storeCmd := func() error {
		command := func() error {
			dsList := jd.getDSList()
			err := jd.internalStoreJobsInTx(ctx, tx.Tx(), dsList[len(dsList)-1], jobList)
			return err
		}
		err := executeDbRequest(jd, newWriteDbRequest("store", nil, command))
		return err
	}

	if tx.storeSafeTxIdentifier() != jd.Identifier() {
		return jd.inStoreSafeCtx(ctx, storeCmd)
	}
	return storeCmd()
}

func (jd *Handle) StoreEachBatchRetry(
	ctx context.Context,
	jobBatches [][]*JobT,
) map[uuid.UUID]string {
	var res map[uuid.UUID]string
	_ = jd.WithStoreSafeTx(ctx, func(tx StoreSafeTx) error {
		var err error
		res, err = jd.StoreEachBatchRetryInTx(ctx, tx, jobBatches)
		return err
	})
	return res
}

func (jd *Handle) StoreEachBatchRetryInTx(
	ctx context.Context,
	tx StoreSafeTx,
	jobBatches [][]*JobT,
) (map[uuid.UUID]string, error) {
	var (
		err error
		res map[uuid.UUID]string
	)
	storeCmd := func() error {
		command := func() map[uuid.UUID]string {
			dsList := jd.getDSList()
			res, err = jd.internalStoreEachBatchRetryInTx(
				ctx,
				tx.Tx(),
				dsList[len(dsList)-1],
				jobBatches,
			)
			return res
		}
		res = executeDbRequest(jd, newWriteDbRequest("store_each_batch_retry", nil, command))
		return err
	}
	if tx.storeSafeTxIdentifier() != jd.Identifier() {
		_ = jd.inStoreSafeCtx(ctx, storeCmd)
		return res, err
	}
	_ = storeCmd()
	return res, err
}

func (jd *Handle) internalStoreEachBatchRetryInTx(
	ctx context.Context,
	tx *Tx,
	ds dataSetT,
	jobBatches [][]*JobT) (
	errorMessagesMap map[uuid.UUID]string, err error,
) {
	const (
		savepointSql = "SAVEPOINT storeBatchWithRetryEach"
		rollbackSql  = "ROLLBACK TO " + savepointSql
	)

	failAll := func(err error) map[uuid.UUID]string {
		errorMessagesMap = make(map[uuid.UUID]string, len(jobBatches))
		for i := range jobBatches {
			errorMessagesMap[jobBatches[i][0].UUID] = err.Error()
		}
		return errorMessagesMap
	}
	defer jd.getTimerStat("store_jobs_retry_each_batch", nil).RecordDuration()()
	_, err = tx.ExecContext(ctx, savepointSql)
	if err != nil {
		return failAll(err), nil
	}
	err = jd.doStoreJobsInTx(ctx, tx, ds, lo.Flatten(jobBatches))
	if err == nil {
		tx.AddSuccessListener(func() {
			jd.invalidateCacheForJobs(ds, lo.Flatten(jobBatches))
		})
		return errorMessagesMap, err
	}
	if errors.Is(err, errStaleDsList) {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, rollbackSql)
	if err != nil {
		return failAll(err), nil
	}

	// retry storing each batch separately
	errorMessagesMap = make(map[uuid.UUID]string)
	var txErr error
	for _, jobBatch := range jobBatches {
		if txErr != nil { // stop trying treat all remaining as failed
			errorMessagesMap[jobBatch[0].UUID] = txErr.Error()
			continue
		}
		// savepoint
		_, txErr = tx.ExecContext(ctx, savepointSql)
		if txErr != nil {
			errorMessagesMap[jobBatch[0].UUID] = txErr.Error()
			continue
		}

		err = jd.doStoreJobsInTx(ctx, tx, ds, jobBatch)
		if err != nil {
			if errors.Is(err, errStaleDsList) {
				return nil, err
			}
			errorMessagesMap[jobBatch[0].UUID] = err.Error()
			// rollback to savepoint
			_, txErr = tx.ExecContext(ctx, rollbackSql)
			continue
		}
		tx.AddSuccessListener(func() {
			jd.invalidateCacheForJobs(ds, jobBatch)
		})
	}
	return errorMessagesMap, err
}

/*
printLists is a debugging function used to print
the current in-memory copy of jobs and job ranges
*/
func (jd *Handle) printLists(console bool) {
	// This being an internal function, we don't lock
	if jd.logger.IsDebugLevel() {
		jd.logger.Debugn("printLists",
			logger.NewStringField("list", jd.getDSList().String()),
			logger.NewStringField("ranges", jd.getDSRangeList().String()),
		)
	}
	if console {
		fmt.Println("List:", jd.getDSList())
		fmt.Println("Ranges:", jd.getDSRangeList())
	}
}

// GetUnprocessed finds unprocessed jobs, i.e. new jobs whose state hasn't been marked in the database yet
func (jd *Handle) GetUnprocessed(ctx context.Context, params GetQueryParams) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetJobs(ctx, []string{Unprocessed.State}, params)
}

// GetImporting finds jobs in importing state
func (jd *Handle) GetImporting(ctx context.Context, params GetQueryParams) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetJobs(ctx, []string{Importing.State}, params)
}

// GetAborted finds jobs in aborted state
func (jd *Handle) GetAborted(ctx context.Context, params GetQueryParams) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetJobs(ctx, []string{Aborted.State}, params)
}

// GetWaiting finds jobs in waiting state
func (jd *Handle) GetWaiting(ctx context.Context, params GetQueryParams) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetJobs(ctx, []string{Waiting.State}, params)
}

// GetSucceeded finds jobs in succeeded state
func (jd *Handle) GetSucceeded(ctx context.Context, params GetQueryParams) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetJobs(ctx, []string{Succeeded.State}, params)
}

// GetFailed finds jobs in failed state
func (jd *Handle) GetFailed(ctx context.Context, params GetQueryParams) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetJobs(ctx, []string{Failed.State}, params)
}

/*
getJobs returns events of a given state. This does not update any state itself and
realises on the caller to update it. That means that successive calls to getJobs("failed")
can return the same set of events. It is the responsibility of the caller to call it from
one thread, update the state (to "waiting") in the same thread and pass on the processors
*/
func (jd *Handle) getJobs(ctx context.Context, params GetQueryParams, more MoreToken) (*MoreJobsResult, error) { // skipcq: CRT-P0003

	mtoken := &moreToken{}
	if more != nil {
		var ok bool
		if mtoken, ok = more.(*moreToken); !ok {
			return nil, fmt.Errorf("invalid token: %+v", more)
		}
	}

	if mtoken.afterJobID != nil {
		params.afterJobID = mtoken.afterJobID
	}

	if params.JobsLimit <= 0 {
		return &MoreJobsResult{JobsResult: JobsResult{}, More: mtoken}, nil
	}
	tags := &statTags{
		StateFilters:     params.stateFilters,
		CustomValFilters: params.CustomValFilters,
		WorkspaceID:      params.WorkspaceID,
		ParameterFilters: params.ParameterFilters,
		MoreToken:        more != nil,
	}
	defer jd.getTimerStat("jobsdb_get_jobs_time", tags).RecordDuration()()

	// The order of lock is very important. The migrateDSLoop
	// takes lock in this order so reversing this will cause
	// deadlocks
	if !jd.dsMigrationLock.RTryLockWithCtx(ctx) {
		return nil, fmt.Errorf("could not acquire a migration read lock: %w", ctx.Err())
	}
	defer jd.dsMigrationLock.RUnlock()
	if !jd.dsListLock.RTryLockWithCtx(ctx) {
		return nil, fmt.Errorf("could not acquire a dslist read lock: %w", ctx.Err())
	}
	dsRangeList := jd.getDSRangeList()
	dsList := jd.getDSList()
	jd.dsListLock.RUnlock()

	limitByEventCount := params.EventsLimit > 0

	limitByPayloadSize := false
	if params.PayloadSizeLimit > 0 {
		limitByPayloadSize = true
	} else if params.PayloadSizeLimit < 0 {
		return &MoreJobsResult{JobsResult: JobsResult{}, More: mtoken}, nil
	}

	res := &MoreJobsResult{More: mtoken}
	dsQueryCount := 0
	cacheHitCount := 0
	var dsLimit int
	if jd.conf.dsLimit != nil {
		dsLimit = jd.conf.dsLimit.Load()
	}
	for idx, ds := range dsList {
		if params.afterJobID != nil {
			if idx < len(dsRangeList) { // ranges are not stored for the last ds
				// so the following condition cannot be applied the last ds
				if *params.afterJobID > dsRangeList[idx].maxJobID {
					continue
				}
			}
		}
		if dsLimit > 0 && dsQueryCount >= dsLimit {
			break
		}
		jobs, dsHit, err := jd.getJobsDS(ctx, ds, len(dsList)-1 == idx, params)
		if err != nil {
			return nil, err
		}
		if dsHit {
			dsQueryCount++
		} else {
			cacheHitCount++
		}
		res.Jobs = append(res.Jobs, jobs.Jobs...)
		res.EventsCount += jobs.EventsCount
		res.PayloadSize += jobs.PayloadSize

		if jobs.LimitsReached {
			res.LimitsReached = true
			break
		}
		// decrement our limits for the next query
		if params.JobsLimit > 0 {
			params.JobsLimit -= len(jobs.Jobs)
		}
		if limitByEventCount {
			params.EventsLimit -= jobs.EventsCount
		}
		if limitByPayloadSize {
			params.PayloadSizeLimit -= jobs.PayloadSize
		}
	}

	if len(res.Jobs) > 0 {
		retryAfterJobID := res.Jobs[len(res.Jobs)-1].JobID
		mtoken.afterJobID = &retryAfterJobID
	}

	statTags := tags.getStatsTags(jd.tablePrefix)
	statTags["query"] = "get"
	jd.stats.NewTaggedStat("jobsdb_tables_queried", stats.CountType, statTags).Count(dsQueryCount) // number of actual ds tables that we queried
	jd.stats.NewTaggedStat("jobsdb_cache_hits", stats.CountType, statTags).Count(cacheHitCount)    // number of ds tables that we skipped querying due to noResultsCache
	if len(res.Jobs) == 0 {
		jd.stats.NewTaggedStat("jobsdb_queried_no_jobs", stats.CountType, statTags).Increment() // number of times that we queried and got no jobs
	}
	jd.stats.NewTaggedStat("jobsdb_queried_jobs", stats.CountType, statTags).Count(len(res.Jobs))                                                         // number of jobs that we queried
	jd.stats.NewTaggedStat("jobsdb_queried_bytes", stats.CountType, statTags).Count(lo.SumBy(res.Jobs, func(j *JobT) int { return len(j.EventPayload) })) // number of bytes that we queried
	return res, nil
}

/*
GetJobs returns events of a given state. This does not update any state itself and
realises on the caller to update it. That means that successive calls to GetJobs("failed")
can return the same set of events. It is the responsibility of the caller to call it from
one thread, update the state (to "waiting") in the same thread and pass on the processors
*/
func (jd *Handle) GetJobs(ctx context.Context, states []string, params GetQueryParams) (JobsResult, error) { // skipcq: CRT-P0003
	if params.JobsLimit == 0 {
		return JobsResult{}, nil
	}
	params.stateFilters = states
	slices.Sort(params.stateFilters)
	tags := statTags{
		StateFilters:     params.stateFilters,
		CustomValFilters: params.CustomValFilters,
		WorkspaceID:      params.WorkspaceID,
		ParameterFilters: params.ParameterFilters,
	}
	command := func() queryResult {
		return queryResultWrapper(jd.getJobs(ctx, params, nil))
	}
	res := executeDbRequest(jd, newReadDbRequest("get_jobs", &tags, command))
	return res.JobsResult, res.err
}

type queryResult struct {
	JobsResult
	err error
}

func queryResultWrapper(res *MoreJobsResult, err error) queryResult {
	if res == nil {
		res = &MoreJobsResult{}
	}
	return queryResult{
		JobsResult: res.JobsResult,
		err:        err,
	}
}

type moreQueryResult struct {
	*MoreJobsResult
	err error
}

func moreQueryResultWrapper(res *MoreJobsResult, err error) moreQueryResult {
	return moreQueryResult{
		MoreJobsResult: res,
		err:            err,
	}
}

func (jd *Handle) getMaxIDForDs(ds dataSetT) int64 {
	var maxID sql.NullInt64
	sqlStatement := fmt.Sprintf(`SELECT MAX(job_id) FROM %s`, ds.JobTable)
	row := jd.dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&maxID)
	if err != nil {
		panic(fmt.Errorf("query for max job_id: %q", err))
	}

	if maxID.Valid {
		return maxID.Int64
	}
	return 0
}

func (jd *Handle) GetLastJob(ctx context.Context) *JobT {
	if !jd.dsListLock.RTryLockWithCtx(ctx) {
		panic(fmt.Errorf("could not acquire a dslist lock: %w", ctx.Err()))
	}
	dsList := jd.getDSList()
	jd.dsListLock.RUnlock()
	maxID := jd.getMaxIDForDs(dsList[len(dsList)-1])
	var job JobT
	sqlStatement := fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at FROM %[1]s WHERE %[1]s.job_id = %[2]d`, dsList[len(dsList)-1].JobTable, maxID)
	err := jd.dbHandle.QueryRow(sqlStatement).Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal, &job.EventPayload, &job.CreatedAt, &job.ExpireAt)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		jd.assertError(err)
	}
	return &job
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
