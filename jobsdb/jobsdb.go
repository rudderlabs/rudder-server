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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"golang.org/x/sync/errgroup"

	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/cache"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

var errStaleDsList = errors.New("stale dataset list")

const (
	preDropTablePrefix               = "pre_drop_"
	pgReadonlyTableExceptionFuncName = "readonly_table_exception()"
	pgErrorCodeTableReadonly         = "RS001"
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

// GetQueryParamsT is a struct to hold jobsdb query params.
type GetQueryParamsT struct {
	// query conditions

	// if IgnoreCustomValFiltersInQuery is true, CustomValFilters is not going to be used
	IgnoreCustomValFiltersInQuery bool
	WorkspaceID                   string
	CustomValFilters              []string
	ParameterFilters              []ParameterFilterT
	StateFilters                  []string
	AfterJobID                    *int64

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

// Tx is a wrapper around sql.Tx that supports registering and executing
// post-commit actions, a.k.a. success listeners.
type Tx struct {
	*sql.Tx
	successListeners []func()
}

// AddSuccessListener registers a listener to be executed after the transaction has been committed successfully.
func (tx *Tx) AddSuccessListener(listener func()) {
	tx.successListeners = append(tx.successListeners, listener)
}

// Commit commits the transaction and executes all listeners.
func (tx *Tx) Commit() error {
	err := tx.Tx.Commit()
	if err == nil {
		for _, successListener := range tx.successListeners {
			successListener()
		}
	}
	return err
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
	*HandleT
}

// DSIndicesList returns the slice of current ds indices
func (h *HandleInspector) DSIndicesList() []string {
	h.HandleT.dsListLock.RLock()
	defer h.HandleT.dsListLock.RUnlock()
	var indicesList []string
	for _, ds := range h.HandleT.getDSList() {
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

	// GetUnprocessed finds unprocessed jobs. Unprocessed are new
	// jobs whose state hasn't been marked in the database yet
	GetUnprocessed(ctx context.Context, params GetQueryParamsT) (JobsResult, error)

	// GetProcessed finds jobs in some state, i.e. not unprocessed
	GetProcessed(ctx context.Context, params GetQueryParamsT) (JobsResult, error)

	// GetToRetry finds jobs in failed state
	GetToRetry(ctx context.Context, params GetQueryParamsT) (JobsResult, error)

	// GetWaiting finds jobs in waiting state
	GetWaiting(ctx context.Context, params GetQueryParamsT) (JobsResult, error)

	// GetExecuting finds jobs in executing state
	GetExecuting(ctx context.Context, params GetQueryParamsT) (JobsResult, error)

	// GetImporting finds jobs in importing state
	GetImporting(ctx context.Context, params GetQueryParamsT) (JobsResult, error)

	// GetToProcess finds jobs in any of the following states: failed, waiting, unprocessed.
	// It also returns a MoreToken that can be used to fetch more jobs, if available, with a subsequent call.
	GetToProcess(ctx context.Context, params GetQueryParamsT, more MoreToken) (*MoreJobsResult, error)

	// GetPileUpCounts returns statistics (counters) of incomplete jobs
	// grouped by workspaceId and destination type
	GetPileUpCounts(ctx context.Context) (statMap map[string]map[string]int, err error)

	// GetActiveWorkspaces returns a list of active workspace ids. If customVal is not empty, it will be used as a filter
	GetActiveWorkspaces(ctx context.Context, customVal string) (workspaces []string, err error)

	// GetDistinctParameterValues returns the list of distinct parameter values inside the jobs tables
	GetDistinctParameterValues(ctx context.Context, parameterName string) (values []string, err error)

	/* Admin */

	Ping() error
	DeleteExecuting()
	FailExecuting()

	/* Journal */

	GetJournalEntries(opType string) (entries []JournalEntryT)
	JournalDeleteEntry(opID int64)
	JournalMarkStart(opType string, opPayload json.RawMessage) (int64, error)
	JournalMarkDone(opID int64) error
}

/*
assertInterface contains public assert methods
*/
type assertInterface interface {
	assert(cond bool, errorString string)
	assertError(err error)
}

var (
	masterBackupEnabled bool
	pathPrefix          string
)

/*
UpdateJobStatusInTx updates the status of a batch of jobs in the past transaction
customValFilters[] is passed, so we can efficiently mark empty cache
Later we can move this to query
IMP NOTE: AcquireUpdateJobStatusLocks Should be called before calling this function
*/
func (jd *HandleT) UpdateJobStatusInTx(ctx context.Context, tx UpdateSafeTx, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error {
	updateCmd := func(dsList []dataSetT, dsRangeList []dataSetRangeT) error {
		if len(statusList) == 0 {
			return nil
		}
		tags := statTags{CustomValFilters: customValFilters, ParameterFilters: parameterFilters}
		command := func() interface{} {
			return jd.internalUpdateJobStatusInTx(ctx, tx.Tx(), dsList, dsRangeList, statusList, customValFilters, parameterFilters)
		}
		err, _ := jd.executeDbRequest(newWriteDbRequest("update_job_status", &tags, command)).(error)
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
	JobState      string          `json:"JobState"` // ENUM waiting, executing, succeeded, waiting_retry,  failed, aborted, migrating, migrated, wont_migrate
	AttemptNum    int             `json:"AttemptNum"`
	ExecTime      time.Time       `json:"ExecTime"`
	RetryTime     time.Time       `json:"RetryTime"`
	ErrorCode     string          `json:"ErrorCode"`
	ErrorResponse json.RawMessage `json:"ErrorResponse"`
	Parameters    json.RawMessage `json:"Parameters"`
	JobParameters json.RawMessage `json:"-"`
	WorkspaceId   string          `json:"WorkspaceId"`
}

func (r *JobStatusT) sanitizeJson() {
	r.ErrorResponse = sanitizeJson(r.ErrorResponse)
	r.Parameters = sanitizeJson(r.Parameters)
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
	PayloadSize   int64           `json:"PayloadSize"`
	LastJobStatus JobStatusT      `json:"LastJobStatus"`
	Parameters    json.RawMessage `json:"Parameters"`
	WorkspaceId   string          `json:"WorkspaceId"`
}

func (job *JobT) String() string {
	return fmt.Sprintf("JobID=%v, UserID=%v, CreatedAt=%v, ExpireAt=%v, CustomVal=%v, Parameters=%v, EventPayload=%v EventCount=%d", job.JobID, job.UserID, job.CreatedAt, job.ExpireAt, job.CustomVal, string(job.Parameters), string(job.EventPayload), job.EventCount)
}

func (job *JobT) sanitizeJson() {
	job.EventPayload = sanitizeJson(job.EventPayload)
	job.Parameters = sanitizeJson(job.Parameters)
}

// The struct fields need to be exposed to JSON package
type dataSetT struct {
	JobTable       string `json:"job"`
	JobStatusTable string `json:"status"`
	Index          string `json:"index"`
}

type dataSetRangeT struct {
	minJobID  int64
	maxJobID  int64
	startTime int64
	endTime   int64
	ds        dataSetT
}

/*
HandleT is the main type implementing the database for implementing
jobs. The caller must call the SetUp function on a HandleT object
*/
type HandleT struct {
	dbHandle                      *sql.DB
	ownerType                     OwnerType
	tablePrefix                   string
	datasetList                   []dataSetT
	datasetRangeList              []dataSetRangeT
	dsListLock                    *lock.Locker
	dsMigrationLock               *lock.Locker
	MinDSRetentionPeriod          time.Duration
	MaxDSRetentionPeriod          time.Duration
	noResultsCache                *cache.NoResultsCache[ParameterFilterT]
	BackupSettings                *backupSettings
	statTableCount                stats.Measurement
	statPreDropTableCount         stats.Measurement
	statDSCount                   stats.Measurement
	statNewDSPeriod               stats.Measurement
	newDSCreationTime             time.Time
	invalidCacheKeyStat           stats.Measurement
	statDropDSPeriod              stats.Measurement
	dsDropTime                    time.Time
	unionQueryTime                stats.Measurement
	logger                        logger.Logger
	writeCapacity                 chan struct{}
	readCapacity                  chan struct{}
	enableWriterQueue             bool
	enableReaderQueue             bool
	clearAll                      bool
	skipMaintenanceError          bool
	isStatNewDSPeriodInitialized  bool
	isStatDropDSPeriodInitialized bool
	dsLimit                       *int
	maxReaders                    int
	maxWriters                    int
	maxOpenConnections            int
	analyzeThreshold              int
	MaxDSSize                     *int
	backgroundCancel              context.CancelFunc
	backgroundGroup               *errgroup.Group
	maxBackupRetryTime            time.Duration
	preBackupHandlers             []prebackup.Handler
	fileUploaderProvider          fileuploader.Provider
	// skipSetupDBSetup is useful for testing as we mock the database client
	// TODO: Remove this flag once we have test setup that uses real database
	skipSetupDBSetup bool

	// TriggerAddNewDS, TriggerMigrateDS is useful for triggering addNewDS to run from tests.
	// TODO: Ideally we should refactor the code to not use this override.
	TriggerAddNewDS  func() <-chan time.Time
	TriggerMigrateDS func() <-chan time.Time
	migrateDSTimeout time.Duration
	TriggerRefreshDS func() <-chan time.Time
	refreshDSTimeout time.Duration

	TriggerJobCleanUp func() <-chan time.Time
	JobMaxAge         time.Duration

	lifecycle struct {
		mu      sync.Mutex
		started bool
	}
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

func (p ParameterFilterT) GetName() string {
	return p.Name
}

func (p ParameterFilterT) GetValue() string {
	return p.Value
}

var dbInvalidJsonErrors = map[string]struct{}{
	"22P02": {},
	"22P05": {},
	"22025": {},
	"22019": {},
}

// registers the backup settings depending on jobdb type the gateway, the router and the processor
// masterBackupEnabled = true => all the jobsdb are eligible for backup
// instanceBackupEnabled = true => the individual jobsdb too is eligible for backup
// instanceBackupFailedAndAborted = true => the individual jobdb backsup failed and aborted jobs only
// pathPrefix = by default is the jobsdb table prefix, is the path appended before instanceID in s3 folder structure
func (jd *HandleT) registerBackUpSettings() {
	config.RegisterBoolConfigVariable(true, &masterBackupEnabled, true, "JobsDB.backup.enabled")
	config.RegisterBoolConfigVariable(false, &jd.BackupSettings.instanceBackupEnabled, true, fmt.Sprintf("JobsDB.backup.%v.enabled", jd.tablePrefix))
	config.RegisterBoolConfigVariable(false, &jd.BackupSettings.FailedOnly, false, fmt.Sprintf("JobsDB.backup.%v.failedOnly", jd.tablePrefix))
	config.RegisterStringConfigVariable(jd.tablePrefix, &pathPrefix, false, fmt.Sprintf("JobsDB.backup.%v.pathPrefix", jd.tablePrefix))
	config.RegisterDurationConfigVariable(10, &jd.maxBackupRetryTime, false, time.Minute, "JobsDB.backup.maxRetry")
	config.RegisterDurationConfigVariable(10, &jd.refreshDSTimeout, true, time.Minute, "JobsDB.refreshDS.timeout")
	config.RegisterDurationConfigVariable(720, &jd.JobMaxAge, false, time.Hour, "JobsDB.jobMaxAge")
	config.RegisterDurationConfigVariable(10, &jd.migrateDSTimeout, true, time.Minute, "JobsDB.migrateDS.timeout")

	jd.BackupSettings.PathPrefix = strings.TrimSpace(pathPrefix)
}

// Some helper functions
func (jd *HandleT) assertError(err error) {
	if err != nil {
		jd.printLists(true)
		jd.logger.Fatalw("assertError failure",
			"error", err,
			"tablePrefix", jd.tablePrefix,
			"ownerType", jd.ownerType,
			"noResultsCache", jd.noResultsCache.String())
		panic(err)
	}
}

func (jd *HandleT) assert(cond bool, errorString string) {
	if !cond {
		jd.printLists(true)
		jd.logger.Fatalw("assert condition failed",
			"errorString", errorString,
			"tablePrefix", jd.tablePrefix,
			"ownerType", jd.ownerType,
			"noResultsCache", jd.noResultsCache.String())
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
	NotProcessed = jobStateT{isValid: false, isTerminal: false, State: "not_picked_yet"}

	// Valid, Not terminal
	Failed    = jobStateT{isValid: true, isTerminal: false, State: "failed"}
	Executing = jobStateT{isValid: true, isTerminal: false, State: "executing"}
	Waiting   = jobStateT{isValid: true, isTerminal: false, State: "waiting"}
	Importing = jobStateT{isValid: true, isTerminal: false, State: "importing"}

	// Valid, Terminal
	Succeeded = jobStateT{isValid: true, isTerminal: true, State: "succeeded"}
	Aborted   = jobStateT{isValid: true, isTerminal: true, State: "aborted"}
	Migrated  = jobStateT{isValid: true, isTerminal: true, State: "migrated"}

	validTerminalStates    []string
	validNonTerminalStates []string
)

// Adding a new state to this list, will require an enum change in postgres db.
var jobStates = []jobStateT{
	NotProcessed,
	Failed,
	Executing,
	Waiting,
	Succeeded,
	Aborted,
	Migrated,
	Importing,
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

var (
	maxDSSize, maxMigrateOnce, maxMigrateDSProbe int
	maxTableSize                                 int64
	vacuumFullStatusTableThreshold               int64
	vacuumAnalyzeStatusTableThreshold            int64
	jobDoneMigrateThres, jobStatusMigrateThres   float64
	jobMinRowsMigrateThres                       float64
	migrateDSLoopSleepDuration                   time.Duration
	addNewDSLoopSleepDuration                    time.Duration
	refreshDSListLoopSleepDuration               time.Duration
	backupCheckSleepDuration                     time.Duration
	cacheExpiration                              time.Duration
	jobCleanupFrequency                          time.Duration
	backupRowsBatchSize                          int64
	backupMaxTotalPayloadSize                    int64
	pkgLogger                                    logger.Logger
)

// Loads db config and migration related config from config file
func loadConfig() {
	/*Migration related parameters
	jobDoneMigrateThres: A DS is migrated when this fraction of the jobs have been processed
	jobStatusMigrateThres: A DS is migrated if the job_status exceeds this (* no_of_jobs)
	jobMinRowsMigrateThres: A DS with a low number of rows should be eligible for migration if the number of rows are
							less than jobMinRowsMigrateThres percent of maxDSSize (e.g. if jobMinRowsMigrateThres is 5
							then DSs that have less than 5% of maxDSSize are eligible for migration)
	maxDSSize: Maximum size of a DS. The process which adds new DS runs in the background
			(every few seconds) so a DS may go beyond this size
	maxMigrateOnce: Maximum number of DSs that are migrated together into one destination
	maxMigrateDSProbe: Maximum number of DSs that are checked from left to right if they are eligible for migration
	migrateDSLoopSleepDuration: How often is the loop (which checks for migrating DS) run
	addNewDSLoopSleepDuration: How often is the loop (which checks for adding new DS) run
	refreshDSListLoopSleepDuration: How often is the loop (which refreshes DSList) run
	maxTableSizeInMB: Maximum Table size in MB
	*/
	config.RegisterFloat64ConfigVariable(0.7, &jobDoneMigrateThres, true, "JobsDB.jobDoneMigrateThres")
	config.RegisterFloat64ConfigVariable(3, &jobStatusMigrateThres, true, "JobsDB.jobStatusMigrateThres")
	config.RegisterFloat64ConfigVariable(0.2, &jobMinRowsMigrateThres, true, "JobsDB.jobMinRowsMigrateThres")
	config.RegisterIntConfigVariable(100000, &maxDSSize, true, 1, "JobsDB.maxDSSize")
	config.RegisterIntConfigVariable(10, &maxMigrateOnce, true, 1, "JobsDB.maxMigrateOnce")
	config.RegisterIntConfigVariable(10, &maxMigrateDSProbe, true, 1, "JobsDB.maxMigrateDSProbe")
	config.RegisterInt64ConfigVariable(300, &maxTableSize, true, 1000000, "JobsDB.maxTableSizeInMB")
	config.RegisterInt64ConfigVariable(10000, &backupRowsBatchSize, true, 1, "JobsDB.backupRowsBatchSize")
	config.RegisterInt64ConfigVariable(500*bytesize.MB, &vacuumFullStatusTableThreshold, true, 1, "JobsDB.vacuumFullStatusTableThreshold")
	config.RegisterInt64ConfigVariable(30000, &vacuumAnalyzeStatusTableThreshold, true, 1, "JobsDB.vacuumAnalyzeStatusTableThreshold")
	config.RegisterInt64ConfigVariable(64*bytesize.MB, &backupMaxTotalPayloadSize, true, 1, "JobsDB.maxBackupTotalPayloadSize")
	config.RegisterDurationConfigVariable(30, &migrateDSLoopSleepDuration, true, time.Second, []string{"JobsDB.migrateDSLoopSleepDuration", "JobsDB.migrateDSLoopSleepDurationInS"}...)
	config.RegisterDurationConfigVariable(5, &addNewDSLoopSleepDuration, true, time.Second, []string{"JobsDB.addNewDSLoopSleepDuration", "JobsDB.addNewDSLoopSleepDurationInS"}...)
	config.RegisterDurationConfigVariable(10, &refreshDSListLoopSleepDuration, true, time.Second, []string{"JobsDB.refreshDSListLoopSleepDuration", "JobsDB.refreshDSListLoopSleepDurationInS"}...)
	config.RegisterDurationConfigVariable(5, &backupCheckSleepDuration, true, time.Second, []string{"JobsDB.backupCheckSleepDuration", "JobsDB.backupCheckSleepDurationIns"}...)
	config.RegisterDurationConfigVariable(120, &cacheExpiration, true, time.Minute, []string{"JobsDB.cacheExpiration"}...)
	config.RegisterDurationConfigVariable(24, &jobCleanupFrequency, true, time.Hour, []string{"JobsDB.jobCleanupFrequency"}...)
}

func Init2() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("jobsdb")
}

type OptsFunc func(jd *HandleT)

// WithClearDB, if set to true it will remove all existing tables
func WithClearDB(clearDB bool) OptsFunc {
	return func(jd *HandleT) {
		jd.clearAll = clearDB
	}
}

// WithPreBackupHandlers, sets pre-backup handlers
func WithPreBackupHandlers(preBackupHandlers []prebackup.Handler) OptsFunc {
	return func(jd *HandleT) {
		jd.preBackupHandlers = preBackupHandlers
	}
}

func WithDSLimit(limit *int) OptsFunc {
	return func(jd *HandleT) {
		jd.dsLimit = limit
	}
}

func WithSkipMaintenanceErr(ignore bool) OptsFunc {
	return func(jd *HandleT) {
		jd.skipMaintenanceError = ignore
	}
}

func WithFileUploaderProvider(fileUploaderProvider fileuploader.Provider) OptsFunc {
	return func(jd *HandleT) {
		jd.fileUploaderProvider = fileUploaderProvider
	}
}

func NewForRead(tablePrefix string, opts ...OptsFunc) *HandleT {
	return newOwnerType(Read, tablePrefix, opts...)
}

func NewForWrite(tablePrefix string, opts ...OptsFunc) *HandleT {
	return newOwnerType(Write, tablePrefix, opts...)
}

func NewForReadWrite(tablePrefix string, opts ...OptsFunc) *HandleT {
	return newOwnerType(ReadWrite, tablePrefix, opts...)
}

func newOwnerType(ownerType OwnerType, tablePrefix string, opts ...OptsFunc) *HandleT {
	j := &HandleT{
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
func (jd *HandleT) Setup(
	ownerType OwnerType, clearAll bool, tablePrefix string,
	preBackupHandlers []prebackup.Handler, fileUploaderProvider fileuploader.Provider,
) error {
	jd.ownerType = ownerType
	jd.clearAll = clearAll
	jd.tablePrefix = tablePrefix
	jd.preBackupHandlers = preBackupHandlers
	jd.fileUploaderProvider = fileUploaderProvider
	jd.init()
	return jd.Start()
}

func (jd *HandleT) init() {
	jd.logger = pkgLogger.Child(jd.tablePrefix)
	jd.dsListLock = lock.NewLocker()
	jd.dsMigrationLock = lock.NewLocker()
	if jd.MaxDSSize == nil {
		// passing `maxDSSize` by reference, so it can be hot reloaded
		jd.MaxDSSize = &maxDSSize
	}

	if jd.TriggerAddNewDS == nil {
		jd.TriggerAddNewDS = func() <-chan time.Time {
			return time.After(addNewDSLoopSleepDuration)
		}
	}

	if jd.TriggerMigrateDS == nil {
		jd.TriggerMigrateDS = func() <-chan time.Time {
			return time.After(migrateDSLoopSleepDuration)
		}
	}

	if jd.TriggerRefreshDS == nil {
		jd.TriggerRefreshDS = func() <-chan time.Time {
			return time.After(refreshDSListLoopSleepDuration)
		}
	}

	if jd.TriggerJobCleanUp == nil {
		jd.TriggerJobCleanUp = func() <-chan time.Time {
			return time.After(jobCleanupFrequency)
		}
	}

	// Initialize dbHandle if not already set
	if jd.dbHandle == nil {
		var err error
		psqlInfo := misc.GetConnectionString()
		sqlDB, err := sql.Open("postgres", psqlInfo)
		jd.assertError(err)

		defer func() {
			if !jd.enableReaderQueue || !jd.enableWriterQueue {
				sqlDB.SetMaxOpenConns(jd.maxOpenConnections)
				return
			}
			maxOpenConnections := 2 // buffer
			maxOpenConnections += jd.maxReaders + jd.maxWriters
			switch jd.ownerType {
			case Read:
				maxOpenConnections += 3 // backup, migrate, refreshDsList
			case Write:
				maxOpenConnections += 1 // addNewDS
			case ReadWrite:
				maxOpenConnections += 4 // backup, migrate, addNewDS, archive
			}
			if maxOpenConnections < jd.maxOpenConnections {
				sqlDB.SetMaxOpenConns(maxOpenConnections)
			} else {
				sqlDB.SetMaxOpenConns(jd.maxOpenConnections)
			}
		}()

		err = sqlDB.Ping()
		jd.assertError(err)

		jd.dbHandle = sqlDB
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
				if writer && jd.clearAll {
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

func (jd *HandleT) workersAndAuxSetup() {
	jd.assert(jd.tablePrefix != "", "tablePrefix received is empty")

	jd.noResultsCache = cache.NewNoResultsCache[ParameterFilterT](
		cacheParameterFilters,
		func() time.Duration { return cacheExpiration },
	)
	jd.BackupSettings = &backupSettings{}
	jd.registerBackUpSettings()

	jd.logger.Infof("Connected to %s DB", jd.tablePrefix)
	jd.statPreDropTableCount = stats.Default.NewTaggedStat("jobsdb.pre_drop_tables_count", stats.GaugeType, stats.Tags{"customVal": jd.tablePrefix})
	jd.statTableCount = stats.Default.NewStat(fmt.Sprintf("jobsdb.%s_tables_count", jd.tablePrefix), stats.GaugeType)
	jd.statDSCount = stats.Default.NewTaggedStat("jobsdb.tables_count", stats.GaugeType, stats.Tags{"customVal": jd.tablePrefix})
	jd.unionQueryTime = stats.Default.NewTaggedStat("union_query_time", stats.TimerType, stats.Tags{
		"state":     "nonterminal",
		"customVal": jd.tablePrefix,
	})
	jd.statNewDSPeriod = stats.Default.NewTaggedStat("jobsdb.new_ds_period", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	jd.statDropDSPeriod = stats.Default.NewTaggedStat("jobsdb.drop_ds_period", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	jd.invalidCacheKeyStat = stats.Default.NewTaggedStat("jobsdb.invalid_cache_key", stats.CountType, stats.Tags{"customVal": jd.tablePrefix})

	enableWriterQueueKeys := []string{"JobsDB." + jd.tablePrefix + "." + "enableWriterQueue", "JobsDB." + "enableWriterQueue"}
	config.RegisterBoolConfigVariable(true, &jd.enableWriterQueue, true, enableWriterQueueKeys...)
	enableReaderQueueKeys := []string{"JobsDB." + jd.tablePrefix + "." + "enableReaderQueue", "JobsDB." + "enableReaderQueue"}
	config.RegisterBoolConfigVariable(true, &jd.enableReaderQueue, true, enableReaderQueueKeys...)
	maxWritersKeys := []string{"JobsDB." + jd.tablePrefix + "." + "maxWriters", "JobsDB." + "maxWriters"}
	config.RegisterIntConfigVariable(3, &jd.maxWriters, false, 1, maxWritersKeys...)
	maxReadersKeys := []string{"JobsDB." + jd.tablePrefix + "." + "maxReaders", "JobsDB." + "maxReaders"}
	config.RegisterIntConfigVariable(6, &jd.maxReaders, false, 1, maxReadersKeys...)
	maxOpenConnectionsKeys := []string{"JobsDB." + jd.tablePrefix + "." + "maxOpenConnections", "JobsDB." + "maxOpenConnections"}
	config.RegisterIntConfigVariable(20, &jd.maxOpenConnections, false, 1, maxOpenConnectionsKeys...)
	analyzeThresholdKeys := []string{"JobsDB." + jd.tablePrefix + "." + "analyzeThreshold", "JobsDB." + "analyzeThreshold"}
	config.RegisterIntConfigVariable(30000, &jd.analyzeThreshold, false, 1, analyzeThresholdKeys...)

	minDSRetentionPeriodKeys := []string{"JobsDB." + jd.tablePrefix + "." + "minDSRetention", "JobsDB." + "minDSRetention"}
	config.RegisterDurationConfigVariable(0, &jd.MinDSRetentionPeriod, true, time.Minute, minDSRetentionPeriodKeys...)
	maxDSRetentionPeriodKeys := []string{"JobsDB." + jd.tablePrefix + "." + "maxDSRetention", "JobsDB." + "maxDSRetention"}
	config.RegisterDurationConfigVariable(90, &jd.MaxDSRetentionPeriod, true, time.Minute, maxDSRetentionPeriodKeys...)
}

// Start starts the jobsdb worker and housekeeping (migration, archive) threads.
// Start should be called before any other jobsdb methods are called.
func (jd *HandleT) Start() error {
	jd.lifecycle.mu.Lock()
	defer jd.lifecycle.mu.Unlock()
	if jd.lifecycle.started {
		return nil
	}
	defer func() { jd.lifecycle.started = true }()

	jd.writeCapacity = make(chan struct{}, jd.maxWriters)
	jd.readCapacity = make(chan struct{}, jd.maxReaders)

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	jd.backgroundCancel = cancel
	jd.backgroundGroup = g

	if !jd.skipSetupDBSetup {
		jd.setUpForOwnerType(ctx, jd.ownerType)
	}
	return nil
}

func (jd *HandleT) setUpForOwnerType(ctx context.Context, ownerType OwnerType) {
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

func (jd *HandleT) startBackupDSLoop(ctx context.Context) {
	jd.backgroundGroup.Go(misc.WithBugsnag(func() error {
		jd.backupDSLoop(ctx)
		return nil
	}))
}

func (jd *HandleT) readerSetup(ctx context.Context, l lock.LockToken) {
	jd.recoverFromJournal(Read)

	// This is a thread-safe operation.
	// Even if two different services (gateway and processor) perform this operation, there should not be any problem.
	jd.recoverFromJournal(ReadWrite)
	jd.assertError(jd.doRefreshDSRangeList(l))

	g := jd.backgroundGroup
	g.Go(misc.WithBugsnag(func() error {
		jd.refreshDSListLoop(ctx)
		return nil
	}))

	jd.startBackupDSLoop(ctx)
	jd.startMigrateDSLoop(ctx)
	jd.startCleanupLoop(ctx)

	g.Go(misc.WithBugsnag(func() error {
		runArchiver(ctx, jd.tablePrefix, jd.dbHandle)
		return nil
	}))
}

func (jd *HandleT) writerSetup(ctx context.Context, l lock.LockToken) {
	jd.recoverFromJournal(Write)
	// This is a thread-safe operation.
	// Even if two different services (gateway and processor) perform this operation, there should not be any problem.
	jd.recoverFromJournal(ReadWrite)
	jd.assertError(jd.doRefreshDSRangeList(l))

	// If no DS present, add one
	if len(jd.getDSList()) == 0 {
		jd.addNewDS(l, newDataSet(jd.tablePrefix, jd.computeNewIdxForAppend(l)))
	}

	jd.backgroundGroup.Go(misc.WithBugsnag(func() error {
		jd.addNewDSLoop(ctx)
		return nil
	}))
}

func (jd *HandleT) readerWriterSetup(ctx context.Context, l lock.LockToken) {
	jd.recoverFromJournal(Read)

	jd.writerSetup(ctx, l)

	jd.startBackupDSLoop(ctx)
	jd.startMigrateDSLoop(ctx)
	jd.startCleanupLoop(ctx)

	jd.backgroundGroup.Go(misc.WithBugsnag(func() error {
		runArchiver(ctx, jd.tablePrefix, jd.dbHandle)
		return nil
	}))
}

// Stop stops the background goroutines and waits until they finish.
// Stop should be called once only after Start.
// Only Start and Close can be called after Stop.
func (jd *HandleT) Stop() {
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
func (jd *HandleT) TearDown() {
	jd.Stop()
	jd.Close()
}

// Close closes the database connection.
//
//	Stop should be called before Close.
func (jd *HandleT) Close() {
	_ = jd.dbHandle.Close()
}

/*
Function to return an ordered list of datasets and datasetRanges
Most callers use the in-memory list of dataset and datasetRanges
Caller must have the dsListLock readlocked
*/
func (jd *HandleT) getDSList() []dataSetT {
	return jd.datasetList
}

// doRefreshDSList refreshes the ds list from the database
func (jd *HandleT) doRefreshDSList(l lock.LockToken) ([]dataSetT, error) {
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
	jd.statDSCount.Gauge(len(jd.datasetList))

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

func (jd *HandleT) getDSRangeList() []dataSetRangeT {
	return jd.datasetRangeList
}

// doRefreshDSRangeList first refreshes the DS list and then calculate the DS range list
func (jd *HandleT) doRefreshDSRangeList(l lock.LockToken) error {
	var prevMax int64

	// At this point we must have write-locked dsListLock
	dsList, err := jd.doRefreshDSList(l)
	if err != nil {
		return fmt.Errorf("refreshDSList %w", err)
	}
	var datasetRangeList []dataSetRangeT

	for idx, ds := range dsList {
		jd.assert(ds.Index != "", "ds.Index is empty")

		getIndex := func() (sql.NullInt64, sql.NullInt64, error) {
			var minID, maxID sql.NullInt64
			sqlStatement := fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %q`, ds.JobTable)
			row := jd.dbHandle.QueryRow(sqlStatement)
			if err := row.Scan(&minID, &maxID); err != nil {
				return sql.NullInt64{}, sql.NullInt64{}, fmt.Errorf("scanning min & max jobID %w", err)
			}
			jd.logger.Debug(sqlStatement, minID, maxID)
			return minID, maxID, nil
		}
		minID, maxID, err := getIndex()
		if err != nil {
			return err
		}

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

		if idx < len(dsList)-1 {
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
	}
	jd.datasetRangeList = datasetRangeList
	return nil
}

func (jd *HandleT) getTableRowCount(jobTable string) int {
	var count int

	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) from %q`, jobTable)
	row := jd.dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&count)
	jd.assertError(err)
	return count
}

func (jd *HandleT) getTableSize(jobTable string) int64 {
	var tableSize int64

	sqlStatement := fmt.Sprintf(`SELECT PG_TOTAL_RELATION_SIZE('%s')`, jobTable)
	row := jd.dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&tableSize)
	jd.assertError(err)
	return tableSize
}

func (jd *HandleT) checkIfFullDSInTx(tx *Tx, ds dataSetT) (bool, error) {
	if jd.MaxDSRetentionPeriod > 0 {
		var minJobCreatedAt sql.NullTime
		sqlStatement := fmt.Sprintf(`SELECT MIN(created_at) FROM %q`, ds.JobTable)
		row := tx.QueryRow(sqlStatement)
		err := row.Scan(&minJobCreatedAt)
		if err != nil && err != sql.ErrNoRows {
			return false, err
		}
		if err == nil && minJobCreatedAt.Valid && time.Since(minJobCreatedAt.Time) > jd.MaxDSRetentionPeriod {
			return true, nil
		}
	}

	tableSize := jd.getTableSize(ds.JobTable)
	if tableSize > maxTableSize {
		jd.logger.Infof("[JobsDB] %s is full in size. Count: %v, Size: %v", ds.JobTable, jd.getTableRowCount(ds.JobTable), tableSize)
		return true, nil
	}

	totalCount := jd.getTableRowCount(ds.JobTable)
	if totalCount > *jd.MaxDSSize {
		jd.logger.Infof("[JobsDB] %s is full by rows. Count: %v, Size: %v", ds.JobTable, totalCount, jd.getTableSize(ds.JobTable))
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
		return
	}
	for _, str := range indexStr {
		levelInt, err = strconv.Atoi(str)
		if err != nil {
			return
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

func (jd *HandleT) addNewDS(l lock.LockToken, ds dataSetT) {
	err := jd.WithTx(func(tx *Tx) error {
		dsList, err := jd.doRefreshDSList(l)
		jd.assertError(err)
		return jd.addNewDSInTx(tx, l, dsList, ds)
	})
	jd.assertError(err)
	jd.assertError(jd.doRefreshDSRangeList(l))
}

// NOTE: If addNewDSInTx is directly called, make sure to explicitly call refreshDSRangeList(l) to update the DS list in cache, once transaction has completed.
func (jd *HandleT) addNewDSInTx(tx *Tx, l lock.LockToken, dsList []dataSetT, ds dataSetT) error {
	defer jd.getTimerStat(
		"add_new_ds",
		&statTags{CustomValFilters: []string{jd.tablePrefix}},
	).RecordDuration()()
	if l == nil {
		return errors.New("nil ds list lock token provided")
	}
	jd.logger.Infof("Creating new DS %+v", ds)
	err := jd.createDSInTx(tx, ds)
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

func (jd *HandleT) addDSInTx(tx *Tx, ds dataSetT) error {
	defer jd.getTimerStat(
		"add_new_ds",
		&statTags{CustomValFilters: []string{jd.tablePrefix}},
	).RecordDuration()()
	jd.logger.Infof("Creating DS %+v", ds)
	return jd.createDSInTx(tx, ds)
}

func (jd *HandleT) computeNewIdxForAppend(l lock.LockToken) string {
	dList, err := jd.doRefreshDSList(l)
	jd.assertError(err)
	return jd.doComputeNewIdxForAppend(dList)
}

func (jd *HandleT) doComputeNewIdxForAppend(dList []dataSetT) string {
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

func (jd *HandleT) createDSInTx(tx *Tx, newDS dataSetT) error {
	ctx := context.TODO()
	// Mark the start of operation. If we crash somewhere here, we delete the
	// DS being added
	opPayload, err := json.Marshal(&journalOpPayloadT{To: newDS})
	if err != nil {
		return err
	}

	opID, err := jd.JournalMarkStartInTx(tx, addDSOperation, opPayload)
	if err != nil {
		return err
	}

	// Create the jobs and job_status tables
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %q (
		job_id BIGSERIAL PRIMARY KEY,
		workspace_id TEXT NOT NULL DEFAULT '',
		uuid UUID NOT NULL,
		user_id TEXT NOT NULL,
		parameters JSONB NOT NULL,
		custom_val VARCHAR(64) NOT NULL,
		event_payload JSONB NOT NULL,
		event_count INTEGER NOT NULL DEFAULT 1,
		created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		expire_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW());`, newDS.JobTable)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_ws" ON %[1]q (workspace_id)`, newDS.JobTable)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_cv" ON %[1]q (custom_val)`, newDS.JobTable)); err != nil {
		return err
	}
	for _, param := range cacheParameterFilters {
		if _, err = tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_%[2]s" ON %[1]q USING BTREE ((parameters->>'%[2]s'))`, newDS.JobTable, param)); err != nil {
			return err
		}
	}

	// TODO : Evaluate a way to handle indexes only for particular tables
	if jd.tablePrefix == "rt" {
		if _, err = tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_cv_ws" ON %[1]q (custom_val,workspace_id)`, newDS.JobTable)); err != nil {
			return err
		}
	}
	if jd.tablePrefix == "batch_rt" { // for retrieving active partitions filtered by destination type when workspace isolation is enabled
		if _, err = tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_ws_cv" ON %[1]q (workspace_id,custom_val)`, newDS.JobTable)); err != nil {
			return err
		}
	}

	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %q (
		id BIGSERIAL,
		job_id BIGINT REFERENCES %q(job_id),
		job_state VARCHAR(64),
		attempt SMALLINT,
		exec_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		retry_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
		error_code VARCHAR(32),
		error_response JSONB DEFAULT '{}'::JSONB,
		parameters JSONB DEFAULT '{}'::JSONB,
		PRIMARY KEY (job_id, job_state, id));`, newDS.JobStatusTable, newDS.JobTable)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`CREATE INDEX "idx_%[1]s_jid_id" ON %[1]q(job_id asc,id desc)`, newDS.JobStatusTable)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, fmt.Sprintf(`CREATE VIEW "v_last_%[1]s" AS SELECT DISTINCT ON (job_id) * FROM %[1]q ORDER BY job_id ASC, id DESC`, newDS.JobStatusTable)); err != nil {
		return err
	}
	err = jd.journalMarkDoneInTx(tx, opID)
	if err != nil {
		return err
	}
	return nil
}

func (jd *HandleT) setSequenceNumberInTx(tx *Tx, l lock.LockToken, dsList []dataSetT, newDSIdx string) error {
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
func (jd *HandleT) GetMaxDSIndex() (maxDSIndex int64) {
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

func (jd *HandleT) prepareAndExecStmtInTx(tx *sql.Tx, sqlStatement string) {
	stmt, err := tx.Prepare(sqlStatement)
	jd.assertError(err)
	defer func() { _ = stmt.Close() }()

	_, err = stmt.Exec()
	jd.assertError(err)
}

func (jd *HandleT) prepareAndExecStmtInTxAllowMissing(tx *sql.Tx, sqlStatement string) {
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
		pqError, ok := err.(*pq.Error)
		if ok && pqError.Code == pq.ErrorCode("42P01") {
			jd.logger.Infof("[%s] sql statement(%s) exec failed because table doesn't exist", jd.tablePrefix, sqlStatement)
			_, err = tx.Exec(rollbackSql)
			jd.assertError(err)
		} else {
			jd.assertError(err)
		}
	}
}

func (jd *HandleT) dropDS(ds dataSetT) error {
	return jd.WithTx(func(tx *Tx) error {
		return jd.dropDSInTx(tx, ds)
	})
}

// dropDS drops a dataset
func (jd *HandleT) dropDSInTx(tx *Tx, ds dataSetT) error {
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
func (jd *HandleT) dropDSForRecovery(ds dataSetT) {
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

func (jd *HandleT) postDropDs(ds dataSetT) {
	jd.noResultsCache.InvalidateDataset(ds.Index)

	// Tracking time interval between drop ds operations. Hence calling end before start
	if jd.isStatDropDSPeriodInitialized {
		jd.statDropDSPeriod.Since(jd.dsDropTime)
	}
	jd.dsDropTime = time.Now()
	jd.isStatDropDSPeriodInitialized = true
}

// mustRenameDS renames a dataset
func (jd *HandleT) mustRenameDSInTx(tx *Tx, ds dataSetT) error {
	var sqlStatement string
	renamedJobStatusTable := fmt.Sprintf(`%s%s`, preDropTablePrefix, ds.JobStatusTable)
	renamedJobTable := fmt.Sprintf(`%s%s`, preDropTablePrefix, ds.JobTable)
	sqlStatement = fmt.Sprintf(`ALTER TABLE %q RENAME TO %q`, ds.JobStatusTable, renamedJobStatusTable)
	_, err := tx.Exec(sqlStatement)
	if err != nil {
		return fmt.Errorf("could not rename status table %s to %s: %w", ds.JobStatusTable, renamedJobStatusTable, err)
	}
	sqlStatement = fmt.Sprintf(`ALTER TABLE %q RENAME TO %q`, ds.JobTable, renamedJobTable)
	_, err = tx.Exec(sqlStatement)
	if err != nil {
		return fmt.Errorf("could not rename job table %s to %s: %w", ds.JobTable, renamedJobTable, err)
	}
	for _, preBackupHandler := range jd.preBackupHandlers {
		err = preBackupHandler.Handle(context.TODO(), tx.Tx, renamedJobTable, renamedJobStatusTable)
		if err != nil {
			return err
		}
	}
	// if jobs table is left empty after prebackup handlers, drop the dataset
	sqlStatement = fmt.Sprintf(`SELECT CASE WHEN EXISTS (SELECT * FROM %q) THEN 1 ELSE 0 END`, renamedJobTable)
	row := tx.QueryRow(sqlStatement)
	var count int
	if err = row.Scan(&count); err != nil {
		return fmt.Errorf("could not rename job table %s to %s: %w", ds.JobTable, renamedJobTable, err)
	}
	if count == 0 {
		if _, err = tx.Exec(fmt.Sprintf(`DROP TABLE %q CASCADE`, renamedJobStatusTable)); err != nil {
			return fmt.Errorf("could not drop empty pre_drop job status table %s: %w", renamedJobStatusTable, err)
		}
		if _, err = tx.Exec(fmt.Sprintf(`DROP TABLE %q CASCADE`, renamedJobTable)); err != nil {
			return fmt.Errorf("could not drop empty pre_drop job table %s: %w", renamedJobTable, err)
		}
	}
	return nil
}

// renameDS renames a dataset if it exists
func (jd *HandleT) renameDS(ds dataSetT) error {
	var sqlStatement string
	renamedJobStatusTable := fmt.Sprintf(`%s%s`, preDropTablePrefix, ds.JobStatusTable)
	renamedJobTable := fmt.Sprintf(`%s%s`, preDropTablePrefix, ds.JobTable)
	return jd.WithTx(func(tx *Tx) error {
		sqlStatement = fmt.Sprintf(`ALTER TABLE IF EXISTS %q RENAME TO %q`, ds.JobStatusTable, renamedJobStatusTable)
		_, err := tx.Exec(sqlStatement)
		if err != nil {
			return err
		}

		sqlStatement = fmt.Sprintf(`ALTER TABLE IF EXISTS %q RENAME TO %q`, ds.JobTable, renamedJobTable)
		_, err = tx.Exec(sqlStatement)
		if err != nil {
			return err
		}
		return nil
	})
}

func (jd *HandleT) getBackupDSList() ([]dataSetT, error) {
	var dsList []dataSetT
	// Read the table names from PG
	tableNames, err := getAllTableNames(jd.dbHandle)
	if err != nil {
		return dsList, err
	}

	jobNameMap := map[string]string{}
	jobStatusNameMap := map[string]string{}
	var dnumList []string

	tablePrefix := preDropTablePrefix + jd.tablePrefix
	for _, t := range tableNames {
		if strings.HasPrefix(t, tablePrefix+"_jobs_") {
			dnum := t[len(tablePrefix+"_jobs_"):]
			jobNameMap[dnum] = t
			dnumList = append(dnumList, dnum)
			continue
		}
		if strings.HasPrefix(t, tablePrefix+"_job_status_") {
			dnum := t[len(tablePrefix+"_job_status_"):]
			jobStatusNameMap[dnum] = t
			continue
		}
	}

	for _, dnum := range dnumList {
		dsList = append(dsList, dataSetT{
			JobTable:       jobNameMap[dnum],
			JobStatusTable: jobStatusNameMap[dnum],
			Index:          dnum,
		})
	}
	return dsList, nil
}

func (jd *HandleT) dropAllBackupDS() error {
	dsList, err := jd.getBackupDSList()
	if err != nil {
		return err
	}
	for _, ds := range dsList {
		if err := jd.dropDS(ds); err != nil {
			return err
		}
	}
	return nil
}

func (jd *HandleT) dropAllDS(l lock.LockToken) error {
	var err error
	dList, err := jd.doRefreshDSList(l)
	if err != nil {
		return fmt.Errorf("refreshDSList: %w", err)
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

func (jd *HandleT) internalStoreJobsInTx(ctx context.Context, tx *Tx, ds dataSetT, jobList []*JobT) error {
	defer jd.getTimerStat(
		"store_jobs",
		&statTags{CustomValFilters: []string{jd.tablePrefix}},
	).RecordDuration()()

	tx.AddSuccessListener(func() {
		jd.invalidateCacheForJobs(ds, jobList)
	})

	return jd.doStoreJobsInTx(ctx, tx, ds, jobList)
}

func (jd *HandleT) WithStoreSafeTx(ctx context.Context, f func(tx StoreSafeTx) error) error {
	return jd.inStoreSafeCtx(ctx, func() error {
		return jd.WithTx(func(tx *Tx) error { return f(&storeSafeTx{tx: tx, identity: jd.tablePrefix}) })
	})
}

func (jd *HandleT) inStoreSafeCtx(ctx context.Context, f func() error) error {
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
			jd.logger.Errorf("[JobsDB] :: Store failed: %v. Retrying after refreshing DS cache", errStaleDsList)
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

func (jd *HandleT) WithUpdateSafeTx(ctx context.Context, f func(tx UpdateSafeTx) error) error {
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

func (jd *HandleT) inUpdateSafeCtx(ctx context.Context, f func(dsList []dataSetT, dsRangeList []dataSetRangeT) error) error {
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

func (jd *HandleT) WithTx(f func(tx *Tx) error) error {
	sqltx, err := jd.dbHandle.Begin()
	if err != nil {
		return err
	}
	tx := &Tx{Tx: sqltx}
	err = f(tx)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("%w; %s", err, rollbackErr)
		}
		return err
	}
	return tx.Commit()
}

func (jd *HandleT) invalidateCacheForJobs(ds dataSetT, jobList []*JobT) {
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
			params = append(params, fmt.Sprintf("%s:%s", key, val))
			parameterFilters = append(parameterFilters, ParameterFilterT{Name: key, Value: val})
		}

		paramsKey := strings.Join(params, "#")
		if _, ok := cacheKeys[workspace][customVal][paramsKey]; !ok {
			cacheKeys[workspace][customVal][paramsKey] = struct{}{}
			jd.noResultsCache.Invalidate(ds.Index, workspace, []string{customVal}, []string{NotProcessed.State}, parameterFilters)
		}
	}
}

type moreToken struct {
	retryAfterJobID       *int64
	waitingAfterJobID     *int64
	unprocessedAfterJobID *int64
}

func (jd *HandleT) GetToProcess(ctx context.Context, params GetQueryParamsT, more MoreToken) (*MoreJobsResult, error) { // skipcq: CRT-P0003

	mtoken := &moreToken{}
	if more != nil {
		var ok bool
		if mtoken, ok = more.(*moreToken); !ok {
			return nil, fmt.Errorf("invalid token: %+v", more)
		}
	}
	updateParams := func(params *GetQueryParamsT, jobs JobsResult, nextAfterJobID *int64) {
		params.JobsLimit -= len(jobs.Jobs)
		if params.EventsLimit > 0 {
			params.EventsLimit -= jobs.EventsCount
		}
		if params.PayloadSizeLimit > 0 {
			params.PayloadSizeLimit -= jobs.PayloadSize
		}
		params.AfterJobID = nextAfterJobID
	}
	var list []*JobT
	params.AfterJobID = mtoken.retryAfterJobID
	toRetry, err := jd.GetToRetry(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(toRetry.Jobs) > 0 {
		retryAfterJobID := toRetry.Jobs[len(toRetry.Jobs)-1].JobID
		mtoken.retryAfterJobID = &retryAfterJobID
	}

	list = append(list, toRetry.Jobs...)
	if toRetry.LimitsReached {
		return &MoreJobsResult{JobsResult: JobsResult{Jobs: list, LimitsReached: true}, More: mtoken}, nil
	}
	updateParams(&params, toRetry, mtoken.waitingAfterJobID)

	waiting, err := jd.GetWaiting(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(waiting.Jobs) > 0 {
		waitingAfterJobID := waiting.Jobs[len(waiting.Jobs)-1].JobID
		mtoken.waitingAfterJobID = &waitingAfterJobID
	}
	list = append(list, waiting.Jobs...)
	if waiting.LimitsReached {
		return &MoreJobsResult{JobsResult: JobsResult{Jobs: list, LimitsReached: true}, More: mtoken}, nil
	}
	updateParams(&params, waiting, mtoken.unprocessedAfterJobID)

	unprocessed, err := jd.GetUnprocessed(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(unprocessed.Jobs) > 0 {
		unprocessedAfterJobID := unprocessed.Jobs[len(unprocessed.Jobs)-1].JobID
		mtoken.unprocessedAfterJobID = &unprocessedAfterJobID
	}
	list = append(list, unprocessed.Jobs...)
	return &MoreJobsResult{JobsResult: JobsResult{Jobs: list, LimitsReached: unprocessed.LimitsReached}, More: mtoken}, nil
}

var cacheParameterFilters = []string{"source_id", "destination_id"}

func (jd *HandleT) GetPileUpCounts(ctx context.Context) (map[string]map[string]int, error) {
	statMap := make(map[string]map[string]int)
	return statMap, nil
}

func (jd *HandleT) GetActiveWorkspaces(ctx context.Context, customVal string) ([]string, error) {
	if !jd.dsMigrationLock.RTryLockWithCtx(ctx) {
		return nil, fmt.Errorf("could not acquire a migration read lock: %w", ctx.Err())
	}
	defer jd.dsMigrationLock.RUnlock()
	if !jd.dsListLock.RTryLockWithCtx(ctx) {
		return nil, fmt.Errorf("could not acquire a dslist read lock: %w", ctx.Err())
	}
	dsList := jd.getDSList()
	jd.dsListLock.RUnlock()
	var workspaceIds []string
	var queries []string
	for _, ds := range dsList {
		if customVal == "" {
			queries = append(queries, fmt.Sprintf(`SELECT * FROM (WITH RECURSIVE t AS (
				(SELECT workspace_id FROM %[1]q ORDER BY workspace_id LIMIT 1)
				UNION ALL
				(SELECT s.* FROM t, LATERAL(
				  SELECT workspace_id FROM %[1]q f
				  WHERE f.workspace_id > t.workspace_id
				  ORDER BY workspace_id LIMIT 1) s)
			  )
			  SELECT * FROM t) a`, ds.JobTable))
		} else {
			queries = append(queries, fmt.Sprintf(`SELECT * FROM (WITH RECURSIVE t AS (
				(SELECT workspace_id FROM %[1]q WHERE custom_val = '%[2]s' ORDER BY workspace_id LIMIT 1)
				UNION ALL
				(SELECT s.* FROM t, LATERAL(
				  SELECT workspace_id FROM %[1]q f
				  WHERE custom_val = '%[2]s' AND f.workspace_id > t.workspace_id
				  ORDER BY workspace_id LIMIT 1) s)
			  )
			  SELECT * FROM t) a`, ds.JobTable, customVal))
		}
	}
	query := strings.Join(queries, " UNION ")
	rows, err := jd.dbHandle.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var workspaceId string
		err := rows.Scan(&workspaceId)
		if err != nil {
			return nil, err
		}
		workspaceIds = append(workspaceIds, workspaceId)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return workspaceIds, nil
}

func (jd *HandleT) GetDistinctParameterValues(ctx context.Context, parameterName string) ([]string, error) {
	if !jd.dsMigrationLock.RTryLockWithCtx(ctx) {
		return nil, fmt.Errorf("could not acquire a migration read lock: %w", ctx.Err())
	}
	defer jd.dsMigrationLock.RUnlock()
	if !jd.dsListLock.RTryLockWithCtx(ctx) {
		return nil, fmt.Errorf("could not acquire a dslist read lock: %w", ctx.Err())
	}
	dsList := jd.getDSList()
	jd.dsListLock.RUnlock()

	var values []string
	var queries []string
	for _, ds := range dsList {
		queries = append(queries, fmt.Sprintf(`SELECT * FROM (WITH RECURSIVE t AS (
				(SELECT parameters->>'%[1]s' as parameter FROM %[2]q ORDER BY parameters->>'%[1]s' LIMIT 1)
				UNION ALL
				(SELECT s.* FROM t, LATERAL(
				  SELECT parameters->>'%[1]s' as parameter FROM %[2]q f
				  WHERE f.parameters->>'%[1]s' > t.parameter
				  ORDER BY parameters->>'%[1]s' LIMIT 1) s)
			  )
			  SELECT * FROM t) a`, parameterName, ds.JobTable))
	}
	query := strings.Join(queries, " UNION ")
	rows, err := jd.dbHandle.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var value string
		err := rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return values, nil
}

func (jd *HandleT) doStoreJobsInTx(ctx context.Context, tx *Tx, ds dataSetT, jobList []*JobT) error {
	store := func() error {
		var stmt *sql.Stmt
		var err error

		stmt, err = tx.PrepareContext(ctx, pq.CopyIn(ds.JobTable, "uuid", "user_id", "custom_val", "parameters", "event_payload", "event_count", "workspace_id"))
		if err != nil {
			return err
		}

		defer func() { _ = stmt.Close() }()
		for _, job := range jobList {
			eventCount := 1
			if job.EventCount > 1 {
				eventCount = job.EventCount
			}

			if _, err = stmt.ExecContext(ctx, job.UUID, job.UserID, job.CustomVal, string(job.Parameters), string(job.EventPayload), eventCount, job.WorkspaceId); err != nil {
				return err
			}
		}
		if _, err = stmt.ExecContext(ctx); err != nil {
			return err
		}
		if len(jobList) > jd.analyzeThreshold {
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
				jobList[i].sanitizeJson()
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
func (jd *HandleT) getProcessedJobsDS(ctx context.Context, ds dataSetT, params GetQueryParamsT) (JobsResult, bool, error) { // skipcq: CRT-P0003
	stateFilters := params.StateFilters
	customValFilters := params.CustomValFilters
	parameterFilters := params.ParameterFilters
	workspaceID := params.WorkspaceID
	checkValidJobState(jd, stateFilters)

	if jd.noResultsCache.Get(ds.Index, workspaceID, customValFilters, stateFilters, parameterFilters) {
		jd.logger.Debugf("[getProcessedJobsDS] Empty cache hit for ds: %v, stateFilters: %v, customValFilters: %v, parameterFilters: %v", ds, stateFilters, customValFilters, parameterFilters)
		return JobsResult{}, false, nil
	}

	tags := statTags{
		CustomValFilters: params.CustomValFilters,
		StateFilters:     params.StateFilters,
		ParameterFilters: params.ParameterFilters,
		WorkspaceID:      workspaceID,
	}

	defer jd.getTimerStat("processed_ds_time", &tags).RecordDuration()()

	skipCacheResult := params.AfterJobID != nil
	var cacheTx *cache.NoResultTx[ParameterFilterT]
	if !skipCacheResult {
		cacheTx = jd.noResultsCache.StartNoResultTx(ds.Index, workspaceID, customValFilters, stateFilters, parameterFilters)
	}

	var stateQuery string
	if len(stateFilters) > 0 {
		stateQuery = " AND " + constructQueryOR("job_latest_state.job_state", stateFilters)
	}

	var filterConditions []string
	if params.AfterJobID != nil {
		filterConditions = append(filterConditions, fmt.Sprintf("jobs.job_id > %d", *params.AfterJobID))
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

	filterQuery := strings.Join(filterConditions, " AND ")
	if filterQuery != "" {
		filterQuery = " AND " + filterQuery
	}

	var limitQuery string
	if params.JobsLimit > 0 {
		limitQuery = fmt.Sprintf(" LIMIT %d ", params.JobsLimit)
	}

	var rows *sql.Rows
	sqlStatement := fmt.Sprintf(`SELECT
									jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count,
									jobs.created_at, jobs.expire_at, jobs.workspace_id,
									pg_column_size(jobs.event_payload) as payload_size,
									sum(jobs.event_count) over (order by jobs.job_id asc) as running_event_counts,
									sum(pg_column_size(jobs.event_payload)) over (order by jobs.job_id) as running_payload_size,
									job_latest_state.job_state, job_latest_state.attempt,
									job_latest_state.exec_time, job_latest_state.retry_time,
									job_latest_state.error_code, job_latest_state.error_response, job_latest_state.parameters
								FROM
									%[1]q AS jobs
									JOIN "v_last_%[2]s" job_latest_state ON jobs.job_id=job_latest_state.job_id
								    %[3]s
									%[4]s
									ORDER BY jobs.job_id %[5]s`,
		ds.JobTable, ds.JobStatusTable, stateQuery, filterQuery, limitQuery)

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

	for rows.Next() {
		var job JobT

		err := rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.EventCount, &job.CreatedAt, &job.ExpireAt, &job.WorkspaceId, &job.PayloadSize, &runningEventCount, &runningPayloadSize,
			&job.LastJobStatus.JobState, &job.LastJobStatus.AttemptNum,
			&job.LastJobStatus.ExecTime, &job.LastJobStatus.RetryTime,
			&job.LastJobStatus.ErrorCode, &job.LastJobStatus.ErrorResponse, &job.LastJobStatus.Parameters)
		if err != nil {
			return JobsResult{}, false, err
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
		if len(jobList) == 0 {
			cacheTx.Commit()
		}
	}

	return JobsResult{
		Jobs:          jobList,
		LimitsReached: limitsReached,
		PayloadSize:   payloadSize,
		EventsCount:   eventCount,
	}, true, nil
}

/*
count == 0 means return all
stateFilters and customValFilters do a OR query on values passed in array
parameterFilters do a AND query on values included in the map.
A JobsLimit less than or equal to zero indicates no limit.
*/
func (jd *HandleT) getUnprocessedJobsDS(ctx context.Context, ds dataSetT, params GetQueryParamsT) (JobsResult, bool, error) { // skipcq: CRT-P0003
	customValFilters := params.CustomValFilters
	parameterFilters := params.ParameterFilters
	workspaceID := params.WorkspaceID

	if jd.noResultsCache.Get(ds.Index, workspaceID, customValFilters, []string{NotProcessed.State}, parameterFilters) {
		jd.logger.Debugf("[getUnprocessedJobsDS] Empty cache hit for ds: %v, stateFilters: NP, customValFilters: %v, parameterFilters: %v", ds, customValFilters, parameterFilters)
		return JobsResult{}, false, nil
	}

	tags := statTags{
		CustomValFilters: params.CustomValFilters,
		ParameterFilters: params.ParameterFilters,
		WorkspaceID:      workspaceID,
	}
	defer jd.getTimerStat("unprocessed_ds_time", &tags).RecordDuration()()

	skipCacheResult := params.AfterJobID != nil
	var cacheTx *cache.NoResultTx[ParameterFilterT]
	if !skipCacheResult {
		cacheTx = jd.noResultsCache.StartNoResultTx(ds.Index, workspaceID, customValFilters, []string{NotProcessed.State}, parameterFilters)
	}

	var rows *sql.Rows
	var err error
	var args []interface{}

	// event_count default 1, number of items in payload
	sqlStatement := fmt.Sprintf(
		`SELECT jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count, jobs.created_at, jobs.expire_at, jobs.workspace_id,`+
			`	pg_column_size(jobs.event_payload) as payload_size, `+
			`	sum(jobs.event_count) over (order by jobs.job_id asc) as running_event_counts, `+
			`	sum(pg_column_size(jobs.event_payload)) over (order by jobs.job_id) as running_payload_size `+
			`FROM %[1]q AS jobs `+
			`LEFT JOIN %[2]q job_status ON jobs.job_id=job_status.job_id `+
			`WHERE job_status.job_id is NULL `,
		ds.JobTable, ds.JobStatusTable)

	if params.AfterJobID != nil {
		sqlStatement += fmt.Sprintf(" AND jobs.job_id > %d", *params.AfterJobID)
	}
	if len(customValFilters) > 0 && !params.IgnoreCustomValFiltersInQuery {
		sqlStatement += " AND " + constructQueryOR("jobs.custom_val", customValFilters)
	}
	if len(parameterFilters) > 0 {
		sqlStatement += " AND " + constructParameterJSONQuery("jobs", parameterFilters)
	}
	if workspaceID != "" {
		sqlStatement += fmt.Sprintf(" AND jobs.workspace_id = '%s'", workspaceID)
	}
	sqlStatement += " ORDER BY jobs.job_id"
	if params.JobsLimit > 0 {
		sqlStatement += fmt.Sprintf(" LIMIT $%d", len(args)+1)
		args = append(args, params.JobsLimit)
	}

	var wrapQuery []string
	if params.EventsLimit > 0 {
		// If there is a single job in the dataset containing more events than the EventsLimit, we should return it,
		// otherwise processing will halt.
		// Therefore, we always retrieve one more job from the database than our limit dictates.
		// This job will only be returned in the result in case of the aforementioned scenario, otherwise it gets filtered out
		// later, during row scanning
		wrapQuery = append(wrapQuery, fmt.Sprintf(`running_event_counts - subquery.event_count <= $%d`, len(args)+1))
		args = append(args, params.EventsLimit)
	}

	if params.PayloadSizeLimit > 0 {
		wrapQuery = append(wrapQuery, fmt.Sprintf(`running_payload_size - subquery.payload_size <= $%d`, len(args)+1))
		args = append(args, params.PayloadSizeLimit)
	}

	if len(wrapQuery) > 0 {
		sqlStatement = `SELECT * FROM (` + sqlStatement + `) subquery WHERE ` + strings.Join(wrapQuery, " AND ")
	}

	rows, err = jd.dbHandle.QueryContext(ctx, sqlStatement, args...)
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

	for rows.Next() {
		var job JobT
		err := rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.EventCount, &job.CreatedAt, &job.ExpireAt, &job.WorkspaceId, &job.PayloadSize, &runningEventCount, &runningPayloadSize)
		if err != nil {
			return JobsResult{}, false, err
		}
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
		dsList := jd.getDSList()
		// if jobsdb owner is a reader and if ds is the right most one, ignoring setting result as noJobs
		if len(jobList) == 0 && (jd.ownerType != Read || ds.Index != dsList[len(dsList)-1].Index) {
			cacheTx.Commit()
		}
	}
	return JobsResult{
		Jobs:          jobList,
		LimitsReached: limitsReached,
		PayloadSize:   payloadSize,
		EventsCount:   eventCount,
	}, true, nil
}

func (jd *HandleT) updateJobStatusDSInTx(ctx context.Context, tx *Tx, ds dataSetT, statusList []*JobStatusT, tags statTags) (updatedStates map[string]map[string]map[ParameterFilterT]struct{}, err error) {
	if len(statusList) == 0 {
		return
	}

	defer jd.getTimerStat(
		"update_job_status_ds_time",
		&tags,
	).RecordDuration()()
	// workspace -> state -> params
	updatedStates = map[string]map[string]map[ParameterFilterT]struct{}{}
	store := func() error {
		stmt, err := tx.PrepareContext(ctx, pq.CopyIn(ds.JobStatusTable, "job_id", "job_state", "attempt", "exec_time",
			"retry_time", "error_code", "error_response", "parameters"))
		if err != nil {
			return err
		}

		defer func() { _ = stmt.Close() }()
		for _, status := range statusList {
			//  Handle the case when google analytics returns gif in response
			if _, ok := updatedStates[status.WorkspaceId]; !ok {
				updatedStates[status.WorkspaceId] = make(map[string]map[ParameterFilterT]struct{})
			}
			if _, ok := updatedStates[status.WorkspaceId][status.JobState]; !ok {
				updatedStates[status.WorkspaceId][status.JobState] = make(map[ParameterFilterT]struct{})
			}
			if status.JobParameters != nil {
				for _, param := range cacheParameterFilters {
					v := gjson.GetBytes(status.JobParameters, param).Str
					updatedStates[status.WorkspaceId][status.JobState][ParameterFilterT{Name: param, Value: v}] = struct{}{}
				}
			}

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

		if len(statusList) > jd.analyzeThreshold {
			_, err = tx.ExecContext(ctx, fmt.Sprintf(`ANALYZE %q`, ds.JobStatusTable))
		}

		return err
	}
	const (
		savepointSql = "SAVEPOINT updateJobStatusDSInTx"
		rollbackSql  = "ROLLBACK TO " + savepointSql
	)
	if _, err = tx.ExecContext(ctx, savepointSql); err != nil {
		return
	}
	err = store()
	var e *pq.Error
	if err != nil && errors.As(err, &e) {
		if _, ok := dbInvalidJsonErrors[string(e.Code)]; ok {
			if _, err = tx.ExecContext(ctx, rollbackSql); err != nil {
				return
			}
			for i := range statusList {
				statusList[i].sanitizeJson()
			}
			err = store()
		}
	}
	return
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
func (jd *HandleT) addNewDSLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-jd.TriggerAddNewDS():
		}
		var dsListLock lock.LockToken
		var releaseDsListLock chan<- lock.LockToken
		addNewDS := func() error {
			defer func() {
				if releaseDsListLock != nil && dsListLock != nil {
					releaseDsListLock <- dsListLock
				}
			}()
			// Adding a new DS only creates a new DS & updates the cache. It doesn't move any data so we only take the list lock.
			// start a transaction
			err := jd.WithTx(func(tx *Tx) error {
				return jd.withDistributedSharedLock(context.TODO(), tx, "schema_migrate", func() error { // cannot run while schema migration is running
					return jd.withDistributedLock(context.TODO(), tx, "add_ds", func() error { // only one add_ds can run at a time
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
								return err
							}
							if _, err = tx.Exec(fmt.Sprintf(`LOCK TABLE %q IN EXCLUSIVE MODE;`, latestDS.JobTable)); err != nil {
								return fmt.Errorf("error locking table %s: %w", latestDS.JobTable, err)
							}

							nextDSIdx = jd.doComputeNewIdxForAppend(dsList)
							jd.logger.Infof("[[ %s : addNewDSLoop ]]: NewDS", jd.tablePrefix)
							if err = jd.addNewDSInTx(tx, dsListLock, dsList, newDataSet(jd.tablePrefix, nextDSIdx)); err != nil {
								return fmt.Errorf("error adding new DS: %w", err)
							}

							// previous DS should become read only
							if err = setReadonlyDsInTx(tx, latestDS); err != nil {
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
			if !jd.skipMaintenanceError && ctx.Err() == nil {
				panic(err)
			}
			jd.logger.Errorw("addNewDSLoop", "error", err)
		}
	}
}

func (jd *HandleT) getAdvisoryLockForOperation(operation string) int64 {
	key := fmt.Sprintf("%s_%s", jd.tablePrefix, operation)
	h := sha256.New()
	h.Write([]byte(key))
	return int64(binary.BigEndian.Uint32(h.Sum(nil)))
}

func setReadonlyDsInTx(tx *Tx, latestDS dataSetT) error {
	sqlStatement := fmt.Sprintf(
		`CREATE TRIGGER readonlyTableTrg
		BEFORE INSERT
		ON %q
		FOR EACH STATEMENT
		EXECUTE PROCEDURE %s;`, latestDS.JobTable, pgReadonlyTableExceptionFuncName)
	_, err := tx.Exec(sqlStatement)
	return err
}

func (jd *HandleT) refreshDSListLoop(ctx context.Context) {
	for {
		select {
		case <-jd.TriggerRefreshDS():
		case <-ctx.Done():
			return
		}
		timeoutCtx, cancel := context.WithTimeout(ctx, jd.refreshDSTimeout)
		if err := jd.refreshDSList(timeoutCtx); err != nil {
			cancel()
			if !jd.skipMaintenanceError && ctx.Err() == nil {
				panic(err)
			}
			jd.logger.Errorw("refreshDSListLoop", "error", err)
		}
		cancel()
	}
}

// refreshDSList refreshes the list of datasets in memory if the database view of the list has changed.
func (jd *HandleT) refreshDSList(ctx context.Context) error {
	jd.logger.Debugw("Start", "operation", "refreshDSListLoop")

	start := time.Now()
	var err error
	defer func() {
		stats.Default.NewTaggedStat("refresh_ds_loop", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix, "error": strconv.FormatBool(err != nil)}).Since(start)
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
	defer stats.Default.NewTaggedStat("refresh_ds_loop_lock", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix}).RecordDuration()()
	err = jd.dsListLock.WithLockInCtx(ctx, func(l lock.LockToken) error {
		return jd.doRefreshDSRangeList(l)
	})
	if err != nil {
		return fmt.Errorf("refreshDSRangeList: %w", err)
	}

	return nil
}

// Identifier returns the identifier of the jobsdb. Here it is tablePrefix.
func (jd *HandleT) Identifier() string {
	return jd.tablePrefix
}

/*
We keep a journal of all the operations. The journal helps
*/
const (
	addDSOperation             = "ADD_DS"
	migrateCopyOperation       = "MIGRATE_COPY"
	postMigrateDSOperation     = "POST_MIGRATE_DS_OP"
	backupDSOperation          = "BACKUP_DS"
	backupDropDSOperation      = "BACKUP_DROP_DS"
	dropDSOperation            = "DROP_DS"
	RawDataDestUploadOperation = "S3_DEST_UPLOAD"
)

type JournalEntryT struct {
	OpID      int64
	OpType    string
	OpDone    bool
	OpPayload json.RawMessage
}

func (jd *HandleT) dropJournal() {
	sqlStatement := fmt.Sprintf(`DROP TABLE IF EXISTS %s_journal`, jd.tablePrefix)
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
}

func (jd *HandleT) JournalMarkStart(opType string, opPayload json.RawMessage) (int64, error) {
	var opID int64
	return opID, jd.WithTx(func(tx *Tx) error {
		var err error
		opID, err = jd.JournalMarkStartInTx(tx, opType, opPayload)
		return err
	})
}

func (jd *HandleT) JournalMarkStartInTx(tx *Tx, opType string, opPayload json.RawMessage) (int64, error) {
	var opID int64
	jd.assert(opType == addDSOperation ||
		opType == migrateCopyOperation ||
		opType == postMigrateDSOperation ||
		opType == backupDSOperation ||
		opType == backupDropDSOperation ||
		opType == dropDSOperation ||
		opType == RawDataDestUploadOperation, fmt.Sprintf("opType: %s is not a supported op", opType))

	sqlStatement := fmt.Sprintf(`INSERT INTO %s_journal (operation, done, operation_payload, start_time, owner)
                                       VALUES ($1, $2, $3, $4, $5) RETURNING id`, jd.tablePrefix)
	err := tx.QueryRow(sqlStatement, opType, false, opPayload, time.Now(), jd.ownerType).Scan(&opID)
	return opID, err
}

// JournalMarkDone marks the end of a journal action
func (jd *HandleT) JournalMarkDone(opID int64) error {
	return jd.WithTx(func(tx *Tx) error {
		return jd.journalMarkDoneInTx(tx, opID)
	})
}

// JournalMarkDoneInTx marks the end of a journal action in a transaction
func (jd *HandleT) journalMarkDoneInTx(tx *Tx, opID int64) error {
	sqlStatement := fmt.Sprintf(`UPDATE %s_journal SET done=$2, end_time=$3 WHERE id=$1 AND owner=$4`, jd.tablePrefix)
	_, err := tx.Exec(sqlStatement, opID, true, time.Now(), jd.ownerType)
	return err
}

func (jd *HandleT) JournalDeleteEntry(opID int64) {
	sqlStatement := fmt.Sprintf(`DELETE from "%s_journal" WHERE id=$1 AND owner=$2`, jd.tablePrefix)
	_, err := jd.dbHandle.Exec(sqlStatement, opID, jd.ownerType)
	jd.assertError(err)
}

func (jd *HandleT) GetJournalEntries(opType string) (entries []JournalEntryT) {
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
	return
}

func (jd *HandleT) recoverFromCrash(owner OwnerType, goRoutineType string) {
	var opTypes []string
	switch goRoutineType {
	case addDSGoRoutine:
		opTypes = []string{addDSOperation}
	case mainGoRoutine:
		opTypes = []string{migrateCopyOperation, postMigrateDSOperation, dropDSOperation}
	case backupGoRoutine:
		opTypes = []string{backupDSOperation, backupDropDSOperation}
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
	err = json.Unmarshal(opPayload, &opPayloadJSON)
	jd.assertError(err)

	switch opType {
	case addDSOperation:
		newDS := opPayloadJSON.To
		undoOp = true
		// Drop the table we were tring to create
		jd.logger.Info("Recovering new DS operation", newDS)
		jd.dropDSForRecovery(newDS)
	case migrateCopyOperation:
		migrateDest := opPayloadJSON.To
		// Delete the destination of the interrupted
		// migration. After we start, code should
		// redo the migration
		jd.logger.Info("Recovering migrateCopy operation", migrateDest)
		jd.dropDSForRecovery(migrateDest)
		undoOp = true
	case postMigrateDSOperation:
		migrateSrc := opPayloadJSON.From
		for _, ds := range migrateSrc {
			if jd.BackupSettings.isBackupEnabled() {
				jd.assertError(jd.renameDS(ds))
			} else {
				jd.dropDSForRecovery(ds)
			}
		}
		jd.logger.Info("Recovering migrateDel operation", migrateSrc)
		undoOp = false
	case backupDSOperation:
		jd.removeTableJSONDumps()
		jd.logger.Info("Removing all stale json dumps of tables")
		undoOp = true
	case dropDSOperation, backupDropDSOperation:
		var dataset dataSetT
		jd.assertError(json.Unmarshal(opPayload, &dataset))
		jd.dropDSForRecovery(dataset)
		jd.logger.Info("Recovering dropDS operation", dataset)
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
	addDSGoRoutine  = "addDS"
	mainGoRoutine   = "main"
	backupGoRoutine = "backup"
)

func (jd *HandleT) recoverFromJournal(owner OwnerType) {
	jd.recoverFromCrash(owner, addDSGoRoutine)
	jd.recoverFromCrash(owner, mainGoRoutine)
	jd.recoverFromCrash(owner, backupGoRoutine)
}

func (jd *HandleT) UpdateJobStatus(ctx context.Context, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error {
	return jd.WithUpdateSafeTx(ctx, func(tx UpdateSafeTx) error {
		return jd.UpdateJobStatusInTx(ctx, tx, statusList, customValFilters, parameterFilters)
	})
}

/*
internalUpdateJobStatusInTx updates the status of a batch of jobs
customValFilters[] is passed, so we can efficiently mark empty cache
Later we can move this to query
*/
func (jd *HandleT) internalUpdateJobStatusInTx(ctx context.Context, tx *Tx, dsList []dataSetT, dsRangeList []dataSetRangeT, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error {
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
		jd.logger.Infof("[[ %s ]]: Error occurred while updating job statuses. Returning err, %v", jd.tablePrefix, err)
		return err
	}

	tx.AddSuccessListener(func() {
		// clear cache
		for ds, dsKeys := range updatedStatesByDS {
			if len(dsKeys) == 0 { // if no keys, we need to invalidate all keys
				jd.noResultsCache.Invalidate(ds.Index, "", nil, nil, nil)
			}
			for workspace, wsKeys := range dsKeys {
				if len(wsKeys) == 0 { // if no keys, we need to invalidate all keys
					jd.noResultsCache.Invalidate(ds.Index, workspace, nil, nil, nil)
				}
				for state, parametersMap := range wsKeys {
					stateList := []string{state}
					if len(parametersMap) == 0 { // if no keys, we need to invalidate all keys
						jd.noResultsCache.Invalidate(ds.Index, workspace, customValFilters, stateList, nil)
					}
					parameterFilters := lo.Keys(parametersMap)
					jd.noResultsCache.Invalidate(ds.Index, workspace, customValFilters, stateList, parameterFilters)
				}
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
func (jd *HandleT) doUpdateJobStatusInTx(ctx context.Context, tx *Tx, dsList []dataSetT, dsRangeList []dataSetRangeT, statusList []*JobStatusT, tags statTags) (updatedStatesByDS map[dataSetT]map[string]map[string]map[ParameterFilterT]struct{}, err error) {
	if len(statusList) == 0 {
		return
	}

	// First we sort by JobID
	sort.Slice(statusList, func(i, j int) bool {
		return statusList[i].JobID < statusList[j].JobID
	})

	// We scan through the list of jobs and map them to DS
	var lastPos int
	updatedStatesByDS = make(map[dataSetT]map[string]map[string]map[ParameterFilterT]struct{})
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
				if i > lastPos {
					jd.logger.Debug("Range:", ds, statusList[lastPos].JobID,
						statusList[i-1].JobID, lastPos, i-1)
				}
				var updatedStates map[string]map[string]map[ParameterFilterT]struct{}
				updatedStates, err = jd.updateJobStatusDSInTx(ctx, tx, ds.ds, statusList[lastPos:i], tags)
				if err != nil {
					return
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
			jd.logger.Debug("Range:", ds, statusList[lastPos].JobID, statusList[i-1].JobID, lastPos, i)
			var updatedStates map[string]map[string]map[ParameterFilterT]struct{}
			updatedStates, err = jd.updateJobStatusDSInTx(ctx, tx, ds.ds, statusList[lastPos:i], tags)
			if err != nil {
				return
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
		jd.logger.Debug("RangeEnd ", statusList[lastPos].JobID, lastPos, len(statusList))
		var updatedStates map[string]map[string]map[ParameterFilterT]struct{}
		updatedStates, err = jd.updateJobStatusDSInTx(ctx, tx, dsList[len(dsList)-1], statusList[lastPos:], tags)
		if err != nil {
			return
		}
		// do not set for ds without any new state written as it would clear emptyCache
		if len(updatedStates) > 0 {
			updatedStatesByDS[dsList[len(dsList)-1]] = updatedStates
		}
	}
	return
}

// Store stores new jobs to the jobsdb.
// If enableWriterQueue is true, this goes through writer worker pool.
func (jd *HandleT) Store(ctx context.Context, jobList []*JobT) error {
	return jd.WithStoreSafeTx(ctx, func(tx StoreSafeTx) error {
		return jd.StoreInTx(ctx, tx, jobList)
	})
}

// StoreInTx stores new jobs to the jobsdb.
// If enableWriterQueue is true, this goes through writer worker pool.
func (jd *HandleT) StoreInTx(ctx context.Context, tx StoreSafeTx, jobList []*JobT) error {
	storeCmd := func() error {
		command := func() interface{} {
			dsList := jd.getDSList()
			err := jd.internalStoreJobsInTx(ctx, tx.Tx(), dsList[len(dsList)-1], jobList)
			return err
		}
		err, _ := jd.executeDbRequest(newWriteDbRequest("store", nil, command)).(error)
		return err
	}

	if tx.storeSafeTxIdentifier() != jd.Identifier() {
		return jd.inStoreSafeCtx(ctx, storeCmd)
	}
	return storeCmd()
}

func (jd *HandleT) StoreEachBatchRetry(
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

func (jd *HandleT) StoreEachBatchRetryInTx(
	ctx context.Context,
	tx StoreSafeTx,
	jobBatches [][]*JobT,
) (map[uuid.UUID]string, error) {
	var (
		err error
		res map[uuid.UUID]string
	)
	storeCmd := func() error {
		command := func() interface{} {
			dsList := jd.getDSList()
			res, err = jd.internalStoreEachBatchRetryInTx(
				ctx,
				tx.Tx(),
				dsList[len(dsList)-1],
				jobBatches,
			)
			return res
		}
		res, _ = jd.executeDbRequest(
			newWriteDbRequest("store_each_batch_retry", nil, command),
		).(map[uuid.UUID]string)
		return err
	}
	if tx.storeSafeTxIdentifier() != jd.Identifier() {
		_ = jd.inStoreSafeCtx(ctx, storeCmd)
		return res, err
	}
	_ = storeCmd()
	return res, err
}

func (jd *HandleT) internalStoreEachBatchRetryInTx(
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
		return
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
	return
}

/*
printLists is a debugging function used to print
the current in-memory copy of jobs and job ranges
*/
func (jd *HandleT) printLists(console bool) {
	// This being an internal function, we don't lock
	jd.logger.Debug("List:", jd.getDSList())
	jd.logger.Debug("Ranges:", jd.getDSRangeList())
	if console {
		fmt.Println("List:", jd.getDSList())
		fmt.Println("Ranges:", jd.getDSRangeList())
	}
}

/*
GetUnprocessed returns the unprocessed events. Unprocessed events are
those whose state hasn't been marked in the DB.
If enableReaderQueue is true, this goes through worker pool, else calls getUnprocessed directly.
*/
func (jd *HandleT) GetUnprocessed(ctx context.Context, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	if params.JobsLimit <= 0 {
		return JobsResult{}, nil
	}

	tags := statTags{
		CustomValFilters: params.CustomValFilters,
		ParameterFilters: params.ParameterFilters,
		WorkspaceID:      params.WorkspaceID,
	}
	command := func() interface{} {
		return queryResultWrapper(jd.getUnprocessed(ctx, params))
	}
	res, _ := jd.executeDbRequest(newReadDbRequest("unprocessed", &tags, command)).(queryResult)
	return res.JobsResult, res.err
}

/*
getUnprocessed returns the unprocessed events. Unprocessed events are
those whose state hasn't been marked in the DB
*/
func (jd *HandleT) getUnprocessed(ctx context.Context, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	if params.JobsLimit <= 0 {
		return JobsResult{}, nil
	}

	tags := statTags{
		CustomValFilters: params.CustomValFilters,
		ParameterFilters: params.ParameterFilters,
		WorkspaceID:      params.WorkspaceID,
	}
	defer jd.getTimerStat(
		"unprocessed_jobs_time",
		&tags,
	).RecordDuration()()

	// The order of lock is very important. The migrateDSLoop
	// takes lock in this order so reversing this will cause
	// deadlocks
	if !jd.dsMigrationLock.RTryLockWithCtx(ctx) {
		return JobsResult{}, fmt.Errorf("could not acquire a migration read lock: %w", ctx.Err())
	}
	defer jd.dsMigrationLock.RUnlock()
	if !jd.dsListLock.RTryLockWithCtx(ctx) {
		return JobsResult{}, fmt.Errorf("could not acquire a dslist read lock: %w", ctx.Err())
	}
	dsRangeList := jd.getDSRangeList()
	dsList := jd.getDSList()
	jd.dsListLock.RUnlock()

	limitByEventCount := false
	if params.EventsLimit > 0 {
		limitByEventCount = true
	}

	limitByPayloadSize := false
	if params.PayloadSizeLimit > 0 {
		limitByPayloadSize = true
	}

	var completeUnprocessedJobs JobsResult
	var dsQueryCount int
	var cacheHitCount int
	var dsLimit int
	if jd.dsLimit != nil {
		dsLimit = *jd.dsLimit
	}
	for idx, ds := range dsList {
		if params.AfterJobID != nil {
			if idx < len(dsRangeList) { // ranges are not stored for the last ds
				// so the following condition cannot be applied the last ds
				if *params.AfterJobID > dsRangeList[idx].maxJobID {
					continue
				}
			}
		}
		if dsLimit > 0 && dsQueryCount >= dsLimit {
			break
		}
		unprocessedJobs, dsHit, err := jd.getUnprocessedJobsDS(ctx, ds, params)
		if err != nil {
			return JobsResult{}, err
		}
		if dsHit {
			dsQueryCount++
		} else {
			cacheHitCount++
		}
		completeUnprocessedJobs.Jobs = append(completeUnprocessedJobs.Jobs, unprocessedJobs.Jobs...)
		completeUnprocessedJobs.EventsCount += unprocessedJobs.EventsCount
		completeUnprocessedJobs.PayloadSize += unprocessedJobs.PayloadSize

		if unprocessedJobs.LimitsReached {
			completeUnprocessedJobs.LimitsReached = true
			break
		}
		// decrement our limits for the next query
		if params.JobsLimit > 0 {
			params.JobsLimit -= len(unprocessedJobs.Jobs)
		}
		if limitByEventCount {
			params.EventsLimit -= unprocessedJobs.EventsCount
		}
		if limitByPayloadSize {
			params.PayloadSizeLimit -= unprocessedJobs.PayloadSize
		}
	}

	statTags := tags.getStatsTags(jd.tablePrefix)
	statTags["query"] = "unprocessed"
	stats.Default.NewTaggedStat("jobsdb_tables_queried", stats.CountType, statTags).Count(dsQueryCount)
	stats.Default.NewTaggedStat("jobsdb_cache_hits", stats.CountType, statTags).Count(cacheHitCount)
	// Release lock
	return completeUnprocessedJobs, nil
}

func (jd *HandleT) GetImporting(ctx context.Context, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	if params.JobsLimit == 0 {
		return JobsResult{}, nil
	}
	params.StateFilters = []string{Importing.State}
	tags := statTags{
		CustomValFilters: params.CustomValFilters,
		StateFilters:     params.StateFilters,
		ParameterFilters: params.ParameterFilters,
		WorkspaceID:      params.WorkspaceID,
	}
	command := func() interface{} {
		return queryResultWrapper(jd.getImportingList(ctx, params))
	}
	res, _ := jd.executeDbRequest(newReadDbRequest("importing", &tags, command)).(queryResult)
	return res.JobsResult, res.err
}

/*
getImportingList returns events which need are Importing.
This is a wrapper over GetProcessed call above
*/
func (jd *HandleT) getImportingList(ctx context.Context, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetProcessed(ctx, params)
}

/*
GetProcessed returns events of a given state. This does not update any state itself and
realises on the caller to update it. That means that successive calls to GetProcessed("failed")
can return the same set of events. It is the responsibility of the caller to call it from
one thread, update the state (to "waiting") in the same thread and pass on the processors
*/
func (jd *HandleT) GetProcessed(ctx context.Context, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	if params.JobsLimit <= 0 {
		return JobsResult{}, nil
	}
	tags := &statTags{
		CustomValFilters: params.CustomValFilters,
		StateFilters:     params.StateFilters,
		ParameterFilters: params.ParameterFilters,
		WorkspaceID:      params.WorkspaceID,
	}
	defer jd.getTimerStat(
		"processed_jobs_time",
		tags,
	).RecordDuration()()

	// The order of lock is very important. The migrateDSLoop
	// takes lock in this order so reversing this will cause
	// deadlocks
	if !jd.dsMigrationLock.RTryLockWithCtx(ctx) {
		return JobsResult{}, fmt.Errorf("could not acquire a migration read lock: %w", ctx.Err())
	}
	defer jd.dsMigrationLock.RUnlock()
	if !jd.dsListLock.RTryLockWithCtx(ctx) {
		return JobsResult{}, fmt.Errorf("could not acquire a dslist read lock: %w", ctx.Err())
	}
	dsRangeList := jd.getDSRangeList()
	dsList := jd.getDSList()
	jd.dsListLock.RUnlock()

	limitByEventCount := false
	if params.EventsLimit > 0 {
		limitByEventCount = true
	}

	limitByPayloadSize := false
	if params.PayloadSizeLimit > 0 {
		limitByPayloadSize = true
	} else if params.PayloadSizeLimit < 0 {
		return JobsResult{}, nil
	}

	var completeProcessedJobs JobsResult
	dsQueryCount := 0
	cacheHitCount := 0
	var dsLimit int
	if jd.dsLimit != nil {
		dsLimit = *jd.dsLimit
	}
	for idx, ds := range dsList {
		if params.AfterJobID != nil {
			if idx < len(dsRangeList) { // ranges are not stored for the last ds
				// so the following condition cannot be applied the last ds
				if *params.AfterJobID > dsRangeList[idx].maxJobID {
					continue
				}
			}
		}
		if dsLimit > 0 && dsQueryCount >= dsLimit {
			break
		}
		processedJobs, dsHit, err := jd.getProcessedJobsDS(ctx, ds, params)
		if err != nil {
			return JobsResult{}, err
		}
		if dsHit {
			dsQueryCount++
		} else {
			cacheHitCount++
		}
		completeProcessedJobs.Jobs = append(completeProcessedJobs.Jobs, processedJobs.Jobs...)
		completeProcessedJobs.EventsCount += processedJobs.EventsCount
		completeProcessedJobs.PayloadSize += processedJobs.PayloadSize

		if processedJobs.LimitsReached {
			completeProcessedJobs.LimitsReached = true
			break
		}
		// decrement our limits for the next query
		if params.JobsLimit > 0 {
			params.JobsLimit -= len(processedJobs.Jobs)
		}
		if limitByEventCount {
			params.EventsLimit -= processedJobs.EventsCount
		}
		if limitByPayloadSize {
			params.PayloadSizeLimit -= processedJobs.PayloadSize
		}
	}

	statTags := tags.getStatsTags(jd.tablePrefix)
	statTags["query"] = "processed"
	stats.Default.NewTaggedStat("jobsdb_tables_queried", stats.CountType, statTags).Count(dsQueryCount)
	stats.Default.NewTaggedStat("jobsdb_cache_hits", stats.CountType, statTags).Count(cacheHitCount)

	return completeProcessedJobs, nil
}

type queryResult struct {
	JobsResult
	err error
}

func queryResultWrapper(res JobsResult, err error) queryResult {
	return queryResult{
		JobsResult: res,
		err:        err,
	}
}

/*
GetToRetry returns events which need to be retried.
If enableReaderQueue is true, this goes through worker pool, else calls getUnprocessed directly.
*/
func (jd *HandleT) GetToRetry(ctx context.Context, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	if params.JobsLimit == 0 {
		return JobsResult{}, nil
	}
	params.StateFilters = []string{Failed.State}
	tags := statTags{
		CustomValFilters: params.CustomValFilters,
		StateFilters:     params.StateFilters,
		ParameterFilters: params.ParameterFilters,
		WorkspaceID:      params.WorkspaceID,
	}
	command := func() interface{} {
		return queryResultWrapper(jd.getToRetry(ctx, params))
	}
	res, _ := jd.executeDbRequest(newReadDbRequest("processed", &tags, command)).(queryResult)
	return res.JobsResult, res.err
}

/*
getToRetry returns events which need to be retried.
This is a wrapper over GetProcessed call above
*/
func (jd *HandleT) getToRetry(ctx context.Context, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetProcessed(ctx, params)
}

/*
GetWaiting returns events which are under processing
If enableReaderQueue is true, this goes through worker pool, else calls getUnprocessed directly.
*/
func (jd *HandleT) GetWaiting(ctx context.Context, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	if params.JobsLimit == 0 {
		return JobsResult{}, nil
	}
	params.StateFilters = []string{Waiting.State}
	tags := statTags{
		CustomValFilters: params.CustomValFilters,
		StateFilters:     params.StateFilters,
		ParameterFilters: params.ParameterFilters,
		WorkspaceID:      params.WorkspaceID,
	}
	command := func() interface{} {
		return queryResultWrapper(jd.getWaiting(ctx, params))
	}
	res, _ := jd.executeDbRequest(newReadDbRequest("processed", &tags, command)).(queryResult)
	return res.JobsResult, res.err
}

/*
GetWaiting returns events which are under processing
This is a wrapper over GetProcessed call above
*/
func (jd *HandleT) getWaiting(ctx context.Context, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetProcessed(ctx, params)
}

func (jd *HandleT) GetExecuting(ctx context.Context, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	if params.JobsLimit == 0 {
		return JobsResult{}, nil
	}
	params.StateFilters = []string{Executing.State}
	tags := statTags{
		CustomValFilters: params.CustomValFilters,
		StateFilters:     params.StateFilters,
		ParameterFilters: params.ParameterFilters,
		WorkspaceID:      params.WorkspaceID,
	}
	command := func() interface{} {
		return queryResultWrapper(jd.getExecuting(ctx, params))
	}
	res, _ := jd.executeDbRequest(newReadDbRequest("processed", &tags, command)).(queryResult)
	return res.JobsResult, res.err
}

/*
getExecuting returns events which  in executing state
*/
func (jd *HandleT) getExecuting(ctx context.Context, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	return jd.GetProcessed(ctx, params)
}

func (jd *HandleT) getMaxIDForDs(ds dataSetT) int64 {
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

func (jd *HandleT) GetLastJob() *JobT {
	ctx := context.TODO()
	if !jd.dsListLock.RTryLockWithCtx(ctx) {
		panic(fmt.Errorf("could not acquire a dslist lock: %w", ctx.Err()))
	}
	dsList := jd.getDSList()
	jd.dsListLock.RUnlock()
	maxID := jd.getMaxIDForDs(dsList[len(dsList)-1])
	var job JobT
	sqlStatement := fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at FROM %[1]s WHERE %[1]s.job_id = %[2]d`, dsList[len(dsList)-1].JobTable, maxID)
	err := jd.dbHandle.QueryRow(sqlStatement).Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal, &job.EventPayload, &job.CreatedAt, &job.ExpireAt)
	if err != nil && err != sql.ErrNoRows {
		jd.assertError(err)
	}
	return &job
}

func sanitizeJson(input json.RawMessage) json.RawMessage {
	v := bytes.ReplaceAll(input, []byte(`\u0000`), []byte(""))
	if len(v) == 0 {
		v = []byte(`{}`)
	}
	return v
}

type smallDS struct {
	ds          dataSetT
	recordsLeft int
}

func (jd *HandleT) withDistributedLock(ctx context.Context, tx *Tx, operation string, f func() error) error {
	advisoryLock := jd.getAdvisoryLockForOperation(operation)
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`SELECT pg_advisory_xact_lock(%d);`, advisoryLock))
	if err != nil {
		return fmt.Errorf("error while acquiring advisory lock %d for operation %s: %w", advisoryLock, operation, err)
	}
	return f()
}

func (jd *HandleT) withDistributedSharedLock(ctx context.Context, tx *Tx, operation string, f func() error) error {
	advisoryLock := jd.getAdvisoryLockForOperation(operation)
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`SELECT pg_advisory_xact_lock_shared(%d);`, advisoryLock))
	if err != nil {
		return fmt.Errorf("error while acquiring a shared advisory lock %d for operation %s: %w", advisoryLock, operation, err)
	}
	return f()
}
