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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/cenkalti/backoff"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/rudderlabs/rudder-server/jobsdb/prebackup"
	"github.com/rudderlabs/rudder-server/utils/bytesize"
	"github.com/rudderlabs/rudder-server/utils/logger"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/metric"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/gofrs/uuid"
	"github.com/lib/pq"
)

const preDropTablePrefix = "pre_drop_"

// backupSettings is for capturing the backup
// configuration from the config/env files to
// instantiate jobdb correctly
type backupSettings struct {
	instanceBackupEnabled bool
	FailedOnly            bool
	PathPrefix            string
}

func (b *backupSettings) isBackupEnabled() bool {
	return masterBackupEnabled && b.instanceBackupEnabled
}

func IsMasterBackupEnabled() bool {
	return masterBackupEnabled
}

// QueryConditions holds jobsdb query conditions
type QueryConditions struct {
	// if IgnoreCustomValFiltersInQuery is true, CustomValFilters is not going to be used
	IgnoreCustomValFiltersInQuery bool
	CustomValFilters              []string
	ParameterFilters              []ParameterFilterT
	StateFilters                  []string
}

// GetQueryParamsT is a struct to hold jobsdb query params.
type GetQueryParamsT struct {
	// query conditions

	// if IgnoreCustomValFiltersInQuery is true, CustomValFilters is not going to be used
	IgnoreCustomValFiltersInQuery bool
	CustomValFilters              []string
	ParameterFilters              []ParameterFilterT
	StateFilters                  []string

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

// statTags is a struct to hold tags for stats
type statTags struct {
	CustomValFilters []string
	ParameterFilters []ParameterFilterT
	StateFilters     []string
}

var getTimeNowFunc = time.Now

// StoreSafeTx sealed interface
type StoreSafeTx interface {
	Tx() *sql.Tx
	storeSafeTxIdentifier() string
}

type storeSafeTx struct {
	tx       *sql.Tx
	identity string
}

func (r *storeSafeTx) storeSafeTxIdentifier() string {
	return r.identity
}

func (r *storeSafeTx) Tx() *sql.Tx {
	return r.tx
}

// EmptyStoreSafeTx returns an empty interface usable only for tests
func EmptyStoreSafeTx() StoreSafeTx {
	return &storeSafeTx{}
}

// UpdateSafeTx sealed interface
type UpdateSafeTx interface {
	Tx() *sql.Tx
	updateSafeTxSealIdentifier() string
}
type updateSafeTx struct {
	tx       *sql.Tx
	identity string
}

func (r *updateSafeTx) updateSafeTxSealIdentifier() string {
	return r.identity
}

func (r *updateSafeTx) Tx() *sql.Tx {
	return r.tx
}

// EmptyUpdateSafeTx returns an empty interface usable only for tests
func EmptyUpdateSafeTx() UpdateSafeTx {
	return &updateSafeTx{}
}

// HandleInspector is only intended to be used by tests for verifying the handle's internal state
type HandleInspector struct {
	*HandleT
}

// DSListSize returns the current size of the handle's dsList
func (h *HandleInspector) DSListSize() int {
	return len(h.HandleT.getDSList())
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
	WithTx(func(tx *sql.Tx) error) error

	// WithStoreSafeTx prepares a store-safe environment and then starts a transaction
	// that can be used by the provided function.
	WithStoreSafeTx(func(tx StoreSafeTx) error) error

	// Store stores the provided jobs to the database
	Store(ctx context.Context, jobList []*JobT) error

	// StoreInTx stores the provided jobs to the database using an existing transaction.
	// Please ensure that you are using an StoreSafeTx, e.g.
	//    jobsdb.WithStoreSafeTx(func(tx StoreSafeTx) error {
	//	      jobsdb.StoreInTx(ctx, tx, jobList)
	//    })
	StoreInTx(ctx context.Context, tx StoreSafeTx, jobList []*JobT) error

	// StoreWithRetryEach tries to store all the provided jobs to the database and returns the job uuids which failed
	StoreWithRetryEach(ctx context.Context, jobList []*JobT) map[uuid.UUID]string

	// StoreWithRetryEachInTx tries to store all the provided jobs to the database and returns the job uuids which failed, using an existing transaction.
	// Please ensure that you are using an StoreSafeTx, e.g.
	//    jobsdb.WithStoreSafeTx(func(tx StoreSafeTx) error {
	//	      jobsdb.StoreWithRetryEachInTx(ctx, tx, jobList)
	//    })
	StoreWithRetryEachInTx(ctx context.Context, tx StoreSafeTx, jobList []*JobT) map[uuid.UUID]string

	// WithUpdateSafeTx prepares an update-safe environment and then starts a transaction
	// that can be used by the provided function. An update-safe transaction shall be used if the provided function
	// needs to call UpdateJobStatusInTx.
	WithUpdateSafeTx(func(tx UpdateSafeTx) error) error

	// UpdateJobStatus updates the provided job statuses
	UpdateJobStatus(ctx context.Context, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error

	// UpdateJobStatusInTx updates the provided job statuses in an existing transaction.
	// Please ensure that you are using an UpdateSafeTx, e.g.
	//    jobsdb.WithUpdateSafeTx(func(tx UpdateSafeTx) error {
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

	// GetToRetry finds jobs in waiting state
	GetWaiting(ctx context.Context, params GetQueryParamsT) (JobsResult, error)

	// GetExecuting finds jobs in executing state
	GetExecuting(ctx context.Context, params GetQueryParamsT) (JobsResult, error)

	// GetImporting finds jobs in importing state
	GetImporting(ctx context.Context, params GetQueryParamsT) (JobsResult, error)

	// GetPileUpCounts returns statistics (counters) of incomplete jobs
	// grouped by workspaceId and destination type
	GetPileUpCounts(ctx context.Context) (statMap map[string]map[string]int, err error)

	/* Admin */

	Status() interface{}
	Ping() error
	DeleteExecuting()

	/* Journal */

	GetJournalEntries(opType string) (entries []JournalEntryT)
	JournalDeleteEntry(opID int64)
	JournalMarkStart(opType string, opPayload json.RawMessage) int64
}

/*
assertInterface contains public assert methods
*/
type assertInterface interface {
	assert(cond bool, errorString string)
	assertError(err error)
}

const (
	allWorkspaces = "_all_"
)

var (
	masterBackupEnabled bool
	pathPrefix          string
)

/*
UpdateJobStatusInTx updates the status of a batch of jobs in the passed transaction
customValFilters[] is passed so we can efficinetly mark empty cache
Later we can move this to query
IMP NOTE: AcquireUpdateJobStatusLocks Should be called before calling this function
*/
func (jd *HandleT) UpdateJobStatusInTx(ctx context.Context, tx UpdateSafeTx, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error {
	updateCmd := func() error {
		if len(statusList) == 0 {
			return nil
		}
		tags := statTags{CustomValFilters: customValFilters, ParameterFilters: parameterFilters}
		command := func() interface{} {
			return jd.internalUpdateJobStatusInTx(ctx, tx.Tx(), statusList, customValFilters, parameterFilters)
		}
		err, _ := jd.executeDbRequest(newWriteDbRequest("update_job_status", &tags, command)).(error)
		return err
	}

	if tx.updateSafeTxSealIdentifier() != jd.Identifier() {
		return jd.inUpdateSafeCtx(func() error {
			return updateCmd()
		})
	}
	return updateCmd()
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

// migrationState maintains the state required during the migration process
type migrationState struct {
	dsForNewEvents             dataSetT
	dsForImport                dataSetT
	lastDsForExport            dataSetT
	importLock                 *sync.RWMutex
	migrationMode              string
	fromVersion                int
	toVersion                  int
	nonExportedJobsCountByDS   map[string]int64
	doesDSHaveJobsToMigrateMap map[string]bool
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
	dsListLock                    lock.DSListLocker
	dsMigrationLock               sync.RWMutex
	MinDSRetentionPeriod          time.Duration
	MaxDSRetentionPeriod          time.Duration
	dsEmptyResultCache            map[dataSetT]map[string]map[string]map[string]map[string]cacheEntry // DS -> workspace -> customVal -> params -> state -> cacheEntry
	dsCacheLock                   sync.Mutex
	BackupSettings                *backupSettings
	jobsFileUploader              filemanager.FileManager
	statTableCount                stats.RudderStats
	statDSCount                   stats.RudderStats
	statNewDSPeriod               stats.RudderStats
	invalidCacheKeyStat           stats.RudderStats
	isStatNewDSPeriodInitialized  bool
	statDropDSPeriod              stats.RudderStats
	unionQueryTime                stats.RudderStats
	tablesQueriedStat             stats.RudderStats
	isStatDropDSPeriodInitialized bool
	migrationState                migrationState
	inProgressMigrationTargetDS   *dataSetT
	logger                        logger.LoggerI
	writeCapacity                 chan struct{}
	readCapacity                  chan struct{}
	registerStatusHandler         bool
	enableWriterQueue             bool
	enableReaderQueue             bool
	clearAll                      bool
	maxReaders                    int
	maxWriters                    int
	maxOpenConnections            int
	analyzeThreshold              int
	MaxDSSize                     *int
	queryFilterKeys               QueryFiltersT
	backgroundCancel              context.CancelFunc
	backgroundGroup               *errgroup.Group
	maxBackupRetryTime            time.Duration
	preBackupHandlers             []prebackup.Handler

	// skipSetupDBSetup is useful for testing as we mock the database client
	// TODO: Remove this flag once we have test setup that uses real database
	skipSetupDBSetup bool

	// TriggerAddNewDS, TriggerMigrateDS is useful for triggering addNewDS to run from tests.
	// TODO: Ideally we should refactor the code to not use this override.
	TriggerAddNewDS  func() <-chan time.Time
	TriggerMigrateDS func() <-chan time.Time

	lifecycle struct {
		mu      sync.Mutex
		started bool
	}
}

type QueryFiltersT struct {
	CustomVal        bool
	ParameterFilters []string
}

// The struct which is written to the journal
type journalOpPayloadT struct {
	From []dataSetT `json:"from"`
	To   dataSetT   `json:"to"`
}

type ParameterFilterT struct {
	Name     string
	Value    string
	Optional bool
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
	jd.BackupSettings.PathPrefix = strings.TrimSpace(pathPrefix)
}

// Some helper functions
func (jd *HandleT) assertError(err error) {
	if err != nil {
		jd.printLists(true)
		jd.logger.Fatal(jd.tablePrefix, jd.ownerType, jd.dsEmptyResultCache)
		panic(err)
	}
}

func (jd *HandleT) assertErrorAndRollbackTx(err error, tx *sql.Tx) {
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			jd.logger.Errorf("Failed to rollback transaction: %v", rollbackErr)
		}
		jd.printLists(true)
		jd.logger.Fatal(jd.dsEmptyResultCache)
		panic(err)
	}
}

func (jd *HandleT) assert(cond bool, errorString string) {
	if !cond {
		jd.printLists(true)
		jd.logger.Fatal(jd.dsEmptyResultCache)
		panic(fmt.Errorf("[[ %s ]]: %s", jd.tablePrefix, errorString))
	}
}

func (jd *HandleT) Status() interface{} {
	statusObj := map[string]interface{}{
		"dataset-list":    jd.getDSList(),
		"dataset-ranges":  jd.getDSRangeList(),
		"backups-enabled": jd.BackupSettings.isBackupEnabled(),
	}
	emptyResults := make(map[string]interface{})
	for ds, entry := range jd.dsEmptyResultCache {
		emptyResults[ds.JobTable] = entry
	}
	statusObj["empty-results-cache"] = emptyResults

	if db.IsValidMigrationMode(jd.migrationState.migrationMode) {
		statusObj["migration-state"] = jd.migrationState
	}
	pendingEventMetrics := metric.GetManager().
		GetRegistry(metric.PUBLISHED_METRICS).
		GetMetricsByName(fmt.Sprintf(metric.JOBSDB_PENDING_EVENTS_COUNT, jd.tablePrefix))

	if len(pendingEventMetrics) == 0 {
		return statusObj
	}

	pendingEvents := []map[string]interface{}{}
	for _, pendingEvent := range pendingEventMetrics {
		count := pendingEvent.Value.(metric.Gauge).IntValue()
		if count != 0 {
			pendingEvents = append(pendingEvents, map[string]interface{}{
				"tags":  pendingEvent.Tags,
				"count": count,
			})
		}
	}
	statusObj["pending-events"] = pendingEvents

	return statusObj
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
	Failed       = jobStateT{isValid: true, isTerminal: false, State: "failed"}
	Executing    = jobStateT{isValid: true, isTerminal: false, State: "executing"}
	Waiting      = jobStateT{isValid: true, isTerminal: false, State: "waiting"}
	WaitingRetry = jobStateT{isValid: true, isTerminal: false, State: "waiting_retry"}
	Migrating    = jobStateT{isValid: true, isTerminal: false, State: "migrating"}
	Importing    = jobStateT{isValid: true, isTerminal: false, State: "importing"}

	// Valid, Terminal
	Succeeded   = jobStateT{isValid: true, isTerminal: true, State: "succeeded"}
	Aborted     = jobStateT{isValid: true, isTerminal: true, State: "aborted"}
	Migrated    = jobStateT{isValid: true, isTerminal: true, State: "migrated"}
	WontMigrate = jobStateT{isValid: true, isTerminal: true, State: "wont_migrate"}
)

// Adding a new state to this list, will require an enum change in postgres db.
var jobStates = []jobStateT{
	NotProcessed,
	Failed,
	Executing,
	Waiting,
	WaitingRetry,
	Migrating,
	Succeeded,
	Aborted,
	Migrated,
	WontMigrate,
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

func getValidTerminalStates() (validTerminalStates []string) {
	for _, js := range jobStates {
		if js.isValid && js.isTerminal {
			validTerminalStates = append(validTerminalStates, js.State)
		}
	}
	return
}

func getValidNonTerminalStates() (validNonTerminalStates []string) {
	for _, js := range jobStates {
		if js.isValid && !js.isTerminal {
			validNonTerminalStates = append(validNonTerminalStates, js.State)
		}
	}
	return
}

var (
	host, user, password, dbname, sslmode, appName string
	port                                           int
)

var (
	maxDSSize, maxMigrateOnce, maxMigrateDSProbe int
	maxTableSize                                 int64
	jobDoneMigrateThres, jobStatusMigrateThres   float64
	migrateDSLoopSleepDuration                   time.Duration
	addNewDSLoopSleepDuration                    time.Duration
	refreshDSListLoopSleepDuration               time.Duration
	backupCheckSleepDuration                     time.Duration
	cacheExpiration                              time.Duration
	useJoinForUnprocessed                        bool
	backupRowsBatchSize                          int64
	backupMaxTotalPayloadSize                    int64
	pkgLogger                                    logger.LoggerI
	useNewCacheBurst                             bool
	skipZeroAssertionForMultitenant              bool
)

// Loads db config and migration related config from config file
func loadConfig() {
	host = config.GetEnv("JOBS_DB_HOST", "localhost")
	user = config.GetEnv("JOBS_DB_USER", "ubuntu")
	dbname = config.GetEnv("JOBS_DB_DB_NAME", "ubuntu")
	port, _ = strconv.Atoi(config.GetEnv("JOBS_DB_PORT", "5432"))
	password = config.GetEnv("JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from
	sslmode = config.GetEnv("JOBS_DB_SSL_MODE", "disable")
	// Application Name can be any string of less than NAMEDATALEN characters (64 characters in a standard PostgreSQL build).
	// There is no need to truncate the string on our own though since PostgreSQL auto-truncates this identifier and issues a relevant notice if necessary.
	appName = misc.DefaultString("rudder-server").OnError(os.Hostname())

	/*Migration related parameters
	jobDoneMigrateThres: A DS is migrated when this fraction of the jobs have been processed
	jobStatusMigrateThres: A DS is migrated if the job_status exceeds this (* no_of_jobs)
	maxDSSize: Maximum size of a DS. The process which adds new DS runs in the background
			(every few seconds) so a DS may go beyond this size
	maxMigrateOnce: Maximum number of DSs that are migrated together into one destination
	maxMigrateDSProbe: Maximum number of DSs that are checked from left to right if they are eligible for migration
	migrateDSLoopSleepDuration: How often is the loop (which checks for migrating DS) run
	addNewDSLoopSleepDuration: How often is the loop (which checks for adding new DS) run
	refreshDSListLoopSleepDuration: How often is the loop (which refreshes DSList) run
	maxTableSizeInMB: Maximum Table size in MB
	*/
	config.RegisterFloat64ConfigVariable(0.8, &jobDoneMigrateThres, true, "JobsDB.jobDoneMigrateThres")
	config.RegisterFloat64ConfigVariable(5, &jobStatusMigrateThres, true, "JobsDB.jobStatusMigrateThres")
	config.RegisterIntConfigVariable(100000, &maxDSSize, true, 1, "JobsDB.maxDSSize")
	config.RegisterIntConfigVariable(10, &maxMigrateOnce, true, 1, "JobsDB.maxMigrateOnce")
	config.RegisterIntConfigVariable(10, &maxMigrateDSProbe, true, 1, "JobsDB.maxMigrateDSProbe")
	config.RegisterInt64ConfigVariable(300, &maxTableSize, true, 1000000, "JobsDB.maxTableSizeInMB")
	config.RegisterInt64ConfigVariable(1000, &backupRowsBatchSize, true, 1, "JobsDB.backupRowsBatchSize")
	config.RegisterInt64ConfigVariable(64*bytesize.MB, &backupMaxTotalPayloadSize, true, 1, "JobsDB.maxBackupTotalPayloadSize")
	config.RegisterDurationConfigVariable(30, &migrateDSLoopSleepDuration, true, time.Second, []string{"JobsDB.migrateDSLoopSleepDuration", "JobsDB.migrateDSLoopSleepDurationInS"}...)
	config.RegisterDurationConfigVariable(5, &addNewDSLoopSleepDuration, true, time.Second, []string{"JobsDB.addNewDSLoopSleepDuration", "JobsDB.addNewDSLoopSleepDurationInS"}...)
	config.RegisterDurationConfigVariable(5, &refreshDSListLoopSleepDuration, true, time.Second, []string{"JobsDB.refreshDSListLoopSleepDuration", "JobsDB.refreshDSListLoopSleepDurationInS"}...)
	config.RegisterDurationConfigVariable(5, &backupCheckSleepDuration, true, time.Second, []string{"JobsDB.backupCheckSleepDuration", "JobsDB.backupCheckSleepDurationIns"}...)
	config.RegisterDurationConfigVariable(60, &cacheExpiration, true, time.Minute, []string{"JobsDB.cacheExpiration"}...)
	useJoinForUnprocessed = config.GetBool("JobsDB.useJoinForUnprocessed", true)
	config.RegisterBoolConfigVariable(true, &useNewCacheBurst, true, "JobsDB.useNewCacheBurst")
	config.RegisterBoolConfigVariable(false, &skipZeroAssertionForMultitenant, true, "JobsDB.skipZeroAssertionForMultitenant")
}

func Init2() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("jobsdb")
}

// GetConnectionString Returns Jobs DB connection configuration
func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s application_name=%s",
		host, port, user, password, dbname, sslmode, appName)
}

type OptsFunc func(jd *HandleT)

// WithClearDB, if set to true it will remove all existing tables
func WithClearDB(clearDB bool) OptsFunc {
	return func(jd *HandleT) {
		jd.clearAll = clearDB
	}
}

func WithQueryFilterKeys(filters QueryFiltersT) OptsFunc {
	return func(jd *HandleT) {
		jd.queryFilterKeys = filters
	}
}

func WithMigrationMode(mode string) OptsFunc {
	return func(jd *HandleT) {
		jd.migrationState.migrationMode = mode
	}
}

func WithStatusHandler() OptsFunc {
	return func(jd *HandleT) {
		jd.registerStatusHandler = true
	}
}

// WithPreBackupHandlers, sets pre-backup handlers
func WithPreBackupHandlers(preBackupHandlers []prebackup.Handler) OptsFunc {
	return func(jd *HandleT) {
		jd.preBackupHandlers = preBackupHandlers
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
		// default values:
		migrationState: migrationState{
			migrationMode: "",
			importLock:    &sync.RWMutex{},
		},
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
	ownerType OwnerType, clearAll bool, tablePrefix, migrationMode string,
	registerStatusHandler bool, queryFilterKeys QueryFiltersT, preBackupHandlers []prebackup.Handler,
) error {
	jd.ownerType = ownerType
	jd.clearAll = clearAll
	jd.tablePrefix = tablePrefix
	jd.migrationState.migrationMode = migrationMode
	jd.registerStatusHandler = registerStatusHandler
	jd.queryFilterKeys = queryFilterKeys
	jd.preBackupHandlers = preBackupHandlers

	jd.init()
	return jd.Start()
}

func (jd *HandleT) init() {
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

	// Initialize dbHandle if not already set
	if jd.dbHandle == nil {
		var err error
		psqlInfo := GetConnectionString()
		db, err := sql.Open("postgres", psqlInfo)
		jd.assertError(err)

		defer func() {
			if !jd.enableReaderQueue || !jd.enableWriterQueue {
				db.SetMaxOpenConns(jd.maxOpenConnections)
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
				db.SetMaxOpenConns(maxOpenConnections)
			} else {
				db.SetMaxOpenConns(jd.maxOpenConnections)
			}
		}()

		err = db.Ping()
		jd.assertError(err)

		jd.dbHandle = db
	}

	jd.workersAndAuxSetup()
}

func (jd *HandleT) workersAndAuxSetup() {
	jd.assert(jd.tablePrefix != "", "tablePrefix received is empty")

	jd.logger = pkgLogger.Child(jd.tablePrefix)
	jd.dsEmptyResultCache = map[dataSetT]map[string]map[string]map[string]map[string]cacheEntry{}
	if jd.registerStatusHandler {
		admin.RegisterStatusHandler(jd.tablePrefix+"-jobsdb", jd)
	}
	jd.BackupSettings = &backupSettings{}
	jd.registerBackUpSettings()

	jd.logger.Infof("Connected to %s DB", jd.tablePrefix)

	jd.statTableCount = stats.DefaultStats.NewStat(fmt.Sprintf("jobsdb.%s_tables_count", jd.tablePrefix), stats.GaugeType)
	jd.statDSCount = stats.NewTaggedStat("jobsdb.tables_count", stats.GaugeType, stats.Tags{"customVal": jd.tablePrefix})
	jd.tablesQueriedStat = stats.NewTaggedStat("tables_queried_gauge", stats.GaugeType, stats.Tags{
		"state":     "nonterminal",
		"customVal": jd.tablePrefix,
	})
	jd.unionQueryTime = stats.NewTaggedStat("union_query_time", stats.TimerType, stats.Tags{
		"state":     "nonterminal",
		"customVal": jd.tablePrefix,
	})
	jd.statNewDSPeriod = stats.NewTaggedStat("jobsdb.new_ds_period", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	jd.statDropDSPeriod = stats.NewTaggedStat("jobsdb.drop_ds_period", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	jd.invalidCacheKeyStat = stats.NewTaggedStat("jobsdb.invalid_cache_key", stats.CountType, stats.Tags{"customVal": jd.tablePrefix})

	enableWriterQueueKeys := []string{"JobsDB." + jd.tablePrefix + "." + "enableWriterQueue", "JobsDB." + "enableWriterQueue"}
	config.RegisterBoolConfigVariable(true, &jd.enableWriterQueue, true, enableWriterQueueKeys...)
	enableReaderQueueKeys := []string{"JobsDB." + jd.tablePrefix + "." + "enableReaderQueue", "JobsDB." + "enableReaderQueue"}
	config.RegisterBoolConfigVariable(true, &jd.enableReaderQueue, true, enableReaderQueueKeys...)
	maxWritersKeys := []string{"JobsDB." + jd.tablePrefix + "." + "maxWriters", "JobsDB." + "maxWriters"}
	config.RegisterIntConfigVariable(1, &jd.maxWriters, false, 1, maxWritersKeys...)
	maxReadersKeys := []string{"JobsDB." + jd.tablePrefix + "." + "maxReaders", "JobsDB." + "maxReaders"}
	config.RegisterIntConfigVariable(3, &jd.maxReaders, false, 1, maxReadersKeys...)
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
		jd.setUpForOwnerType(ctx, jd.ownerType, jd.clearAll)

		// Avoid clearing the database, if .Start() is called again.
		jd.clearAll = false
	}
	return nil
}

func (jd *HandleT) setUpForOwnerType(ctx context.Context, ownerType OwnerType, clearAll bool) {
	jd.dsListLock.WithLock(func(l lock.DSListLockToken) {
		switch ownerType {
		case Read:
			jd.readerSetup(ctx, l)
		case Write:
			jd.setupDatabaseTables(l, clearAll)
			jd.writerSetup(ctx, l)
		case ReadWrite:
			jd.setupDatabaseTables(l, clearAll)
			jd.readerWriterSetup(ctx, l)
		}
	})
}

func (jd *HandleT) startBackupDSLoop(ctx context.Context) {
	var err error
	jd.jobsFileUploader, err = jd.getFileUploader(ctx)
	if err != nil {
		jd.logger.Errorf("failed to get a file uploader for %s", jd.tablePrefix)
		return
	}
	jd.backgroundGroup.Go(misc.WithBugsnag(func() error {
		jd.backupDSLoop(ctx)
		return nil
	}))
}

func (jd *HandleT) startMigrateDSLoop(ctx context.Context) {
	jd.backgroundGroup.Go(misc.WithBugsnag(func() error {
		jd.migrateDSLoop(ctx)
		return nil
	}))
}

func (jd *HandleT) readerSetup(ctx context.Context, l lock.DSListLockToken) {
	jd.recoverFromJournal(Read)

	// This is a thread-safe operation.
	// Even if two different services (gateway and processor) perform this operation, there should not be any problem.
	jd.recoverFromJournal(ReadWrite)

	jd.refreshDSRangeList(l)

	g := jd.backgroundGroup

	g.Go(misc.WithBugsnag(func() error {
		jd.refreshDSListLoop(ctx)
		return nil
	}))

	jd.startBackupDSLoop(ctx)
	jd.startMigrateDSLoop(ctx)

	g.Go(misc.WithBugsnag(func() error {
		runArchiver(ctx, jd.tablePrefix, jd.dbHandle)
		return nil
	}))
}

func (jd *HandleT) writerSetup(ctx context.Context, l lock.DSListLockToken) {
	jd.recoverFromJournal(Write)
	// This is a thread-safe operation.
	// Even if two different services (gateway and processor) perform this operation, there should not be any problem.
	jd.recoverFromJournal(ReadWrite)

	jd.refreshDSRangeList(l)
	// If no DS present, add one
	if len(jd.getDSList()) == 0 {
		jd.addNewDS(l, newDataSet(jd.tablePrefix, jd.computeNewIdxForAppend(l)))
	}

	jd.backgroundGroup.Go(misc.WithBugsnag(func() error {
		jd.addNewDSLoop(ctx)
		return nil
	}))
}

func (jd *HandleT) readerWriterSetup(ctx context.Context, l lock.DSListLockToken) {
	jd.recoverFromJournal(Read)

	jd.writerSetup(ctx, l)

	jd.startBackupDSLoop(ctx)
	jd.startMigrateDSLoop(ctx)

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
	jd.dbHandle.Close()
}

/*
Function to return an ordered list of datasets and datasetRanges
Most callers use the in-memory list of dataset and datasetRanges
Caller must have the dsListLock readlocked
*/
func (jd *HandleT) getDSList() []dataSetT {
	return jd.datasetList
}

// refreshDSList refreshes the ds list from the database
func (jd *HandleT) refreshDSList(l lock.DSListLockToken) []dataSetT {
	jd.assert(l != nil, "cannot refresh DS list without a valid lock token")
	// Reset the global list
	jd.datasetList = nil
	jd.datasetList = getDSList(jd, jd.dbHandle, jd.tablePrefix)

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

	return jd.datasetList
}

func (jd *HandleT) getDSRangeList() []dataSetRangeT {
	return jd.datasetRangeList
}

// refreshDSRangeList first refreshes the DS list and then calculate the DS range list
func (jd *HandleT) refreshDSRangeList(l lock.DSListLockToken) {
	var minID, maxID sql.NullInt64
	var prevMax int64

	// At this point we must have write-locked dsListLock
	dsList := jd.refreshDSList(l)

	jd.datasetRangeList = nil

	for idx, ds := range dsList {
		jd.assert(ds.Index != "", "ds.Index is empty")
		sqlStatement := fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %q`, ds.JobTable)
		// Note: Using Query instead of QueryRow, because the sqlmock library doesn't have support for QueryRow
		rows, err := jd.dbHandle.Query(sqlStatement)
		jd.assertError(err)
		for rows.Next() {
			err := rows.Scan(&minID, &maxID)
			jd.assertError(err)
			break
		}
		jd.logger.Debug(sqlStatement, minID, maxID)

		rows.Close()
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

		if idx < len(dsList)-1 && (jd.inProgressMigrationTargetDS == nil || jd.inProgressMigrationTargetDS.Index != ds.Index) {
			// TODO: Cleanup - Remove the line below and jd.inProgressMigrationTargetDS
			jd.assert(minID.Valid && maxID.Valid, fmt.Sprintf("minID.Valid: %v, maxID.Valid: %v. Either of them is false for table: %s", minID.Valid, maxID.Valid, ds.JobTable))
			jd.assert(idx == 0 || prevMax < minID.Int64, fmt.Sprintf("idx: %d != 0 and prevMax: %d >= minID.Int64: %v of table: %s", idx, prevMax, minID.Int64, ds.JobTable))
			jd.datasetRangeList = append(jd.datasetRangeList,
				dataSetRangeT{
					minJobID: minID.Int64,
					maxJobID: maxID.Int64,
					ds:       ds,
				})
			prevMax = maxID.Int64
		}
	}
}

/*
Functions for checking when DB is full or DB needs to be migrated.
We migrate the DB ONCE most of the jobs have been processed (succeeded/aborted)
Or when the job_status table gets too big because of lots of retries/failures
*/

func (jd *HandleT) checkIfMigrateDS(ds dataSetT) (bool, int) {
	queryStat := stats.NewTaggedStat("migration_ds_check", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()
	var delCount, totalCount, statusCount int
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) from %q`, ds.JobTable)
	row := jd.dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&totalCount)
	jd.assertError(err)

	// Jobs which have either succeeded or expired
	sqlStatement = fmt.Sprintf(`SELECT COUNT(DISTINCT(job_id))
                                      from %q
                                      WHERE job_state IN ('%s')`,
		ds.JobStatusTable, strings.Join(getValidTerminalStates(), "', '"))
	row = jd.dbHandle.QueryRow(sqlStatement)
	err = row.Scan(&delCount)
	jd.assertError(err)

	// Total number of job status. If this table grows too big (e.g. lot of retries)
	// we migrate to a new table and get rid of old job status
	sqlStatement = fmt.Sprintf(`SELECT COUNT(*) from %q`, ds.JobStatusTable)
	row = jd.dbHandle.QueryRow(sqlStatement)
	err = row.Scan(&statusCount)
	jd.assertError(err)

	if totalCount == 0 {
		jd.assert(delCount == 0 && statusCount == 0, fmt.Sprintf("delCount: %d, statusCount: %d. Either of them is not 0", delCount, statusCount))
		return false, 0
	}

	// If records are newer than what is required. One example use case is
	// gateway DB where records are kept to dedup

	var minCreatedAt, maxCreatedAt time.Time
	sqlStatement = fmt.Sprintf(`SELECT MIN(created_at), MAX(created_at) from %q`, ds.JobTable)
	row = jd.dbHandle.QueryRow(sqlStatement)
	err = row.Scan(&minCreatedAt, &maxCreatedAt)
	jd.assertError(err)

	if jd.MinDSRetentionPeriod > time.Duration(0) && time.Since(maxCreatedAt) < jd.MinDSRetentionPeriod {
		return false, totalCount - delCount
	}
	if jd.MaxDSRetentionPeriod > time.Duration(0) && time.Since(minCreatedAt) > jd.MaxDSRetentionPeriod {
		return true, totalCount - delCount
	}

	if (float64(delCount)/float64(totalCount) > jobDoneMigrateThres) ||
		(float64(statusCount)/float64(totalCount) > jobStatusMigrateThres) {
		return true, totalCount - delCount
	}
	return false, totalCount - delCount
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

func (jd *HandleT) checkIfFullDS(ds dataSetT) bool {
	var full bool
	jd.assertError(jd.WithTx(func(tx *sql.Tx) error {
		var err error
		full, err = jd.checkIfFullDSInTx(tx, ds)
		if err != nil {
			return err
		}
		return nil
	}))
	return full
}

func (jd *HandleT) checkIfFullDSInTx(tx *sql.Tx, ds dataSetT) (bool, error) {
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
1 & 2, we would need to create a new DS between 2 & 3. This is assigned the
the number 2_1. This is called a level1 dataset and the Index (2_1) is called level1
Index. We may migrate 2_1 into 2_2 and so on so there may be multiple level 1 datasets.

Immediately after creating a level_1 dataset (2_1 above), everything prior to it is
deleted. Hence there should NEVER be any requirement for having more than two levels.

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

func (jd *HandleT) addNewDS(l lock.DSListLockToken, ds dataSetT) {
	err := jd.WithTx(func(tx *sql.Tx) error {
		return jd.addNewDSInTx(tx, l, jd.refreshDSList(l), ds)
	})
	jd.assertError(err)
	jd.refreshDSRangeList(l)
}

// NOTE: If addNewDSInTx is directly called, make sure to explicitly call refreshDSRangeList(l) to update the DS list in cache, once transaction has completed.
func (jd *HandleT) addNewDSInTx(tx *sql.Tx, l lock.DSListLockToken, dsList []dataSetT, ds dataSetT) error {
	if l == nil {
		return errors.New("nil ds list lock token provided")
	}
	jd.logger.Infof("Creating new DS %+v", ds)
	queryStat := stats.NewTaggedStat("add_new_ds", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()
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
		jd.statNewDSPeriod.End()
	}
	jd.statNewDSPeriod.Start()
	jd.isStatNewDSPeriodInitialized = true

	return nil
}

func (jd *HandleT) addDS(ds dataSetT) {
	jd.logger.Infof("Creating DS %+v", ds)
	queryStat := stats.NewTaggedStat("add_new_ds", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()
	jd.assertError(jd.WithTx(func(tx *sql.Tx) error {
		return jd.createDSInTx(tx, ds)
	}))
}

func (jd *HandleT) computeNewIdxForAppend(l lock.DSListLockToken) string {
	dList := jd.refreshDSList(l)
	return jd.doComputeNewIdxForAppend(dList)
}

func (jd *HandleT) doComputeNewIdxForAppend(dList []dataSetT) string {
	newDSIdx := ""
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

func (jd *HandleT) computeNewIdxForInterNodeMigration(l lock.DSListLockToken, insertBeforeDS dataSetT) string { // ClusterMigration
	jd.logger.Debugf("computeNewIdxForInterNodeMigration, insertBeforeDS : %v", insertBeforeDS)
	dList := jd.refreshDSList(l)
	newIdx, err := computeIdxForClusterMigration(jd.tablePrefix, dList, insertBeforeDS)
	jd.assertError(err)
	return newIdx
}

func computeIdxForClusterMigration(tablePrefix string, dList []dataSetT, insertBeforeDS dataSetT) (newDSIdx string, err error) {
	pkgLogger.Debugf("dlist in which we are trying to find %v is %v", insertBeforeDS, dList)
	if len(dList) <= 0 {
		return "", fmt.Errorf("len(dList): %d <= 0", len(dList))
	}
	for idx, ds := range dList {
		if ds.Index == insertBeforeDS.Index {
			var levels int
			var levelVals []int
			levels, levelVals, err = mapDSToLevel(ds)
			if err != nil {
				return
			}
			if levels != 1 {
				err = fmt.Errorf("insertBeforeDS called for ds : %s_%s. insertBeforeDS should always be called for dsForNewEvents and this should always be a Level0 dataset", tablePrefix, ds.Index)
				return
			}
			var (
				levelsPre    int
				levelPreVals []int
			)
			if idx == 0 {
				pkgLogger.Debugf("idx = 0 case with insertForImport and ds at idx 0 is %v", ds)
				levelsPre = 1
				levelPreVals = []int{levelVals[0] - 1}
			} else {
				pkgLogger.Debugf("ds to insert before found in dList is %v", ds)
				levelsPre, levelPreVals, err = mapDSToLevel(dList[idx-1])
				if err != nil {
					return
				}
			}
			if levelPreVals[0] >= levelVals[0] {
				err = fmt.Errorf("First level val of previous ds should be less than the first(and only) levelVal of insertBeforeDS. Found %s_%d and %s_%s instead", tablePrefix, levelPreVals[0], tablePrefix, ds.Index)
				return
			}
			switch levelsPre {
			case 1:
				/*
					| prevDS    | insertBeforeDS | newDSIdx |
					| --------- | -------------- | -------- |
					| 0         | 1              | 0_1      |
				*/
				newDSIdx = fmt.Sprintf("%d_%d", levelPreVals[0], 1)
			case 2:
				/*
					| prevDS    | insertBeforeDS | newDSIdx |
					| --------- | -------------- | -------- |
					| 0_1       | 1              | 0_2      |
				*/

				newDSIdx = fmt.Sprintf("%d_%d", levelPreVals[0], levelPreVals[1]+1)
			default:
				err = fmt.Errorf("The previous ds can only be a Level0 or Level1. Found %s_%s instead", tablePrefix, dList[idx-1].Index)
				return

			}
		}
	}
	return
}

// Tries to give a slice between before and after by incrementing last value in before. If the order doesn't maintain, it adds a level and recurses.
func computeInsertVals(before, after []string) ([]string, error) {
	for {
		if before == nil || after == nil {
			return nil, fmt.Errorf("before or after is nil")
		}
		// Safe check: In the current jobsdb implementation, indices don't go more
		// than 3 levels deep. Breaking out of the loop if the before is of size more than 4.
		if len(before) > 4 {
			return before, fmt.Errorf("can't compute insert index due to bad inputs. before: %v, after: %v", before, after)
		}

		calculatedVals := make([]string, len(before))
		copy(calculatedVals, before)
		lastVal, err := strconv.Atoi(calculatedVals[len(calculatedVals)-1])
		if err != nil {
			return calculatedVals, err
		}
		// Just increment the last value of the index as a possible candidate
		calculatedVals[len(calculatedVals)-1] = fmt.Sprintf("%d", lastVal+1)
		var equals bool
		if len(calculatedVals) == len(after) {
			equals = true
			for k := 0; k < len(calculatedVals); k++ {
				if calculatedVals[k] == after[k] {
					continue
				}
				equals = false
			}
		}
		if !equals {
			comparison, err := dsComparitor(calculatedVals, after)
			if err != nil {
				return calculatedVals, err
			}
			if !comparison {
				return calculatedVals, fmt.Errorf("computed index is invalid. before: %v, after: %v, calculatedVals: %v", before, after, calculatedVals)
			}
		}
		// Only when the index starts with 0, we allow three levels and when we are using the legacy migration
		// In all other cases, we allow only two levels
		var comparatorBool bool
		if skipZeroAssertionForMultitenant {
			comparatorBool = len(calculatedVals) == 2
		} else {
			comparatorBool = (before[0] == "0" && len(calculatedVals) == 3) ||
				(before[0] != "0" && len(calculatedVals) == 2)
		}

		if comparatorBool {
			if equals {
				return calculatedVals, fmt.Errorf("calculatedVals and after are same. computed index is invalid. before: %v, after: %v, calculatedVals: %v", before, after, calculatedVals)
			} else {
				return calculatedVals, nil
			}
		}

		before = append(before, "0")
	}
}

func computeInsertIdx(beforeIndex, afterIndex string) (string, error) {
	comparison, err := dsComparitor(strings.Split(beforeIndex, "_"), strings.Split(afterIndex, "_"))
	if err != nil {
		return "", fmt.Errorf("Error while comparing beforeIndex: %s and afterIndex: %s with error : %w", beforeIndex, afterIndex, err)
	}
	if !comparison {
		return "", fmt.Errorf("Not a valid insert request between %s and %s", beforeIndex, afterIndex)
	}

	// No dataset should have 0 as the index.
	// 0_1, 0_2 are allowed.
	if beforeIndex == "0" && !skipZeroAssertionForMultitenant {
		return "", fmt.Errorf("Unsupported beforeIndex: %s", beforeIndex)
	}

	beforeVals := strings.Split(beforeIndex, "_")
	afterVals := strings.Split(afterIndex, "_")
	calculatedInsertVals, err := computeInsertVals(beforeVals, afterVals)
	if err != nil {
		return "", fmt.Errorf("Failed to calculate InserVals with error: %w", err)
	}
	calculatedIdx := strings.Join(calculatedInsertVals, "_")
	if len(calculatedInsertVals) > 3 {
		return "", fmt.Errorf("We don't expect a ds to be computed to Level3. We got %s while trying to insert between %s and %s", calculatedIdx, beforeIndex, afterIndex)
	}

	return calculatedIdx, nil
}

func (jd *HandleT) computeNewIdxForIntraNodeMigration(l lock.DSListLockToken, insertBeforeDS dataSetT) string { // Within the node
	jd.logger.Debugf("computeNewIdxForIntraNodeMigration, insertBeforeDS : %v", insertBeforeDS)
	dList := jd.refreshDSList(l)
	jd.logger.Debugf("dlist in which we are trying to find %v is %v", insertBeforeDS, dList)
	newDSIdx := ""
	var err error
	jd.assert(len(dList) > 0, fmt.Sprintf("len(dList): %d <= 0", len(dList)))
	for idx, ds := range dList {
		if ds.Index == insertBeforeDS.Index {
			jd.assert(idx > 0, "We never want to insert before first dataset")
			newDSIdx, err = computeInsertIdx(dList[idx-1].Index, insertBeforeDS.Index)
			jd.assertError(err)
		}
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

func (jd *HandleT) createDSInTx(tx *sql.Tx, newDS dataSetT) error {
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
	sqlStatement := fmt.Sprintf(`CREATE TABLE %q (
                                      job_id BIGSERIAL PRIMARY KEY,
									  workspace_id TEXT NOT NULL DEFAULT '',
									  uuid UUID NOT NULL,
									  user_id TEXT NOT NULL,
									  parameters JSONB NOT NULL,
                                      custom_val VARCHAR(64) NOT NULL,
                                      event_payload JSONB NOT NULL,
									  event_count INTEGER NOT NULL DEFAULT 1,
                                      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                                      expire_at TIMESTAMP NOT NULL DEFAULT NOW());`, newDS.JobTable)

	_, err = tx.ExecContext(context.TODO(), sqlStatement)
	if err != nil {
		return err
	}

	// TODO : Evaluate a way to handle indexes only for particular tables
	if jd.tablePrefix == "rt" {
		sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "customval_workspace_%s" ON %q (custom_val,workspace_id)`, newDS.Index, newDS.JobTable)
		_, err = tx.ExecContext(context.TODO(), sqlStatement)
		if err != nil {
			return err
		}
	}

	sqlStatement = fmt.Sprintf(`CREATE TABLE %q (
                                     id BIGSERIAL,
                                     job_id BIGINT REFERENCES %q(job_id),
                                     job_state VARCHAR(64),
                                     attempt SMALLINT,
                                     exec_time TIMESTAMP,
                                     retry_time TIMESTAMP,
                                     error_code VARCHAR(32),
                                     error_response JSONB DEFAULT '{}'::JSONB,
									 parameters JSONB DEFAULT '{}'::JSONB,
									 PRIMARY KEY (job_id, job_state, id));`, newDS.JobStatusTable, newDS.JobTable)

	_, err = tx.ExecContext(context.TODO(), sqlStatement)
	if err != nil {
		return err
	}

	err = jd.journalMarkDoneInTx(tx, opID)
	if err != nil {
		return err
	}

	return nil
}

func (jd *HandleT) setSequenceNumberInTx(tx *sql.Tx, l lock.DSListLockToken, dsList []dataSetT, newDSIdx string) error {
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

/*
 * Function to return max dataset index in the DB
 */
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
	defer stmt.Close()

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
	defer stmt.Close()

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

// mustDropDS drops a dataset and panics if it fails to do so
func (jd *HandleT) mustDropDS(ds dataSetT) {
	err := jd.dropDS(ds)
	jd.assertError(err)
}

// dropDS drops a dataset
func (jd *HandleT) dropDS(ds dataSetT) error {
	return jd.WithTx(func(tx *sql.Tx) error {
		var err error
		if _, err = tx.Exec(fmt.Sprintf(`LOCK TABLE %q IN ACCESS EXCLUSIVE MODE;`, ds.JobStatusTable)); err != nil {
			return err
		}
		if _, err = tx.Exec(fmt.Sprintf(`LOCK TABLE %q IN ACCESS EXCLUSIVE MODE;`, ds.JobTable)); err != nil {
			return err
		}
		if _, err = tx.Exec(fmt.Sprintf(`DROP TABLE %q`, ds.JobStatusTable)); err != nil {
			return err
		}
		if _, err = tx.Exec(fmt.Sprintf(`DROP TABLE %q`, ds.JobTable)); err != nil {
			return err
		}
		jd.postDropDs(ds)
		return nil
	})
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

	sqlStatement = fmt.Sprintf(`DROP TABLE IF EXISTS %q`, ds.JobStatusTable)
	jd.prepareAndExecStmtInTx(tx, sqlStatement)
	sqlStatement = fmt.Sprintf(`DROP TABLE IF EXISTS %q`, ds.JobTable)
	jd.prepareAndExecStmtInTx(tx, sqlStatement)
	err = tx.Commit()
	jd.assertError(err)
}

func (jd *HandleT) postDropDs(ds dataSetT) {
	// Bursting Cache for this dataset
	jd.invalidateCache(ds)

	// Tracking time interval between drop ds operations. Hence calling end before start
	if jd.isStatDropDSPeriodInitialized {
		jd.statDropDSPeriod.End()
	}
	jd.statDropDSPeriod.Start()
	jd.isStatDropDSPeriodInitialized = true
}

func (jd *HandleT) invalidateCache(ds dataSetT) {
	// Trimming pre_drop from the table name
	if strings.HasPrefix(ds.JobTable, preDropTablePrefix) {
		parentDS := dataSetT{
			JobTable:       strings.ReplaceAll(ds.JobTable, preDropTablePrefix, ""),
			JobStatusTable: strings.ReplaceAll(ds.JobStatusTable, preDropTablePrefix, ""),
			Index:          ds.Index,
		}
		jd.dropDSFromCache(parentDS)
	} else {
		jd.dropDSFromCache(ds)
	}
}

// mustRenameDS renames a dataset
func (jd *HandleT) mustRenameDS(ds dataSetT) error {
	var sqlStatement string
	renamedJobStatusTable := fmt.Sprintf(`%s%s`, preDropTablePrefix, ds.JobStatusTable)
	renamedJobTable := fmt.Sprintf(`%s%s`, preDropTablePrefix, ds.JobTable)
	return jd.WithTx(func(tx *sql.Tx) error {
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
			err = preBackupHandler.Handle(context.TODO(), tx, renamedJobTable, renamedJobStatusTable)
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
			if _, err = tx.Exec(fmt.Sprintf(`DROP TABLE %q`, renamedJobStatusTable)); err != nil {
				return fmt.Errorf("could not drop empty pre_drop job status table %s: %w", renamedJobStatusTable, err)
			}
			if _, err = tx.Exec(fmt.Sprintf(`DROP TABLE %q`, renamedJobTable)); err != nil {
				return fmt.Errorf("could not drop empty pre_drop job table %s: %w", renamedJobTable, err)
			}
		}
		return nil
	})
}

// renameDS renames a dataset if it exists
func (jd *HandleT) renameDS(ds dataSetT) error {
	var sqlStatement string
	renamedJobStatusTable := fmt.Sprintf(`%s%s`, preDropTablePrefix, ds.JobStatusTable)
	renamedJobTable := fmt.Sprintf(`%s%s`, preDropTablePrefix, ds.JobTable)
	return jd.WithTx(func(tx *sql.Tx) error {
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
	dnumList := []string{}

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

func (jd *HandleT) dropMigrationCheckpointTables() {
	tableNames := mustGetAllTableNames(jd, jd.dbHandle)

	var migrationCheckPointTables []string
	for _, t := range tableNames {
		if strings.HasPrefix(t, jd.tablePrefix) && strings.HasSuffix(t, MigrationCheckpointSuffix) {
			migrationCheckPointTables = append(migrationCheckPointTables, t)
		}
	}

	for _, tableName := range migrationCheckPointTables {
		sqlStatement := fmt.Sprintf(`DROP TABLE %q`, tableName)
		_, err := jd.dbHandle.Exec(sqlStatement)
		jd.assertError(err)
	}
}

func (jd *HandleT) dropAllDS(l lock.DSListLockToken) error {
	var err error
	dList := jd.refreshDSList(l)
	for _, ds := range dList {
		if err = jd.dropDS(ds); err != nil {
			return err
		}
	}

	// Update the lists
	jd.refreshDSRangeList(l)

	return err
}

/*
Function to migrate jobs from src dataset  (srcDS) to destination dataset (dest_ds)
First all the unprocessed jobs are copied over. Then all the jobs which haven't
completed (state is failed or waiting or waiting_retry or executiong) are copied
over. Then the status (only the latest) is set for those jobs
*/

func (jd *HandleT) migrateJobs(ctx context.Context, srcDS, destDS dataSetT) (noJobsMigrated int, err error) {
	queryStat := stats.NewTaggedStat("migration_jobs", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()
	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()

	// Unprocessed jobs
	unprocessedList, err := jd.getUnprocessedJobsDS(ctx, srcDS, false, GetQueryParamsT{})
	if err != nil {
		return 0, err
	}
	// Jobs which haven't finished processing
	retryList, err := jd.getProcessedJobsDS(ctx, srcDS, true,
		GetQueryParamsT{StateFilters: getValidNonTerminalStates()})
	if err != nil {
		return 0, err
	}
	jobsToMigrate := append(unprocessedList.Jobs, retryList.Jobs...)
	noJobsMigrated = len(jobsToMigrate)

	err = jd.WithTx(func(tx *sql.Tx) error {
		if err := jd.copyJobsDS(tx, destDS, jobsToMigrate); err != nil {
			return err
		}
		// Now copy over the latest status of the unfinished jobs
		var statusList []*JobStatusT
		for _, job := range retryList.Jobs {
			newStatus := JobStatusT{
				JobID:         job.JobID,
				JobState:      job.LastJobStatus.JobState,
				AttemptNum:    job.LastJobStatus.AttemptNum,
				ExecTime:      job.LastJobStatus.ExecTime,
				RetryTime:     job.LastJobStatus.RetryTime,
				ErrorCode:     job.LastJobStatus.ErrorCode,
				ErrorResponse: job.LastJobStatus.ErrorResponse,
				Parameters:    job.LastJobStatus.Parameters,
				WorkspaceId:   job.WorkspaceId,
			}
			statusList = append(statusList, &newStatus)
		}
		return jd.copyJobStatusDS(ctx, tx, destDS, statusList, []string{})
	})
	if err != nil {
		return 0, err
	}
	return noJobsMigrated, nil
}

func (jd *HandleT) postMigrateHandleDS(l lock.DSListLockToken, migrateFrom []dataSetT) error {
	// Rename datasets before dropping them, so that they can be uploaded to s3
	for _, ds := range migrateFrom {
		if jd.BackupSettings.isBackupEnabled() && isBackupConfigured() {
			if err := jd.mustRenameDS(ds); err != nil {
				return err
			}
		} else {
			if err := jd.dropDS(ds); err != nil {
				return err
			}
		}
	}

	jd.inProgressMigrationTargetDS = nil

	// Refresh the in-memory lists
	jd.refreshDSRangeList(l)

	return nil
}

func (jd *HandleT) internalStoreJobsInTx(ctx context.Context, tx *sql.Tx, ds dataSetT, jobList []*JobT) error {
	queryStat := jd.getTimerStat("store_jobs", nil)
	queryStat.Start()
	defer queryStat.End()

	// Always clear cache even in case of an error,
	// since we are not sure about the state of the db
	defer jd.clearCache(ds, jobList)

	return jd.doStoreJobsInTx(ctx, tx, ds, jobList)
}

/*
Next set of functions are for reading/writing jobs and job_status for
a given dataset. The names should be self explainatory
*/
func (jd *HandleT) copyJobsDS(tx *sql.Tx, ds dataSetT, jobList []*JobT) error { // When fixing callers make sure error is handled with assertError
	queryStat := jd.getTimerStat("copy_jobs", nil)
	queryStat.Start()
	defer queryStat.End()

	// Always clear cache even in case of an error,
	// since we are not sure about the state of the db
	defer jd.clearCache(ds, jobList)
	return jd.copyJobsDSInTx(tx, ds, jobList)
}

func (jd *HandleT) WithStoreSafeTx(f func(tx StoreSafeTx) error) error {
	return jd.inStoreSafeCtx(func() error {
		return jd.WithTx(func(tx *sql.Tx) error { return f(&storeSafeTx{tx: tx, identity: jd.tablePrefix}) })
	})
}

func (jd *HandleT) inStoreSafeCtx(f func() error) error {
	// Only locks the list
	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()
	return f()
}

func (jd *HandleT) WithUpdateSafeTx(f func(tx UpdateSafeTx) error) error {
	return jd.inUpdateSafeCtx(func() error {
		return jd.WithTx(func(tx *sql.Tx) error { return f(&updateSafeTx{tx: tx, identity: jd.tablePrefix}) })
	})
}

func (jd *HandleT) inUpdateSafeCtx(f func() error) error {
	// The order of lock is very important. The migrateDSLoop
	// takes lock in this order so reversing this will cause
	// deadlocks
	jd.dsMigrationLock.RLock()
	jd.dsListLock.RLock()
	defer jd.dsMigrationLock.RUnlock()
	defer jd.dsListLock.RUnlock()
	return f()
}

func (jd *HandleT) WithTx(f func(tx *sql.Tx) error) error {
	tx, err := jd.dbHandle.Begin()
	if err != nil {
		return err
	}
	err = f(tx)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return fmt.Errorf("%w; %s", err, rollbackErr)
		}
		return err
	}
	return tx.Commit()
}

func (jd *HandleT) clearCache(ds dataSetT, jobList []*JobT) {
	customValParamMap := make(map[string]map[string]map[string]struct{})
	var workspaces []string // for bursting old cache
	for _, job := range jobList {
		if !misc.ContainsString(workspaces, job.WorkspaceId) {
			workspaces = append(workspaces, job.WorkspaceId)
		}
		jd.populateCustomValParamMap(customValParamMap, job.CustomVal, job.Parameters, job.WorkspaceId)
	}

	if useNewCacheBurst {
		jd.doClearCache(ds, customValParamMap)
	} else {
		// NOTE: Along with clearing cache for a particular workspace key, we also have to clear for allWorkspaces key
		jd.markClearEmptyResult(ds, allWorkspaces, []string{}, []string{}, nil, hasJobs, nil)
		for _, workspace := range workspaces {
			jd.markClearEmptyResult(ds, workspace, []string{}, []string{}, nil, hasJobs, nil)
		}
	}
}

func (jd *HandleT) internalStoreWithRetryEachInTx(ctx context.Context, tx *sql.Tx, ds dataSetT, jobList []*JobT) (errorMessagesMap map[uuid.UUID]string) {
	const (
		savepointSql = "SAVEPOINT storeWithRetryEach"
		rollbackSql  = "ROLLBACK TO " + savepointSql
	)

	failAll := func(err error) map[uuid.UUID]string {
		errorMessagesMap = make(map[uuid.UUID]string)
		for i := range jobList {
			job := jobList[i]
			errorMessagesMap[job.UUID] = err.Error()
		}
		return errorMessagesMap
	}
	queryStat := jd.getTimerStat("store_jobs_retry_each", nil)
	queryStat.Start()
	defer queryStat.End()

	_, err := tx.ExecContext(ctx, savepointSql)
	if err != nil {
		return failAll(err)
	}
	err = jd.internalStoreJobsInTx(ctx, tx, ds, jobList)
	if err == nil {
		return
	}
	_, err = tx.ExecContext(ctx, rollbackSql)
	if err != nil {
		return failAll(err)
	}
	jd.logger.Errorf("Copy In command failed with error %v", err)
	errorMessagesMap = make(map[uuid.UUID]string)

	var txErr error
	for _, job := range jobList {

		if txErr != nil { // stop trying treat all remaining as failed
			errorMessagesMap[job.UUID] = txErr.Error()
			continue
		}

		// savepoint
		_, txErr = tx.ExecContext(ctx, savepointSql)
		if txErr != nil {
			errorMessagesMap[job.UUID] = txErr.Error()
			continue
		}

		// try to store
		err := jd.storeJob(ctx, tx, ds, job)
		if err != nil {
			errorMessagesMap[job.UUID] = err.Error()
			// rollback to savepoint
			_, txErr = tx.ExecContext(ctx, rollbackSql)
		}

	}

	return
}

// Creates a map of workspace:customVal:Params(Dest_type: []Dest_ids for brt and Dest_type: [] for rt)
// and then loop over them to selectively clear cache instead of clearing the cache for the entire dataset
func (jd *HandleT) populateCustomValParamMap(CVPMap map[string]map[string]map[string]struct{}, customVal string, params []byte, workspace string) {
	if _, ok := CVPMap[workspace]; !ok {
		CVPMap[workspace] = make(map[string]map[string]struct{})
	}
	if jd.queryFilterKeys.CustomVal {
		if _, ok := CVPMap[workspace][customVal]; !ok {
			CVPMap[workspace][customVal] = make(map[string]struct{})
		}

		if len(jd.queryFilterKeys.ParameterFilters) > 0 {
			var vals []string
			for _, key := range jd.queryFilterKeys.ParameterFilters {
				val := gjson.GetBytes(params, key).String()
				vals = append(vals, fmt.Sprintf("%s##%s", key, val))
			}
			key := strings.Join(vals, "::")
			if _, ok := CVPMap[workspace][customVal][key]; !ok {
				CVPMap[workspace][customVal][key] = struct{}{}
			}
		}
	}
}

// mark cache empty after going over ds->workspace->customvals->params and for all stateFilters
func (jd *HandleT) doClearCache(ds dataSetT, CVPMap map[string]map[string]map[string]struct{}) {
	// NOTE: Along with clearing cache for a particular workspace key, we also have to clear for allWorkspaces key
	for workspace, workspaceCVPMap := range CVPMap {
		if jd.queryFilterKeys.CustomVal && len(jd.queryFilterKeys.ParameterFilters) > 0 {
			for cv, cVal := range workspaceCVPMap {
				for pv := range cVal {
					parameterFilters := []ParameterFilterT{}
					tokens := strings.Split(pv, "::")
					for _, token := range tokens {
						p := strings.Split(token, "##")
						param := ParameterFilterT{
							Name:  p[0],
							Value: p[1],
						}
						parameterFilters = append(parameterFilters, param)
					}
					jd.markClearEmptyResult(ds, allWorkspaces, []string{NotProcessed.State}, []string{cv}, parameterFilters, hasJobs, nil)
					jd.markClearEmptyResult(ds, workspace, []string{NotProcessed.State}, []string{cv}, parameterFilters, hasJobs, nil)
				}
			}
		} else if jd.queryFilterKeys.CustomVal {
			for cv := range workspaceCVPMap {
				jd.markClearEmptyResult(ds, allWorkspaces, []string{NotProcessed.State}, []string{cv}, nil, hasJobs, nil)
				jd.markClearEmptyResult(ds, workspace, []string{NotProcessed.State}, []string{cv}, nil, hasJobs, nil)
			}
		} else {
			jd.markClearEmptyResult(ds, allWorkspaces, []string{}, []string{}, nil, hasJobs, nil)
			jd.markClearEmptyResult(ds, workspace, []string{}, []string{}, nil, hasJobs, nil)
		}
	}
}

func (jd *HandleT) GetPileUpCounts(ctx context.Context) (map[string]map[string]int, error) {
	jd.dsMigrationLock.RLock()
	jd.dsListLock.RLock()
	defer jd.dsMigrationLock.RUnlock()
	defer jd.dsListLock.RUnlock()
	dsList := jd.getDSList()
	statMap := make(map[string]map[string]int)

	for _, ds := range dsList {
		queryString := fmt.Sprintf(`with joined as (
			select
			  j.job_id as jobID,
			  j.custom_val as customVal,
			  s.id as statusID,
			  s.job_state as jobState,
			  j.workspace_id as workspace
			from
			  %[1]q j
			  left join (
				select * from (select
					  *,
					  ROW_NUMBER() OVER(
						PARTITION BY rs.job_id
						ORDER BY
						  rs.id DESC
					  ) AS row_no
					FROM
					  %[2]q as rs) nq1
				  where
				  nq1.row_no = 1

			  ) s on j.job_id = s.job_id
			where
			  (
				s.job_state not in (
				  'aborted', 'succeeded',
				  'migrated'
				)
				or s.job_id is null
			  )
		  )
		  select
			count(*),
			customVal,
			workspace
		  from
			joined
		  group by
			customVal,
			workspace;`, ds.JobTable, ds.JobStatusTable)
		rows, err := jd.dbHandle.QueryContext(ctx, queryString)
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			var count sql.NullInt64
			var customVal string
			var workspace string
			err := rows.Scan(&count, &customVal, &workspace)
			if err != nil {
				return statMap, err
			}
			if _, ok := statMap[workspace]; !ok {
				statMap[workspace] = make(map[string]int)
			}
			statMap[workspace][customVal] += int(count.Int64)
		}
		if err = rows.Err(); err != nil {
			return statMap, err
		}
	}
	return statMap, nil
}

func (*HandleT) copyJobsDSInTx(txHandler transactionHandler, ds dataSetT, jobList []*JobT) error {
	var stmt *sql.Stmt
	var err error

	stmt, err = txHandler.Prepare(pq.CopyIn(ds.JobTable, "job_id", "uuid", "user_id", "custom_val", "parameters",
		"event_payload", "event_count", "created_at", "expire_at", "workspace_id"))

	if err != nil {
		return err
	}

	defer stmt.Close()

	for _, job := range jobList {
		eventCount := 1
		if job.EventCount > 1 {
			eventCount = job.EventCount
		}

		_, err = stmt.Exec(job.JobID, job.UUID, job.UserID, job.CustomVal, string(job.Parameters),
			string(job.EventPayload), eventCount, job.CreatedAt, job.ExpireAt, job.WorkspaceId)

		if err != nil {
			return err
		}
	}
	if _, err = stmt.Exec(); err != nil {
		return err
	}

	// We are manually triggering ANALYZE to help with query planning since a large
	// amount of rows are being copied in the table in a very short time and
	// AUTOVACUUM might not have a chance to do its work before we start querying
	// this table
	_, err = txHandler.Exec(fmt.Sprintf(`ANALYZE %q`, ds.JobTable))
	return err
}

func (jd *HandleT) doStoreJobsInTx(ctx context.Context, tx *sql.Tx, ds dataSetT, jobList []*JobT) error {
	store := func() error {
		var stmt *sql.Stmt
		var err error

		stmt, err = tx.PrepareContext(ctx, pq.CopyIn(ds.JobTable, "uuid", "user_id", "custom_val", "parameters", "event_payload", "event_count", "workspace_id"))
		if err != nil {
			return err
		}

		defer stmt.Close()
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

func (jd *HandleT) storeJob(ctx context.Context, tx *sql.Tx, ds dataSetT, job *JobT) (err error) {
	sqlStatement := fmt.Sprintf(`INSERT INTO %q (uuid, user_id, custom_val, parameters, event_payload, workspace_id)
	                                   VALUES ($1, $2, $3, $4, $5, $6) RETURNING job_id`, ds.JobTable)
	stmt, err := tx.PrepareContext(ctx, sqlStatement)
	jd.assertError(err)
	defer stmt.Close()
	job.sanitizeJson()
	_, err = stmt.ExecContext(ctx, job.UUID, job.UserID, job.CustomVal, string(job.Parameters), string(job.EventPayload), job.WorkspaceId)
	if err == nil {
		// Empty customValFilters means we want to clear for all
		jd.markClearEmptyResult(ds, allWorkspaces, []string{}, []string{}, nil, hasJobs, nil)
		jd.markClearEmptyResult(ds, job.WorkspaceId, []string{}, []string{}, nil, hasJobs, nil)
		// fmt.Println("Bursting CACHE")
		return
	}
	pqErr, ok := err.(*pq.Error)
	if ok {
		errCode := string(pqErr.Code)
		if _, ok := dbInvalidJsonErrors[errCode]; ok {
			return errors.New("Invalid JSON")
		}
	}
	return
}

type cacheValue string

const (
	hasJobs         cacheValue = "Has Jobs"
	noJobs          cacheValue = "No Jobs"
	dropDSFromCache cacheValue = "Drop DS From Cache"
	/*
	* willTryToSet value is used to prevent wrongly setting empty result when
	* a db update (new jobs or job status updates) happens during get(Un)Processed db query is in progress.
	*
	* getUnprocessedJobs() {  # OR getProcessedJobsDS
	* 0. Sets cache value to willTryToSet
	* 1. out = queryDB()
	* 2. check and set cache to (len(out) == 0) only if cache value is willTryToSet
	* }
	 */
	willTryToSet cacheValue = "Query in progress"
)

type cacheEntry struct {
	Value cacheValue `json:"value"`
	T     time.Time  `json:"set_at"`
}

func (jd *HandleT) dropDSFromCache(ds dataSetT) {
	jd.dsCacheLock.Lock()
	defer jd.dsCacheLock.Unlock()

	delete(jd.dsEmptyResultCache, ds)
}

/*
* If a query returns empty result for a specific dataset, we cache that so that
* future queries don't have to hit the DB.
* markClearEmptyResult() when mark=True marks dataset,customVal,state as empty.
* markClearEmptyResult() when mark=False clears a previous empty mark
 */

func (jd *HandleT) markClearEmptyResult(ds dataSetT, workspace string, stateFilters, customValFilters []string, parameterFilters []ParameterFilterT, value cacheValue, checkAndSet *cacheValue) {
	// Safe check. Every status must have a valid workspace id for the cache to work efficiently.
	if workspace == "" {
		jd.logger.Debugf("[%s] Empty workspace key provided while looking into jobsdb cachemap", jd.tablePrefix)
		jd.invalidCacheKeyStat.Increment()
	}

	jd.dsCacheLock.Lock()
	defer jd.dsCacheLock.Unlock()

	// This means we want to mark/clear all customVals and stateFilters
	// When clearing, we remove the entire dataset entry. Not a big issue
	// We process ALL only during internal migration and caching empty
	// results is not important
	if len(stateFilters) == 0 || len(customValFilters) == 0 {
		if value == hasJobs || value == dropDSFromCache {
			delete(jd.dsEmptyResultCache, ds)
		}
		return
	}

	_, ok := jd.dsEmptyResultCache[ds]
	if !ok {
		jd.dsEmptyResultCache[ds] = map[string]map[string]map[string]map[string]cacheEntry{}
	}

	if _, ok := jd.dsEmptyResultCache[ds][workspace]; !ok {
		jd.dsEmptyResultCache[ds][workspace] = map[string]map[string]map[string]cacheEntry{}
	}
	for _, cVal := range customValFilters {
		_, ok := jd.dsEmptyResultCache[ds][workspace][cVal]
		if !ok {
			jd.dsEmptyResultCache[ds][workspace][cVal] = map[string]map[string]cacheEntry{}
		}

		pVals := []string{}
		for _, parameterFilter := range parameterFilters {
			pVals = append(pVals, fmt.Sprintf(`%s_%s`, parameterFilter.Name, parameterFilter.Value))
		}
		sort.Strings(pVals)
		pVal := strings.Join(pVals, "_")

		_, ok = jd.dsEmptyResultCache[ds][workspace][cVal][pVal]
		if !ok {
			jd.dsEmptyResultCache[ds][workspace][cVal][pVal] = map[string]cacheEntry{}
		}

		for _, st := range stateFilters {
			previous := jd.dsEmptyResultCache[ds][workspace][cVal][pVal][st]
			if checkAndSet == nil || *checkAndSet == previous.Value {
				jd.dsEmptyResultCache[ds][workspace][cVal][pVal][st] = cacheEntry{
					Value: value,
					T:     time.Now(),
				}
			}
		}
	}
}

// isEmptyResult will return true if:
//
//		For all the combinations of stateFilters, customValFilters, parameterFilters.
//	 All of the condition above apply:
//		* There is a cache entry for this dataset, customVal, parameterFilter, stateFilter
//	 * The entry is noJobs
//	 * The entry is not expired (entry time + cache expiration > now)
func (jd *HandleT) isEmptyResult(ds dataSetT, workspace string, stateFilters, customValFilters []string, parameterFilters []ParameterFilterT) bool {
	queryStat := stats.NewTaggedStat("isEmptyCheck", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	jd.dsCacheLock.Lock()
	defer jd.dsCacheLock.Unlock()

	_, ok := jd.dsEmptyResultCache[ds]
	if !ok {
		return false
	}

	_, ok = jd.dsEmptyResultCache[ds][workspace]
	if !ok {
		return false
	}
	// We want to check for all states and customFilters. Cannot
	// assert that from cache
	if len(stateFilters) == 0 || len(customValFilters) == 0 {
		return false
	}

	for _, cVal := range customValFilters {
		_, ok := jd.dsEmptyResultCache[ds][workspace][cVal]
		if !ok {
			return false
		}

		pVals := []string{}
		for _, parameterFilter := range parameterFilters {
			pVals = append(pVals, fmt.Sprintf(`%s_%s`, parameterFilter.Name, parameterFilter.Value))
		}
		sort.Strings(pVals)
		pVal := strings.Join(pVals, "_")

		_, ok = jd.dsEmptyResultCache[ds][workspace][cVal][pVal]
		if !ok {
			return false
		}

		for _, st := range stateFilters {
			mark, ok := jd.dsEmptyResultCache[ds][workspace][cVal][pVal][st]
			if !ok || mark.Value != noJobs || time.Now().After(mark.T.Add(cacheExpiration)) {
				return false
			}
		}
	}
	// Every state and every customVal in the DS is empty
	// so can return
	return true
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
func (jd *HandleT) getProcessedJobsDS(ctx context.Context, ds dataSetT, getAll bool, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	stateFilters := params.StateFilters
	customValFilters := params.CustomValFilters
	parameterFilters := params.ParameterFilters

	checkValidJobState(jd, stateFilters)

	if jd.isEmptyResult(ds, allWorkspaces, stateFilters, customValFilters, parameterFilters) {
		jd.logger.Debugf("[getProcessedJobsDS] Empty cache hit for ds: %v, stateFilters: %v, customValFilters: %v, parameterFilters: %v", ds, stateFilters, customValFilters, parameterFilters)
		return JobsResult{}, nil
	}

	tags := statTags{CustomValFilters: params.CustomValFilters, StateFilters: params.StateFilters, ParameterFilters: params.ParameterFilters}
	queryStat := jd.getTimerStat("processed_ds_time", &tags)
	queryStat.Start()
	defer queryStat.End()

	// We don't reset this in case of error for now, as any error in this function causes panic
	jd.markClearEmptyResult(ds, allWorkspaces, stateFilters, customValFilters, parameterFilters, willTryToSet, nil)

	var stateQuery, customValQuery, limitQuery, sourceQuery string

	if len(stateFilters) > 0 {
		stateQuery = " AND " + constructQueryOR("job_state", stateFilters)
	} else {
		stateQuery = ""
	}
	if len(customValFilters) > 0 && !params.IgnoreCustomValFiltersInQuery {
		jd.assert(!getAll, "getAll is true")
		customValQuery = " AND " +
			constructQueryOR("jobs.custom_val", customValFilters)
	} else {
		customValQuery = ""
	}

	if len(parameterFilters) > 0 {
		jd.assert(!getAll, "getAll is true")
		sourceQuery += " AND " + constructParameterJSONQuery("jobs", parameterFilters)
	} else {
		sourceQuery = ""
	}

	if params.JobsLimit > 0 {
		jd.assert(!getAll, "getAll is true")
		limitQuery = fmt.Sprintf(" LIMIT %d ", params.JobsLimit)
	} else {
		limitQuery = ""
	}

	var rows *sql.Rows
	if getAll {
		sqlStatement := fmt.Sprintf(`SELECT
                                	jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters,  jobs.custom_val, jobs.event_payload, jobs.event_count,
                                	jobs.created_at, jobs.expire_at, jobs.workspace_id,
									pg_column_size(jobs.event_payload) as payload_size,
									sum(jobs.event_count) over (order by jobs.job_id asc) as running_event_counts,
									sum(pg_column_size(jobs.event_payload)) over (order by jobs.job_id) as running_payload_size,
                                	job_latest_state.job_state, job_latest_state.attempt,
                                	job_latest_state.exec_time, job_latest_state.retry_time,
                                	job_latest_state.error_code, job_latest_state.error_response, job_latest_state.parameters
                                 FROM
                                	%[1]q AS jobs,
                                	(SELECT job_id, job_state, attempt, exec_time, retry_time,
                                		error_code, error_response,parameters FROM %[2]q WHERE id IN
                                		(SELECT MAX(id) from %[2]q GROUP BY job_id) %[3]s)
                                	AS job_latest_state
                                WHERE jobs.job_id=job_latest_state.job_id`,
			ds.JobTable, ds.JobStatusTable, stateQuery)
		var err error
		rows, err = jd.dbHandle.QueryContext(ctx, sqlStatement)
		if err != nil {
			return JobsResult{}, err
		}
		defer rows.Close()

	} else {
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
									%[1]q AS jobs,
									(SELECT job_id, job_state, attempt, exec_time, retry_time,
										error_code, error_response, parameters FROM %[2]q WHERE id IN
										(SELECT MAX(id) from %[2]q GROUP BY job_id) %[3]s)
									AS job_latest_state
								WHERE jobs.job_id=job_latest_state.job_id
									%[4]s %[5]s
									AND job_latest_state.retry_time < $1 ORDER BY jobs.job_id %[6]s`,
			ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, limitQuery)

		args := []interface{}{getTimeNowFunc()}

		var wrapQuery []string
		if params.EventsLimit > 0 {
			// If there is a single job in the dataset containing more events than the EventsLimit, we should return it,
			// otherwise processing will halt.
			// Therefore, we always retrieve one more job from the database than our limit dictates.
			// This job will only be returned in the result in case of the aforementioned scenario, otherwise it gets filtered out
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
			return JobsResult{}, err
		}
		defer stmt.Close()

		rows, err = stmt.QueryContext(ctx, args...)
		if err != nil {
			return JobsResult{}, err
		}
		defer rows.Close()

	}

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
			return JobsResult{}, err
		}

		if !getAll { // if getAll is true, limits do not apply
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
		}
		// we are adding the job only after testing for limitsReached
		// so that we don't always overflow
		jobList = append(jobList, &job)
		payloadSize = runningPayloadSize
		eventCount = runningEventCount
	}
	if !limitsReached &&
		(params.JobsLimit > 0 && len(jobList) == params.JobsLimit) || // we reached the jobs limit
		(params.EventsLimit > 0 && eventCount >= params.EventsLimit) || // we reached the events limit
		(params.PayloadSizeLimit > 0 && payloadSize >= params.PayloadSizeLimit) { // we reached the payload limit
		limitsReached = true
	}

	result := hasJobs
	if len(jobList) == 0 {
		jd.logger.Debugf("[getProcessedJobsDS] Setting empty cache for ds: %v, stateFilters: %v, customValFilters: %v, parameterFilters: %v", ds, stateFilters, customValFilters, parameterFilters)
		result = noJobs
	}
	_willTryToSet := willTryToSet
	jd.markClearEmptyResult(ds, allWorkspaces, stateFilters, customValFilters, parameterFilters, result, &_willTryToSet)

	return JobsResult{
		Jobs:          jobList,
		LimitsReached: limitsReached,
		PayloadSize:   payloadSize,
		EventsCount:   eventCount,
	}, nil
}

/*
count == 0 means return all
stateFilters and customValFilters do a OR query on values passed in array
parameterFilters do a AND query on values included in the map.
A JobsLimit less than or equal to zero indicates no limit.
*/
func (jd *HandleT) getUnprocessedJobsDS(ctx context.Context, ds dataSetT, order bool, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	customValFilters := params.CustomValFilters
	parameterFilters := params.ParameterFilters

	if jd.isEmptyResult(ds, allWorkspaces, []string{NotProcessed.State}, customValFilters, parameterFilters) {
		jd.logger.Debugf("[getUnprocessedJobsDS] Empty cache hit for ds: %v, stateFilters: NP, customValFilters: %v, parameterFilters: %v", ds, customValFilters, parameterFilters)
		return JobsResult{}, nil
	}

	tags := statTags{CustomValFilters: params.CustomValFilters, ParameterFilters: params.ParameterFilters}
	queryStat := jd.getTimerStat("unprocessed_ds_time", &tags)
	queryStat.Start()
	defer queryStat.End()

	// We don't reset this in case of error for now, as any error in this function causes panic
	jd.markClearEmptyResult(ds, allWorkspaces, []string{NotProcessed.State}, customValFilters, parameterFilters, willTryToSet, nil)

	var rows *sql.Rows
	var err error
	var args []interface{}

	var sqlStatement string

	if useJoinForUnprocessed {
		// event_count default 1, number of items in payload
		sqlStatement = fmt.Sprintf(
			`SELECT jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count, jobs.created_at, jobs.expire_at, jobs.workspace_id,`+
				`	pg_column_size(jobs.event_payload) as payload_size, `+
				`	sum(jobs.event_count) over (order by jobs.job_id asc) as running_event_counts, `+
				`	sum(pg_column_size(jobs.event_payload)) over (order by jobs.job_id) as running_payload_size `+
				`FROM %[1]q AS jobs `+
				`LEFT JOIN %[2]q AS job_status ON jobs.job_id=job_status.job_id `+
				`WHERE job_status.job_id is NULL `,
			ds.JobTable, ds.JobStatusTable)
	} else {
		sqlStatement = fmt.Sprintf(
			`SELECT jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count, jobs.created_at, jobs.expire_at, jobs.workspace_id,`+
				`	pg_column_size(jobs.event_payload) as payload_size, `+
				`	sum(jobs.event_count) over (order by jobs.job_id asc) as running_event_counts, `+
				`	sum(pg_column_size(jobs.event_payload)) over (order by jobs.job_id) as running_payload_size `+
				` FROM %[1]q AS jobs `+
				`WHERE jobs.job_id NOT IN (SELECT DISTINCT(job_status.job_id) FROM %[2]q AS job_status)`,
			ds.JobTable, ds.JobStatusTable)
	}

	if len(customValFilters) > 0 && !params.IgnoreCustomValFiltersInQuery {
		sqlStatement += " AND " + constructQueryOR("jobs.custom_val", customValFilters)
	}

	if len(parameterFilters) > 0 {
		sqlStatement += " AND " + constructParameterJSONQuery("jobs", parameterFilters)
	}

	if order {
		sqlStatement += " ORDER BY jobs.job_id"
	}
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
		return JobsResult{}, err
	}
	defer rows.Close()
	if err != nil {
		return JobsResult{}, err
	}
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
			return JobsResult{}, err
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
	if !limitsReached &&
		(params.JobsLimit > 0 && len(jobList) == params.JobsLimit) || // we reached the jobs limit
		(params.EventsLimit > 0 && eventCount >= params.EventsLimit) || // we reached the events limit
		(params.PayloadSizeLimit > 0 && payloadSize >= params.PayloadSizeLimit) { // we reached the payload limit
		limitsReached = true
	}

	result := hasJobs
	dsList := jd.getDSList()
	// if jobsdb owner is a reader and if ds is the right most one, ignoring setting result as noJobs
	if len(jobList) == 0 && (jd.ownerType != Read || ds.Index != dsList[len(dsList)-1].Index) {
		jd.logger.Debugf("[getUnprocessedJobsDS] Setting empty cache for ds: %v, stateFilters: NP, customValFilters: %v, parameterFilters: %v", ds, customValFilters, parameterFilters)
		result = noJobs
	}
	_willTryToSet := willTryToSet
	jd.markClearEmptyResult(ds, allWorkspaces, []string{NotProcessed.State}, customValFilters, parameterFilters, result, &_willTryToSet)

	return JobsResult{
		Jobs:          jobList,
		LimitsReached: limitsReached,
		PayloadSize:   payloadSize,
		EventsCount:   eventCount,
	}, nil
}

// copyJobStatusDS is expected to be called only during a migration
func (jd *HandleT) copyJobStatusDS(ctx context.Context, tx *sql.Tx, ds dataSetT, statusList []*JobStatusT, customValFilters []string) (err error) {
	var parameterFilters []ParameterFilterT
	if len(statusList) == 0 {
		return nil
	}

	var stateFiltersByWorkspace map[string][]string
	tags := statTags{CustomValFilters: customValFilters, ParameterFilters: parameterFilters}
	stateFiltersByWorkspace, err = jd.updateJobStatusDSInTx(ctx, tx, ds, statusList, tags)
	if err != nil {
		return err
	}
	// We are manually triggering ANALYZE to help with query planning since a large
	// amount of rows are being copied in the table in a very short time and
	// AUTOVACUUM might not have a chance to do its work before we start querying
	// this table
	_, err = tx.ExecContext(ctx, fmt.Sprintf(`ANALYZE %q`, ds.JobStatusTable))
	if err != nil {
		return err
	}

	allUpdatedStates := make([]string, 0)
	for workspaceID, stateFilters := range stateFiltersByWorkspace {
		jd.markClearEmptyResult(ds, workspaceID, stateFilters, customValFilters, parameterFilters, hasJobs, nil)
		allUpdatedStates = append(allUpdatedStates, stateFilters...)
	}
	// NOTE: Along with clearing cache for a particular workspace key, we also have to clear for allWorkspaces key
	jd.markClearEmptyResult(ds, allWorkspaces, misc.Unique(allUpdatedStates), customValFilters, parameterFilters, hasJobs, nil)
	return nil
}

func (jd *HandleT) updateJobStatusDSInTx(ctx context.Context, tx *sql.Tx, ds dataSetT, statusList []*JobStatusT, tags statTags) (updatedStates map[string][]string, err error) {
	if len(statusList) == 0 {
		return
	}

	queryStat := jd.getTimerStat("update_job_status_ds_time", &tags)
	queryStat.Start()
	defer queryStat.End()
	updatedStatesMap := map[string]map[string]bool{}
	store := func() error {
		stmt, err := tx.PrepareContext(ctx, pq.CopyIn(ds.JobStatusTable, "job_id", "job_state", "attempt", "exec_time",
			"retry_time", "error_code", "error_response", "parameters"))
		if err != nil {
			return err
		}
		for _, status := range statusList {
			//  Handle the case when google analytics returns gif in response
			if _, ok := updatedStatesMap[status.WorkspaceId]; !ok {
				updatedStatesMap[status.WorkspaceId] = make(map[string]bool)
			}
			updatedStatesMap[status.WorkspaceId][status.JobState] = true
			if !utf8.ValidString(string(status.ErrorResponse)) {
				status.ErrorResponse = []byte(`{}`)
			}
			_, err = stmt.ExecContext(ctx, status.JobID, status.JobState, status.AttemptNum, status.ExecTime,
				status.RetryTime, status.ErrorCode, string(status.ErrorResponse), string(status.Parameters))
			if err != nil {
				return err
			}
		}
		updatedStates = make(map[string][]string)
		for k := range updatedStatesMap {
			if _, ok := updatedStates[k]; !ok {
				updatedStates[k] = make([]string, 0, len(updatedStatesMap[k]))
			}
			for state := range updatedStatesMap[k] {
				updatedStates[k] = append(updatedStates[k], state)
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

/**
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
time and can potentially block the StoreJob() call. Blocking StoreJob()
is bad since user ACK won't be sent unless StoreJob() returns.

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

		// Adding a new DS only creates a new DS & updates the cache. It doesn't move any data so we only take the list lock.
		var dsListLock lock.DSListLockToken
		var releaseDsListLock chan<- lock.DSListLockToken
		// start a transaction
		err := jd.WithTx(func(tx *sql.Tx) error {
			// acquire a advisory transaction level blocking lock, which is released once the transaction ends.
			sqlStatement := fmt.Sprintf(`SELECT pg_advisory_xact_lock(%d);`, misc.JobsDBAddDsAdvisoryLock)
			_, err := tx.ExecContext(context.TODO(), sqlStatement)
			if err != nil {
				return err
			}

			// We acquire the list lock only after we have acquired the advisory lock.
			// We will release the list lock after the transaction ends, that's why we need to use an async lock
			dsListLock, releaseDsListLock = jd.dsListLock.AsyncLock()
			// refresh ds list
			var dsList []dataSetT
			var nextDSIdx string
			// make sure we are operating on the latest version of the list
			dsList = getDSList(jd, tx, jd.tablePrefix)
			latestDS := dsList[len(dsList)-1]
			full, err := jd.checkIfFullDSInTx(tx, latestDS)
			if err != nil {
				return err
			}
			// checkIfFullDS is true for last DS in the list
			if full {
				nextDSIdx = jd.doComputeNewIdxForAppend(dsList)
				jd.logger.Infof("[[ %s : addNewDSLoop ]]: NewDS", jd.tablePrefix)
				if err := jd.addNewDSInTx(tx, dsListLock, dsList, newDataSet(jd.tablePrefix, nextDSIdx)); err != nil {
					return err
				}
			}
			return nil
		})
		jd.assertError(err)

		// to get the updated DS list in the cache after createDS transaction has been committed.
		jd.refreshDSRangeList(dsListLock)
		releaseDsListLock <- dsListLock
	}
}

func (jd *HandleT) refreshDSListLoop(ctx context.Context) {
	for {
		select {
		case <-time.After(refreshDSListLoopSleepDuration):
		case <-ctx.Done():
			return
		}

		jd.logger.Debugf("[[ %s : refreshDSListLoop ]]: Start", jd.tablePrefix)
		jd.dsListLock.WithLock(func(l lock.DSListLockToken) {
			jd.refreshDSRangeList(l)
		})
	}
}

func (jd *HandleT) migrateDSLoop(ctx context.Context) {
	for {
		select {
		case <-jd.TriggerMigrateDS():
		case <-ctx.Done():
			return
		}
		jd.logger.Debugf("[[ %s : migrateDSLoop ]]: Start", jd.tablePrefix)

		// This block disables internal migration/consolidation while cluster-level migration is in progress
		if db.IsValidMigrationMode(jd.migrationState.migrationMode) {
			jd.logger.Debugf("[[ %s : migrateDSLoop ]]: migration mode = %s, so skipping internal migrations", jd.tablePrefix, jd.migrationState.migrationMode)
			continue
		}

		jd.dsListLock.RLock()
		dsList := jd.getDSList()
		jd.dsListLock.RUnlock()

		var migrateFrom []dataSetT
		var insertBeforeDS dataSetT
		var liveJobCount int
		var liveDSCount int
		var migrateDSProbeCount int
		// we don't want `maxDSSize` value to change, during dsList loop
		maxDSSize := *jd.MaxDSSize
		for idx, ds := range dsList {

			var idxCheck bool
			if jd.ownerType == Read {
				// if jobsdb owner is read, expempting the last two datasets from migration.
				// This is done to avoid dslist conflicts between reader and writer
				idxCheck = idx == len(dsList)-1 || idx == len(dsList)-2
			} else {
				idxCheck = idx == len(dsList)-1
			}

			if liveDSCount >= maxMigrateOnce || liveJobCount >= maxDSSize || idxCheck {
				break
			}

			ifMigrate, remCount := jd.checkIfMigrateDS(ds)
			jd.logger.Debugf("[[ %s : migrateDSLoop ]]: Migrate check %v, ds: %v", jd.tablePrefix, ifMigrate, ds)

			if ifMigrate {
				migrateFrom = append(migrateFrom, ds)
				insertBeforeDS = dsList[idx+1]
				liveJobCount += remCount
				liveDSCount++
			} else if liveDSCount > 0 || migrateDSProbeCount > maxMigrateDSProbe {
				// DS is not eligible for migration. But there are data sets on the left eligible to migrate, so break.
				break
			}

			migrateDSProbeCount++
		}

		// Take the lock and run actual migration
		jd.dsMigrationLock.Lock()

		migrationLoopStat := stats.NewTaggedStat("migration_loop", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
		migrationLoopStat.Start()
		var opID int64
		// Add a temp DS to append to
		if len(migrateFrom) > 0 {
			if liveJobCount > 0 {
				var migrateTo dataSetT
				jd.dsListLock.WithLock(func(l lock.DSListLockToken) {
					migrateTo = newDataSet(jd.tablePrefix, jd.computeNewIdxForIntraNodeMigration(l, insertBeforeDS))

					jd.logger.Infof("[[ %s : migrateDSLoop ]]: Migrate from: %v", jd.tablePrefix, migrateFrom)
					jd.logger.Infof("[[ %s : migrateDSLoop ]]: Next: %v", jd.tablePrefix, insertBeforeDS)
					jd.logger.Infof("[[ %s : migrateDSLoop ]]: To: %v", jd.tablePrefix, migrateTo)
					// Mark the start of copy operation. If we fail here
					// we just delete the new DS being copied into. The
					// sources are still around
					opPayload, err := json.Marshal(&journalOpPayloadT{From: migrateFrom, To: migrateTo})
					jd.assertError(err)
					opID = jd.JournalMarkStart(migrateCopyOperation, opPayload)

					jd.addDS(migrateTo)
					jd.inProgressMigrationTargetDS = &migrateTo
				})

				totalJobsMigrated := 0
				for _, ds := range migrateFrom {
					jd.logger.Infof("[[ %s : migrateDSLoop ]]: Migrate: %v to: %v", jd.tablePrefix, ds, migrateTo)
					noJobsMigrated, _ := jd.migrateJobs(ctx, ds, migrateTo)
					totalJobsMigrated += noJobsMigrated
				}
				jd.logger.Infof("[[ %s : migrateDSLoop ]]: Total migrated %d jobs", jd.tablePrefix, totalJobsMigrated)

				if totalJobsMigrated <= 0 {
					jd.dsListLock.WithLock(func(_ lock.DSListLockToken) {
						jd.mustDropDS(migrateTo)
						jd.inProgressMigrationTargetDS = nil
					})
				}
				jd.logger.Infof("[[ %s : migrateDSLoop ]]: Migrate DONE", jd.tablePrefix)

			}

			err := jd.WithTx(func(tx *sql.Tx) error {
				if opID > 0 {
					// marking the previous operation as done and
					// starting the next operation must be performed
					// as a single atomic action (all or nothing)
					err := jd.journalMarkDoneInTx(tx, opID)
					if err != nil {
						return err
					}
				}
				// Mark the start of del operation. If we fail in between
				// we need to finish deleting the source datasets. Cannot
				// del the destination as some sources may have been deleted
				opPayload, err := json.Marshal(&journalOpPayloadT{From: migrateFrom})
				if err != nil {
					return err
				}
				opID, err = jd.JournalMarkStartInTx(tx, postMigrateDSOperation, opPayload)
				return err
			})
			jd.assertError(err)

			jd.dsListLock.WithLock(func(l lock.DSListLockToken) {
				jd.assertError(jd.postMigrateHandleDS(l, migrateFrom))
			})

			jd.JournalMarkDone(opID)
		}
		migrationLoopStat.End()
		jd.dsMigrationLock.Unlock()

	}
}

func (jd *HandleT) backupDSLoop(ctx context.Context) {
	sleepMultiplier := time.Duration(1)

	jd.logger.Info("BackupDS loop is running")

	for {
		select {
		case <-time.After(sleepMultiplier * backupCheckSleepDuration):
			if !jd.BackupSettings.isBackupEnabled() {
				jd.logger.Debugf("backupDSLoop backup disabled %s", jd.tablePrefix)
				continue
			}
		case <-ctx.Done():
			return
		}
		jd.logger.Debugf("backupDSLoop backup enabled %s", jd.tablePrefix)
		backupDSRange := jd.getBackupDSRange()
		// check if non empty dataset is present to backup
		// else continue
		sleepMultiplier = 1
		if (dataSetRangeT{} == *backupDSRange) {
			// sleep for more duration if no dataset is found
			sleepMultiplier = 6
			continue
		}

		backupDS := backupDSRange.ds

		opPayload, err := json.Marshal(&backupDS)
		jd.assertError(err)

		var opID int64
		if isBackupConfigured() {
			opID = jd.JournalMarkStart(backupDSOperation, opPayload)
			err := jd.backupDS(ctx, backupDSRange)
			if err != nil {
				stats.NewTaggedStat("backup_ds_failed", stats.CountType, stats.Tags{"customVal": jd.tablePrefix, "provider": config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "S3")}).Increment()
				jd.logger.Errorf("[JobsDB] :: Failed to backup jobs table %v. Err: %v", backupDSRange.ds.JobStatusTable, err)
			}
			jd.JournalMarkDone(opID)
		}

		// drop dataset after successfully uploading both jobs and jobs_status to s3
		opID = jd.JournalMarkStart(backupDropDSOperation, opPayload)
		// Currently, we retry uploading a table for sometime & if it fails. We only drop that table & not all `pre_drop` tables.
		// So, in situation when new table creation rate is more than drop. We will still have pipe up issue.
		// An easy way to fix this is, if at any point of time exponential retry fails then instead of just dropping that particular
		// table drop all subsequent `pre_drop` table. As, most likely the upload of rest of the table will also fail with the same error.
		jd.mustDropDS(backupDS)
		jd.JournalMarkDone(opID)
	}
}

// backupDS writes both jobs and job_staus table to JOBS_BACKUP_STORAGE_PROVIDER
func (jd *HandleT) backupDS(ctx context.Context, backupDSRange *dataSetRangeT) error {
	// return after backing up aboprted jobs if the flag is turned on
	// backupDS is only called when BackupSettings.BackupEnabled is true
	if jd.BackupSettings.FailedOnly {
		jd.logger.Info("[JobsDB] ::  backupDS: starting backing up aborted")
		err := jd.backupTable(ctx, backupDSRange, false)
		if err != nil {
			return err
		}
	} else {
		// write jobs table to JOBS_BACKUP_STORAGE_PROVIDER
		err := jd.backupTable(ctx, backupDSRange, false)
		if err != nil {
			return err
		}

		// write job_status table to JOBS_BACKUP_STORAGE_PROVIDER
		err = jd.backupTable(ctx, backupDSRange, true)
		if err != nil {
			return err
		}

	}

	return nil
}

func (jd *HandleT) removeTableJSONDumps() {
	backupPathDirName := "/rudder-s3-dumps/"
	tmpDirPath, err := misc.CreateTMPDIR()
	jd.assertError(err)
	files, err := filepath.Glob(fmt.Sprintf("%v%v_job*", tmpDirPath+backupPathDirName, jd.tablePrefix))
	jd.assertError(err)
	for _, f := range files {
		err = os.Remove(f)
		jd.assertError(err)
	}
}

// getBackUpQuery individual queries for getting rows in json
func (jd *HandleT) getBackUpQuery(backupDSRange *dataSetRangeT, isJobStatusTable bool, offset int64) string {
	var stmt string
	if jd.BackupSettings.FailedOnly {
		// check failed and aborted state, order the output based on destination, job_id, exec_time
		stmt = fmt.Sprintf(
			`SELECT
				json_build_object(
					'job_id', failed_jobs.job_id,
					'workspace_id',failed_jobs.workspace_id,
					'uuid',failed_jobs.uuid,
					'user_id',failed_jobs.user_id,
					'parameters',failed_jobs.parameters,
					'custom_val',failed_jobs.custom_val,
					'event_payload',failed_jobs.event_payload,
					'event_count',failed_jobs.event_count,
					'created_at',failed_jobs.created_at,
					'expire_at',failed_jobs.expire_at,
					'id',failed_jobs.id,
					'job_id',failed_jobs.status_job_id,
					'job_state',failed_jobs.job_state,
					'attempt',failed_jobs.attempt,
					'exec_time',failed_jobs.exec_time,
					'retry_time',failed_jobs.retry_time,
					'error_code',failed_jobs.error_code,
					'error_response',failed_jobs.error_response,
					'parameters',failed_jobs.status_parameters
				)
			FROM
				(
				SELECT
					*
				FROM
					(
					SELECT *,
					sum(
					pg_column_size(jobs.event_payload)
					) OVER (
					ORDER BY
						jobs.custom_val,
						jobs.status_job_id,
						jobs.exec_time
					) AS running_payload_size,
					ROW_NUMBER()
					OVER (
					ORDER BY
						jobs.custom_val,
						jobs.status_job_id,
						jobs.exec_time
					) AS row_num
					FROM
						(
						SELECT
							job.job_id,
							job.workspace_id,
							job.uuid,
							job.user_id,
							job.parameters,
							job.custom_val,
							job.event_payload,
							job.event_count,
							job.created_at,
							job.expire_at,
							job_status.id,
							job_status.job_id AS status_job_id,
							job_status.job_state,
							job_status.attempt,
							job_status.exec_time,
							job_status.retry_time,
							job_status.error_code,
							job_status.error_response,
							job_status.parameters AS status_parameters
						FROM
							%[1]q "job_status"
							INNER JOIN %[2]q "job" ON job_status.job_id = job.job_id
						WHERE
							job_status.job_state IN ('%[3]s', '%[4]s')
						ORDER BY
						job.custom_val,
							job_status.job_id,
							job_status.exec_time ASC
						LIMIT
							%[5]d
						OFFSET
							%[6]d
						) jobs
					) subquery
				WHERE
					subquery.running_payload_size <= %[7]d OR subquery.row_num = 1
				) AS failed_jobs
		  `, backupDSRange.ds.JobStatusTable, backupDSRange.ds.JobTable, Failed.State, Aborted.State, backupRowsBatchSize, offset, backupMaxTotalPayloadSize)
	} else {
		if isJobStatusTable {
			stmt = fmt.Sprintf(`
			SELECT
			 	json_build_object(
					'id', dump_table.id,
			 		'job_id', dump_table.job_id,
				 	'job_state', dump_table.job_state,
				 	'attempt', dump_table.attempt,
			 		'exec_time', dump_table.exec_time,
			 		'retry_time', dump_table.retry_time,
			 		'error_code', dump_table.error_code,
			 		'error_response', dump_table.error_response,
			 		'parameters', dump_table.parameters
	)
			FROM
				(
				SELECT
					*
				FROM
					%[1]q
				ORDER BY
					job_id ASC
				LIMIT
					%[2]d
				OFFSET
					%[3]d
				)
				AS dump_table
			`, backupDSRange.ds.JobStatusTable, backupRowsBatchSize, offset)
		} else {
			stmt = fmt.Sprintf(`
			SELECT
				jsonb_build_object(
					'job_id', dump_table.job_id,
					'workspace_id', dump_table.workspace_id,
					'uuid', dump_table.uuid,
					'user_id', dump_table.user_id,
					'parameters', dump_table.parameters,
					'custom_val', dump_table.custom_val,
					'event_payload', dump_table.event_payload,
					'event_count', dump_table.event_count,
					'created_at', dump_table.created_at,
					'expire_at', dump_table.expire_at
				)
		  	FROM
				(
				SELECT
					*
				FROM
					(
						SELECT
							*,
							sum(
							pg_column_size(jobs.event_payload)
							) OVER (
							ORDER BY
								jobs.job_id
							) AS running_payload_size,
							ROW_NUMBER()
							OVER (
							ORDER BY
								job_id ASC
							) AS row_num
						FROM
							(
							SELECT
								*
							FROM
								%[1]q job
							ORDER BY
								job_id ASC
							LIMIT
								%[2]d
							OFFSET
								%[3]d
							) jobs
					) subquery
				WHERE
					subquery.running_payload_size <= %[4]d OR subquery.row_num = 1
			) AS dump_table
			`, backupDSRange.ds.JobTable, backupRowsBatchSize, offset, backupMaxTotalPayloadSize)
		}
	}

	return stmt
}

// getFileUploader get a file uploader
func (jd *HandleT) getFileUploader(ctx context.Context) (filemanager.FileManager, error) {
	var err error
	if jd.jobsFileUploader == nil {
		jd.jobsFileUploader, err = filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
			Provider: config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "S3"),
			Config:   filemanager.GetProviderConfigForBackupsFromEnv(ctx),
		})
	}
	return jd.jobsFileUploader, err
}

func isBackupConfigured() bool {
	return config.GetEnv("JOBS_BACKUP_BUCKET", "") != ""
}

func (jd *HandleT) isEmpty(ds dataSetT) bool {
	var count sql.NullInt64
	sqlStatement := fmt.Sprintf(`SELECT count(*) from %q`, ds.JobTable)
	row := jd.dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&count)
	jd.assertError(err)
	if count.Valid {
		return count.Int64 == 0
	}
	panic("Unable to get count on this dataset")
}

// Identifier returns the identifier of the jobsdb. Here it is tablePrefix.
func (jd *HandleT) Identifier() string {
	return jd.tablePrefix
}

// GetTablePrefix returns the table prefix of the jobsdb.
func (jd *HandleT) GetTablePrefix() string {
	return jd.tablePrefix
}

func (jd *HandleT) backupTable(ctx context.Context, backupDSRange *dataSetRangeT, isJobStatusTable bool) error {
	tableFileDumpTimeStat := stats.NewTaggedStat("table_FileDump_TimeStat", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	tableFileDumpTimeStat.Start()
	totalTableDumpTimeStat := stats.NewTaggedStat("total_TableDump_TimeStat", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	totalTableDumpTimeStat.Start()
	var tableName, path, pathPrefix, countStmt string
	backupPathDirName := "/rudder-s3-dumps/"
	tmpDirPath, err := misc.CreateTMPDIR()
	jd.assertError(err)

	// if backupOnlyAborted, process join of aborted rows of jobstatus with jobs table
	// else upload entire jobstatus and jobs table from pre_drop
	if jd.BackupSettings.FailedOnly {
		jd.logger.Info("[JobsDB] :: backupTable: backing up aborted/failed entries")
		tableName = backupDSRange.ds.JobStatusTable
		pathPrefix = strings.TrimPrefix(tableName, preDropTablePrefix)
		path = fmt.Sprintf(`%v%v_%v.gz`, tmpDirPath+backupPathDirName, pathPrefix, Aborted.State)
		// checked failed and aborted state
		countStmt = fmt.Sprintf(`SELECT COUNT(*) from %q where job_state in ('%s', '%s')`, tableName, Failed.State, Aborted.State)
	} else {
		if isJobStatusTable {
			tableName = backupDSRange.ds.JobStatusTable
			pathPrefix = strings.TrimPrefix(tableName, preDropTablePrefix)
			path = fmt.Sprintf(`%v%v.gz`, tmpDirPath+backupPathDirName, pathPrefix)
			countStmt = fmt.Sprintf(`SELECT COUNT(*) from %q`, tableName)
		} else {
			tableName = backupDSRange.ds.JobTable
			pathPrefix = strings.TrimPrefix(tableName, preDropTablePrefix)
			path = fmt.Sprintf(`%v%v.%v.%v.%v.%v.gz`,
				tmpDirPath+backupPathDirName,
				pathPrefix,
				backupDSRange.minJobID,
				backupDSRange.maxJobID,
				backupDSRange.startTime,
				backupDSRange.endTime,
			)
			countStmt = fmt.Sprintf(`SELECT COUNT(*) from %q`, tableName)
		}
	}

	jd.logger.Infof("[JobsDB] :: Backing up table: %v", tableName)

	var totalCount int64
	err = jd.dbHandle.QueryRow(countStmt).Scan(&totalCount)
	if err != nil {
		panic(err)
	}

	// return without doing anything as no jobs not present in ds
	if totalCount == 0 {
		//  Do not record stat for this case?
		jd.logger.Infof("[JobsDB] ::  not processiong table dump as no rows match criteria. %v", tableName)
		return nil
	}

	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		panic(err)
	}

	gzWriter, err := misc.CreateGZ(path)
	if err != nil {
		return fmt.Errorf("creating gz file %q: %w", path, err)
	}
	defer os.Remove(path)
	var offset int64

	for {
		stmt := jd.getBackUpQuery(backupDSRange, isJobStatusTable, offset)
		var rawJSONRows json.RawMessage
		rows, err := jd.dbHandle.Query(stmt)
		jd.assertError(err)
		defer rows.Close()

		for rows.Next() {
			err = rows.Scan(&rawJSONRows)
			if err != nil {
				panic(fmt.Errorf("Scanning row failed with error : %w", err))
			}
			rawJSONRows = append(rawJSONRows, '\n') // appending '\n'
			_, err = gzWriter.Write(rawJSONRows)
			if err != nil {
				return fmt.Errorf("writing gz file %q: %w", path, err)
			}
			offset++
		}

		if offset >= totalCount {
			break
		}
	}

	if err := gzWriter.CloseGZ(); err != nil {
		return fmt.Errorf("closing gz file %q: %w", path, err)
	}
	tableFileDumpTimeStat.End()

	fileUploadTimeStat := stats.NewTaggedStat("fileUpload_TimeStat", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	fileUploadTimeStat.Start()
	file, err := os.Open(path)
	jd.assertError(err)
	defer file.Close()

	pathPrefixes := make([]string, 0)
	// For empty path prefix, don't need to add anything to the array
	if jd.BackupSettings.PathPrefix != "" {
		pathPrefixes = append(pathPrefixes, jd.BackupSettings.PathPrefix, config.GetEnv("INSTANCE_ID", "1"))
	} else {
		pathPrefixes = append(pathPrefixes, config.GetEnv("INSTANCE_ID", "1"))
	}

	jd.logger.Infof("[JobsDB] :: Uploading backup table to object storage: %v", tableName)
	var output filemanager.UploadOutput
	output, err = jd.backupUploadWithExponentialBackoff(ctx, file, pathPrefixes...)
	if err != nil {
		storageProvider := config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "S3")
		jd.logger.Errorf("[JobsDB] :: Failed to upload table %v dump to %s. Error: %s", tableName, storageProvider, err.Error())
		return err
	}
	// Do not record stat in error case as error case time might be low and skew stats
	fileUploadTimeStat.End()
	totalTableDumpTimeStat.End()
	jd.logger.Infof("[JobsDB] :: Backed up table: %v at %v", tableName, output.Location)
	return nil
}

func (jd *HandleT) backupUploadWithExponentialBackoff(ctx context.Context, file *os.File, pathPrefixes ...string) (filemanager.UploadOutput, error) {
	// get a file uploader
	fileUploader, err := jd.getFileUploader(ctx)
	if err != nil {
		return filemanager.UploadOutput{}, err
	}
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = time.Minute
	bo.MaxElapsedTime = jd.maxBackupRetryTime
	boCtx := backoff.WithContext(bo, ctx)

	var output filemanager.UploadOutput
	backup := func() error {
		output, err = fileUploader.Upload(ctx, file, pathPrefixes...)
		return err
	}

	err = backoff.Retry(backup, boCtx)
	return output, err
}

func (jd *HandleT) getBackupDSRange() *dataSetRangeT {
	var backupDS dataSetT
	var backupDSRange dataSetRangeT

	// Read the table names from PG
	tableNames := mustGetAllTableNames(jd, jd.dbHandle)

	// We check for job_status because that is renamed after job
	dnumList := []string{}
	for _, t := range tableNames {
		if strings.HasPrefix(t, preDropTablePrefix+jd.tablePrefix+"_jobs_") {
			dnum := t[len(preDropTablePrefix+jd.tablePrefix+"_jobs_"):]
			dnumList = append(dnumList, dnum)
			continue
		}
	}
	if len(dnumList) == 0 {
		return &backupDSRange
	}

	sortDnumList(jd, dnumList)

	backupDS = dataSetT{
		JobTable:       fmt.Sprintf("%s%s_jobs_%s", preDropTablePrefix, jd.tablePrefix, dnumList[0]),
		JobStatusTable: fmt.Sprintf("%s%s_job_status_%s", preDropTablePrefix, jd.tablePrefix, dnumList[0]),
		Index:          dnumList[0],
	}

	var minID, maxID sql.NullInt64
	jobIDSQLStatement := fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) from %q`, backupDS.JobTable)
	row := jd.dbHandle.QueryRow(jobIDSQLStatement)
	err := row.Scan(&minID, &maxID)
	jd.assertError(err)

	var minCreatedAt, maxCreatedAt time.Time
	jobTimeSQLStatement := fmt.Sprintf(`SELECT MIN(created_at), MAX(created_at) from %q`, backupDS.JobTable)
	row = jd.dbHandle.QueryRow(jobTimeSQLStatement)
	err = row.Scan(&minCreatedAt, &maxCreatedAt)
	jd.assertError(err)

	backupDSRange = dataSetRangeT{
		minJobID:  minID.Int64,
		maxJobID:  maxID.Int64,
		startTime: minCreatedAt.UnixNano() / int64(time.Millisecond),
		endTime:   maxCreatedAt.UnixNano() / int64(time.Millisecond),
		ds:        backupDS,
	}
	return &backupDSRange
}

/*
We keep a journal of all the operations. The journal helps
*/
const (
	addDSOperation             = "ADD_DS"
	migrateCopyOperation       = "MIGRATE_COPY"
	migrateImportOperation     = "MIGRATE_IMPORT"
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

func (jd *HandleT) JournalMarkStart(opType string, opPayload json.RawMessage) int64 {
	var opID int64
	err := jd.WithTx(func(tx *sql.Tx) error {
		var err error
		opID, err = jd.JournalMarkStartInTx(tx, opType, opPayload)
		return err
	})
	jd.assertError(err)
	return opID
}

func (jd *HandleT) JournalMarkStartInTx(tx *sql.Tx, opType string, opPayload json.RawMessage) (int64, error) {
	var opID int64
	jd.assert(opType == addDSOperation ||
		opType == migrateCopyOperation ||
		opType == migrateImportOperation ||
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
func (jd *HandleT) JournalMarkDone(opID int64) {
	err := jd.WithTx(func(tx *sql.Tx) error {
		return jd.journalMarkDoneInTx(tx, opID)
	})
	jd.assertError(err)
}

// JournalMarkDoneInTx marks the end of a journal action in a transaction
func (jd *HandleT) journalMarkDoneInTx(tx *sql.Tx, opID int64) error {
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
	defer stmt.Close()

	rows, err := stmt.Query()
	jd.assertError(err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		entries = append(entries, JournalEntryT{})
		err = rows.Scan(&entries[count].OpID, &entries[count].OpType, &entries[count].OpDone, &entries[count].OpPayload)
		jd.assertError(err)
		count++
	}
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
	case migratorRoutine:
		opTypes = []string{migrateImportOperation}
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
	defer stmt.Close()

	rows, err := stmt.Query(pq.Array(opTypes))
	jd.assertError(err)
	defer rows.Close()

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
	jd.assert(count <= 1, fmt.Sprintf("count:%d > 1", count))

	if count == 0 {
		// Nothing to recoer
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
	case migrateImportOperation:
		jd.assert(db.IsValidMigrationMode(jd.migrationState.migrationMode), "If migration mode is not valid, then this operation shouldn't have been unfinished. Go debug")
		var importDest dataSetT
		jd.assertError(json.Unmarshal(opPayload, &importDest))
		jd.dropDSForRecovery(importDest)
		jd.deleteSetupCheckpoint(ImportOp)
		undoOp = true
	case postMigrateDSOperation:
		// Some of the source datasets would have been
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
		// Some of the source datasets would have been
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
	migratorRoutine = "migrator"
)

func (jd *HandleT) recoverFromJournal(owner OwnerType) {
	jd.recoverFromCrash(owner, addDSGoRoutine)
	jd.recoverFromCrash(owner, mainGoRoutine)
	jd.recoverFromCrash(owner, backupGoRoutine)
}

// RecoverFromMigrationJournal is an exposed function for migrator package to handle journal crashes during migration
func (jd *HandleT) RecoverFromMigrationJournal() {
	jd.recoverFromCrash(Write, migratorRoutine)
	jd.recoverFromCrash(ReadWrite, migratorRoutine)
}

func (jd *HandleT) UpdateJobStatus(ctx context.Context, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error {
	return jd.WithUpdateSafeTx(func(tx UpdateSafeTx) error {
		return jd.UpdateJobStatusInTx(ctx, tx, statusList, customValFilters, parameterFilters)
	})
}

/*
internalUpdateJobStatusInTx updates the status of a batch of jobs
customValFilters[] is passed so we can efficinetly mark empty cache
Later we can move this to query
*/
func (jd *HandleT) internalUpdateJobStatusInTx(ctx context.Context, tx *sql.Tx, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error {
	// capture stats
	tags := statTags{CustomValFilters: customValFilters, ParameterFilters: parameterFilters}
	queryStat := jd.getTimerStat("update_job_status_time", &tags)
	queryStat.Start()
	defer queryStat.End()

	// do update
	updatedStatesByDS, err := jd.doUpdateJobStatusInTx(ctx, tx, statusList, tags)
	if err != nil {
		jd.logger.Infof("[[ %s ]]: Error occurred while updating job statuses. Returning err, %v", jd.tablePrefix, err)
		return err
	}

	// clear cache
	for ds, stateListByWorkspace := range updatedStatesByDS {
		allUpdatedStates := make([]string, 0)
		for workspace, stateList := range stateListByWorkspace {
			jd.markClearEmptyResult(ds, workspace, stateList, customValFilters, parameterFilters, hasJobs, nil)
			allUpdatedStates = append(allUpdatedStates, stateList...)
		}
		// NOTE: Along with clearing cache for a particular workspace key, we also have to clear for allWorkspaces key
		jd.markClearEmptyResult(ds, allWorkspaces, misc.Unique(allUpdatedStates), customValFilters, parameterFilters, hasJobs, nil)
	}

	return nil
}

/*
doUpdateJobStatusInTx updates the status of a batch of jobs
customValFilters[] is passed so we can efficinetly mark empty cache
Later we can move this to query
*/
func (jd *HandleT) doUpdateJobStatusInTx(ctx context.Context, tx *sql.Tx, statusList []*JobStatusT, tags statTags) (updatedStatesByDS map[dataSetT]map[string][]string, err error) {
	if len(statusList) == 0 {
		return
	}

	// First we sort by JobID
	sort.Slice(statusList, func(i, j int) bool {
		return statusList[i].JobID < statusList[j].JobID
	})

	// We scan through the list of jobs and map them to DS
	var lastPos int
	dsRangeList := jd.getDSRangeList()
	updatedStatesByDS = make(map[dataSetT]map[string][]string)
	for _, ds := range dsRangeList {
		minID := ds.minJobID
		maxID := ds.maxJobID
		// We have processed upto (but excluding) lastPos on statusList.
		// Hence that element must lie in this or subsequent dataset's
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
				var updatedStates map[string][]string
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
			var updatedStates map[string][]string
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
		dsList := jd.getDSList()
		jd.assert(len(dsRangeList) >= len(dsList)-2, fmt.Sprintf("len(dsRangeList):%d < len(dsList):%d-2", len(dsRangeList), len(dsList)))
		// Update status in the last element
		jd.logger.Debug("RangeEnd", statusList[lastPos].JobID, lastPos, len(statusList))
		var updatedStates map[string][]string
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
	return jd.WithStoreSafeTx(func(tx StoreSafeTx) error {
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
		return jd.inStoreSafeCtx(storeCmd)
	}
	return storeCmd()
}

func (jd *HandleT) StoreWithRetryEach(ctx context.Context, jobList []*JobT) map[uuid.UUID]string {
	var res map[uuid.UUID]string
	_ = jd.WithStoreSafeTx(func(tx StoreSafeTx) error {
		res = jd.StoreWithRetryEachInTx(ctx, tx, jobList)
		return nil
	})
	return res
}

func (jd *HandleT) StoreWithRetryEachInTx(ctx context.Context, tx StoreSafeTx, jobList []*JobT) map[uuid.UUID]string {
	var res map[uuid.UUID]string
	storeCmd := func() error {
		command := func() interface{} {
			dsList := jd.getDSList()
			res = jd.internalStoreWithRetryEachInTx(ctx, tx.Tx(), dsList[len(dsList)-1], jobList)
			return res
		}
		res, _ = jd.executeDbRequest(newWriteDbRequest("store_retry_each", nil, command)).(map[uuid.UUID]string)
		return nil
	}

	if tx.storeSafeTxIdentifier() != jd.Identifier() {
		_ = jd.inStoreSafeCtx(storeCmd)
		return res
	}
	_ = storeCmd()
	return res
}

/*
printLists is a debuggging function used to print
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

	tags := statTags{CustomValFilters: params.CustomValFilters, ParameterFilters: params.ParameterFilters}
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

	tags := statTags{CustomValFilters: params.CustomValFilters, ParameterFilters: params.ParameterFilters}
	queryStat := jd.getTimerStat("unprocessed_jobs_time", &tags)
	queryStat.Start()
	defer queryStat.End()

	// The order of lock is very important. The migrateDSLoop
	// takes lock in this order so reversing this will cause
	// deadlocks
	jd.dsMigrationLock.RLock()
	jd.dsListLock.RLock()
	defer jd.dsMigrationLock.RUnlock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList()

	limitByEventCount := false
	if params.EventsLimit > 0 {
		limitByEventCount = true
	}

	limitByPayloadSize := false
	if params.PayloadSizeLimit > 0 {
		limitByPayloadSize = true
	}

	var completeUnprocessedJobs JobsResult
	for _, ds := range dsList {
		unprocessedJobs, err := jd.getUnprocessedJobsDS(ctx, ds, true, params)
		if err != nil {
			return JobsResult{}, err
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
	// Release lock
	return completeUnprocessedJobs, nil
}

func (jd *HandleT) GetImporting(ctx context.Context, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	if params.JobsLimit == 0 {
		return JobsResult{}, nil
	}
	params.StateFilters = []string{Importing.State}
	tags := statTags{CustomValFilters: params.CustomValFilters, StateFilters: params.StateFilters, ParameterFilters: params.ParameterFilters}
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
deleteJobStatus deletes the latest status of a batch of jobs
This is only done during recovery, which happens during the server start.
So, we don't have to worry about dsEmptyResultCache
*/
func (jd *HandleT) deleteJobStatus(conditions QueryConditions) {
	tx, err := jd.dbHandle.Begin()
	jd.assertError(err)

	err = jd.deleteJobStatusInTx(tx, conditions)
	jd.assertErrorAndRollbackTx(err, tx)

	err = tx.Commit()
	jd.assertError(err)
}

/*
if count passed is less than 0, then delete happens on the entire dsList;
deleteJobStatusInTx deletes the latest status of a batch of jobs
*/
func (jd *HandleT) deleteJobStatusInTx(txHandler transactionHandler, conditions QueryConditions) error {
	tags := statTags{CustomValFilters: conditions.CustomValFilters, StateFilters: conditions.StateFilters, ParameterFilters: conditions.ParameterFilters}
	queryStat := jd.getTimerStat("delete_job_status_time", &tags)
	queryStat.Start()
	defer queryStat.End()

	// The order of lock is very important. The migrateDSLoop
	// takes lock in this order so reversing this will cause
	// deadlocks
	jd.dsMigrationLock.RLock()
	jd.dsListLock.RLock()
	defer jd.dsMigrationLock.RUnlock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList()

	totalDeletedCount := 0
	for _, ds := range dsList {
		deletedCount, err := jd.deleteJobStatusDSInTx(txHandler, ds, conditions)
		if err != nil {
			return err
		}
		totalDeletedCount += deletedCount
	}

	return nil
}

/*
stateFilters and customValFilters do a OR query on values passed in array
parameterFilters do a AND query on values included in the map
*/
func (jd *HandleT) deleteJobStatusDSInTx(txHandler transactionHandler, ds dataSetT, conditions QueryConditions) (int, error) {
	stateFilters := conditions.StateFilters
	customValFilters := conditions.CustomValFilters
	parameterFilters := conditions.ParameterFilters

	checkValidJobState(jd, stateFilters)

	tags := statTags{CustomValFilters: conditions.CustomValFilters, StateFilters: conditions.StateFilters, ParameterFilters: conditions.ParameterFilters}
	queryStat := jd.getTimerStat("delete_job_status_ds_time", &tags)
	queryStat.Start()
	defer queryStat.End()

	var stateQuery, customValQuery, sourceQuery string

	if len(stateFilters) > 0 {
		stateQuery = " AND " + constructQueryOR("job_state", stateFilters)
	} else {
		stateQuery = ""
	}
	if len(customValFilters) > 0 {
		customValQuery = " WHERE " +
			constructQueryOR(fmt.Sprintf(`%q.custom_val`, ds.JobTable), customValFilters)
	} else {
		customValQuery = ""
	}

	if customValQuery == "" {
		sourceQuery += " WHERE "
	} else {
		sourceQuery += " AND "
	}

	if len(parameterFilters) > 0 {
		sourceQuery += constructParameterJSONQuery(ds.JobTable, parameterFilters)
	} else {
		sourceQuery = ""
	}

	var sqlStatement string
	if customValQuery == "" && sourceQuery == "" {
		sqlStatement = fmt.Sprintf(`DELETE FROM %[1]q WHERE id IN
                                                   (SELECT MAX(id) from %[1]q GROUP BY job_id) %[2]s
                                             AND retry_time < $1`,
			ds.JobStatusTable, stateQuery)
	} else {
		sqlStatement = fmt.Sprintf(`DELETE FROM %[1]q WHERE id IN
                                                   (SELECT MAX(id) from %[1]q where job_id IN (SELECT job_id from %[2]q %[4]s %[5]s) GROUP BY job_id) %[3]s
                                             AND retry_time < $1`,
			ds.JobStatusTable, ds.JobTable, stateQuery, customValQuery, sourceQuery)
	}

	stmt, err := txHandler.Prepare(sqlStatement)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	res, err := stmt.Exec(getTimeNowFunc())
	if err != nil {
		return 0, err
	}
	deleteCount, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return int(deleteCount), nil
}

/*
GetProcessed returns events of a given state. This does not update any state itself and
realises on the caller to update it. That means that successive calls to GetProcessed("failed")
can return the same set of events. It is the responsibility of the caller to call it from
one thread, update the state (to "waiting") in the same thread and pass on the the processors
*/
func (jd *HandleT) GetProcessed(ctx context.Context, params GetQueryParamsT) (JobsResult, error) { // skipcq: CRT-P0003
	if params.JobsLimit <= 0 {
		return JobsResult{}, nil
	}

	tags := statTags{CustomValFilters: params.CustomValFilters, StateFilters: params.StateFilters, ParameterFilters: params.ParameterFilters}
	queryStat := jd.getTimerStat("processed_jobs_time", &tags)
	queryStat.Start()
	defer queryStat.End()

	// The order of lock is very important. The migrateDSLoop
	// takes lock in this order so reversing this will cause
	// deadlocks
	jd.dsMigrationLock.RLock()
	jd.dsListLock.RLock()
	defer jd.dsMigrationLock.RUnlock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList()

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
	for _, ds := range dsList {
		processedJobs, err := jd.getProcessedJobsDS(ctx, ds, false, params)
		if err != nil {
			return JobsResult{}, err
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
	tags := statTags{CustomValFilters: params.CustomValFilters, StateFilters: params.StateFilters, ParameterFilters: params.ParameterFilters}
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
	tags := statTags{CustomValFilters: params.CustomValFilters, StateFilters: params.StateFilters, ParameterFilters: params.ParameterFilters}
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
	tags := statTags{CustomValFilters: params.CustomValFilters, StateFilters: params.StateFilters, ParameterFilters: params.ParameterFilters}
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

/*
DeleteExecuting deletes events whose latest job state is executing.
This is only done during recovery, which happens during the server start.
*/
func (jd *HandleT) DeleteExecuting() {
	conditions := QueryConditions{
		StateFilters: []string{Executing.State},
	}
	tags := statTags{CustomValFilters: conditions.CustomValFilters, StateFilters: conditions.StateFilters, ParameterFilters: conditions.ParameterFilters}
	command := func() interface{} {
		jd.deleteJobStatus(conditions)
		return nil
	}
	_ = jd.executeDbRequest(newWriteDbRequest("delete_job_status", &tags, command))
}

/*
Ping returns health check for pg database
*/
func (jd *HandleT) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := jd.dbHandle.QueryContext(ctx, `SELECT 'Rudder DB Health Check'::text as message`)
	if err != nil {
		return err
	}
	defer rows.Close()
	return nil
}

func (jd *HandleT) GetLastJobID() int64 {
	jd.dsListLock.RLock()
	dsList := jd.getDSList()
	jd.dsListLock.RUnlock()
	return jd.GetMaxIDForDs(dsList[len(dsList)-1])
}

func (jd *HandleT) GetLastJob() *JobT {
	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()
	dsList := jd.getDSList()
	maxID := jd.GetMaxIDForDs(dsList[len(dsList)-1])

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

func QueryJobsWithRetries(parentContext context.Context, timeout time.Duration, maxAttempts int, f func(ctx context.Context) ([]*JobT, error)) ([]*JobT, error) {
	res, err := misc.QueryWithRetries(parentContext, timeout, maxAttempts, func(ctx context.Context) (interface{}, error) {
		return f(ctx)
	})
	if err != nil {
		return nil, err
	}
	return res.([]*JobT), err
}

func QueryJobsResultWithRetries(parentContext context.Context, timeout time.Duration, maxAttempts int, f func(ctx context.Context) (JobsResult, error)) (JobsResult, error) {
	res, err := misc.QueryWithRetries(parentContext, timeout, maxAttempts, func(ctx context.Context) (interface{}, error) {
		return f(ctx)
	})
	if err != nil {
		return JobsResult{}, err
	}
	return res.(JobsResult), err
}

func QueryWorkspacePileupWithRetries(parentContext context.Context, timeout time.Duration, maxAttempts int, f func(ctx context.Context) (map[string]map[string]int, error)) (map[string]map[string]int, error) {
	res, err := misc.QueryWithRetries(parentContext, timeout, maxAttempts, func(ctx context.Context) (interface{}, error) {
		return f(ctx)
	})
	if err != nil {
		return nil, err
	}
	return res.(map[string]map[string]int), err
}
