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
operations in DB. Instead, we just use WRITE (append)  and DELETE TABLE (deleting a file)
operations which are fast.
Also, keeping each dataset small (enough to cache in memory) ensures that reads are
mostly serviced from memory cache.
*/

package jobsdb

//go:generate mockgen -destination=../mocks/jobsdb/mock_jobsdb.go -package=mocks_jobsdb github.com/rudderlabs/rudder-server/jobsdb JobsDB

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"unicode/utf8"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/utils/logger"

	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/db"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/lib/pq"
	uuid "github.com/satori/go.uuid"
)

// BackupSettingsT is for capturing the backup
// configuration from the config/env files to
// instantiate jobdb correctly
type BackupSettingsT struct {
	BackupEnabled bool
	FailedOnly    bool
	PathPrefix    string
}

//GetQueryParamsT is a struct to hold jobsdb query params
type GetQueryParamsT struct {
	CustomValFilters              []string
	ParameterFilters              []ParameterFilterT
	StateFilters                  []string
	Count                         int
	IgnoreCustomValFiltersInQuery bool
}

/*
JobsDB interface contains public methods to access JobsDB data
*/
type JobsDB interface {
	Store(jobList []*JobT)
	StoreWithRetryEach(jobList []*JobT) map[uuid.UUID]string
	CheckPGHealth() bool
	UpdateJobStatus(statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error

	GetToRetry(params GetQueryParamsT) []*JobT
	GetUnprocessed(params GetQueryParamsT) []*JobT
	GetExecuting(params GetQueryParamsT) []*JobT

	Status() interface{}
}

/*
AssertInterface contains public assert methods
*/
type AssertInterface interface {
	assert(cond bool, errorString string)
	assertError(err error)
}

/*
JobStatusT is used for storing status of the job. It is
the responsibility of the user of this module to set appropriate
job status. State can be one of
ENUM waiting, executing, succeeded, waiting_retry,  failed, aborted
*/
type JobStatusT struct {
	JobID         int64           `json:"JobID"`
	JobState      string          `json:"JobState"` //ENUM waiting, executing, succeeded, waiting_retry,  failed, aborted, migrating, migrated, wont_migrate
	AttemptNum    int             `json:"AttemptNum"`
	ExecTime      time.Time       `json:"ExecTime"`
	RetryTime     time.Time       `json:"RetryTime"`
	ErrorCode     string          `json:"ErrorCode"`
	ErrorResponse json.RawMessage `json:"ErrorResponse"`
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
	EventPayload  json.RawMessage `json:"EventPayload"`
	LastJobStatus JobStatusT      `json:"LastJobStatus"`
	Parameters    json.RawMessage `json:"Parameters"`
}

//The struct fields need to be exposed to JSON package
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

//MigrationState maintains the state required during the migration process
type MigrationState struct {
	dsForNewEvents             dataSetT
	dsForImport                dataSetT
	lastDsForExport            dataSetT
	importLock                 sync.RWMutex
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
	tablePrefix                   string
	datasetList                   []dataSetT
	datasetRangeList              []dataSetRangeT
	dsListLock                    sync.RWMutex
	dsMigrationLock               sync.RWMutex
	dsRetentionPeriod             time.Duration
	dsEmptyResultCache            map[dataSetT]map[string]map[string]map[string]cacheValue
	dsCacheLock                   sync.Mutex
	BackupSettings                *BackupSettingsT
	jobsFileUploader              filemanager.FileManager
	statTableCount                stats.RudderStats
	statNewDSPeriod               stats.RudderStats
	isStatNewDSPeriodInitialized  bool
	statDropDSPeriod              stats.RudderStats
	isStatDropDSPeriodInitialized bool
	migrationState                MigrationState
	inProgressMigrationTargetDS   *dataSetT
	logger                        logger.LoggerI
	ownerType                     OwnerType
}

//The struct which is written to the journal
type journalOpPayloadT struct {
	From []dataSetT `json:"from"`
	To   dataSetT   `json:"to"`
}

type StoreJobRespT struct {
	JobID        int64
	ErrorMessage string
}

type ParameterFilterT struct {
	Name     string
	Value    string
	Optional bool
}

var dbErrorMap = map[string]string{
	"Invalid JSON":             "22P02",
	"Invalid Unicode":          "22P05",
	"Invalid Escape Sequence":  "22025",
	"Invalid Escape Character": "22019",
}

// return backup settings depending on jobdb type
// the gateway, the router and the processor
// BackupEnabled = true => all the jobsdb are eligible for backup
// instanceBackupEnabled = true => the individual jobsdb too is eligible for backup
// instanceBackupFailedAndAborted = true => the individual jobdb backsup failed and aborted jobs only
// pathPrefix = by default is the jobsdb table prefix, is the path appended before instanceID in s3 folder structure
func (jd *HandleT) getBackUpSettings() *BackupSettingsT {
	masterBackupEnabled := config.GetBool("JobsDB.backup.enabled", false)
	instanceBackupEnabled := config.GetBool(fmt.Sprintf("JobsDB.backup.%v.enabled", jd.tablePrefix), false)
	instanceBackupFailedAndAborted := config.GetBool(fmt.Sprintf("JobsDB.backup.%v.failedOnly", jd.tablePrefix), false)
	pathPrefix := config.GetString(fmt.Sprintf("JobsDB.backup.%v.pathPrefix", jd.tablePrefix), jd.tablePrefix)

	backupSettings := BackupSettingsT{BackupEnabled: masterBackupEnabled && instanceBackupEnabled,
		FailedOnly: instanceBackupFailedAndAborted, PathPrefix: strings.TrimSpace(pathPrefix)}

	return &backupSettings
}

//Some helper functions
func (jd *HandleT) assertError(err error) {
	if err != nil {
		jd.printLists(true)
		jd.logger.Fatal(jd.dsEmptyResultCache)
		panic(err)
	}
}

func (jd *HandleT) assertErrorAndRollbackTx(err error, tx *sql.Tx) {
	if err != nil {
		tx.Rollback()
		jd.printLists(true)
		jd.logger.Fatal(jd.dsEmptyResultCache)
		panic(err)
	}
}

func (jd *HandleT) rollbackTx(err error, tx *sql.Tx) {
	if err != nil {
		tx.Rollback()
		jd.printLists(true)
		jd.logger.Fatal(jd.dsEmptyResultCache)
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
		"dataset-list":    jd.getDSList(false),
		"dataset-ranges":  jd.getDSRangeList(false),
		"backups-enabled": jd.BackupSettings.BackupEnabled,
	}
	emptyResults := make(map[string]interface{})
	for ds, entry := range jd.dsEmptyResultCache {
		emptyResults[ds.JobTable] = entry
	}
	statusObj["empty-results-cache"] = emptyResults

	if db.IsValidMigrationMode(jd.migrationState.migrationMode) {
		statusObj["migration-state"] = jd.migrationState
	}
	return statusObj
}

type jobStateT struct {
	isValid    bool
	isTerminal bool
	State      string
}

//State definitions
var (
	//Not valid, Not terminal
	NotProcessed = jobStateT{isValid: false, isTerminal: false, State: "not_picked_yet"}

	//Valid, Not terminal
	Failed       = jobStateT{isValid: true, isTerminal: false, State: "failed"}
	Executing    = jobStateT{isValid: true, isTerminal: false, State: "executing"}
	Waiting      = jobStateT{isValid: true, isTerminal: false, State: "waiting"}
	WaitingRetry = jobStateT{isValid: true, isTerminal: false, State: "waiting_retry"}
	Migrating    = jobStateT{isValid: true, isTerminal: false, State: "migrating"}
	Throttled    = jobStateT{isValid: true, isTerminal: false, State: "throttled"}

	//Valid, Terminal
	Succeeded   = jobStateT{isValid: true, isTerminal: true, State: "succeeded"}
	Aborted     = jobStateT{isValid: true, isTerminal: true, State: "aborted"}
	Migrated    = jobStateT{isValid: true, isTerminal: true, State: "migrated"}
	WontMigrate = jobStateT{isValid: true, isTerminal: true, State: "wont_migrate"}
)

//Adding a new state to this list, will require an enum change in postgres db.
var jobStates []jobStateT = []jobStateT{
	NotProcessed,
	Failed,
	Executing,
	Waiting,
	WaitingRetry,
	Throttled,
	Migrating,
	Succeeded,
	Aborted,
	Migrated,
	WontMigrate,
}

//OwnerType for this jobsdb instance
type OwnerType string

const (
	//Read : Only Reader of this jobsdb instance
	Read OwnerType = "READ"
	//Write : Only Writer of this jobsdb instance
	Write OwnerType = "WRITE"
	//ReadWrite : Reader and Writer of this jobsdb instance
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
	host, user, password, dbname, sslmode string
	port                                  int
)

var (
	maxDSSize, maxMigrateOnce, maxMigrateDSProbe int
	maxTableSize                                 int64
	jobDoneMigrateThres, jobStatusMigrateThres   float64
	migrateDSLoopSleepDuration                   time.Duration
	addNewDSLoopSleepDuration                    time.Duration
	refreshDSListLoopSleepDuration               time.Duration
	backupCheckSleepDuration                     time.Duration
	useJoinForUnprocessed                        bool
	backupRowsBatchSize                          int64
	pkgLogger                                    logger.LoggerI
)

//Different scenarios for addNewDS
const (
	appendToDsList     = "appendToDsList"
	insertForMigration = "insertForMigration"
	insertForImport    = "insertForImport"
)

// Loads db config and migration related config from config file
func loadConfig() {
	host = config.GetEnv("JOBS_DB_HOST", "localhost")
	user = config.GetEnv("JOBS_DB_USER", "ubuntu")
	dbname = config.GetEnv("JOBS_DB_DB_NAME", "ubuntu")
	port, _ = strconv.Atoi(config.GetEnv("JOBS_DB_PORT", "5432"))
	password = config.GetEnv("JOBS_DB_PASSWORD", "ubuntu") // Reading secrets from
	sslmode = config.GetEnv("JOBS_DB_SSL_MODE", "disable")

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
	jobDoneMigrateThres = config.GetFloat64("JobsDB.jobDoneMigrateThres", 0.8)
	jobStatusMigrateThres = config.GetFloat64("JobsDB.jobStatusMigrateThres", 5)
	maxDSSize = config.GetInt("JobsDB.maxDSSize", 100000)
	maxMigrateOnce = config.GetInt("JobsDB.maxMigrateOnce", 10)
	maxMigrateDSProbe = config.GetInt("JobsDB.maxMigrateDSProbe", 10)
	maxTableSize = (config.GetInt64("JobsDB.maxTableSizeInMB", 300) * 1000000)
	backupRowsBatchSize = config.GetInt64("JobsDB.backupRowsBatchSize", 1000)
	migrateDSLoopSleepDuration = (config.GetDuration("JobsDB.migrateDSLoopSleepDurationInS", time.Duration(30)) * time.Second)
	addNewDSLoopSleepDuration = (config.GetDuration("JobsDB.addNewDSLoopSleepDurationInS", time.Duration(5)) * time.Second)
	refreshDSListLoopSleepDuration = (config.GetDuration("JobsDB.refreshDSListLoopSleepDurationInS", time.Duration(5)) * time.Second)
	backupCheckSleepDuration = (config.GetDuration("JobsDB.backupCheckSleepDurationIns", time.Duration(2)) * time.Second)
	useJoinForUnprocessed = config.GetBool("JobsDB.useJoinForUnprocessed", true)

}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("jobsdb")
}

// GetConnectionString Returns Jobs DB connection configuration
func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)

}

/*
Setup is used to initialize the HandleT structure.
clearAll = True means it will remove all existing tables
tablePrefix must be unique and is used to separate
multiple users of JobsDB
dsRetentionPeriod = A DS is not deleted if it has some activity
in the retention time
*/
func (jd *HandleT) Setup(ownerType OwnerType, clearAll bool, tablePrefix string, retentionPeriod time.Duration, migrationMode string, registerStatusHandler bool) {

	jd.ownerType = ownerType
	jd.logger = pkgLogger.Child(tablePrefix)
	var err error
	jd.migrationState.migrationMode = migrationMode
	psqlInfo := GetConnectionString()
	jd.assert(tablePrefix != "", "tablePrefix received is empty")
	jd.tablePrefix = tablePrefix
	jd.dsRetentionPeriod = retentionPeriod
	jd.dsEmptyResultCache = map[dataSetT]map[string]map[string]map[string]cacheValue{}
	if registerStatusHandler {
		admin.RegisterStatusHandler(tablePrefix+"-jobsdb", jd)
	}

	jd.BackupSettings = jd.getBackUpSettings()

	jd.dbHandle, err = sql.Open("postgres", psqlInfo)
	jd.assertError(err)

	err = jd.dbHandle.Ping()
	jd.assertError(err)

	jd.logger.Infof("Connected to %s DB", tablePrefix)

	jd.statTableCount = stats.NewStat(fmt.Sprintf("jobsdb.%s_tables_count", jd.tablePrefix), stats.GaugeType)
	jd.statNewDSPeriod = stats.NewStat(fmt.Sprintf("jobsdb.%s_new_ds_period", jd.tablePrefix), stats.TimerType)
	jd.statDropDSPeriod = stats.NewStat(fmt.Sprintf("jobsdb.%s_drop_ds_period", jd.tablePrefix), stats.TimerType)

	switch ownerType {
	case Read:
		jd.readerSetup()
	case Write:
		jd.setupDatabaseTables(clearAll)
		jd.writerSetup()
	case ReadWrite:
		jd.setupDatabaseTables(clearAll)
		jd.readerWriterSetup()
	}
}

func (jd *HandleT) startBackupDSLoop() {
	var err error
	if jd.BackupSettings.BackupEnabled {
		jd.jobsFileUploader, err = jd.getFileUploader()
		jd.assertError(err)
		rruntime.Go(func() {
			jd.backupDSLoop()
		})
	}

}

func (jd *HandleT) startMigrateDSLoop() {
	rruntime.Go(func() {
		jd.migrateDSLoop()
	})
}

func (jd *HandleT) readerSetup() {
	jd.recoverFromJournal(Read)

	//This is a thread-safe operation.
	//Even if two different services (gateway and processor) perform this operation, there should not be any problem.
	jd.recoverFromJournal(ReadWrite)

	//Refresh in memory list. We don't take lock
	//here because this is called before anything
	//else
	jd.getDSList(true)
	jd.getDSRangeList(true)

	rruntime.Go(func() {
		jd.refreshDSListLoop()
	})

	jd.startBackupDSLoop()
	jd.startMigrateDSLoop()

	rruntime.Go(func() {
		runArchiver(jd.tablePrefix, jd.dbHandle)
	})
}

func (jd *HandleT) writerSetup() {
	jd.recoverFromJournal(Write)
	//This is a thread-safe operation.
	//Even if two different services (gateway and processor) perform this operation, there should not be any problem.
	jd.recoverFromJournal(ReadWrite)

	//Refresh in memory list. We don't take lock
	//here because this is called before anything
	//else
	jd.getDSList(true)
	jd.getDSRangeList(true)

	//If no DS present, add one
	if len(jd.datasetList) == 0 {
		jd.addNewDS(appendToDsList, dataSetT{})
	}

	rruntime.Go(func() {
		jd.addNewDSLoop()
	})
}

func (jd *HandleT) readerWriterSetup() {
	jd.recoverFromJournal(Read)

	jd.writerSetup()

	jd.startBackupDSLoop()
	jd.startMigrateDSLoop()

	rruntime.Go(func() {
		runArchiver(jd.tablePrefix, jd.dbHandle)
	})
}

/*
TearDown releases all the resources
*/
func (jd *HandleT) TearDown() {
	jd.dbHandle.Close()
}

//removeExtraKey : removes extra key present in map1 and not in map2
//Assumption is keys in map1 and map2 are same, except that map1 has one key more than map2
func removeExtraKey(map1, map2 map[string]string) string {
	var deleteKey, key string
	for key = range map1 {
		if _, ok := map2[key]; !ok {
			deleteKey = key
			break
		}
	}

	if deleteKey != "" {
		delete(map1, deleteKey)
	}

	return deleteKey
}

func remove(slice []string, idx int) []string {
	return append(slice[:idx], slice[idx+1:]...)
}

/*
Function to return an ordered list of datasets and datasetRanges
Most callers use the in-memory list of dataset and datasetRanges
Caller must have the dsListLock readlocked
*/
func (jd *HandleT) getDSList(refreshFromDB bool) []dataSetT {

	if !refreshFromDB {
		return jd.datasetList
	}

	//At this point we MUST have write-locked dsListLock
	//since we are modiying the list

	//Reset the global list
	jd.datasetList = nil

	jd.datasetList = getDSList(jd, jd.dbHandle, jd.tablePrefix)

	//if the owner of this jobsdb is a writer, then shrinking datasetList to have only last two datasets
	//this shrinked datasetList is used to compute DSRangeList
	//This is done because, writers don't care about the left datasets in the sorted datasetList
	if jd.ownerType == Write {
		if len(jd.datasetList) > 2 {
			jd.datasetList = jd.datasetList[len(jd.datasetList)-2 : len(jd.datasetList)]
		}
	}

	jd.statTableCount.Gauge(len(jd.datasetList))
	return jd.datasetList
}

//Function must be called with read-lock held in dsListLock
func (jd *HandleT) getDSRangeList(refreshFromDB bool) []dataSetRangeT {

	var minID, maxID sql.NullInt64
	var prevMax int64

	if !refreshFromDB {
		return jd.datasetRangeList
	}

	//At this point we must have write-locked dsListLock
	dsList := jd.getDSList(true)
	jd.datasetRangeList = nil

	for idx, ds := range dsList {
		jd.assert(ds.Index != "", "ds.Index is empty")
		sqlStatement := fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %s`, ds.JobTable)
		row := jd.dbHandle.QueryRow(sqlStatement)
		err := row.Scan(&minID, &maxID)
		jd.assertError(err)
		jd.logger.Debug(sqlStatement, minID, maxID)
		//We store ranges EXCEPT for
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
			//TODO: Cleanup - Remove the line below and jd.inProgressMigrationTargetDS
			jd.assert(minID.Valid && maxID.Valid, fmt.Sprintf("minID.Valid: %v, maxID.Valid: %v. Either of them is false for table: %s", minID.Valid, maxID.Valid, ds.JobTable))
			jd.assert(idx == 0 || prevMax < minID.Int64, fmt.Sprintf("idx: %d != 0 and prevMax: %d >= minID.Int64: %v of table: %s", idx, prevMax, minID.Int64, ds.JobTable))
			jd.datasetRangeList = append(jd.datasetRangeList,
				dataSetRangeT{minJobID: int64(minID.Int64),
					maxJobID: int64(maxID.Int64), ds: ds})
			prevMax = maxID.Int64
		}
	}
	return jd.datasetRangeList
}

/*
Functions for checking when DB is full or DB needs to be migrated.
We migrate the DB ONCE most of the jobs have been processed (suceeded/aborted)
Or when the job_status table gets too big because of lot of retries/failures
*/

func (jd *HandleT) checkIfMigrateDS(ds dataSetT) (bool, int) {
	queryStat := stats.NewTaggedStat("migration_ds_check", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()
	var delCount, totalCount, statusCount int
	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, ds.JobTable)
	row := jd.dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&totalCount)
	jd.assertError(err)

	//Jobs which have either succeded or expired
	sqlStatement = fmt.Sprintf(`SELECT COUNT(DISTINCT(job_id))
                                      FROM %s
                                      WHERE job_state IN ('%s')`,
		ds.JobStatusTable, strings.Join(getValidTerminalStates(), "', '"))
	row = jd.dbHandle.QueryRow(sqlStatement)
	err = row.Scan(&delCount)
	jd.assertError(err)

	//Total number of job status. If this table grows too big (e.g. lot of retries)
	//we migrate to a new table and get rid of old job status
	sqlStatement = fmt.Sprintf(`SELECT COUNT(*) FROM %s`, ds.JobStatusTable)
	row = jd.dbHandle.QueryRow(sqlStatement)
	err = row.Scan(&statusCount)
	jd.assertError(err)

	if totalCount == 0 {
		jd.assert(delCount == 0 && statusCount == 0, fmt.Sprintf("delCount: %d, statusCount: %d. Either of them is not 0", delCount, statusCount))
		return false, 0
	}

	//If records are newer than what is required. One example use case is
	//gateway DB where records are kept to dedup

	var lastUpdate time.Time
	sqlStatement = fmt.Sprintf(`SELECT MAX(created_at) FROM %s`, ds.JobTable)
	row = jd.dbHandle.QueryRow(sqlStatement)
	err = row.Scan(&lastUpdate)
	jd.assertError(err)

	if jd.dsRetentionPeriod > time.Duration(0) && time.Since(lastUpdate) < jd.dsRetentionPeriod {
		return false, totalCount - delCount
	}

	if (float64(delCount)/float64(totalCount) > jobDoneMigrateThres) ||
		(float64(statusCount)/float64(totalCount) > jobStatusMigrateThres) {
		return true, totalCount - delCount
	}
	return false, totalCount - delCount
}

func (jd *HandleT) getTableRowCount(jobTable string) int {
	var count int

	sqlStatement := fmt.Sprintf(`SELECT COUNT(*) FROM %s`, jobTable)
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

	tableSize := jd.getTableSize(ds.JobTable)
	if tableSize > maxTableSize {
		jd.logger.Infof("[JobsDB] %s is full in size. Count: %v, Size: %v", ds.JobTable, jd.getTableRowCount(ds.JobTable), tableSize)
		return true
	}

	totalCount := jd.getTableRowCount(ds.JobTable)
	if totalCount > maxDSSize {
		jd.logger.Infof("[JobsDB] %s is full by rows. Count: %v, Size: %v", ds.JobTable, totalCount, jd.getTableSize(ds.JobTable))
		return true
	}

	return false
}

/*
Function to add a new dataset. DataSet can be added to the end (e.g when last
becomes full OR in between during migration. DataSets are assigned numbers
monotonically when added  to end. So, with just add to end, numbers would be
like 1,2,3,4, and so on. Theese are called level0 datasets. And the Index is
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
is to happen on one of the level1 dataset, we ahve to migrate them to level2 dataset

Eg. When the node has 1, 2, 3, 4 data sets and an import is triggered, new events start
going to 5, 6, 7... so on. And the imported data start going to 4_1, 4_2, 4_3... so on
Now if an internal migration is to happen and we migrate 1, 2, 3, 4, 4_1, we need to
create a newDS between 4_1 and 4_2. This is assigned to 4_1_1, 4_1_2 and so on.
*/

func mapDSToLevel(ds dataSetT) (levelInt int, levelVals []int, err error) {
	indexStr := strings.Split(ds.Index, "_")
	//Currently we don't have a scenario where we need more than 3 levels.
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

func (jd *HandleT) createTableNames(dsIdx string) (string, string) {
	jobTable := fmt.Sprintf("%s_jobs_%s", jd.tablePrefix, dsIdx)
	jobStatusTable := fmt.Sprintf("%s_job_status_%s", jd.tablePrefix, dsIdx)
	return jobTable, jobStatusTable
}

func (jd *HandleT) addNewDS(newDSType string, insertBeforeDS dataSetT) dataSetT {
	jd.logger.Infof("Creating new DS of type %s before ds %s for %s jobsdb", newDSType, insertBeforeDS.Index, jd.tablePrefix)
	var newDSIdx string
	appendLast := newDSType == appendToDsList

	queryStat := stats.NewTaggedStat("add_new_ds", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})

	queryStat.Start()
	defer queryStat.End()

	switch newDSType {
	case appendToDsList:
		newDSIdx = jd.computeNewIdxForAppend()
	case insertForMigration:
		newDSIdx = jd.computeNewIdxForIntraNodeMigration(insertBeforeDS)
	case insertForImport:
		newDSIdx = jd.computeNewIdxForInterNodeMigration(insertBeforeDS)
	default:
		panic("Unknown usage for newDSType : " + newDSType)

	}
	jd.assert(newDSIdx != "", "newDSIdx is empty")

	defer func() {
		if appendLast {
			// Tracking time interval between new ds creations. Hence calling end before start
			if jd.isStatNewDSPeriodInitialized {
				jd.statNewDSPeriod.End()
			}
			jd.statNewDSPeriod.Start()
			jd.isStatNewDSPeriodInitialized = true
		}
	}()

	return jd.createDS(appendLast, newDSIdx)
}

func (jd *HandleT) computeNewIdxForAppend() string {
	dList := jd.getDSList(true)
	newDSIdx := ""
	if len(dList) == 0 {
		newDSIdx = "1"
	} else {
		levels, levelVals, err := mapDSToLevel(dList[len(dList)-1])
		jd.assertError(err)
		//Last one can only be Level0
		jd.assert(levels == 1, fmt.Sprintf("levels:%d != 1", levels))
		newDSIdx = fmt.Sprintf("%d", levelVals[0]+1)
	}
	return newDSIdx
}

func (jd *HandleT) computeNewIdxForInterNodeMigration(insertBeforeDS dataSetT) string { //ClusterMigration
	jd.logger.Debugf("computeNewIdxForInterNodeMigration, insertBeforeDS : %v", insertBeforeDS)
	dList := jd.getDSList(true)
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

//Tries to give a slice between before and after by incrementing last value in before. If the order doesn't maintain, it adds a level and recurses.
func computeInsertVals(before, after []string) ([]string, error) {
	for {
		calculatedVals := make([]string, len(before))
		copy(calculatedVals, before)
		lastVal, err := strconv.Atoi(calculatedVals[len(calculatedVals)-1])
		if err != nil {
			return calculatedVals, err
		}
		//Just increment the last value of the index as a possible candidate
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

		var comparison bool
		if !equals {
			comparison, err = dsComparitor(calculatedVals, after)
			if err != nil {
				return calculatedVals, err
			}
		} else {
			comparison = false
		}

		//The basic requirement is that the possible candidate should be smaller compared to the insertBeforeDS.
		if comparison {
			//Only when the index starts with 0, we allow three levels. This would be when we have to insert an internal migration DS between two import DSs
			//In all other cases, we allow only two levels
			if (before[0] == "0" && len(calculatedVals) == 3) ||
				(before[0] != "0" && len(calculatedVals) == 2) {
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

func (jd *HandleT) computeNewIdxForIntraNodeMigration(insertBeforeDS dataSetT) string { //Within the node
	jd.logger.Debugf("computeNewIdxForIntraNodeMigration, insertBeforeDS : %v", insertBeforeDS)
	dList := jd.getDSList(true)
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
	//If required, add other definitions that are common between *sql.DB and *sql.Tx
	//Never include Commit and Rollback in this interface
	//That ensures that whoever is acting on a transactionHandler can't commit or rollback
	//Only the function that passes *sql.Tx should do the commit or rollback based on the error it receives
}

func (jd *HandleT) createDS(appendLast bool, newDSIdx string) dataSetT {
	var newDS dataSetT
	newDS.JobTable, newDS.JobStatusTable = jd.createTableNames(newDSIdx)
	newDS.Index = newDSIdx

	//Mark the start of operation. If we crash somewhere here, we delete the
	//DS being added
	opPayload, err := json.Marshal(&journalOpPayloadT{To: newDS})
	jd.assertError(err)
	opID := jd.JournalMarkStart(addDSOperation, opPayload)

	//Create the jobs and job_status tables
	sqlStatement := fmt.Sprintf(`CREATE TABLE %s (
                                      job_id BIGSERIAL PRIMARY KEY,
									  uuid UUID NOT NULL,
									  user_id TEXT NOT NULL,
									  parameters JSONB NOT NULL,
                                      custom_val VARCHAR(64) NOT NULL,
                                      event_payload JSONB NOT NULL,
                                      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                                      expire_at TIMESTAMP NOT NULL DEFAULT NOW());`, newDS.JobTable)

	_, err = jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)

	sqlStatement = fmt.Sprintf(`CREATE TABLE %s (
                                     id BIGSERIAL PRIMARY KEY,
                                     job_id BIGINT REFERENCES %s(job_id),
                                     job_state VARCHAR(64),
                                     attempt SMALLINT,
                                     exec_time TIMESTAMP,
                                     retry_time TIMESTAMP,
                                     error_code VARCHAR(32),
                                     error_response JSONB);`, newDS.JobStatusTable, newDS.JobTable)
	_, err = jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)

	if appendLast {
		newDSWithSeqNumber := jd.setSequenceNumber(newDSIdx)
		jd.JournalMarkDone(opID)
		return newDSWithSeqNumber
	}

	//This is the migration case. We don't yet update the in-memory list till
	//we finish the migration
	jd.JournalMarkDone(opID)
	return newDS
}

func (jd *HandleT) setSequenceNumber(newDSIdx string) dataSetT {
	//Refresh the in-memory list. We only need to refresh the
	//last DS, not the entire but we do it anyway.
	//For the range list, we use the cached data. Internally
	//it queries the new dataset which was added.
	dList := jd.getDSList(true)
	dRangeList := jd.getDSRangeList(true)

	//We should not have range values for the last element (the new DS) and migrationTargetDS (if found)
	jd.assert(len(dList) == len(dRangeList)+1 || len(dList) == len(dRangeList)+2, fmt.Sprintf("len(dList):%d != len(dRangeList):%d (+1 || +2)", len(dList), len(dRangeList)))

	//Now set the min JobID for the new DS just added to be 1 more than previous max
	if len(dRangeList) > 0 {
		newDSMin := dRangeList[len(dRangeList)-1].maxJobID
		jd.assert(newDSMin > 0, fmt.Sprintf("newDSMin:%d <= 0", newDSMin))
		sqlStatement := fmt.Sprintf(`SELECT setval(pg_get_serial_sequence('%s_jobs_%s', 'job_id'), %d)`,
			jd.tablePrefix, newDSIdx, newDSMin)
		_, err := jd.dbHandle.Exec(sqlStatement)
		jd.assertError(err)
	}
	return dList[len(dList)-1]
}

/*
 * Function to return max dataset index in the DB
 */
func (jd *HandleT) GetMaxDSIndex() (maxDSIndex int64) {

	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()

	//dList is already sorted.
	dList := jd.getDSList(false)
	ds := dList[len(dList)-1]
	maxDSIndex, err := strconv.ParseInt(ds.Index, 10, 64)
	if err != nil {
		panic(err)
	}

	return maxDSIndex
}

//Drop a dataset
func (jd *HandleT) dropDS(ds dataSetT, allowMissing bool) {

	//Doing if exists only if caller explicitly mentions
	//that its okay for DB to be missing. This scenario
	//happens during recovering from failed migration.
	//For every other case, the table must exist
	var sqlStatement string
	if allowMissing {
		sqlStatement = fmt.Sprintf(`DROP TABLE IF EXISTS %s`, ds.JobStatusTable)
	} else {
		sqlStatement = fmt.Sprintf(`DROP TABLE %s`, ds.JobStatusTable)
	}
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)

	if allowMissing {
		sqlStatement = fmt.Sprintf(`DROP TABLE IF EXISTS %s`, ds.JobTable)
	} else {
		sqlStatement = fmt.Sprintf(`DROP TABLE %s`, ds.JobTable)
	}
	_, err = jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)

	// Tracking time interval between drop ds operations. Hence calling end before start
	if jd.isStatDropDSPeriodInitialized {
		jd.statDropDSPeriod.End()
	}
	jd.statDropDSPeriod.Start()
	jd.isStatDropDSPeriodInitialized = true
}

//Rename a dataset
func (jd *HandleT) renameDS(ds dataSetT, allowMissing bool) {
	var sqlStatement string
	var renamedJobStatusTable = fmt.Sprintf(`pre_drop_%s`, ds.JobStatusTable)
	var renamedJobTable = fmt.Sprintf(`pre_drop_%s`, ds.JobTable)

	if allowMissing {
		sqlStatement = fmt.Sprintf(`ALTER TABLE IF EXISTS %s RENAME TO %s`, ds.JobStatusTable, renamedJobStatusTable)
	} else {
		sqlStatement = fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, ds.JobStatusTable, renamedJobStatusTable)
	}
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)

	if allowMissing {
		sqlStatement = fmt.Sprintf(`ALTER TABLE IF EXISTS %s RENAME TO %s`, ds.JobTable, renamedJobTable)
	} else {
		sqlStatement = fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, ds.JobTable, renamedJobTable)
	}
	_, err = jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
}

func (jd *HandleT) getBackupDSList() []dataSetT {
	//Read the table names from PG
	tableNames := getAllTableNames(jd, jd.dbHandle)

	jobNameMap := map[string]string{}
	jobStatusNameMap := map[string]string{}
	dnumList := []string{}

	var dsList []dataSetT

	tablePrefix := "pre_drop_" + jd.tablePrefix
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
	return dsList
}

func (jd *HandleT) dropAllBackupDS() error {
	dsList := jd.getBackupDSList()
	for _, ds := range dsList {
		jd.dropDS(ds, false)
	}
	return nil
}

func (jd *HandleT) dropMigrationCheckpointTables() {
	tableNames := getAllTableNames(jd, jd.dbHandle)

	var migrationCheckPointTables []string
	for _, t := range tableNames {
		if strings.HasPrefix(t, jd.tablePrefix) && strings.HasSuffix(t, MigrationCheckpointSuffix) {
			migrationCheckPointTables = append(migrationCheckPointTables, t)
		}
	}

	for _, tableName := range migrationCheckPointTables {
		sqlStatement := fmt.Sprintf(`DROP TABLE %s`, tableName)
		_, err := jd.dbHandle.Exec(sqlStatement)
		jd.assertError(err)
	}
}

func (jd *HandleT) dropAllDS() error {

	jd.dsListLock.Lock()
	defer jd.dsListLock.Unlock()

	dList := jd.getDSList(true)
	for _, ds := range dList {
		jd.dropDS(ds, false)
	}

	//Update the list
	jd.getDSList(true)
	jd.getDSRangeList(true)

	return nil
}

/*
Function to migrate jobs from src dataset  (srcDS) to destination dataset (dest_ds)
First all the unprocessed jobs are copied over. Then all the jobs which haven't
completed (state is failed or waiting or waiting_retry or executiong) are copied
over. Then the status (only the latest) is set for those jobs
*/

func (jd *HandleT) migrateJobs(srcDS dataSetT, destDS dataSetT) (noJobsMigrated int, err error) {
	queryStat := stats.NewTaggedStat("migration_jobs", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()
	//Unprocessed jobs
	unprocessedList := jd.getUnprocessedJobsDS(srcDS, false, 0, GetQueryParamsT{})

	//Jobs which haven't finished processing
	retryList := jd.getProcessedJobsDS(srcDS, true,
		0, GetQueryParamsT{StateFilters: getValidNonTerminalStates()})
	jobsToMigrate := append(unprocessedList, retryList...)
	noJobsMigrated = len(jobsToMigrate)
	//Copy the jobs over. Second parameter (true) makes sure job_id is copied over
	//instead of getting auto-assigned
	err = jd.storeJobsDS(destDS, true, jobsToMigrate) //TODO: switch to transaction
	jd.assertError(err)

	//Now copy over the latest status of the unfinished jobs
	var statusList []*JobStatusT
	for _, job := range retryList {
		newStatus := JobStatusT{
			JobID:         job.JobID,
			JobState:      job.LastJobStatus.JobState,
			AttemptNum:    job.LastJobStatus.AttemptNum,
			ExecTime:      job.LastJobStatus.ExecTime,
			RetryTime:     job.LastJobStatus.RetryTime,
			ErrorCode:     job.LastJobStatus.ErrorCode,
			ErrorResponse: job.LastJobStatus.ErrorResponse,
		}
		statusList = append(statusList, &newStatus)
	}
	err = jd.updateJobStatusDS(destDS, statusList, []string{}, nil) //TODO: switch to transaction
	jd.assertError(err)

	return
}

func (jd *HandleT) postMigrateHandleDS(migrateFrom []dataSetT) error {

	//Rename datasets before dropping them, so that they can be uploaded to s3
	for _, ds := range migrateFrom {
		if jd.BackupSettings.BackupEnabled {
			jd.renameDS(ds, false)
		} else {
			jd.dropDS(ds, false)
		}
	}

	jd.inProgressMigrationTargetDS = nil

	//Refresh the in-memory lists
	jd.getDSList(true)
	jd.getDSRangeList(true)

	return nil
}

/*
Next set of functions are for reading/writing jobs and job_status for
a given dataset. The names should be self explainatory
*/
func (jd *HandleT) storeJobsDS(ds dataSetT, copyID bool, jobList []*JobT) error { //When fixing callers make sure error is handled with assertError
	queryStat := stats.NewTaggedStat("store_jobs", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	txn, err := jd.dbHandle.Begin()
	if err != nil {
		return err
	}

	err = jd.storeJobsDSInTxn(txn, ds, copyID, jobList)
	if err != nil {
		txn.Rollback()
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	//Empty customValFilters means we want to clear for all
	jd.markClearEmptyResult(ds, []string{}, []string{}, nil, hasJobs, nil)
	// fmt.Println("Bursting CACHE")

	return nil
}

func (jd *HandleT) storeJobsDSWithRetryEach(ds dataSetT, copyID bool, jobList []*JobT) (errorMessagesMap map[uuid.UUID]string) {
	queryStat := stats.NewTaggedStat("store_jobs", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	err := jd.storeJobsDS(ds, copyID, jobList)
	if err == nil {
		return
	}

	errorMessagesMap = make(map[uuid.UUID]string)

	for _, job := range jobList {
		err := jd.storeJobDS(ds, job)
		if err != nil {
			errorMessagesMap[job.UUID] = err.Error()
		}
	}

	return
}

func (jd *HandleT) storeJobsDSInTxn(txHandler transactionHandler, ds dataSetT, copyID bool, jobList []*JobT) error {
	var stmt *sql.Stmt
	var err error

	if copyID {
		stmt, err = txHandler.Prepare(pq.CopyIn(ds.JobTable, "job_id", "uuid", "user_id", "custom_val", "parameters",
			"event_payload", "created_at", "expire_at"))
		jd.assertError(err)
	} else {
		stmt, err = txHandler.Prepare(pq.CopyIn(ds.JobTable, "uuid", "user_id", "custom_val", "parameters", "event_payload"))
		jd.assertError(err)
	}

	defer stmt.Close()
	for _, job := range jobList {
		if copyID {
			_, err = stmt.Exec(job.JobID, job.UUID, job.UserID, job.CustomVal, string(job.Parameters),
				string(job.EventPayload), job.CreatedAt, job.ExpireAt)
		} else {
			_, err = stmt.Exec(job.UUID, job.UserID, job.CustomVal, string(job.Parameters), string(job.EventPayload))
		}
		jd.assertError(err)
	}
	_, err = stmt.Exec()

	return err
}

func (jd *HandleT) storeJobDS(ds dataSetT, job *JobT) (err error) {
	sqlStatement := fmt.Sprintf(`INSERT INTO %s (uuid, user_id, custom_val, parameters, event_payload)
	                                   VALUES ($1, $2, $3, $4, (regexp_replace($5::text, '\\u0000', '', 'g'))::json) RETURNING job_id`, ds.JobTable)
	stmt, err := jd.dbHandle.Prepare(sqlStatement)
	jd.assertError(err)
	defer stmt.Close()
	_, err = stmt.Exec(job.UUID, job.UserID, job.CustomVal, string(job.Parameters), string(job.EventPayload))
	if err == nil {
		//Empty customValFilters means we want to clear for all
		jd.markClearEmptyResult(ds, []string{}, []string{}, nil, hasJobs, nil)
		// fmt.Println("Bursting CACHE")
		return
	}
	pqErr := err.(*pq.Error)
	errCode := string(pqErr.Code)
	if errCode == dbErrorMap["Invalid JSON"] || errCode == dbErrorMap["Invalid Unicode"] ||
		errCode == dbErrorMap["Invalid Escape Sequence"] || errCode == dbErrorMap["Invalid Escape Character"] {
		return errors.New("Invalid JSON")
	}
	return
}

type cacheValue string

const (
	hasJobs cacheValue = "Has Jobs"
	noJobs  cacheValue = "No Jobs"
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

/*
* If a query returns empty result for a specific dataset, we cache that so that
* future queries don't have to hit the DB.
* markClearEmptyResult() when mark=True marks dataset,customVal,state as empty.
* markClearEmptyResult() when mark=False clears a previous empty mark
 */

func (jd *HandleT) markClearEmptyResult(ds dataSetT, stateFilters []string, customValFilters []string, parameterFilters []ParameterFilterT, value cacheValue, checkAndSet *cacheValue) {

	jd.dsCacheLock.Lock()
	defer jd.dsCacheLock.Unlock()

	//This means we want to mark/clear all customVals and stateFilters
	//When clearing, we remove the entire dataset entry. Not a big issue
	//We process ALL only during internal migration and caching empty
	//results is not important
	if len(stateFilters) == 0 || len(customValFilters) == 0 {
		if value == hasJobs {
			delete(jd.dsEmptyResultCache, ds)
		}
		return
	}

	_, ok := jd.dsEmptyResultCache[ds]
	if !ok {
		jd.dsEmptyResultCache[ds] = map[string]map[string]map[string]cacheValue{}
	}

	for _, cVal := range customValFilters {
		_, ok := jd.dsEmptyResultCache[ds][cVal]
		if !ok {
			jd.dsEmptyResultCache[ds][cVal] = map[string]map[string]cacheValue{}
		}

		pVals := []string{}
		for _, parameterFilter := range parameterFilters {
			pVals = append(pVals, fmt.Sprintf(`%s_%s`, parameterFilter.Name, parameterFilter.Value))
		}
		sort.Strings(pVals)
		pVal := strings.Join(pVals, "_")

		_, ok = jd.dsEmptyResultCache[ds][cVal][pVal]
		if !ok {
			jd.dsEmptyResultCache[ds][cVal][pVal] = map[string]cacheValue{}
		}

		for _, st := range stateFilters {
			previous := jd.dsEmptyResultCache[ds][cVal][pVal][st]
			if checkAndSet == nil || *checkAndSet == previous {
				jd.dsEmptyResultCache[ds][cVal][pVal][st] = value
			}
		}
	}
}

func (jd *HandleT) isEmptyResult(ds dataSetT, stateFilters []string, customValFilters []string, parameterFilters []ParameterFilterT) bool {
	queryStat := stats.NewTaggedStat("isEmptyCheck", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	jd.dsCacheLock.Lock()
	defer jd.dsCacheLock.Unlock()

	_, ok := jd.dsEmptyResultCache[ds]
	if !ok {
		return false
	}
	//We want to check for all states and customFilters. Cannot
	//assert that from cache
	if len(stateFilters) == 0 || len(customValFilters) == 0 {
		return false
	}

	for _, cVal := range customValFilters {
		_, ok := jd.dsEmptyResultCache[ds][cVal]
		if !ok {
			return false
		}

		pVals := []string{}
		for _, parameterFilter := range parameterFilters {
			pVals = append(pVals, fmt.Sprintf(`%s_%s`, parameterFilter.Name, parameterFilter.Value))
		}
		sort.Strings(pVals)
		pVal := strings.Join(pVals, "_")

		_, ok = jd.dsEmptyResultCache[ds][cVal][pVal]
		if !ok {
			return false
		}

		for _, st := range stateFilters {
			mark, ok := jd.dsEmptyResultCache[ds][cVal][pVal][st]
			if !ok || mark != noJobs {
				return false
			}
		}
	}
	//Every state and every customVal in the DS is empty
	//so can return
	return true
}

/*
limitCount == 0 means return all
stateFilters and customValFilters do a OR query on values passed in array
parameterFilters do a AND query on values included in the map
*/
func (jd *HandleT) getProcessedJobsDS(ds dataSetT, getAll bool, limitCount int, params GetQueryParamsT) []*JobT {
	stateFilters := params.StateFilters
	customValFilters := params.CustomValFilters
	parameterFilters := params.ParameterFilters

	checkValidJobState(jd, stateFilters)

	if jd.isEmptyResult(ds, stateFilters, customValFilters, parameterFilters) {
		jd.logger.Debugf("[getProcessedJobsDS] Empty cache hit for ds: %v, stateFilters: %v, customValFilters: %v, parameterFilters: %v", ds, stateFilters, customValFilters, parameterFilters)
		return []*JobT{}
	}

	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	if len(stateFilters) > 0 {
		statName = statName + stateFilters[0] + "_"
	}
	queryStat = stats.NewTaggedStat(statName+"processed_jobs", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	// We don't reset this in case of error for now, as any error in this function causes panic
	jd.markClearEmptyResult(ds, stateFilters, customValFilters, parameterFilters, willTryToSet, nil)

	var stateQuery, customValQuery, limitQuery, sourceQuery string

	if len(stateFilters) > 0 {
		stateQuery = " AND " + constructQuery(jd, "job_state", stateFilters, "OR")
	} else {
		stateQuery = ""
	}
	if len(customValFilters) > 0 && !params.IgnoreCustomValFiltersInQuery {
		jd.assert(!getAll, "getAll is true")
		customValQuery = " AND " +
			constructQuery(jd, fmt.Sprintf("%s.custom_val", ds.JobTable),
				customValFilters, "OR")
	} else {
		customValQuery = ""
	}

	if len(parameterFilters) > 0 {
		jd.assert(!getAll, "getAll is true")
		sourceQuery += " AND " + constructParameterJSONQuery(jd, ds.JobTable, parameterFilters)
	} else {
		sourceQuery = ""
	}

	if limitCount > 0 {
		jd.assert(!getAll, "getAll is true")
		limitQuery = fmt.Sprintf(" LIMIT %d ", limitCount)
	} else {
		limitQuery = ""
	}

	var rows *sql.Rows
	if getAll {
		sqlStatement := fmt.Sprintf(`SELECT
                                  %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters,  %[1]s.custom_val, %[1]s.event_payload,
                                  %[1]s.created_at, %[1]s.expire_at,
                                  job_latest_state.job_state, job_latest_state.attempt,
                                  job_latest_state.exec_time, job_latest_state.retry_time,
                                  job_latest_state.error_code, job_latest_state.error_response
                                 FROM
                                  %[1]s,
                                  (SELECT job_id, job_state, attempt, exec_time, retry_time,
                                    error_code, error_response FROM %[2]s WHERE id IN
                                    (SELECT MAX(id) from %[2]s GROUP BY job_id) %[3]s)
                                  AS job_latest_state
                                   WHERE %[1]s.job_id=job_latest_state.job_id`,
			ds.JobTable, ds.JobStatusTable, stateQuery)
		var err error
		rows, err = jd.dbHandle.Query(sqlStatement)
		jd.assertError(err)
		defer rows.Close()
	} else {
		sqlStatement := fmt.Sprintf(`SELECT
                                               %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload,
                                               %[1]s.created_at, %[1]s.expire_at,
                                               job_latest_state.job_state, job_latest_state.attempt,
                                               job_latest_state.exec_time, job_latest_state.retry_time,
                                               job_latest_state.error_code, job_latest_state.error_response
                                            FROM
                                               %[1]s,
                                               (SELECT job_id, job_state, attempt, exec_time, retry_time,
                                                 error_code, error_response FROM %[2]s WHERE id IN
                                                   (SELECT MAX(id) from %[2]s GROUP BY job_id) %[3]s)
                                               AS job_latest_state
                                            WHERE %[1]s.job_id=job_latest_state.job_id
                                             %[4]s %[5]s
                                             AND job_latest_state.retry_time < $1 ORDER BY %[1]s.job_id %[6]s`,
			ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, limitQuery)

		stmt, err := jd.dbHandle.Prepare(sqlStatement)
		jd.assertError(err)
		defer stmt.Close()
		rows, err = stmt.Query(time.Now())
		jd.assertError(err)
		defer rows.Close()
	}

	var jobList []*JobT
	for rows.Next() {
		var job JobT
		err := rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.CreatedAt, &job.ExpireAt,
			&job.LastJobStatus.JobState, &job.LastJobStatus.AttemptNum,
			&job.LastJobStatus.ExecTime, &job.LastJobStatus.RetryTime,
			&job.LastJobStatus.ErrorCode, &job.LastJobStatus.ErrorResponse)
		jd.assertError(err)
		jobList = append(jobList, &job)
	}

	result := hasJobs
	if len(jobList) == 0 {
		jd.logger.Debugf("[getProcessedJobsDS] Setting empty cache for ds: %v, stateFilters: %v, customValFilters: %v, parameterFilters: %v", ds, stateFilters, customValFilters, parameterFilters)
		result = noJobs
	}
	_willTryToSet := willTryToSet
	jd.markClearEmptyResult(ds, stateFilters, customValFilters, parameterFilters, result, &_willTryToSet)

	return jobList
}

/*
count == 0 means return all
stateFilters and customValFilters do a OR query on values passed in array
parameterFilters do a AND query on values included in the map
*/
func (jd *HandleT) getUnprocessedJobsDS(ds dataSetT, order bool, count int, params GetQueryParamsT) []*JobT {
	customValFilters := params.CustomValFilters
	parameterFilters := params.ParameterFilters

	if jd.isEmptyResult(ds, []string{NotProcessed.State}, customValFilters, parameterFilters) {
		jd.logger.Debugf("[getUnprocessedJobsDS] Empty cache hit for ds: %v, stateFilters: NP, customValFilters: %v, parameterFilters: %v", ds, customValFilters, parameterFilters)
		return []*JobT{}
	}

	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	queryStat = stats.NewTaggedStat(statName+"unprocessed_jobs", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	// We don't reset this in case of error for now, as any error in this function causes panic
	jd.markClearEmptyResult(ds, []string{NotProcessed.State}, customValFilters, parameterFilters, willTryToSet, nil)

	var rows *sql.Rows
	var err error

	var sqlStatement string

	if useJoinForUnprocessed {
		sqlStatement = fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val,
                                               %[1]s.event_payload, %[1]s.created_at,
                                               %[1]s.expire_at
                                             FROM %[1]s LEFT JOIN %[2]s ON %[1]s.job_id=%[2]s.job_id
                                             WHERE %[2]s.job_id is NULL`, ds.JobTable, ds.JobStatusTable)
	} else {
		sqlStatement = fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val,
                                               %[1]s.event_payload, %[1]s.created_at,
                                               %[1]s.expire_at
                                             FROM %[1]s WHERE %[1]s.job_id NOT IN (SELECT DISTINCT(%[2]s.job_id)
                                             FROM %[2]s)`, ds.JobTable, ds.JobStatusTable)
	}

	if len(customValFilters) > 0 && !params.IgnoreCustomValFiltersInQuery {
		sqlStatement += " AND " + constructQuery(jd, fmt.Sprintf("%s.custom_val", ds.JobTable),
			customValFilters, "OR")
	}

	if len(parameterFilters) > 0 {
		sqlStatement += " AND " + constructParameterJSONQuery(jd, ds.JobTable, parameterFilters)
	}

	if order {
		sqlStatement += fmt.Sprintf(" ORDER BY %s.job_id", ds.JobTable)
	}
	if count > 0 {
		sqlStatement += fmt.Sprintf(" LIMIT %d", count)
	}

	rows, err = jd.dbHandle.Query(sqlStatement)
	jd.assertError(err)
	defer rows.Close()

	var jobList []*JobT
	for rows.Next() {
		var job JobT
		err := rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.CreatedAt, &job.ExpireAt)
		jd.assertError(err)
		jobList = append(jobList, &job)
	}

	result := hasJobs
	dsList := jd.getDSList(false)
	//if jobsdb owner is a reader and if ds is the right most one, ignoring setting result as noJobs
	if len(jobList) == 0 && (jd.ownerType != Read || ds.Index != dsList[len(dsList)-1].Index) {
		jd.logger.Debugf("[getUnprocessedJobsDS] Setting empty cache for ds: %v, stateFilters: NP, customValFilters: %v, parameterFilters: %v", ds, customValFilters, parameterFilters)
		result = noJobs
	}
	_willTryToSet := willTryToSet
	jd.markClearEmptyResult(ds, []string{NotProcessed.State}, customValFilters, parameterFilters, result, &_willTryToSet)

	return jobList
}

func (jd *HandleT) updateJobStatusDS(ds dataSetT, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) (err error) {
	if len(statusList) == 0 {
		return nil
	}

	txn, err := jd.dbHandle.Begin()
	if err != nil {
		return err
	}

	stateFilters, err := jd.updateJobStatusDSInTxn(txn, ds, statusList)
	if err != nil {
		txn.Rollback()
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}
	jd.markClearEmptyResult(ds, stateFilters, customValFilters, parameterFilters, hasJobs, nil)
	return nil
}

func (jd *HandleT) updateJobStatusDSInTxn(txHandler transactionHandler, ds dataSetT, statusList []*JobStatusT) (updatedStates []string, err error) {

	if len(statusList) == 0 {
		return
	}

	stmt, err := txHandler.Prepare(pq.CopyIn(ds.JobStatusTable, "job_id", "job_state", "attempt", "exec_time",
		"retry_time", "error_code", "error_response"))
	if err != nil {
		return
	}

	updatedStatesMap := map[string]bool{}
	for _, status := range statusList {
		//  Handle the case when google analytics returns gif in response
		updatedStatesMap[status.JobState] = true
		if !utf8.ValidString(string(status.ErrorResponse)) {
			status.ErrorResponse = []byte(`{}`)
		}
		_, err = stmt.Exec(status.JobID, status.JobState, status.AttemptNum, status.ExecTime,
			status.RetryTime, status.ErrorCode, string(status.ErrorResponse))
		if err != nil {
			return
		}
	}
	updatedStates = make([]string, 0, len(updatedStatesMap))
	for k := range updatedStatesMap {
		updatedStates = append(updatedStates, k)
	}

	_, err = stmt.Exec()
	if err != nil {
		return
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

func (jd *HandleT) addNewDSLoop() {
	for {
		time.Sleep(addNewDSLoopSleepDuration)
		jd.logger.Debugf("[[ %s : addNewDSLoop ]]: Start", jd.tablePrefix)
		jd.dsListLock.RLock()
		dsList := jd.getDSList(false)
		jd.dsListLock.RUnlock()
		latestDS := dsList[len(dsList)-1]
		if jd.checkIfFullDS(latestDS) {
			//Adding a new DS updates the list
			//Doesn't move any data so we only
			//take the list lock
			jd.dsListLock.Lock()
			jd.logger.Infof("[[ %s : addNewDSLoop ]]: NewDS", jd.tablePrefix)
			jd.addNewDS(appendToDsList, dataSetT{})
			jd.dsListLock.Unlock()
		}
	}
}

func (jd *HandleT) refreshDSListLoop() {
	for {
		time.Sleep(refreshDSListLoopSleepDuration)
		jd.logger.Debugf("[[ %s : refreshDSListLoop ]]: Start", jd.tablePrefix)

		jd.dsListLock.Lock()
		jd.getDSList(true)
		jd.getDSRangeList(true)

		jd.dsListLock.Unlock()
	}
}

func (jd *HandleT) migrateDSLoop() {

	for {
		time.Sleep(migrateDSLoopSleepDuration)
		jd.logger.Debugf("[[ %s : migrateDSLoop ]]: Start", jd.tablePrefix)

		//This block disables internal migration/consolidation while cluster-level migration is in progress
		if db.IsValidMigrationMode(jd.migrationState.migrationMode) {
			jd.logger.Debugf("[[ %s : migrateDSLoop ]]: migration mode = %s, so skipping internal migrations", jd.tablePrefix, jd.migrationState.migrationMode)
			continue
		}

		jd.dsListLock.RLock()
		dsList := jd.getDSList(false)
		jd.dsListLock.RUnlock()

		var migrateFrom []dataSetT
		var insertBeforeDS dataSetT
		var liveJobCount int
		var liveDSCount int
		var migrateDSProbeCount int
		for idx, ds := range dsList {

			ifMigrate, remCount := jd.checkIfMigrateDS(ds)
			jd.logger.Debugf("[[ %s : migrateDSLoop ]]: Migrate check %v, ds: %v", jd.tablePrefix, ifMigrate, ds)

			var idxCheck bool
			if jd.ownerType == Read {
				//if jobsdb owner is read, expempting the last two datasets from migration.
				//This is done to avoid dslist conflicts between reader and writer
				idxCheck = (idx == len(dsList)-1 || idx == len(dsList)-2)
			} else {
				idxCheck = (idx == len(dsList)-1)
			}

			if liveDSCount >= maxMigrateOnce || liveJobCount >= maxDSSize || idxCheck {
				break
			}

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

		//Take the lock and run actual migration
		jd.dsMigrationLock.Lock()

		migrationLoopStat := stats.NewTaggedStat("migration_loop", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
		migrationLoopStat.Start()
		//Add a temp DS to append to
		if len(migrateFrom) > 0 {
			if liveJobCount > 0 {
				jd.dsListLock.Lock()
				migrateTo := jd.addNewDS(insertForMigration, insertBeforeDS)
				jd.inProgressMigrationTargetDS = &migrateTo
				jd.dsListLock.Unlock()

				jd.logger.Infof("[[ %s : migrateDSLoop ]]: Migrate from: %v", jd.tablePrefix, migrateFrom)
				jd.logger.Infof("[[ %s : migrateDSLoop ]]: Next: %v", jd.tablePrefix, insertBeforeDS)
				jd.logger.Infof("[[ %s : migrateDSLoop ]]: To: %v", jd.tablePrefix, migrateTo)
				//Mark the start of copy operation. If we fail here
				//we just delete the new DS being copied into. The
				//sources are still around

				opPayload, err := json.Marshal(&journalOpPayloadT{From: migrateFrom, To: migrateTo})
				jd.assertError(err)
				opID := jd.JournalMarkStart(migrateCopyOperation, opPayload)

				totalJobsMigrated := 0
				for _, ds := range migrateFrom {
					jd.logger.Infof("[[ %s : migrateDSLoop ]]: Migrate: %v to: %v", jd.tablePrefix, ds, migrateTo)
					noJobsMigrated, _ := jd.migrateJobs(ds, migrateTo)
					totalJobsMigrated += noJobsMigrated
				}

				if totalJobsMigrated <= 0 {
					jd.dsListLock.Lock()
					jd.dropDS(migrateTo, false)
					jd.inProgressMigrationTargetDS = nil
					jd.dsListLock.Unlock()
				}

				jd.JournalMarkDone(opID)
			}

			//Mark the start of del operation. If we fail in between
			//we need to finish deleting the source datasets. Cannot
			//del the destination as some sources may have been deleted
			opPayload, err := json.Marshal(&journalOpPayloadT{From: migrateFrom})
			jd.assertError(err)
			opID := jd.JournalMarkStart(postMigrateDSOperation, opPayload)

			jd.dsListLock.Lock()
			jd.postMigrateHandleDS(migrateFrom)
			jd.dsListLock.Unlock()

			jd.JournalMarkDone(opID)
		}
		migrationLoopStat.End()
		jd.dsMigrationLock.Unlock()

	}
}

func (jd *HandleT) backupDSLoop() {
	for {
		time.Sleep(backupCheckSleepDuration)
		jd.logger.Info("BackupDS check:Start")
		backupDSRange := jd.getBackupDSRange()
		// check if non empty dataset is present to backup
		// else continue
		if (dataSetRangeT{} == backupDSRange) {
			// sleep for more duration if no dataset is found
			time.Sleep(5 * backupCheckSleepDuration)
			continue
		}

		backupDS := backupDSRange.ds

		opPayload, err := json.Marshal(&backupDS)
		jd.assertError(err)

		opID := jd.JournalMarkStart(backupDSOperation, opPayload)
		success := jd.backupDS(backupDSRange)
		if !success {
			jd.removeTableJSONDumps()
			jd.JournalMarkDone(opID)
			continue
		}
		jd.JournalMarkDone(opID)

		// drop dataset after successfully uploading both jobs and jobs_status to s3
		opID = jd.JournalMarkStart(backupDropDSOperation, opPayload)
		jd.dropDS(backupDS, false)
		jd.JournalMarkDone(opID)
	}
}

//backupDS writes both jobs and job_staus table to JOBS_BACKUP_STORAGE_PROVIDER
func (jd *HandleT) backupDS(backupDSRange dataSetRangeT) bool {
	// return after backing up aboprted jobs if the flag is turned on
	// backupDS is only called when BackupSettings.BackupEnabled is true
	if jd.BackupSettings.FailedOnly {
		jd.logger.Info("[JobsDB] ::  backupDS: starting backing up aborted")
		_, err := jd.backupTable(backupDSRange, false)
		if err != nil {
			jd.logger.Errorf("[JobsDB] :: Failed to backup aborted jobs table %v. Err: %v", backupDSRange.ds.JobStatusTable, err)
			return false
		}
	} else {
		// write jobs table to JOBS_BACKUP_STORAGE_PROVIDER
		_, err := jd.backupTable(backupDSRange, false)
		if err != nil {
			jd.logger.Errorf("[JobsDB] :: Failed to backup table %v. Err: %v", backupDSRange.ds.JobTable, err)
			return false
		}

		// write job_status table to JOBS_BACKUP_STORAGE_PROVIDER
		_, err = jd.backupTable(backupDSRange, true)
		if err != nil {
			jd.logger.Errorf("[JobsDB] :: Failed to backup table %v. Err: %v", backupDSRange.ds.JobStatusTable, err)
			return false
		}

	}

	return true
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
func (jd *HandleT) getBackUpQuery(backupDSRange dataSetRangeT, isJobStatusTable bool, offset int64) string {
	var stmt string
	if jd.BackupSettings.FailedOnly {
		// check failed and aborted state, order the output based on destination, job_id, exec_time
		stmt = fmt.Sprintf(`SELECT coalesce(json_agg(failed_jobs), '[]'::json) FROM (select * from %[1]s %[2]s INNER JOIN %[3]s %[4]s ON  %[2]s.job_id = %[4]s.job_id
			where %[2]s.job_state in ('%[5]s', '%[6]s') order by  %[4]s.custom_val, %[2]s.job_id, %[2]s.exec_time asc limit %[7]d offset %[8]d) AS failed_jobs`, backupDSRange.ds.JobStatusTable, "job_status", backupDSRange.ds.JobTable, "job",
			Failed.State, Aborted.State, backupRowsBatchSize, offset)
	} else {
		if isJobStatusTable {
			stmt = fmt.Sprintf(`SELECT json_agg(dump_table) FROM (select * from %[1]s order by job_id asc limit %[2]d offset %[3]d) AS dump_table`, backupDSRange.ds.JobStatusTable, backupRowsBatchSize, offset)
		} else {
			stmt = fmt.Sprintf(`SELECT json_agg(dump_table) FROM (select * from %[1]s order by job_id asc limit %[2]d offset %[3]d) AS dump_table`, backupDSRange.ds.JobTable, backupRowsBatchSize, offset)
		}
	}

	return stmt

}

// getFileUploader get a file uploader
func (jd *HandleT) getFileUploader() (filemanager.FileManager, error) {
	if jd.jobsFileUploader != nil {
		return jd.jobsFileUploader, nil
	}
	return filemanager.New(&filemanager.SettingsT{
		Provider: config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "S3"),
		Config:   filemanager.GetProviderConfigFromEnv(),
	})
}

func (jd *HandleT) isEmpty(ds dataSetT) bool {
	var count sql.NullInt64
	sqlStatement := fmt.Sprintf(`SELECT count(*) from %s`, ds.JobTable)
	row := jd.dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&count)
	jd.assertError(err)
	if count.Valid {
		return int64(count.Int64) == int64(0)
	}
	panic("Unable to get count on this dataset")
}

//GetTablePrefix returns the table prefix of the jobsdb
func (jd *HandleT) GetTablePrefix() string {
	return jd.tablePrefix
}

func (jd *HandleT) backupTable(backupDSRange dataSetRangeT, isJobStatusTable bool) (success bool, err error) {
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
		pathPrefix = strings.TrimPrefix(tableName, "pre_drop_")
		path = fmt.Sprintf(`%v%v_%v.gz`, tmpDirPath+backupPathDirName, pathPrefix, Aborted.State)
		// checked failed and aborted state
		countStmt = fmt.Sprintf(`SELECT COUNT(*) FROM %s where job_state in ('%s', '%s')`, tableName, Failed.State, Aborted.State)
	} else {
		if isJobStatusTable {
			tableName = backupDSRange.ds.JobStatusTable
			pathPrefix = strings.TrimPrefix(tableName, "pre_drop_")
			path = fmt.Sprintf(`%v%v.gz`, tmpDirPath+backupPathDirName, pathPrefix)
			countStmt = fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableName)
		} else {
			tableName = backupDSRange.ds.JobTable
			pathPrefix = strings.TrimPrefix(tableName, "pre_drop_")
			path = fmt.Sprintf(`%v%v.%v.%v.%v.%v.gz`,
				tmpDirPath+backupPathDirName,
				pathPrefix,
				backupDSRange.minJobID,
				backupDSRange.maxJobID,
				backupDSRange.startTime,
				backupDSRange.endTime,
			)
			countStmt = fmt.Sprintf(`SELECT COUNT(*) FROM %s`, tableName)
		}

	}

	jd.logger.Infof("[JobsDB] :: Backing up table: %v", tableName)

	var totalCount, rowEndPatternMatchCount int64
	err = jd.dbHandle.QueryRow(countStmt).Scan(&totalCount)
	if err != nil {
		panic(err)
	}

	// return without doing anything as no jobs not present in ds
	if totalCount == 0 {
		//  Do not record stat for this case?
		jd.logger.Infof("[JobsDB] ::  not processiong table dump as no rows match criteria. %v", tableName)
		return true, nil
	}

	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		panic(err)
	}

	gzWriter, err := misc.CreateGZ(path)
	defer os.Remove(path)

	var offset, batchCount int64
	for {
		stmt := jd.getBackUpQuery(backupDSRange, isJobStatusTable, offset)
		var rawJSONRows json.RawMessage
		row := jd.dbHandle.QueryRow(stmt)
		err = row.Scan(&rawJSONRows)
		if err != nil {
			panic(fmt.Errorf("Scanning row failed with error : %w", err))
		}

		rowEndPatternMatchCount += int64(bytes.Count(rawJSONRows, []byte("}, \n {")))
		rawJSONRows = bytes.Replace(rawJSONRows, []byte("}, \n {"), []byte("}\n{"), -1) //replacing ", \n " with "\n"
		batchCount++

		//Asserting that the first character is '[' and last character is ']'
		jd.assert(rawJSONRows[0] == byte('[') && rawJSONRows[len(rawJSONRows)-1] == byte(']'), "json agg output is not in the expected format. Excepted format: JSON Array [{}]")
		rawJSONRows = rawJSONRows[1 : len(rawJSONRows)-1] //stripping starting '[' and ending ']'
		rawJSONRows = append(rawJSONRows, '\n')           //appending '\n'

		gzWriter.Write(rawJSONRows)
		offset += backupRowsBatchSize
		if offset >= totalCount {
			break
		}
	}

	gzWriter.CloseGZ()
	tableFileDumpTimeStat.End()

	jd.assert(rowEndPatternMatchCount == totalCount-batchCount, fmt.Sprintf("rowEndPatternMatchCount:%d != (totalCount:%d-batchCount:%d). Ill formed json bytes could be written to a file. Panicking.", rowEndPatternMatchCount, totalCount, batchCount))

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
	// get a file uploader
	fileUploader, errored := jd.getFileUploader()
	jd.assertError(errored)
	output, err = fileUploader.Upload(file, pathPrefixes...)

	if err != nil {
		storageProvider := config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "S3")
		jd.logger.Errorf("[JobsDB] :: Failed to upload table %v dump to %s. Error: %s", tableName, storageProvider, err.Error())
		return false, err
	}

	// Do not record stat in error case as error case time might be low and skew stats
	fileUploadTimeStat.End()
	totalTableDumpTimeStat.End()
	jd.logger.Infof("[JobsDB] :: Backed up table: %v at %v", tableName, output.Location)
	return true, nil
}

func (jd *HandleT) getBackupDSRange() dataSetRangeT {
	var backupDS dataSetT
	var backupDSRange dataSetRangeT

	//Read the table names from PG
	tableNames := getAllTableNames(jd, jd.dbHandle)

	//We check for job_status because that is renamed after job
	dnumList := []string{}
	for _, t := range tableNames {
		if strings.HasPrefix(t, "pre_drop_"+jd.tablePrefix+"_jobs_") {
			dnum := t[len("pre_drop_"+jd.tablePrefix+"_jobs_"):]
			dnumList = append(dnumList, dnum)
			continue
		}
	}
	if len(dnumList) == 0 {
		return backupDSRange
	}

	sortDnumList(jd, dnumList)

	backupDS = dataSetT{
		JobTable:       fmt.Sprintf("pre_drop_%s_jobs_%s", jd.tablePrefix, dnumList[0]),
		JobStatusTable: fmt.Sprintf("pre_drop_%s_job_status_%s", jd.tablePrefix, dnumList[0]),
		Index:          dnumList[0],
	}

	var minID, maxID sql.NullInt64
	jobIDSQLStatement := fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %s`, backupDS.JobTable)
	row := jd.dbHandle.QueryRow(jobIDSQLStatement)
	err := row.Scan(&minID, &maxID)
	jd.assertError(err)

	var minCreatedAt, maxCreatedAt time.Time
	jobTimeSQLStatement := fmt.Sprintf(`SELECT MIN(created_at), MAX(created_at) FROM %s`, backupDS.JobTable)
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
	return backupDSRange
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
	stmt, err := jd.dbHandle.Prepare(sqlStatement)
	jd.assertError(err)
	defer stmt.Close()

	var opID int64
	err = stmt.QueryRow(opType, false, opPayload, time.Now(), jd.ownerType).Scan(&opID)
	jd.assertError(err)

	return opID

}

//JournalMarkDone marks the end of a journal action
func (jd *HandleT) JournalMarkDone(opID int64) {
	err := jd.journalMarkDoneInTxn(jd.dbHandle, opID)
	jd.assertError(err)
}

//JournalMarkDoneInTxn marks the end of a journal action in a transaction
func (jd *HandleT) journalMarkDoneInTxn(txHandler transactionHandler, opID int64) error {
	sqlStatement := fmt.Sprintf(`UPDATE %s_journal SET done=$2, end_time=$3 WHERE id=$1 AND owner=$4`, jd.tablePrefix)
	_, err := txHandler.Exec(sqlStatement, opID, true, time.Now(), jd.ownerType)
	if err != nil {
		return err
	}
	return nil
}

func (jd *HandleT) JournalDeleteEntry(opID int64) {
	sqlStatement := fmt.Sprintf(`DELETE FROM %s_journal WHERE id=$1 AND owner=$2`, jd.tablePrefix)
	_, err := jd.dbHandle.Exec(sqlStatement, opID, jd.ownerType)
	jd.assertError(err)
}

func (jd *HandleT) GetJournalEntries(opType string) (entries []JournalEntryT) {
	sqlStatement := fmt.Sprintf(`SELECT id, operation, done, operation_payload
                                	FROM %s_journal
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
                                	FROM %s_journal
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
	var undoOp = false
	var count int

	for rows.Next() {
		err = rows.Scan(&opID, &opType, &opDone, &opPayload)
		jd.assertError(err)
		jd.assert(!opDone, "opDone is true")
		count++
	}
	jd.assert(count <= 1, fmt.Sprintf("count:%d > 1", count))

	if count == 0 {
		//Nothing to recoer
		return
	}

	//Need to recover the last failed operation
	//Get the payload and undo
	err = json.Unmarshal(opPayload, &opPayloadJSON)
	jd.assertError(err)

	switch opType {
	case addDSOperation:
		newDS := opPayloadJSON.To
		undoOp = true
		//Drop the table we were tring to create
		jd.logger.Info("Recovering new DS operation", newDS)
		jd.dropDS(newDS, true)
	case migrateCopyOperation:
		migrateDest := opPayloadJSON.To
		//Delete the destination of the interrupted
		//migration. After we start, code should
		//redo the migration
		jd.logger.Info("Recovering migrateCopy operation", migrateDest)
		jd.dropDS(migrateDest, true)
		undoOp = true
	case migrateImportOperation:
		jd.assert(db.IsValidMigrationMode(jd.migrationState.migrationMode), "If migration mode is not valid, then this operation shouldn't have been unfinished. Go debug")
		var importDest dataSetT
		json.Unmarshal(opPayload, &importDest)
		jd.dropDS(importDest, true)
		jd.deleteSetupCheckpoint(ImportOp)
		undoOp = true
	case postMigrateDSOperation:
		//Some of the source datasets would have been
		migrateSrc := opPayloadJSON.From
		for _, ds := range migrateSrc {
			if jd.BackupSettings.BackupEnabled {
				jd.renameDS(ds, true)
			} else {
				jd.dropDS(ds, true)
			}
		}
		jd.logger.Info("Recovering migrateDel operation", migrateSrc)
		undoOp = false
	case backupDSOperation:
		jd.removeTableJSONDumps()
		jd.logger.Info("Removing all stale json dumps of tables")
		undoOp = true
	case dropDSOperation, backupDropDSOperation:
		//Some of the source datasets would have been
		var dataset dataSetT
		json.Unmarshal(opPayload, &dataset)
		jd.dropDS(dataset, true)
		jd.logger.Info("Recovering dropDS operation", dataset)
		undoOp = false
	}

	if undoOp {
		sqlStatement = fmt.Sprintf(`DELETE FROM %s_journal WHERE id=$1`, jd.tablePrefix)
	} else {
		sqlStatement = fmt.Sprintf(`UPDATE %s_journal SET done=True WHERE id=$1`, jd.tablePrefix)
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

//RecoverFromMigrationJournal is an exposed function for migrator package to handle journal crashes during migration
func (jd *HandleT) RecoverFromMigrationJournal() {
	jd.recoverFromCrash(Write, migratorRoutine)
	jd.recoverFromCrash(ReadWrite, migratorRoutine)
}

/*
UpdateJobStatus updates the status of a batch of jobs
customValFilters[] is passed so we can efficinetly mark empty cache
Later we can move this to query
*/
func (jd *HandleT) UpdateJobStatus(statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error {

	if len(statusList) == 0 {
		return nil
	}

	txn, err := jd.dbHandle.Begin()
	jd.assertError(err)

	updatedStatesByDS, err := jd.updateJobStatusInTxn(txn, statusList)
	if err != nil {
		jd.rollbackTx(err, txn)
		jd.logger.Infof("[[ %s ]]: Error occured while updating job statuses. Returning err, %v", jd.tablePrefix, err)
		return err
	}

	err = txn.Commit()
	jd.assertError(err)
	for ds, stateList := range updatedStatesByDS {
		jd.markClearEmptyResult(ds, stateList, customValFilters, parameterFilters, hasJobs, nil)
	}

	return nil
}

/*
UpdateJobStatusInTxn updates the status of a batch of jobs
customValFilters[] is passed so we can efficinetly mark empty cache
Later we can move this to query
*/
func (jd *HandleT) updateJobStatusInTxn(txHandler transactionHandler, statusList []*JobStatusT) (updatedStatesByDS map[dataSetT][]string, err error) {

	if len(statusList) == 0 {
		return
	}

	//First we sort by JobID
	sort.Slice(statusList, func(i, j int) bool {
		return statusList[i].JobID < statusList[j].JobID
	})

	//The order of lock is very important. The migrateDSLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	jd.dsMigrationLock.RLock()
	jd.dsListLock.RLock()
	defer jd.dsMigrationLock.RUnlock()
	defer jd.dsListLock.RUnlock()

	//We scan through the list of jobs and map them to DS
	var lastPos int
	dsRangeList := jd.getDSRangeList(false)
	updatedStatesByDS = make(map[dataSetT][]string)
	for _, ds := range dsRangeList {
		minID := ds.minJobID
		maxID := ds.maxJobID
		//We have processed upto (but excluding) lastPos on statusList.
		//Hence that element must lie in this or subsequent dataset's
		//range
		jd.assert(statusList[lastPos].JobID >= minID, fmt.Sprintf("statusList[lastPos].JobID: %d < minID:%d", statusList[lastPos].JobID, minID))
		var i int
		for i = lastPos; i < len(statusList); i++ {
			//The JobID is outside this DS's range
			if statusList[i].JobID > maxID {
				if i > lastPos {
					jd.logger.Debug("Range:", ds, statusList[lastPos].JobID,
						statusList[i-1].JobID, lastPos, i-1)
				}
				var updatedStates []string
				updatedStates, err = jd.updateJobStatusDSInTxn(txHandler, ds.ds, statusList[lastPos:i])
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
		//Reached the end. Need to process this range
		if i == len(statusList) && lastPos < i {
			jd.logger.Debug("Range:", ds, statusList[lastPos].JobID, statusList[i-1].JobID, lastPos, i)
			var updatedStates []string
			updatedStates, err = jd.updateJobStatusDSInTxn(txHandler, ds.ds, statusList[lastPos:i])
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

	//The last (most active DS) might not have range element as it is being written to
	if lastPos < len(statusList) {
		//Make sure range is missing for the last ds and migration ds (if at all present)
		dsList := jd.getDSList(false)
		jd.assert(len(dsRangeList) >= len(dsList)-2, fmt.Sprintf("len(dsRangeList):%d < len(dsList):%d-2", len(dsRangeList), len(dsList)))
		//Update status in the last element
		jd.logger.Debug("RangeEnd", statusList[lastPos].JobID, lastPos, len(statusList))
		var updatedStates []string
		updatedStates, err = jd.updateJobStatusDSInTxn(txHandler, dsList[len(dsList)-1], statusList[lastPos:])
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

/*
Store call is used to create new Jobs
*/
func (jd *HandleT) Store(jobList []*JobT) {
	//Only locks the list
	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList(false)
	err := jd.storeJobsDS(dsList[len(dsList)-1], false, jobList)
	jd.assertError(err)
}

/*
StoreWithRetryEach call is used to create new Jobs. This retries if the bulk store fails and retries for each job returning error messages for jobs failed to store
*/
func (jd *HandleT) StoreWithRetryEach(jobList []*JobT) map[uuid.UUID]string {

	//Only locks the list
	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList(false)
	return jd.storeJobsDSWithRetryEach(dsList[len(dsList)-1], false, jobList)
}

/*
printLists is a debuggging function used to print
the current in-memory copy of jobs and job ranges
*/
func (jd *HandleT) printLists(console bool) {

	//This being an internal function, we don't lock
	jd.logger.Debug("List:", jd.getDSList(false))
	jd.logger.Debug("Ranges:", jd.getDSRangeList(false))
	if console {
		fmt.Println("List:", jd.getDSList(false))
		fmt.Println("Ranges:", jd.getDSRangeList(false))
	}

}

/*
GetUnprocessed returns the unprocessed events. Unprocessed events are
those whose state hasn't been marked in the DB
*/
func (jd *HandleT) GetUnprocessed(params GetQueryParamsT) []*JobT {
	if params.Count == 0 {
		return []*JobT{}
	}

	customValFilters := params.CustomValFilters
	count := params.Count

	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	queryStat = stats.NewTaggedStat(statName+"unprocessed", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	//The order of lock is very important. The migrateDSLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	jd.dsMigrationLock.RLock()
	jd.dsListLock.RLock()
	defer jd.dsMigrationLock.RUnlock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList(false)
	outJobs := make([]*JobT, 0)
	jd.assert(count >= 0, fmt.Sprintf("count:%d received is less than 0", count))
	if count == 0 {
		return outJobs
	}
	for _, ds := range dsList {
		jd.assert(count > 0, fmt.Sprintf("count:%d is less than or equal to 0", count))
		jobs := jd.getUnprocessedJobsDS(ds, true, count, params)
		outJobs = append(outJobs, jobs...)
		count -= len(jobs)
		jd.assert(count >= 0, fmt.Sprintf("count:%d received is less than 0", count))
		if count == 0 {
			break
		}
	}

	//Release lock
	return outJobs
}

/*
DeleteJobStatus deletes the latest status of a batch of jobs
This is only done during recovery, which happens during the server start.
So, we don't have to worry about dsEmptyResultCache
*/
func (jd *HandleT) DeleteJobStatus(params GetQueryParamsT) {
	if params.Count == 0 {
		return
	}

	txn, err := jd.dbHandle.Begin()
	jd.assertError(err)

	err = jd.deleteJobStatusInTxn(txn, params)
	jd.assertErrorAndRollbackTx(err, txn)

	err = txn.Commit()
	jd.assertError(err)
}

/*
if count passed is less than 0, then delete happens on the entire dsList;
deleteJobStatusInTxn deletes the latest status of a batch of jobs
*/
func (jd *HandleT) deleteJobStatusInTxn(txHandler transactionHandler, params GetQueryParamsT) error {
	if params.Count == 0 {
		return nil
	}

	//The order of lock is very important. The migrateDSLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	jd.dsMigrationLock.RLock()
	jd.dsListLock.RLock()
	defer jd.dsMigrationLock.RUnlock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList(false)

	totalDeletedCount := 0
	for _, ds := range dsList {
		deletedCount, err := jd.deleteJobStatusDSInTxn(txHandler, ds, params)
		if err != nil {
			return err
		}
		totalDeletedCount += deletedCount

		//since count is less than 0, iterating on complete dsList
		if params.Count < 0 {
			continue
		}

		if totalDeletedCount >= params.Count {
			break
		}
	}

	return nil
}

/*
stateFilters and customValFilters do a OR query on values passed in array
parameterFilters do a AND query on values included in the map
*/
func (jd *HandleT) deleteJobStatusDSInTxn(txHandler transactionHandler, ds dataSetT, params GetQueryParamsT) (int, error) {
	stateFilters := params.StateFilters
	customValFilters := params.CustomValFilters
	parameterFilters := params.ParameterFilters

	checkValidJobState(jd, stateFilters)

	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	if len(stateFilters) > 0 {
		statName = statName + stateFilters[0] + "_"
	}
	queryStat = stats.NewTaggedStat(statName+"delete_job_status", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	var stateQuery, customValQuery, sourceQuery string

	if len(stateFilters) > 0 {
		stateQuery = " AND " + constructQuery(jd, "job_state", stateFilters, "OR")
	} else {
		stateQuery = ""
	}
	if len(customValFilters) > 0 {
		customValQuery = " WHERE " +
			constructQuery(jd, fmt.Sprintf("%s.custom_val", ds.JobTable),
				customValFilters, "OR")
	} else {
		customValQuery = ""
	}

	if customValQuery == "" {
		sourceQuery += " WHERE "
	} else {
		sourceQuery += " AND "
	}

	if len(parameterFilters) > 0 {
		sourceQuery += constructParameterJSONQuery(jd, ds.JobTable, parameterFilters)
	} else {
		sourceQuery = ""
	}

	var sqlStatement string
	if customValQuery == "" && sourceQuery == "" {
		sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s WHERE id IN
                                                   (SELECT MAX(id) from %[1]s GROUP BY job_id) %[2]s
                                             AND retry_time < $1`,
			ds.JobStatusTable, stateQuery)
	} else {
		sqlStatement = fmt.Sprintf(`DELETE FROM %[1]s WHERE id IN
                                                   (SELECT MAX(id) from %[1]s where job_id IN (SELECT job_id from %[2]s %[4]s %[5]s) GROUP BY job_id) %[3]s
                                             AND retry_time < $1`,
			ds.JobStatusTable, ds.JobTable, stateQuery, customValQuery, sourceQuery)
	}

	stmt, err := txHandler.Prepare(sqlStatement)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	res, err := stmt.Exec(time.Now())
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
func (jd *HandleT) GetProcessed(params GetQueryParamsT) []*JobT {
	if params.Count == 0 {
		return []*JobT{}
	}

	stateFilter := params.StateFilters
	customValFilters := params.CustomValFilters
	count := params.Count

	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	if len(stateFilter) > 0 {
		statName = statName + stateFilter[0] + "_"
	}
	queryStat = stats.NewTaggedStat(statName+"processed", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	//The order of lock is very important. The migrateDSLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	jd.dsMigrationLock.RLock()
	jd.dsListLock.RLock()
	defer jd.dsMigrationLock.RUnlock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList(false)
	outJobs := make([]*JobT, 0)

	jd.assert(count >= 0, fmt.Sprintf("count:%d received is less than 0", count))
	if count == 0 {
		return outJobs
	}

	for _, ds := range dsList {
		//count==0 means return all which we don't want
		jd.assert(count > 0, fmt.Sprintf("count:%d is less than or equal to 0", count))
		jobs := jd.getProcessedJobsDS(ds, false, count, params)
		outJobs = append(outJobs, jobs...)
		count -= len(jobs)
		jd.assert(count >= 0, fmt.Sprintf("count:%d after subtracting len(jobs):%d is less than 0", count, len(jobs)))
		if count == 0 {
			break
		}
	}

	return outJobs
}

/*
GetToRetry returns events which need to be retried.
This is a wrapper over GetProcessed call above
*/
func (jd *HandleT) GetToRetry(params GetQueryParamsT) []*JobT {
	params.StateFilters = []string{Failed.State}
	return jd.GetProcessed(params)
}

/*
GetWaiting returns events which are under processing
This is a wrapper over GetProcessed call above
*/
func (jd *HandleT) GetWaiting(params GetQueryParamsT) []*JobT {
	params.StateFilters = []string{Waiting.State}
	return jd.GetProcessed(params)
}

/*
GetThrottled returns events which were throttled before
This is a wrapper over GetProcessed call above
*/
func (jd *HandleT) GetThrottled(params GetQueryParamsT) []*JobT {
	params.StateFilters = []string{Throttled.State}
	return jd.GetProcessed(params)
}

/*
GetExecuting returns events which  in executing state
*/
func (jd *HandleT) GetExecuting(params GetQueryParamsT) []*JobT {
	params.StateFilters = []string{Executing.State}
	return jd.GetProcessed(params)
}

/*
DeleteExecuting deletes events whose latest job state is executing.
This is only done during recovery, which happens during the server start.
*/
func (jd *HandleT) DeleteExecuting(params GetQueryParamsT) {
	params.StateFilters = []string{Executing.State}
	jd.DeleteJobStatus(params)
}

/*
CheckPGHealth returns health check for pg database
*/
func (jd *HandleT) CheckPGHealth() bool {
	rows, err := jd.dbHandle.Query(`SELECT 'Rudder DB Health Check'::text as message`)
	if err != nil {
		fmt.Println(err)
		return false
	}
	defer rows.Close()
	return true
}

func (jd *HandleT) GetLastJobID() int64 {
	jd.dsListLock.RLock()
	dsList := jd.getDSList(false)
	jd.dsListLock.RUnlock()
	return jd.GetMaxIDForDs(dsList[len(dsList)-1])
}
