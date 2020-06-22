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

/*
JobsDB interface contains public methods to access JobsDB data
*/
type JobsDB interface {
	Store(jobList []*JobT)
	StoreWithRetryEach(jobList []*JobT) map[uuid.UUID]string
	CheckPGHealth() bool
	UpdateJobStatus(statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT)

	GetToRetry(customValFilters []string, count int, parameterFilters []ParameterFilterT) []*JobT
	GetUnprocessed(customValFilters []string, count int, parameterFilters []ParameterFilterT) []*JobT
	GetExecuting(customValFilters []string, count int, parameterFilters []ParameterFilterT) []*JobT
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
	dsForNewEvents  dataSetT
	dsForImport     dataSetT
	lastDsForExport dataSetT
	importLock      sync.RWMutex
	migrationMode   string
	fromVersion     int
	toVersion       int
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
	dsEmptyResultCache            map[dataSetT]map[string]map[string]map[string]bool
	dsCacheLock                   sync.Mutex
	BackupSettings                *BackupSettingsT
	jobsFileUploader              filemanager.FileManager
	statTableCount                stats.RudderStats
	statNewDSPeriod               stats.RudderStats
	isStatNewDSPeriodInitialized  bool
	statDropDSPeriod              stats.RudderStats
	isStatDropDSPeriodInitialized bool
	jobsdbQueryTimeStat           stats.RudderStats
	migrationState                MigrationState
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
	// for replay server, we are changing the gateway backup to false in main.go
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
		logger.Fatal(jd.dsEmptyResultCache)
		panic(err)
	}
}

func (jd *HandleT) assertErrorAndRollbackTx(err error, tx *sql.Tx) {
	if err != nil {
		tx.Rollback()
		jd.printLists(true)
		logger.Fatal(jd.dsEmptyResultCache)
		panic(err)
	}
}

func (jd *HandleT) assert(cond bool, errorString string) {
	if !cond {
		jd.printLists(true)
		logger.Fatal(jd.dsEmptyResultCache)
		panic(errorString)
	}
}

type jobStateT struct {
	isValid    bool
	isTerminal bool
	State      string
}

//State definitions
var (
	//Not valid, Not terminal
	NotProcessed = jobStateT{isValid: false, isTerminal: false, State: "NP"}

	//Valid, Not terminal
	Failed       = jobStateT{isValid: true, isTerminal: false, State: "failed"}
	Executing    = jobStateT{isValid: true, isTerminal: false, State: "executing"}
	Waiting      = jobStateT{isValid: true, isTerminal: false, State: "waiting"}
	WaitingRetry = jobStateT{isValid: true, isTerminal: false, State: "waiting_retry"}
	Migrating    = jobStateT{isValid: true, isTerminal: false, State: "migrating"}

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
	Migrating,
	Succeeded,
	Aborted,
	Migrated,
	WontMigrate,
}

func getValidStates() (validStates []string) {
	for _, js := range jobStates {
		if js.isValid {
			validStates = append(validStates, js.State)
		}
	}
	return
}

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

func (jd *HandleT) checkValidJobState(stateFilters []string) {
	jobStateMap := make(map[string]jobStateT)
	for _, js := range jobStates {
		jobStateMap[js.State] = js
	}
	for _, st := range stateFilters {
		js, ok := jobStateMap[st]
		jd.assert(ok, fmt.Sprintf("state %s is not found in jobStates: %v", st, jobStates))
		jd.assert(js.isValid, fmt.Sprintf("jobState : %v is not valid", js))
	}
}

var (
	host, user, password, dbname string
	port                         int
)

var (
	maxDSSize, maxMigrateOnce                  int
	maxTableSize                               int64
	jobDoneMigrateThres, jobStatusMigrateThres float64
	mainCheckSleepDuration                     time.Duration
	backupCheckSleepDuration                   time.Duration
	useJoinForUnprocessed                      bool
	backupRowsBatchSize                        int64
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

	/*Migration related parameters
	jobDoneMigrateThres: A DS is migrated when this fraction of the jobs have been processed
	jobStatusMigrateThres: A DS is migrated if the job_status exceeds this (* no_of_jobs)
	maxDSSize: Maximum size of a DS. The process which adds new DS runs in the background
			(every few seconds) so a DS may go beyond this size
	maxMigrateOnce: Maximum number of DSs that are migrated together into one destination
	mainCheckSleepDuration: How often is the loop (which checks for adding/migrating DS) run
	maxTableSizeInMB: Maximum Table size in MB
	*/
	jobDoneMigrateThres = config.GetFloat64("JobsDB.jobDoneMigrateThres", 0.8)
	jobStatusMigrateThres = config.GetFloat64("JobsDB.jobStatusMigrateThres", 5)
	maxDSSize = config.GetInt("JobsDB.maxDSSize", 100000)
	maxMigrateOnce = config.GetInt("JobsDB.maxMigrateOnce", 10)
	maxTableSize = (config.GetInt64("JobsDB.maxTableSizeInMB", 300) * 1000000)
	backupRowsBatchSize = config.GetInt64("JobsDB.backupRowsBatchSize", 10000)
	mainCheckSleepDuration = (config.GetDuration("JobsDB.mainCheckSleepDurationInS", time.Duration(2)) * time.Second)
	backupCheckSleepDuration = (config.GetDuration("JobsDB.backupCheckSleepDurationIns", time.Duration(2)) * time.Second)
	useJoinForUnprocessed = config.GetBool("JobsDB.useJoinForUnprocessed", true)

}

func init() {
	loadConfig()
}

func GetConnectionString() string {
	return fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

}

/*
Setup is used to initialize the HandleT structure.
clearAll = True means it will remove all existing tables
tablePrefix must be unique and is used to separate
multiple users of JobsDB
dsRetentionPeriod = A DS is not deleted if it has some activity
in the retention time
*/
func (jd *HandleT) Setup(clearAll bool, tablePrefix string, retentionPeriod time.Duration, migrationMode string) {

	var err error
	jd.migrationState.migrationMode = migrationMode
	psqlInfo := GetConnectionString()
	jd.assert(tablePrefix != "", "tablePrefix received is empty")
	jd.tablePrefix = tablePrefix
	jd.dsRetentionPeriod = retentionPeriod
	jd.dsEmptyResultCache = map[dataSetT]map[string]map[string]map[string]bool{}

	jd.BackupSettings = jd.getBackUpSettings()

	jd.dbHandle, err = sql.Open("postgres", psqlInfo)
	jd.assertError(err)

	err = jd.dbHandle.Ping()
	jd.assertError(err)

	logger.Infof("Connected to %s DB", tablePrefix)

	//Kill any pending queries
	jd.terminateQueries()

	jd.statTableCount = stats.NewStat(fmt.Sprintf("jobsdb.%s_tables_count", jd.tablePrefix), stats.GaugeType)
	jd.statNewDSPeriod = stats.NewStat(fmt.Sprintf("jobsdb.%s_new_ds_period", jd.tablePrefix), stats.TimerType)
	jd.statDropDSPeriod = stats.NewStat(fmt.Sprintf("jobsdb.%s_drop_ds_period", jd.tablePrefix), stats.TimerType)

	if clearAll {
		jd.dropAllDS()
		jd.dropJournal()
		jd.dropAllBackupDS()
		jd.dropMigrationCheckpointTables()
	}

	jd.setupDatabaseTables()

	jd.recoverFromJournal()

	//Refresh in memory list. We don't take lock
	//here because this is called before anything
	//else
	jd.getDSList(true)
	jd.getDSRangeList(true)

	//If no DS present, add one
	if len(jd.datasetList) == 0 {
		jd.addNewDS(appendToDsList, dataSetT{})
	}

	if jd.BackupSettings.BackupEnabled {
		jd.jobsFileUploader, err = jd.getFileUploader()
		jd.assertError(err)
		rruntime.Go(func() {
			jd.backupDSLoop()
		})
	}
	rruntime.Go(func() {
		jd.mainCheckLoop()
	})

}

/*
TearDown releases all the resources
*/
func (jd *HandleT) TearDown() {
	jd.dbHandle.Close()
}

/*
Function to sort table suffixes. We should not have any use case
for having > 2 len suffixes (e.g. 1_1_1 - see comment below)
but this sort handles the general case
*/
func (jd *HandleT) sortDnumList(dnumList []string) {
	sort.Slice(dnumList, func(i, j int) bool {
		src := strings.Split(dnumList[i], "_")
		dst := strings.Split(dnumList[j], "_")
		k := 0
		for {
			if k >= len(src) {
				//src has same prefix but is shorter
				//For example, src=1.1 while dest=1.1.1
				jd.assert(k < len(dst), fmt.Sprintf("k:%d >= len(dst):%d", k, len(dst)))
				jd.assert(k > 0, fmt.Sprintf("k:%d <= 0", k))
				return true
			}
			if k >= len(dst) {
				//Opposite of case above
				jd.assert(k > 0, fmt.Sprintf("k:%d <= 0", k))
				jd.assert(k < len(src), fmt.Sprintf("k:%d >= len(src):%d", k, len(src)))
				return false
			}
			if src[k] == dst[k] {
				//Loop
				k++
				continue
			}
			//Strictly ordered. Return
			srcInt, err := strconv.Atoi(src[k])
			jd.assertError(err)
			dstInt, err := strconv.Atoi(dst[k])
			jd.assertError(err)
			return srcInt < dstInt
		}
	})
}

//Function to get all table names form Postgres
func (jd *HandleT) getAllTableNames() []string {
	//Read the table names from PG
	stmt, err := jd.dbHandle.Prepare(`SELECT tablename
                                        FROM pg_catalog.pg_tables
                                        WHERE schemaname != 'pg_catalog' AND
                                        schemaname != 'information_schema'`)
	jd.assertError(err)
	defer stmt.Close()

	rows, err := stmt.Query()
	jd.assertError(err)
	defer rows.Close()

	tableNames := []string{}
	for rows.Next() {
		var tbName string
		err = rows.Scan(&tbName)
		jd.assertError(err)
		tableNames = append(tableNames, tbName)
	}

	return tableNames
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

	//Read the table names from PG
	tableNames := jd.getAllTableNames()

	//Tables are of form jobs_ and job_status_. Iterate
	//through them and sort them to produce and
	//ordered list of datasets

	jobNameMap := map[string]string{}
	jobStatusNameMap := map[string]string{}
	dnumList := []string{}

	for _, t := range tableNames {
		if strings.HasPrefix(t, jd.tablePrefix+"_jobs_") {
			dnum := t[len(jd.tablePrefix+"_jobs_"):]
			jobNameMap[dnum] = t
			dnumList = append(dnumList, dnum)
			continue
		}
		if strings.HasPrefix(t, jd.tablePrefix+"_job_status_") {
			dnum := t[len(jd.tablePrefix+"_job_status_"):]
			jobStatusNameMap[dnum] = t
			continue
		}
	}

	if len(dnumList) == 0 {
		return jd.datasetList
	}

	jd.sortDnumList(dnumList)

	//Create the structure
	for _, dnum := range dnumList {
		jobName, ok := jobNameMap[dnum]
		jd.assert(ok, fmt.Sprintf("dnum %s is not found in jobNameMap", dnum))
		jobStatusName, ok := jobStatusNameMap[dnum]
		jd.assert(ok, fmt.Sprintf("dnum %s is not found in jobStatusNameMap", dnum))
		jd.datasetList = append(jd.datasetList,
			dataSetT{JobTable: jobName,
				JobStatusTable: jobStatusName, Index: dnum})
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
		logger.Debug(sqlStatement, minID, maxID)
		//We store ranges EXCEPT for the last element
		//which is being actively written to.
		if idx < len(dsList)-1 {
			jd.assert(minID.Valid && maxID.Valid, fmt.Sprintf("minID.Valid: %v, maxID.Valid: %v. Either of them is false for table: %s", minID.Valid, maxID.Valid, ds.JobTable))
			jd.assert(idx == 0 || prevMax < minID.Int64, fmt.Sprintf("idx: %d != 0 and prevMax: %d >= minID.Int64: %v", idx, prevMax, minID.Int64))
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
	queryStat := stats.NewJobsDBStat("migration_ds_check", stats.TimerType, jd.tablePrefix)
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
		logger.Infof("[JobsDB] %s is full in size. Count: %v, Size: %v", ds.JobTable, jd.getTableRowCount(ds.JobTable), tableSize)
		return true
	}

	totalCount := jd.getTableRowCount(ds.JobTable)
	if totalCount > maxDSSize {
		logger.Infof("[JobsDB] %s is full by rows. Count: %v, Size: %v", ds.JobTable, totalCount, jd.getTableSize(ds.JobTable))
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

func (jd *HandleT) mapDSToLevel(ds dataSetT) (int, []int) {
	indexStr := strings.Split(ds.Index, "_")
	//Currently we don't have a scenario where we need more than 3 levels.
	jd.assert(len(indexStr) <= 3, fmt.Sprintf("len(indexStr): %d > 3", len(indexStr)))
	var (
		levelVals []int
		levelInt  int
		err       error
	)
	for _, str := range indexStr {
		levelInt, err = strconv.Atoi(str)
		jd.assertError(err)
		levelVals = append(levelVals, levelInt)
	}
	return len(levelVals), levelVals
}

func (jd *HandleT) createTableNames(dsIdx string) (string, string) {
	jobTable := fmt.Sprintf("%s_jobs_%s", jd.tablePrefix, dsIdx)
	jobStatusTable := fmt.Sprintf("%s_job_status_%s", jd.tablePrefix, dsIdx)
	return jobTable, jobStatusTable
}

func (jd *HandleT) addNewDS(newDSType string, insertBeforeDS dataSetT) dataSetT {
	var newDSIdx string
	appendLast := newDSType == appendToDsList

	queryStat := stats.NewJobsDBStat("add_new_ds", stats.TimerType, jd.tablePrefix)
	queryStat.Start()
	defer queryStat.End()

	switch newDSType {
	case appendToDsList:
		newDSIdx = jd.computeNewIdxForAppend()
	case insertForMigration:
		newDSIdx = jd.computeNewIdxForInsert(newDSType, insertBeforeDS)
	case insertForImport:
		newDSIdx = jd.computeNewIdxForInsert(newDSType, insertBeforeDS)
	default:
		panic("Unknown usage for newDSType : " + newDSType)

	}
	jd.assert(newDSIdx != "", fmt.Sprintf("newDSIdx is empty"))

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
	newDS := jd.createDS(newDSIdx)
	if appendLast {
		newDSWithSeqNumber := jd.setSequenceNumber(newDSIdx)
		return newDSWithSeqNumber
	}
	//This is the migration case. We don't yet update the in-memory list till
	//we finish the migration
	return newDS
}

func (jd *HandleT) computeNewIdxForAppend() string {
	dList := jd.getDSList(true)
	newDSIdx := ""
	if len(dList) == 0 {
		newDSIdx = "1"
	} else {
		//Last one can only be Level0
		levels, levelVals := jd.mapDSToLevel(dList[len(dList)-1])
		jd.assert(levels == 1, fmt.Sprintf("levels:%d != 1", levels))
		newDSIdx = fmt.Sprintf("%d", levelVals[0]+1)
	}
	return newDSIdx
}

func (jd *HandleT) computeNewIdxForInsert(newDSType string, insertBeforeDS dataSetT) string {
	logger.Infof("newDSType : %s, insertBeforeDS : %v", newDSType, insertBeforeDS)
	dList := jd.getDSList(true)
	logger.Infof("dlist in which we are trying to find %v is %v", insertBeforeDS, dList)
	newDSIdx := ""
	jd.assert(len(dList) > 0, fmt.Sprintf("len(dList): %d <= 0", len(dList)))
	for idx, ds := range dList {
		if ds.Index == insertBeforeDS.Index {
			levels, levelVals := jd.mapDSToLevel(ds)
			var (
				levelsPre    int
				levelPreVals []int
			)
			if idx == 0 && newDSType == insertForImport {
				logger.Infof("idx = 0 case with insertForImport and ds at idx0 is %v", ds)
				levelsPre = 1
				levelPreVals = []int{levelVals[0] - 1}
			} else {
				logger.Infof("ds to insert before found in dList is %v", ds)
				//We never insert before the first element
				jd.assert(idx > 0, fmt.Sprintf("idx: %d <= 0", idx))
				levelsPre, levelPreVals = jd.mapDSToLevel(dList[idx-1])
			}
			//Some sanity checks (see comment above)
			//Insert before is never required on level3 or above.
			jd.assert(levels <= 2, fmt.Sprintf("levels:%d  > 2", levels))
			if levelsPre == 1 {
				//dsPre.Index 		1
				//ds.Index 			2
				//The level0 must be different by one
				jd.assert(levelVals[0] == levelPreVals[0]+1, fmt.Sprintf("levelVals[0]:%d != (levelPreVals[0]:%d)+1", levelVals[0], levelPreVals[0]))
				newDSIdx = fmt.Sprintf("%d_%d", levelPreVals[0], 1)
			} else if levelsPre == 2 && levels == 1 {
				//dsPre.Index 		1_1
				//ds.Index 			2
				//The level0 must be different by one
				jd.assert(levelVals[0] == levelPreVals[0]+1, fmt.Sprintf("levelVals[0]:%d != (levelPreVals[0]:%d)+1", levelVals[0], levelPreVals[0]))
				newDSIdx = fmt.Sprintf("%d_%d", levelPreVals[0], levelPreVals[1]+1)
			} else if levelsPre == 2 && levels == 2 {
				jd.assert(levelVals[0] == 0 && levelPreVals[0] == 0, fmt.Sprintf("levelsPre:%d != 3", levelsPre))
				//dsPre.Index 		0_1
				//ds.Index 			0_2
				//The level1 must be different by one
				jd.assert(levelVals[1] == levelPreVals[1]+1, fmt.Sprintf("levelVals[0]:%d != (levelPreVals[0]:%d)+1", levelVals[0], levelPreVals[0]))
				newDSIdx = fmt.Sprintf("%d_%d_%d", levelPreVals[0], levelPreVals[1], 1)
			} else if levelsPre == 3 && levels == 2 {
				jd.assert(levelVals[0] == 0 && levelPreVals[0] == 0, fmt.Sprintf("levelsPre:%d != 3", levelsPre))
				//dsPre.Index 		0_1_2
				//ds.Index 			0_2
				//The level1 must be different by one
				jd.assert(levelVals[1] == levelPreVals[1]+1, fmt.Sprintf("levelVals[0]:%d != (levelPreVals[0]:%d)+1", levelVals[0], levelPreVals[0]))
				newDSIdx = fmt.Sprintf("%d_%d_%d", levelPreVals[0], levelPreVals[1], levelPreVals[2]+1)
			} else {
				logger.Infof("Unhandled scenario. levelsPre : %v and levels : %v", levelsPre, levels)
				jd.assert(false, fmt.Sprintf("Unexpected insert between %s and %s", dList[idx-1].Index, ds.Index))
			}
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

func (jd *HandleT) createDS(newDSIdx string) dataSetT {
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

	//We should not have range values for the last element (the new DS)
	jd.assert(len(dList) == len(dRangeList)+1, fmt.Sprintf("len(dList):%d != len(dRangeList):%d+1", len(dList), len(dRangeList)))

	//Now set the min JobID for the new DS just added to be 1 more than previous max
	if len(dRangeList) > 0 {
		newDSMin := dRangeList[len(dRangeList)-1].maxJobID
		jd.assert(newDSMin > 0, fmt.Sprintf("newDSMin:%d <= 0", newDSMin))
		sqlStatement := fmt.Sprintf(`SELECT setval('%s_jobs_%s_job_id_seq', %d)`,
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

func (jd *HandleT) terminateQueries() {
	sqlStatement := `SELECT pg_terminate_backend(pg_stat_activity.pid)
                           FROM pg_stat_activity
                         WHERE datname = current_database()
                            AND pid <> pg_backend_pid()`
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
}

func (jd *HandleT) getBackupDSList() []dataSetT {
	//Read the table names from PG
	tableNames := jd.getAllTableNames()

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
	tableNames := jd.getAllTableNames()

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
	queryStat := stats.NewJobsDBStat("migration_jobs", stats.TimerType, jd.tablePrefix)
	queryStat.Start()
	defer queryStat.End()
	//Unprocessed jobs
	unprocessedList, err := jd.getUnprocessedJobsDS(srcDS, []string{}, false, 0, nil)
	jd.assertError(err)

	//Jobs which haven't finished processing
	retryList, err := jd.getProcessedJobsDS(srcDS, true,
		getValidNonTerminalStates(), []string{}, 0, nil)

	jd.assertError(err)
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
	queryStat := stats.NewJobsDBStat("store_jobs", stats.TimerType, jd.tablePrefix)
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
	jd.markClearEmptyResult(ds, []string{}, []string{}, nil, false)
	// fmt.Println("Bursting CACHE")

	return nil
}

func (jd *HandleT) storeJobsDSWithRetryEach(ds dataSetT, copyID bool, jobList []*JobT) (errorMessagesMap map[uuid.UUID]string) {
	queryStat := stats.NewJobsDBStat("store_jobs", stats.TimerType, jd.tablePrefix)
	queryStat.Start()
	defer queryStat.End()

	err := jd.storeJobsDS(ds, copyID, jobList)
	if err == nil {
		return
	}

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
		stmt, err = txHandler.Prepare(pq.CopyIn(ds.JobTable, "job_id", "uuid", "user_id", "parameters", "custom_val",
			"event_payload", "created_at", "expire_at"))
		jd.assertError(err)
	} else {
		stmt, err = txHandler.Prepare(pq.CopyIn(ds.JobTable, "uuid", "user_id", "parameters", "custom_val", "event_payload"))
		jd.assertError(err)
	}

	defer stmt.Close()
	for _, job := range jobList {
		if copyID {
			_, err = stmt.Exec(job.JobID, job.UUID, job.UserID, job.Parameters, job.CustomVal,
				string(job.EventPayload), job.CreatedAt, job.ExpireAt)
		} else {
			_, err = stmt.Exec(job.UUID, job.UserID, job.Parameters, job.CustomVal, string(job.EventPayload))
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
		jd.markClearEmptyResult(ds, []string{}, []string{}, nil, false)
		// fmt.Println("Bursting CACHE")
		return
	}
	pqErr := err.(*pq.Error)
	errCode := string(pqErr.Code)
	if errCode == dbErrorMap["Invalid JSON"] || errCode == dbErrorMap["Invalid Unicode"] ||
		errCode == dbErrorMap["Invalid Escape Sequence"] || errCode == dbErrorMap["Invalid Escape Character"] {
		return errors.New("Invalid JSON")
	}
	jd.assertError(err)
	return
}

func (jd *HandleT) constructQuery(paramKey string, paramList []string, queryType string) string {
	jd.assert(queryType == "OR" || queryType == "AND", fmt.Sprintf("queryType:%s is neither OR nor AND", queryType))
	var queryList []string
	for _, p := range paramList {
		queryList = append(queryList, "("+paramKey+"='"+p+"')")
	}
	return "(" + strings.Join(queryList, " "+queryType+" ") + ")"
}

func (jd *HandleT) constructParameterJSONQuery(table string, parameterFilters []ParameterFilterT) string {
	// eg. query with optional destination_id (batch_rt_jobs_1.parameters @> '{"source_id":"<source_id>","destination_id":"<destination_id>"}'  OR (batch_rt_jobs_1.parameters @> '{"source_id":"<source_id>"}' AND batch_rt_jobs_1.parameters -> 'destination_id' IS NULL))
	var allKeyValues, mandatoryKeyValues, opNullConditions []string
	for _, parameter := range parameterFilters {
		allKeyValues = append(allKeyValues, fmt.Sprintf(`"%s":"%s"`, parameter.Name, parameter.Value))
		if parameter.Optional {
			opNullConditions = append(opNullConditions, fmt.Sprintf(`%s.parameters -> '%s' IS NULL`, table, parameter.Name))
		} else {
			mandatoryKeyValues = append(mandatoryKeyValues, fmt.Sprintf(`"%s":"%s"`, parameter.Name, parameter.Value))
		}
	}
	opQuery := ""
	if len(opNullConditions) > 0 {
		opQuery += fmt.Sprintf(` OR (%s.parameters @> '{%s}' AND %s)`, table, strings.Join(mandatoryKeyValues, ","), strings.Join(opNullConditions, " AND "))
	}
	return fmt.Sprintf(`(%s.parameters @> '{%s}' %s)`, table, strings.Join(allKeyValues, ","), opQuery)
}

/*
* If a query returns empty result for a specific dataset, we cache that so that
* future queries don't have to hit the DB.
* markClearEmptyResult() when mark=True marks dataset,customVal,state as empty.
* markClearEmptyResult() when mark=False clears a previous empty mark
 */

func (jd *HandleT) markClearEmptyResult(ds dataSetT, stateFilters []string, customValFilters []string, parameterFilters []ParameterFilterT, mark bool) {

	jd.dsCacheLock.Lock()
	defer jd.dsCacheLock.Unlock()

	//This means we want to mark/clear all customVals and stateFilters
	//When clearing, we remove the entire dataset entry. Not a big issue
	//We process ALL only during internal migration and caching empty
	//results is not important
	if len(stateFilters) == 0 || len(customValFilters) == 0 {
		if mark == false {
			delete(jd.dsEmptyResultCache, ds)
		}
		return
	}

	_, ok := jd.dsEmptyResultCache[ds]
	if !ok {
		jd.dsEmptyResultCache[ds] = map[string]map[string]map[string]bool{}
	}

	for _, cVal := range customValFilters {
		_, ok := jd.dsEmptyResultCache[ds][cVal]
		if !ok {
			jd.dsEmptyResultCache[ds][cVal] = map[string]map[string]bool{}
		}

		pVals := []string{}
		for _, parameterFilter := range parameterFilters {
			pVals = append(pVals, fmt.Sprintf(`%s_%s`, parameterFilter.Name, parameterFilter.Value))
		}
		sort.Strings(pVals)
		pVal := strings.Join(pVals, "_")

		_, ok = jd.dsEmptyResultCache[ds][cVal][pVal]
		if !ok {
			jd.dsEmptyResultCache[ds][cVal][pVal] = map[string]bool{}
		}

		for _, st := range stateFilters {
			if mark {
				jd.dsEmptyResultCache[ds][cVal][pVal][st] = true
			} else {
				jd.dsEmptyResultCache[ds][cVal][pVal][st] = false
			}
		}
	}
}

func (jd *HandleT) isEmptyResult(ds dataSetT, stateFilters []string, customValFilters []string, parameterFilters []ParameterFilterT) bool {

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
			if !ok || mark == false {
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
func (jd *HandleT) getProcessedJobsDS(ds dataSetT, getAll bool, stateFilters []string,
	customValFilters []string, limitCount int, parameterFilters []ParameterFilterT) ([]*JobT, error) {
	jd.checkValidJobState(stateFilters)

	if jd.isEmptyResult(ds, stateFilters, customValFilters, parameterFilters) {
		logger.Debugf("[getProcessedJobsDS] Empty cache hit for ds: %v, stateFilters: %v, customValFilters: %v, parameterFilters: %v", ds, stateFilters, customValFilters, parameterFilters)
		return []*JobT{}, nil
	}

	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	if len(stateFilters) > 0 {
		statName = statName + stateFilters[0] + "_"
	}
	queryStat = stats.NewJobsDBStat(statName+"processed_jobs", stats.TimerType, jd.tablePrefix)
	queryStat.Start()
	defer queryStat.End()

	var stateQuery, customValQuery, limitQuery, sourceQuery string

	if len(stateFilters) > 0 {
		stateQuery = " AND " + jd.constructQuery("job_state", stateFilters, "OR")
	} else {
		stateQuery = ""
	}
	if len(customValFilters) > 0 {
		jd.assert(!getAll, "getAll is true")
		customValQuery = " AND " +
			jd.constructQuery(fmt.Sprintf("%s.custom_val", ds.JobTable),
				customValFilters, "OR")
	} else {
		customValQuery = ""
	}

	if len(parameterFilters) > 0 {
		jd.assert(!getAll, "getAll is true")
		sourceQuery += " AND " + jd.constructParameterJSONQuery(ds.JobTable, parameterFilters)
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

	if len(jobList) == 0 {
		logger.Debugf("[getProcessedJobsDS] Setting empty cache for ds: %v, stateFilters: %v, customValFilters: %v, parameterFilters: %v", ds, stateFilters, customValFilters, parameterFilters)
		jd.markClearEmptyResult(ds, stateFilters, customValFilters, parameterFilters, true)
	}

	return jobList, nil
}

/*
count == 0 means return all
stateFilters and customValFilters do a OR query on values passed in array
parameterFilters do a AND query on values included in the map
*/
func (jd *HandleT) getUnprocessedJobsDS(ds dataSetT, customValFilters []string,
	order bool, count int, parameterFilters []ParameterFilterT) ([]*JobT, error) {
	if jd.isEmptyResult(ds, []string{NotProcessed.State}, customValFilters, parameterFilters) {
		logger.Debugf("[getUnprocessedJobsDS] Empty cache hit for ds: %v, stateFilters: NP, customValFilters: %v, parameterFilters: %v", ds, customValFilters, parameterFilters)
		return []*JobT{}, nil
	}

	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	queryStat = stats.NewJobsDBStat(statName+"unprocessed_jobs", stats.TimerType, jd.tablePrefix)
	queryStat.Start()
	defer queryStat.End()

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

	if len(customValFilters) > 0 {
		sqlStatement += " AND " + jd.constructQuery(fmt.Sprintf("%s.custom_val", ds.JobTable),
			customValFilters, "OR")
	}

	if len(parameterFilters) > 0 {
		sqlStatement += " AND " + jd.constructParameterJSONQuery(ds.JobTable, parameterFilters)
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

	if len(jobList) == 0 {
		logger.Debugf("[getUnprocessedJobsDS] Setting empty cache for ds: %v, stateFilters: NP, customValFilters: %v, parameterFilters: %v", ds, customValFilters, parameterFilters)
		jd.markClearEmptyResult(ds, []string{NotProcessed.State}, customValFilters, parameterFilters, true)
	}

	return jobList, nil
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
	jd.markClearEmptyResult(ds, stateFilters, customValFilters, parameterFilters, false)
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

func (jd *HandleT) mainCheckLoop() {

	for {
		time.Sleep(mainCheckSleepDuration)
		logger.Debug("Main check:Start")
		jd.dsListLock.RLock()
		dsList := jd.getDSList(false)
		jd.dsListLock.RUnlock()
		latestDS := dsList[len(dsList)-1]
		if jd.checkIfFullDS(latestDS) {
			//Adding a new DS updates the list
			//Doesn't move any data so we only
			//take the list lock
			jd.dsListLock.Lock()
			logger.Info("Main check:NewDS")
			jd.addNewDS(appendToDsList, dataSetT{})
			jd.dsListLock.Unlock()
		}

		//This block disables internal migration/consolidation while cluster-level migration is in progress
		if db.IsValidMigrationMode(jd.migrationState.migrationMode) {
			logger.Debugf("[[ MainCheckLoop ]]: migration mode = %s, so skipping internal migrations", jd.migrationState.migrationMode)
			continue
		}

		//Take the lock and run actual migration
		jd.dsMigrationLock.Lock()

		var migrateFrom []dataSetT
		var insertBeforeDS dataSetT
		var liveCount int
		for idx, ds := range dsList {
			ifMigrate, remCount := jd.checkIfMigrateDS(ds)
			logger.Debug("Migrate check", ifMigrate, ds)
			if idx < len(dsList)-1 && ifMigrate && idx < maxMigrateOnce && liveCount < maxDSSize {
				migrateFrom = append(migrateFrom, ds)
				insertBeforeDS = dsList[idx+1]
				liveCount += remCount
			} else {
				//We migrate from the leftmost onwards
				//If we cannot migrate one, we stop
				break
			}
		}
		migrationLoopStat := stats.NewJobsDBStat("migration_loop", stats.TimerType, jd.tablePrefix)
		migrationLoopStat.Start()
		//Add a temp DS to append to
		if len(migrateFrom) > 0 {
			if liveCount > 0 {
				jd.dsListLock.Lock()
				migrateTo := jd.addNewDS(insertForMigration, insertBeforeDS)
				jd.dsListLock.Unlock()

				logger.Info("Migrate from:", migrateFrom)
				logger.Info("Next:", insertBeforeDS)
				logger.Info("To:", migrateTo)
				//Mark the start of copy operation. If we fail here
				//we just delete the new DS being copied into. The
				//sources are still around

				opPayload, err := json.Marshal(&journalOpPayloadT{From: migrateFrom, To: migrateTo})
				jd.assertError(err)
				opID := jd.JournalMarkStart(migrateCopyOperation, opPayload)

				totalJobsMigrated := 0
				for _, ds := range migrateFrom {
					logger.Info("Main check:Migrate", ds, migrateTo)
					noJobsMigrated, _ := jd.migrateJobs(ds, migrateTo)
					totalJobsMigrated += noJobsMigrated
				}
				jd.assert(totalJobsMigrated > 0, "The number of jobs to migrate is 0 or less. Shouldn't be the case given we have a liveCount check")
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
		logger.Info("BackupDS check:Start")
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
		logger.Info("[JobsDB] ::  backupDS: starting backing up aborted")
		_, err := jd.backupTable(backupDSRange, false)
		if err != nil {
			logger.Errorf("[JobsDB] :: Failed to backup aborted jobs table %v. Err: %v", backupDSRange.ds.JobStatusTable, err)
			return false
		}
	} else {
		// write jobs table to JOBS_BACKUP_STORAGE_PROVIDER
		_, err := jd.backupTable(backupDSRange, false)
		if err != nil {
			logger.Errorf("[JobsDB] :: Failed to backup table %v. Err: %v", backupDSRange.ds.JobTable, err)
			return false
		}

		// write job_status table to JOBS_BACKUP_STORAGE_PROVIDER
		_, err = jd.backupTable(backupDSRange, true)
		if err != nil {
			logger.Errorf("[JobsDB] :: Failed to backup table %v. Err: %v", backupDSRange.ds.JobStatusTable, err)
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
	tableFileDumpTimeStat := stats.NewJobsDBStat("table_FileDump_TimeStat", stats.TimerType, jd.tablePrefix)
	tableFileDumpTimeStat.Start()
	totalTableDumpTimeStat := stats.NewJobsDBStat("total_TableDump_TimeStat", stats.TimerType, jd.tablePrefix)
	totalTableDumpTimeStat.Start()
	var tableName, path, pathPrefix, countStmt string
	backupPathDirName := "/rudder-s3-dumps/"
	tmpDirPath, err := misc.CreateTMPDIR()
	jd.assertError(err)

	// if backupOnlyAborted, process join of aborted rows of jobstatus with jobs table
	// else upload entire jobstatus and jobs table from pre_drop
	if jd.BackupSettings.FailedOnly {
		logger.Info("[JobsDB] :: backupTable: backing up aborted/failed entries")
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

	logger.Infof("[JobsDB] :: Backing up table: %v", tableName)

	var totalCount int64
	err = jd.dbHandle.QueryRow(countStmt).Scan(&totalCount)
	if err != nil {
		panic(err)
	}

	// return without doing anything as no jobs not present in ds
	if totalCount == 0 {
		//  Do not record stat for this case?
		logger.Infof("[JobsDB] ::  not processiong table dump as no rows match criteria. %v", tableName)
		return true, nil
	}

	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		panic(err)
	}

	gzWriter, err := misc.CreateGZ(path)
	defer os.Remove(path)

	var offset int64
	for {
		stmt := jd.getBackUpQuery(backupDSRange, isJobStatusTable, offset)
		var rawJSONRows json.RawMessage
		row := jd.dbHandle.QueryRow(stmt)
		err = row.Scan(&rawJSONRows)
		if err != nil {
			panic(err)
		}

		var rows []interface{}
		err = json.Unmarshal(rawJSONRows, &rows)
		if err != nil {
			panic(err)
		}
		contentSlice := make([][]byte, len(rows))
		for idx, row := range rows {
			rowBytes, err := json.Marshal(row)
			if err != nil {
				panic(err)
			}
			contentSlice[idx] = rowBytes
		}
		content := bytes.Join(contentSlice[:], []byte("\n"))
		gzWriter.Write(content)
		offset += backupRowsBatchSize
		if offset >= totalCount {
			break
		}
	}

	gzWriter.CloseGZ()
	tableFileDumpTimeStat.End()

	fileUploadTimeStat := stats.NewJobsDBStat("fileUpload_TimeStat", stats.TimerType, jd.tablePrefix)
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

	logger.Infof("[JobsDB] :: Uploading backup table to object storage: %v", tableName)
	var output filemanager.UploadOutput
	// get a file uploader
	fileUploader, errored := jd.getFileUploader()
	jd.assertError(errored)
	output, err = fileUploader.Upload(file, pathPrefixes...)

	if err != nil {
		storageProvider := config.GetEnv("JOBS_BACKUP_STORAGE_PROVIDER", "S3")
		logger.Errorf("[JobsDB] :: Failed to upload table %v dump to %s. Error: %s", tableName, storageProvider, err.Error())
		return false, err
	}

	// Do not record stat in error case as error case time might be low and skew stats
	fileUploadTimeStat.End()
	totalTableDumpTimeStat.End()
	logger.Infof("[JobsDB] :: Backed up table: %v at %v", tableName, output.Location)
	return true, nil
}

func (jd *HandleT) getBackupDSRange() dataSetRangeT {
	var backupDS dataSetT
	var backupDSRange dataSetRangeT

	//Read the table names from PG
	tableNames := jd.getAllTableNames()

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

	jd.sortDnumList(dnumList)

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

	sqlStatement := fmt.Sprintf(`INSERT INTO %s_journal (operation, done, operation_payload, start_time)
                                       VALUES ($1, $2, $3, $4) RETURNING id`, jd.tablePrefix)
	stmt, err := jd.dbHandle.Prepare(sqlStatement)
	jd.assertError(err)
	defer stmt.Close()

	var opID int64
	err = stmt.QueryRow(opType, false, opPayload, time.Now()).Scan(&opID)
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
	sqlStatement := fmt.Sprintf(`UPDATE %s_journal SET done=$2, end_time=$3 WHERE id=$1`, jd.tablePrefix)
	_, err := txHandler.Exec(sqlStatement, opID, true, time.Now())
	if err != nil {
		return err
	}
	return nil
}

func (jd *HandleT) JournalDeleteEntry(opID int64) {
	sqlStatement := fmt.Sprintf(`DELETE FROM %s_journal WHERE id=$1`, jd.tablePrefix)
	_, err := jd.dbHandle.Exec(sqlStatement, opID)
	jd.assertError(err)
}

func (jd *HandleT) GetJournalEntries(opType string) (entries []JournalEntryT) {
	sqlStatement := fmt.Sprintf(`SELECT id, operation, done, operation_payload
                                	FROM %s_journal
                                	WHERE
									done=False
									AND
									operation = '%s'
									ORDER BY id`, jd.tablePrefix, opType)
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

func (jd *HandleT) recoverFromCrash(goRoutineType string) {

	var opTypes []string
	switch goRoutineType {
	case mainGoRoutine:
		opTypes = []string{addDSOperation, migrateCopyOperation, postMigrateDSOperation, dropDSOperation}
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
                                	ORDER BY id`, jd.tablePrefix)

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
		jd.assert(opDone == false, "opDone is true")
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
		logger.Info("Recovering new DS operation", newDS)
		jd.dropDS(newDS, true)
	case migrateCopyOperation:
		migrateDest := opPayloadJSON.To
		//Delete the destination of the interrupted
		//migration. After we start, code should
		//redo the migration
		logger.Info("Recovering migrateCopy operation", migrateDest)
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
		logger.Info("Recovering migrateDel operation", migrateSrc)
		undoOp = false
	case backupDSOperation:
		jd.removeTableJSONDumps()
		logger.Info("Removing all stale json dumps of tables")
		undoOp = true
	case dropDSOperation, backupDropDSOperation:
		//Some of the source datasets would have been
		var dataset dataSetT
		json.Unmarshal(opPayload, &dataset)
		jd.dropDS(dataset, true)
		logger.Info("Recovering dropDS operation", dataset)
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
	mainGoRoutine   = "main"
	backupGoRoutine = "backup"
	migratorRoutine = "migrator"
)

func (jd *HandleT) recoverFromJournal() {
	jd.recoverFromCrash(mainGoRoutine)
	jd.recoverFromCrash(backupGoRoutine)
	if db.IsValidMigrationMode(jd.migrationState.migrationMode) {
		jd.recoverFromCrash(migratorRoutine)
	}
}

/*
UpdateJobStatus updates the status of a batch of jobs
customValFilters[] is passed so we can efficinetly mark empty cache
Later we can move this to query
*/
func (jd *HandleT) UpdateJobStatus(statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) {

	if len(statusList) == 0 {
		return
	}

	txn, err := jd.dbHandle.Begin()
	jd.assertError(err)

	updatedStatesByDS, err := jd.updateJobStatusInTxn(txn, statusList)
	jd.assertErrorAndRollbackTx(err, txn)

	err = txn.Commit()
	jd.assertError(err)
	for ds, stateList := range updatedStatesByDS {
		jd.markClearEmptyResult(ds, stateList, customValFilters, parameterFilters, false)
	}
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

	//The order of lock is very important. The mainCheckLoop
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
					logger.Debug("Range:", ds, statusList[lastPos].JobID,
						statusList[i-1].JobID, lastPos, i-1)
				}
				var updatedStates []string
				updatedStates, err = jd.updateJobStatusDSInTxn(txHandler, ds.ds, statusList[lastPos:i])
				if err != nil {
					return
				}
				updatedStatesByDS[ds.ds] = updatedStates
				lastPos = i
				break
			}
		}
		//Reached the end. Need to process this range
		if i == len(statusList) && lastPos < i {
			logger.Debug("Range:", ds, statusList[lastPos].JobID, statusList[i-1].JobID, lastPos, i)
			var updatedStates []string
			updatedStates, err = jd.updateJobStatusDSInTxn(txHandler, ds.ds, statusList[lastPos:i])
			if err != nil {
				return
			}
			updatedStatesByDS[ds.ds] = updatedStates
			lastPos = i
			break
		}
	}

	//The last (most active DS) might not have range element as it is being written to
	if lastPos < len(statusList) {
		//Make sure the last range is missing
		dsList := jd.getDSList(false)
		jd.assert(len(dsRangeList) == len(dsList)-1, fmt.Sprintf("len(dsRangeList):%d != len(dsList):%d-1", len(dsRangeList), len(dsList)))
		//Update status in the last element
		logger.Debug("RangeEnd", statusList[lastPos].JobID, lastPos, len(statusList))
		var updatedStates []string
		updatedStates, err = jd.updateJobStatusDSInTxn(txHandler, dsList[len(dsList)-1], statusList[lastPos:])
		if err != nil {
			return
		}
		updatedStatesByDS[dsList[len(dsList)-1]] = updatedStates
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
	logger.Debug("List:", jd.getDSList(false))
	logger.Debug("Ranges:", jd.getDSRangeList(false))
	if console {
		fmt.Println("List:", jd.getDSList(false))
		fmt.Println("Ranges:", jd.getDSRangeList(false))
	}

}

/*
GetUnprocessed returns the unprocessed events. Unprocessed events are
those whose state hasn't been marked in the DB
*/
func (jd *HandleT) GetUnprocessed(customValFilters []string, count int, parameterFilters []ParameterFilterT) []*JobT {

	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	queryStat = stats.NewJobsDBStat(statName+"unprocessed", stats.TimerType, jd.tablePrefix)
	queryStat.Start()
	defer queryStat.End()

	//The order of lock is very important. The mainCheckLoop
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
		jobs, err := jd.getUnprocessedJobsDS(ds, customValFilters, true, count, parameterFilters)
		jd.assertError(err)
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
GetProcessed returns events of a given state. This does not update any state itself and
relises on the caller to update it. That means that successive calls to GetProcessed("failed")
can return the same set of events. It is the responsibility of the caller to call it from
one thread, update the state (to "waiting") in the same thread and pass on the the processors
*/
func (jd *HandleT) GetProcessed(stateFilter []string, customValFilters []string, count int, parameterFilters []ParameterFilterT) []*JobT {

	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	if len(stateFilter) > 0 {
		statName = statName + stateFilter[0] + "_"
	}
	queryStat = stats.NewJobsDBStat(statName+"processed", stats.TimerType, jd.tablePrefix)
	queryStat.Start()
	defer queryStat.End()

	//The order of lock is very important. The mainCheckLoop
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
		jobs, err := jd.getProcessedJobsDS(ds, false, stateFilter, customValFilters, count, parameterFilters)
		jd.assertError(err)
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
func (jd *HandleT) GetToRetry(customValFilters []string, count int, parameterFilters []ParameterFilterT) []*JobT {
	return jd.GetProcessed([]string{Failed.State}, customValFilters, count, parameterFilters)
}

/*
GetWaiting returns events which are under processing
This is a wrapper over GetProcessed call above
*/
func (jd *HandleT) GetWaiting(customValFilters []string, count int, parameterFilters []ParameterFilterT) []*JobT {
	return jd.GetProcessed([]string{Waiting.State}, customValFilters, count, parameterFilters)
}

/*
GetExecuting returns events which  in executing state
*/
func (jd *HandleT) GetExecuting(customValFilters []string, count int, parameterFilters []ParameterFilterT) []*JobT {
	return jd.GetProcessed([]string{Executing.State}, customValFilters, count, parameterFilters)
}

/*
CheckPGHealth returns health check for pg database
*/
func (jd *HandleT) CheckPGHealth() bool {
	rows, err := jd.dbHandle.Query(fmt.Sprintf(`SELECT 'Rudder DB Health Check'::text as message`))
	if err != nil {
		fmt.Println(err)
		return false
	}
	defer rows.Close()
	return true
}
