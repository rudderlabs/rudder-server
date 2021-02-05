package jobsdb

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/rudderlabs/rudder-server/utils/logger"

	"time"

	"github.com/rudderlabs/rudder-server/services/stats"
)

/*
JobsDB interface contains public methods to access JobsDB data
*/
type ReadonlyJobsDB interface {
	GetPendingJobsCount(customValFilters []string, count int, parameterFilters []ParameterFilterT) int64
}

type ReadonlyHandleT struct {
	dbHandle    *sql.DB
	tablePrefix string
	logger      logger.LoggerI
	datasetList []dataSetT
	dsListLock  sync.RWMutex
}

/*
Setup is used to initialize the ReadonlyHandleT structure.
*/
func (jd *ReadonlyHandleT) Setup(tablePrefix string) {
	jd.logger = pkgLogger.Child("readonly-" + tablePrefix)
	var err error
	psqlInfo := GetConnectionString()
	jd.tablePrefix = tablePrefix

	jd.dbHandle, err = sql.Open("postgres", psqlInfo)
	jd.assertError(err)

	err = jd.dbHandle.Ping()
	jd.assertError(err)

	jd.logger.Infof("Readonly user connected to %s DB", tablePrefix)
}

/*
TearDown releases all the resources
*/
func (jd *ReadonlyHandleT) TearDown() {
	jd.dbHandle.Close()
}

//Some helper functions
func (jd *ReadonlyHandleT) assertError(err error) {
	if err != nil {
		panic(err)
	}
}

func (jd *ReadonlyHandleT) assert(cond bool, errorString string) {
	if !cond {
		panic(fmt.Errorf("[[ %s ]]: %s", jd.tablePrefix, errorString))
	}
}

/*
Function to sort table suffixes. We should not have any use case
for having > 2 len suffixes (e.g. 1_1_1 - see comment below)
but this sort handles the general case
*/
func (jd *ReadonlyHandleT) sortDnumList(dnumList []string) {
	sort.Slice(dnumList, func(i, j int) bool {
		src := strings.Split(dnumList[i], "_")
		dst := strings.Split(dnumList[j], "_")
		comparison, err := dsComparitor(src, dst)
		jd.assertError(err)
		return comparison
	})
}

//Function to get all table names form Postgres
func (jd *ReadonlyHandleT) getAllTableNames() []string {
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
func (jd *ReadonlyHandleT) getDSList(refreshFromDB bool) []dataSetT {

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

	jd.sortDnumList(dnumList)

	//If any service has crashed while creating DS, this may happen. Handling such case gracefully.
	if len(jobNameMap) != len(jobStatusNameMap) {
		jd.assert(len(jobNameMap) == len(jobStatusNameMap)+1, fmt.Sprintf("Length of jobNameMap(%d) - length of jobStatusNameMap(%d) is more than 1", len(jobNameMap), len(jobStatusNameMap)))
		deletedDNum := removeExtraKey(jobNameMap, jobStatusNameMap)
		//remove deletedDNum from dnumList
		var idx int
		var dnum string
		var foundDeletedDNum bool
		for idx, dnum = range dnumList {
			if dnum == deletedDNum {
				foundDeletedDNum = true
				break
			}
		}
		if foundDeletedDNum {
			dnumList = remove(dnumList, idx)
		}
	}

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

	return jd.datasetList
}

func (jd *ReadonlyHandleT) checkValidJobState(stateFilters []string) {
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

func (jd *ReadonlyHandleT) constructQuery(paramKey string, paramList []string, queryType string) string {
	jd.assert(queryType == "OR" || queryType == "AND", fmt.Sprintf("queryType:%s is neither OR nor AND", queryType))
	var queryList []string
	for _, p := range paramList {
		queryList = append(queryList, "("+paramKey+"='"+p+"')")
	}
	return "(" + strings.Join(queryList, " "+queryType+" ") + ")"
}

func (jd *ReadonlyHandleT) constructParameterJSONQuery(table string, parameterFilters []ParameterFilterT) string {
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
Count queries
*/
/*
GetPendingJobsCount returns the count of pending events. Pending events are
those whose jobs don't have a state or whose jobs status is neither succeeded nor aborted
*/
func (jd *ReadonlyHandleT) GetPendingJobsCount(customValFilters []string, count int, parameterFilters []ParameterFilterT) int64 {
	unProcessedCount := jd.getUnprocessedCount(customValFilters, parameterFilters)
	nonSucceededCount := jd.getNonSucceededJobsCount(customValFilters, parameterFilters)
	return unProcessedCount + nonSucceededCount
}

/*
GetUnprocessedCount returns the number of unprocessed events. Unprocessed events are
those whose state hasn't been marked in the DB
*/
func (jd *ReadonlyHandleT) getUnprocessedCount(customValFilters []string, parameterFilters []ParameterFilterT) int64 {
	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	queryStat = stats.NewTaggedStat(statName+"unprocessed_count", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	jd.dsListLock.Lock()
	defer jd.dsListLock.Unlock()

	dsList := jd.getDSList(true)
	var totalCount int64
	for _, ds := range dsList {
		count := jd.getUnprocessedJobsDSCount(ds, customValFilters, parameterFilters)
		totalCount += count
	}

	return totalCount
}

/*
stateFilters and customValFilters do a OR query on values passed in array
parameterFilters do a AND query on values included in the map
*/
func (jd *ReadonlyHandleT) getUnprocessedJobsDSCount(ds dataSetT, customValFilters []string, parameterFilters []ParameterFilterT) int64 {
	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	queryStat = stats.NewTaggedStat(statName+"unprocessed_jobs_count", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	var sqlStatement string

	var selectColumn string
	if jd.tablePrefix == "gw" {
		selectColumn = "event_payload->'batch' as batch"
	} else {
		selectColumn = "COUNT(*)"
	}
	sqlStatement = fmt.Sprintf(`SELECT %[3]s FROM %[1]s LEFT JOIN %[2]s ON %[1]s.job_id=%[2]s.job_id
											 WHERE %[2]s.job_id is NULL`, ds.JobTable, ds.JobStatusTable, selectColumn)

	if len(customValFilters) > 0 {
		sqlStatement += " AND " + jd.constructQuery(fmt.Sprintf("%s.custom_val", ds.JobTable),
			customValFilters, "OR")
	}

	if len(parameterFilters) > 0 {
		sqlStatement += " AND " + jd.constructParameterJSONQuery(ds.JobTable, parameterFilters)
	}

	if jd.tablePrefix == "gw" {
		sqlStatement = fmt.Sprintf("select sum(jsonb_array_length(batch)) from (%s) t", sqlStatement)
	}

	row := jd.dbHandle.QueryRow(sqlStatement)
	var count sql.NullInt64
	err := row.Scan(&count)
	if err != nil && err != sql.ErrNoRows {
		pkgLogger.Errorf("Returning 0 because failed to fetch unprocessed count from dataset: %v. Err: %w", ds, err)
		return 0
	}

	if count.Valid {
		return int64(count.Int64)
	}

	pkgLogger.Errorf("Returning 0 because unprocessed count is invalid")
	return 0
}

/*
getNonSucceededJobsCount returns events which are not in terminal state.
This is a wrapper over GetProcessed call above
*/
func (jd *ReadonlyHandleT) getNonSucceededJobsCount(customValFilters []string, parameterFilters []ParameterFilterT) int64 {
	return jd.getProcessedCount([]string{Failed.State, Waiting.State, Throttled.State, Executing.State}, customValFilters, parameterFilters)
}

/*
getProcessedCount returns number of events of a given state.
*/
func (jd *ReadonlyHandleT) getProcessedCount(stateFilter []string, customValFilters []string, parameterFilters []ParameterFilterT) int64 {
	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	if len(stateFilter) > 0 {
		statName = statName + stateFilter[0] + "_"
	}
	queryStat = stats.NewTaggedStat(statName+"processed_count", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	jd.dsListLock.Lock()
	defer jd.dsListLock.Unlock()

	dsList := jd.getDSList(true)
	var totalCount int64
	for _, ds := range dsList {
		count := jd.getProcessedJobsDSCount(ds, stateFilter, customValFilters, parameterFilters)
		totalCount += count
	}

	return totalCount
}

/*
stateFilters and customValFilters do a OR query on values passed in array
parameterFilters do a AND query on values included in the map
*/
func (jd *ReadonlyHandleT) getProcessedJobsDSCount(ds dataSetT, stateFilters []string,
	customValFilters []string, parameterFilters []ParameterFilterT) int64 {
	jd.checkValidJobState(stateFilters)

	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	if len(stateFilters) > 0 {
		statName = statName + stateFilters[0] + "_"
	}
	queryStat = stats.NewTaggedStat(statName+"processed_jobs_count", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	var stateQuery, customValQuery, sourceQuery string

	if len(stateFilters) > 0 {
		stateQuery = " AND " + jd.constructQuery("job_state", stateFilters, "OR")
	} else {
		stateQuery = ""
	}
	if len(customValFilters) > 0 {
		customValQuery = " AND " +
			jd.constructQuery(fmt.Sprintf("%s.custom_val", ds.JobTable),
				customValFilters, "OR")
	} else {
		customValQuery = ""
	}

	if len(parameterFilters) > 0 {
		sourceQuery += " AND " + jd.constructParameterJSONQuery(ds.JobTable, parameterFilters)
	} else {
		sourceQuery = ""
	}

	var selectColumn string
	if jd.tablePrefix == "gw" {
		selectColumn = fmt.Sprintf("%[1]s.event_payload->'batch' as batch", ds.JobTable)
	} else {
		selectColumn = fmt.Sprintf("COUNT(%[1]s.job_id)", ds.JobTable)
	}
	sqlStatement := fmt.Sprintf(`SELECT %[6]s FROM
                                               %[1]s,
                                               (SELECT job_id, retry_time FROM %[2]s WHERE id IN
                                                   (SELECT MAX(id) from %[2]s GROUP BY job_id) %[3]s)
                                               AS job_latest_state
                                            WHERE %[1]s.job_id=job_latest_state.job_id
                                             %[4]s %[5]s
                                             AND job_latest_state.retry_time < $1`,
		ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, selectColumn)

	if jd.tablePrefix == "gw" {
		sqlStatement = fmt.Sprintf("select sum(jsonb_array_length(batch)) from (%s) t", sqlStatement)
	}

	row := jd.dbHandle.QueryRow(sqlStatement, time.Now())
	var count sql.NullInt64
	err := row.Scan(&count)
	if err != nil && err != sql.ErrNoRows {
		pkgLogger.Errorf("Returning 0 because failed to fetch processed count from dataset: %v. Err: %w", ds, err)
		return 0
	}

	if count.Valid {
		return int64(count.Int64)
	}

	pkgLogger.Errorf("Returning 0 because processed count is invalid")
	return 0
}
