package jobsdb

import (
	"database/sql"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/logger"

	"time"

	"github.com/rudderlabs/rudder-server/services/stats"
)

//NOTE: Module name for logging: jobsdb.readonly-<prefix>
//To enable this module logging using rudder-cli, do the following
//rudder-cli logging -m=jobsdb.readonly-<prefix> -l=DEBUG

/*
ReadonlyJobsDB interface contains public methods to access JobsDB data
*/
type ReadonlyJobsDB interface {
	GetPendingJobsCount(customValFilters []string, count int, parameterFilters []ParameterFilterT) int64
	GetDSList() []dataSetT
}

type ReadonlyHandleT struct {
	DbHandle    *sql.DB
	tablePrefix string
	logger      logger.LoggerI
}

/*
Setup is used to initialize the ReadonlyHandleT structure.
*/
func (jd *ReadonlyHandleT) Setup(tablePrefix string) {
	jd.logger = pkgLogger.Child("readonly-" + tablePrefix)
	var err error
	psqlInfo := GetConnectionString()
	jd.tablePrefix = tablePrefix

	jd.DbHandle, err = sql.Open("postgres", psqlInfo)
	jd.assertError(err)

	err = jd.DbHandle.Ping()
	jd.assertError(err)

	jd.logger.Infof("Readonly user connected to %s DB", tablePrefix)
}

/*
TearDown releases all the resources
*/
func (jd *ReadonlyHandleT) TearDown() {
	jd.DbHandle.Close()
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
Function to return an ordered list of datasets and datasetRanges
Most callers use the in-memory list of dataset and datasetRanges
*/
func (jd *ReadonlyHandleT) GetDSList() []dataSetT {
	return getDSList(jd, jd.DbHandle, jd.tablePrefix)
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

	dsList := jd.GetDSList()
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
		sqlStatement += " AND " + constructQuery(jd, fmt.Sprintf("%s.custom_val", ds.JobTable),
			customValFilters, "OR")
	}

	if len(parameterFilters) > 0 {
		sqlStatement += " AND " + constructParameterJSONQuery(jd, ds.JobTable, parameterFilters)
	}

	if jd.tablePrefix == "gw" {
		sqlStatement = fmt.Sprintf("select sum(jsonb_array_length(batch)) from (%s) t", sqlStatement)
	}

	jd.logger.Debug(sqlStatement)

	row := jd.DbHandle.QueryRow(sqlStatement)
	var count sql.NullInt64
	err := row.Scan(&count)
	if err != nil && err != sql.ErrNoRows {
		jd.logger.Errorf("Returning 0 because failed to fetch unprocessed count from dataset: %v. Err: %w", ds, err)
		return 0
	}

	if count.Valid {
		return int64(count.Int64)
	}

	jd.logger.Debugf("Returning 0 because unprocessed count is invalid. This could be because there are no unprocessed jobs. Jobs table: %s. Query: %s", ds.JobTable, sqlStatement)
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

	dsList := jd.GetDSList()
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
	checkValidJobState(jd, stateFilters)

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
		stateQuery = " AND " + constructQuery(jd, "job_state", stateFilters, "OR")
	} else {
		stateQuery = ""
	}
	if len(customValFilters) > 0 {
		customValQuery = " AND " +
			constructQuery(jd, fmt.Sprintf("%s.custom_val", ds.JobTable),
				customValFilters, "OR")
	} else {
		customValQuery = ""
	}

	if len(parameterFilters) > 0 {
		sourceQuery += " AND " + constructParameterJSONQuery(jd, ds.JobTable, parameterFilters)
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

	jd.logger.Debug(sqlStatement)

	row := jd.DbHandle.QueryRow(sqlStatement, time.Now())
	var count sql.NullInt64
	err := row.Scan(&count)
	if err != nil && err != sql.ErrNoRows {
		jd.logger.Errorf("Returning 0 because failed to fetch processed count from dataset: %v. Err: %w", ds, err)
		return 0
	}

	if count.Valid {
		return int64(count.Int64)
	}

	jd.logger.Debugf("Returning 0 because processed count is invalid. This could be because there are no jobs in non terminal state. Jobs table: %s. Query: %s", ds.JobTable, sqlStatement)
	return 0
}
