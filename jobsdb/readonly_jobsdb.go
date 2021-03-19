package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

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
	GetUnprocessedCount(customValFilters []string, parameterFilters []ParameterFilterT) int64
	GetJobSummaryCount(arg string, prefix string) (string, error)
	GetLatestFailedJobs(arg string, prefix string) (string, error)
	GetJobIDsForUser(args []string) (string, error)
	GetFailedStatusErrorCodeCountsByDestination(args []string) (string, error)
	GetDSListString() (string, error)
	GetJobIDStatus(job_id string, prefix string) (string, error)
	GetJobByID(job_id string, prefix string) (string, error)
}

type ReadonlyHandleT struct {
	DbHandle    *sql.DB
	tablePrefix string
	logger      logger.LoggerI
}

type DSPair struct {
	JobTableName       string
	JobStatusTableName string
}

type EventStatusDetailed struct {
	Status        string
	SourceID      string
	DestinationID string
	CustomVal     string
	Count         int
}

type EventStatusStats struct {
	StatsNums []EventStatusDetailed
	DSList    string
}

type FailedJobs struct {
	JobID         int
	UserID        string
	CustomVal     string
	ExecTime      time.Time
	ErrorCode     string
	ErrorResponse string
}

type ErrorCodeCountsByDestination struct {
	Count         int
	ErrorCode     string
	Destination   string
	DestinationID string
}

type ErrorCodeCountStats struct {
	ErrorCodeCounts []ErrorCodeCountsByDestination
}
type FailedJobsStats struct {
	FailedNums []FailedJobs
}

type FailedStatusStats struct {
	FailedStatusStats []JobStatusT
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
func (jd *ReadonlyHandleT) getDSList() []dataSetT {
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
	unProcessedCount := jd.GetUnprocessedCount(customValFilters, parameterFilters)
	nonSucceededCount := jd.getNonSucceededJobsCount(customValFilters, parameterFilters)
	return unProcessedCount + nonSucceededCount
}

/*
GetUnprocessedCount returns the number of unprocessed events. Unprocessed events are
those whose state hasn't been marked in the DB
*/
func (jd *ReadonlyHandleT) GetUnprocessedCount(customValFilters []string, parameterFilters []ParameterFilterT) int64 {
	var queryStat stats.RudderStats
	statName := ""
	if len(customValFilters) > 0 {
		statName = statName + customValFilters[0] + "_"
	}
	queryStat = stats.NewTaggedStat(statName+"unprocessed_count", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	dsList := jd.getDSList()
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

	dsList := jd.getDSList()
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

func getStatusPrefix(jobPrefix string) string {
	var response string
	switch jobPrefix {
	case "gw_jobs_":
		response = "gw_job_status_"
	case "proc_error_jobs_":
		response = "proc_error_job_status_"
	case "gw":
		response = "gw_job_status_"
	case "proc_error":
		response = "proc_error_job_status_"
	case "rt":
		response = "rt_job_status_"
	case "brt":
		response = "batch_rt_job_status_"
	case "batch_rt":
		response = "batch_rt_job_status_"
	}

	return response
}

func getJobPrefix(prefix string) string {
	var response string
	switch prefix {
	case "gw_jobs_":
		response = "gw_jobs_"
	case "proc_error_jobs_":
		response = "proc_error_jobs_"
	case "gw":
		response = "gw_jobs_"
	case "proc_error":
		response = "proc_error_jobs_"
	case "rt":
		response = "rt_jobs_"
	case "brt":
		response = "batch_rt_jobs_"
	case "batch_rt":
		response = "batch_rt_jobs_"
	}

	return response
}

func (jd *ReadonlyHandleT) GetJobSummaryCount(arg string, prefix string) (string, error) {
	dsListArr := make([]DSPair, 0)
	argList := strings.Split(arg, ":")
	if argList[0] != "" {
		statusPrefix := getStatusPrefix(prefix)
		jobPrefix := getJobPrefix(prefix)
		dsListArr = append(dsListArr, DSPair{JobTableName: jobPrefix + argList[0], JobStatusTableName: statusPrefix + argList[0]})
	} else if argList[1] != "" {
		maxCount, err := strconv.Atoi(argList[1])
		if err != nil {
			return "", err
		}
		dsList := jd.getDSList()
		for index, ds := range dsList {
			if index < maxCount {
				dsListArr = append(dsListArr, DSPair{JobTableName: ds.JobTable, JobStatusTableName: ds.JobStatusTable})
			}
		}
	} else {
		dsList := jd.getDSList()
		dsListArr = append(dsListArr, DSPair{JobTableName: dsList[0].JobTable, JobStatusTableName: dsList[0].JobStatusTable})
	}
	eventStatusDetailed := make([]EventStatusDetailed, 0)
	eventStatusMap := make(map[string][]EventStatusDetailed)
	var dsString string
	for _, dsPair := range dsListArr {
		sqlStatement := fmt.Sprintf(`SELECT COUNT(*), 
     					%[1]s.parameters->'source_id' as source, 
     					%[1]s.custom_val ,%[1]s.parameters->'destination_id' as destination, 
     					job_latest_state.job_state
						FROM %[1]s 
     					LEFT JOIN 
      					(SELECT job_id, job_state, attempt, exec_time, retry_time,error_code, error_response FROM %[2]s 
						WHERE id IN (SELECT MAX(id) from %[2]s GROUP BY job_id)) AS job_latest_state
     					ON %[1]s.job_id=job_latest_state.job_id GROUP BY job_latest_state.job_state,%[1]s.parameters->'source_id',%[1]s.parameters->'destination_id', %[1]s.custom_val;`, dsPair.JobTableName, dsPair.JobStatusTableName)
		row, err := jd.DbHandle.Query(sqlStatement)
		if err != nil {
			return "", err
		}
		dsString = dsPair.JobTableName + "    " + dsString
		defer row.Close()
		for row.Next() {
			event := EventStatusDetailed{}
			var destinationID sql.NullString
			var status sql.NullString
			err = row.Scan(&event.Count, &event.SourceID, &event.CustomVal, &destinationID, &status)
			if err != nil {
				return "", err
			}
			if destinationID.Valid {
				event.DestinationID = destinationID.String
			}
			if status.Valid {
				event.Status = status.String
			}
			if _, ok := eventStatusMap[event.SourceID+":"+event.DestinationID]; ok {
				eventStatusMap[event.SourceID+":"+event.DestinationID] = append(eventStatusMap[event.SourceID+":"+event.DestinationID], event)
			} else {
				eventStatusMap[event.SourceID+":"+event.DestinationID] = make([]EventStatusDetailed, 0)
				eventStatusMap[event.SourceID+":"+event.DestinationID] = append(eventStatusMap[event.SourceID+":"+event.DestinationID], event)
			}
		}
	}
	for _, val := range eventStatusMap {
		eventStatusDetailed = append(eventStatusDetailed, val...)
	}
	response, err := json.MarshalIndent(EventStatusStats{StatsNums: eventStatusDetailed, DSList: dsString}, "", " ")
	if err != nil {
		return "", err
	}
	return string(response), nil
}

func (jd *ReadonlyHandleT) GetLatestFailedJobs(arg string, prefix string) (string, error) {
	var dsList DSPair
	argList := strings.Split(arg, ":")
	if argList[0] != "" {
		statusPrefix := getStatusPrefix(prefix)
		jobPrefix := getJobPrefix(prefix)
		dsList = DSPair{JobTableName: jobPrefix + argList[0], JobStatusTableName: statusPrefix + argList[0]}
	} else {
		dsListTotal := jd.getDSList()
		dsList = DSPair{JobTableName: dsListTotal[0].JobTable, JobStatusTableName: dsListTotal[0].JobStatusTable}
	}
	sqlStatement := fmt.Sprintf(`SELECT %[1]s.job_id, %[1]s.user_id, %[1]s.custom_val,
					job_latest_state.exec_time,
					job_latest_state.error_code, job_latest_state.error_response
					FROM %[1]s,
					(SELECT job_id, job_state, attempt, exec_time, retry_time,error_code, error_response FROM %[2]s WHERE id IN
					(SELECT MAX(id) from %[2]s GROUP BY job_id) AND (job_state = 'failed'))
					AS job_latest_state
 					WHERE %[1]s.job_id=job_latest_state.job_id
  					`, dsList.JobTableName, dsList.JobStatusTableName)
	if argList[1] != "" {
		sqlStatement = sqlStatement + fmt.Sprintf(`AND %[1]s.custom_val = '%[2]s'`, dsList.JobTableName, argList[1])
	}
	sqlStatement = sqlStatement + fmt.Sprintf(`ORDER BY %[1]s.job_id desc LIMIT 5;`, dsList.JobTableName)
	row, err := jd.DbHandle.Query(sqlStatement)
	if err != nil {
		return "", err
	}
	failedJobsDetiled := make([]FailedJobs, 0)

	defer row.Close()
	for row.Next() {
		event := FailedJobs{}
		var statusCode sql.NullString
		err = row.Scan(&event.JobID, &event.UserID, &event.CustomVal, &event.ExecTime, &statusCode, &event.ErrorResponse)
		if err != nil {
			return "", err
		}
		if statusCode.Valid {
			event.ErrorCode = statusCode.String
		}
		failedJobsDetiled = append(failedJobsDetiled, event)
	}
	response, err := json.MarshalIndent(FailedJobsStats{failedJobsDetiled}, "", " ")
	if err != nil {
		return "", err
	}
	return string(response), nil
}

func (jd *ReadonlyHandleT) GetJobByID(job_id string, prefix string) (string, error) {
	dsListTotal := jd.getDSList()
	var response []byte
	for _, dsPair := range dsListTotal {
		var min, max sql.NullInt32
		sqlStatement := fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %s`, dsPair.JobTable)
		row := jd.DbHandle.QueryRow(sqlStatement)
		err := row.Scan(&min, &max)
		if err != nil {
			return "", err
		}
		if !min.Valid || !max.Valid {
			continue
		}
		jobId, err := strconv.Atoi(job_id)
		if err != nil {
			return "", err
		}
		if jobId < int(min.Int32) || jobId > int(max.Int32) {
			continue
		}
		sqlStatement = fmt.Sprintf(`SELECT
						%[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload,
						%[1]s.created_at, %[1]s.expire_at,
						job_latest_state.job_state, job_latest_state.attempt,
						job_latest_state.exec_time, job_latest_state.retry_time,
						job_latest_state.error_code, job_latest_state.error_response
					FROM
						%[1]s
					LEFT JOIN 
						(SELECT job_id, job_state, attempt, exec_time, retry_time,
						error_code, error_response FROM %[2]s WHERE id IN
							(SELECT MAX(id) from %[2]s GROUP BY job_id))
						AS job_latest_state
					ON %[1]s.job_id=job_latest_state.job_id
					WHERE %[1]s.job_id = %[3]s;`, dsPair.JobTable, dsPair.JobStatusTable, job_id)

		event := JobT{}
		row = jd.DbHandle.QueryRow(sqlStatement)
		err = row.Scan(&event.JobID, &event.UUID, &event.UserID, &event.Parameters, &event.CustomVal, &event.EventPayload,
			&event.CreatedAt, &event.ExpireAt, &event.LastJobStatus.JobState, &event.LastJobStatus.AttemptNum,
			&event.LastJobStatus.ExecTime, &event.LastJobStatus.RetryTime, &event.LastJobStatus.ErrorCode,
			&event.LastJobStatus.ErrorResponse)
		if err != nil {
			sqlStatement = fmt.Sprintf(`SELECT
						%[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val, %[1]s.event_payload,
						%[1]s.created_at, %[1]s.expire_at
					FROM
						%[1]s
					WHERE %[1]s.job_id = %[2]s;`, dsPair.JobTable, job_id)
			row = jd.DbHandle.QueryRow(sqlStatement)
			err1 := row.Scan(&event.JobID, &event.UUID, &event.UserID, &event.Parameters, &event.CustomVal, &event.EventPayload,
				&event.CreatedAt, &event.ExpireAt)
			if err1 != nil {
				return "", err1
			}
		}
		response, err = json.MarshalIndent(event, "", " ")
		if err != nil {
			return "", err
		}
	}
	return string(response), nil
}

func (jd *ReadonlyHandleT) GetJobIDStatus(job_id string, prefix string) (string, error) {
	dsListTotal := jd.getDSList()
	var response []byte
	for _, dsPair := range dsListTotal {
		var min, max sql.NullInt32
		sqlStatement := fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %s`, dsPair.JobTable)
		row := jd.DbHandle.QueryRow(sqlStatement)
		err := row.Scan(&min, &max)
		if err != nil {
			return "", err
		}
		if !min.Valid || !max.Valid {
			continue
		}
		jobId, err := strconv.Atoi(job_id)
		if err != nil {
			return "", err
		}
		if jobId < int(min.Int32) || jobId > int(max.Int32) {
			continue
		}
		sqlStatement = fmt.Sprintf(`SELECT job_id, job_state, attempt, exec_time, retry_time,error_code, error_response FROM %[1]s WHERE job_id = %[2]s;`, dsPair.JobStatusTable, job_id)
		var statusCode sql.NullString
		eventList := []JobStatusT{}
		rows, err := jd.DbHandle.Query(sqlStatement)
		if err != nil {
			return "", err
		}
		defer rows.Close()
		for rows.Next() {
			event := JobStatusT{}
			err = rows.Scan(&event.JobID, &event.JobState, &event.AttemptNum, &event.ExecTime, &event.RetryTime, &statusCode, &event.ErrorResponse)
			if err != nil {
				return "", nil
			}
			if statusCode.Valid {
				event.ErrorCode = statusCode.String
			}
			eventList = append(eventList, event)
		}
		response, err = json.MarshalIndent(FailedStatusStats{FailedStatusStats: eventList}, "", " ")
		if err != nil {
			return "", err
		}
	}
	return string(response), nil
}

func (jd *ReadonlyHandleT) GetJobIDsForUser(args []string) (string, error) {
	dsListTotal := jd.getDSList()
	var response string
	for _, dsPair := range dsListTotal {
		jobId1, err := strconv.Atoi(args[2])
		if err != nil {
			return "", err
		}
		_, err = strconv.Atoi(args[3])
		if err != nil {
			return "", err
		}
		userID := args[4]
		if userID == "" {
			return "", nil
		}
		var min, max sql.NullInt32
		sqlStatement := fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %s`, dsPair.JobTable)
		row := jd.DbHandle.QueryRow(sqlStatement)
		err = row.Scan(&min, &max)
		if err != nil {
			return "", err
		}
		if !min.Valid || !max.Valid {
			continue
		}
		if jobId1 < int(min.Int32) || jobId1 > int(max.Int32) {
			continue
		}
		sqlStatement = fmt.Sprintf(`SELECT job_id FROM %[1]s WHERE job_id >= %[2]s AND job_id <= %[3]s AND user_id = '%[4]s';`, dsPair.JobTable, args[2], args[3], userID)
		rows, err := jd.DbHandle.Query(sqlStatement)
		if err != nil {
			return "", err
		}
		defer rows.Close()
		for rows.Next() {
			var jobID string
			err = rows.Scan(&jobID)
			if err != nil {
				return "", err
			}
			response = response + jobID + "\n"
		}
	}
	return response, nil
}

func (jd *ReadonlyHandleT) GetFailedStatusErrorCodeCountsByDestination(args []string) (string, error) {
	var response []byte
	statusPrefix := getStatusPrefix(args[0])
	jobPrefix := getJobPrefix(args[0])
	dsList := DSPair{JobTableName: jobPrefix + args[2], JobStatusTableName: statusPrefix + args[2]}
	sqlStatement := fmt.Sprintf(`select count(*), a.error_code, a.custom_val, a.d from
	(select count(*), rt.job_id, st.error_code as error_code, rt.custom_val as custom_val,
		rt.parameters -> 'destination_id' as d from %[1]s rt inner join %[2]s st
		on st.job_id=rt.job_id where st.job_state in ('failed', 'aborted')
		group by rt.job_id, st.error_code, rt.custom_val, rt.parameters -> 'destination_id')
	as  a group by a.custom_val, a.error_code, a.d order by a.custom_val;`, dsList.JobTableName, dsList.JobStatusTableName)
	rows, err := jd.DbHandle.Query(sqlStatement)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	errorcount := ErrorCodeCountStats{}
	for rows.Next() {
		result := ErrorCodeCountsByDestination{}
		err = rows.Scan(&result.Count, &result.ErrorCode, &result.Destination, &result.DestinationID)
		if err != nil {
			return "", err
		}
		errorcount.ErrorCodeCounts = append(errorcount.ErrorCodeCounts, result)
	}
	response, err = json.MarshalIndent(errorcount, "", " ")
	if err != nil {
		return "", err
	}
	return string(response), nil
}

func (jd *ReadonlyHandleT) GetDSListString() (string, error) {
	var response string
	dsList := jd.getDSList()
	for _, ds := range dsList {
		response = response + ds.JobTable + "\n"
	}
	return response, nil
}
