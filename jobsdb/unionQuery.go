//go:generate mockgen -destination=../mocks/jobsdb/mock_unionQuery.go -package=mocks_jobsdb github.com/rudderlabs/rudder-server/jobsdb MultiTenantJobsDB

package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

type MultiTenantHandleT struct {
	*HandleT
}

type MultiTenantJobsDB interface {
	GetAllJobs(map[string]int, GetQueryParamsT, int) []*JobT

	WithUpdateSafeTx(func(tx UpdateSafeTx) error) error
	UpdateJobStatusInTx(tx UpdateSafeTx, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error
	UpdateJobStatus(statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error

	DeleteExecuting()

	GetJournalEntries(opType string) (entries []JournalEntryT)
	JournalMarkStart(opType string, opPayload json.RawMessage) int64
	JournalDeleteEntry(opID int64)
	GetPileUpCounts(map[string]map[string]int)
}

func (*MultiTenantHandleT) getSingleWorkspaceQueryString(workspace string, jobsLimit int, payloadLimit int64) string {
	var sqlStatement string

	//some stats
	orderQuery := " ORDER BY jobs.job_id"
	limitQuery := fmt.Sprintf(" LIMIT %d ", jobsLimit)

	sqlStatement = fmt.Sprintf(
		`SELECT
			jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count,
			jobs.created_at, jobs.expire_at, jobs.workspace_id,
			jobs.payload_size,
			sum(jobs.payload_size) over (order by jobs.job_id) as running_payload_size,
			jobs.job_state, jobs.attempt,
			jobs.exec_time, jobs.retry_time,
			jobs.error_code, jobs.error_response, jobs.status_parameters
		FROM
			%[1]s
			AS jobs
		WHERE jobs.workspace_id='%[3]s' %[4]s %[2]s`,
		"rt_jobs_view", limitQuery, workspace, orderQuery)

	if payloadLimit > 0 {
		return `select * from (` + sqlStatement + fmt.Sprintf(`) subquery where subquery.running_payload_size - subquery.payload_size <= %d`, payloadLimit)
	}
	return sqlStatement
}

//All Jobs

func (mj *MultiTenantHandleT) GetAllJobs(workspaceCount map[string]int, params GetQueryParamsT, maxDSQuerySize int) []*JobT {

	//The order of lock is very important. The migrateDSLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	mj.dsMigrationLock.RLock()
	mj.dsListLock.RLock()
	defer mj.dsMigrationLock.RUnlock()
	defer mj.dsListLock.RUnlock()

	dsList := mj.getDSList(false)
	outJobs := make([]*JobT, 0)

	workspacePayloadLimitMap := make(map[string]int64)
	for workspace, count := range workspaceCount {
		percentage := math.Max(float64(count), 0) / float64(params.JobsLimit)
		payloadLimit := percentage * float64(params.PayloadSizeLimit)
		workspacePayloadLimitMap[workspace] = int64(payloadLimit)
	}

	var tablesQueried int
	params.StateFilters = []string{NotProcessed.State, Waiting.State, Failed.State}
	conditions := QueryConditions{
		IgnoreCustomValFiltersInQuery: params.IgnoreCustomValFiltersInQuery,
		CustomValFilters:              params.CustomValFilters,
		ParameterFilters:              params.ParameterFilters,
		StateFilters:                  params.StateFilters,
	}
	start := time.Now()
	for _, ds := range dsList {
		jobs := mj.getUnionDS(ds, workspaceCount, workspacePayloadLimitMap, conditions)
		outJobs = append(outJobs, jobs...)
		if len(jobs) > 0 {
			tablesQueried++
		}

		if len(workspaceCount) == 0 {
			break
		}
		if tablesQueried >= maxDSQuerySize {
			break
		}
	}

	mj.unionQueryTime.SendTiming(time.Since(start))

	mj.tablesQueriedStat.Gauge(tablesQueried)

	return outJobs
}

func (mj *MultiTenantHandleT) getUnionDS(ds dataSetT, workspaceCount map[string]int, workspacePayloadLimitMap map[string]int64, conditions QueryConditions) []*JobT {
	var jobList []*JobT
	queryString, workspacesToQuery := mj.getUnionQuerystring(workspaceCount, workspacePayloadLimitMap, ds, conditions)

	if len(workspacesToQuery) == 0 {
		return jobList
	}
	for _, workspace := range workspacesToQuery {
		mj.markClearEmptyResult(ds, workspace, conditions.StateFilters, conditions.CustomValFilters, conditions.ParameterFilters,
			willTryToSet, nil)
	}

	cacheUpdateByWorkspace := make(map[string]string)
	for _, workspace := range workspacesToQuery {
		cacheUpdateByWorkspace[workspace] = string(noJobs)
	}

	var rows *sql.Rows
	var err error

	stmt, err := mj.dbHandle.Prepare(queryString)
	mj.logger.Debug(queryString)
	mj.assertError(err)
	defer func(stmt *sql.Stmt) {
		err := stmt.Close()
		if err != nil {
			mj.logger.Errorf("failed to closed the sql statement: %s", err.Error())
		}
	}(stmt)

	rows, err = stmt.Query(getTimeNowFunc())
	mj.assertError(err)
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			mj.logger.Errorf("failed to closed the result of sql query: %s", err.Error())
		}
	}(rows)

	for rows.Next() {
		var job JobT
		var _null int

		var _nullJS sql.NullString
		var _nullA sql.NullInt64
		var _nullET sql.NullTime
		var _nullRT sql.NullTime
		var _nullEC sql.NullString
		// TODO: Last two columns in the data are not supposed to be of type string,
		// they are supposed to be of type RAWJSON but SQL does not give a nullable RAWJSON type.
		//So look for the possible scenarios
		var _nullER sql.NullString
		var _nullSP sql.NullString
		err = rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.EventCount, &job.CreatedAt, &job.ExpireAt, &job.WorkspaceId, &job.PayloadSize, &_null,
			&_nullJS, &_nullA, &_nullET, &_nullRT, &_nullEC, &_nullER, &_nullSP)

		mj.assertError(err)

		job.LastJobStatus = JobStatusT{}
		if _nullJS.Valid {
			job.LastJobStatus.JobState = _nullJS.String
		}
		if _nullA.Valid {
			job.LastJobStatus.AttemptNum = int(_nullA.Int64)
		}
		if _nullET.Valid {
			job.LastJobStatus.ExecTime = _nullET.Time
		}
		if _nullRT.Valid {
			job.LastJobStatus.RetryTime = _nullRT.Time
		}
		if _nullEC.Valid {
			job.LastJobStatus.ErrorCode = _nullEC.String
		}
		if _nullER.Valid {
			job.LastJobStatus.ErrorResponse = json.RawMessage(_nullER.String)
		}
		if _nullSP.Valid {
			job.LastJobStatus.Parameters = json.RawMessage(_nullSP.String)
		}

		jobList = append(jobList, &job)

		workspaceCount[job.WorkspaceId] -= 1
		if workspaceCount[job.WorkspaceId] == 0 {
			delete(workspaceCount, job.WorkspaceId)
			delete(workspacePayloadLimitMap, job.WorkspaceId)
		}

		// payload limit is enabled only if > 0
		if workspacePayloadLimitMap[job.WorkspaceId] > 0 {
			workspacePayloadLimitMap[job.WorkspaceId] -= job.PayloadSize // decrement
			if workspacePayloadLimitMap[job.WorkspaceId] <= 0 {          // there is a possibility for an overflow
				delete(workspaceCount, job.WorkspaceId)
				delete(workspacePayloadLimitMap, job.WorkspaceId)
			}
		}

		cacheUpdateByWorkspace[job.WorkspaceId] = string(hasJobs)
	}
	if err = rows.Err(); err != nil {
		mj.assertError(err)
	}

	//do cache stuff here
	_willTryToSet := willTryToSet
	for workspace, cacheUpdate := range cacheUpdateByWorkspace {
		mj.markClearEmptyResult(ds, workspace, conditions.StateFilters, conditions.CustomValFilters, conditions.ParameterFilters,
			cacheValue(cacheUpdate), &_willTryToSet)
	}

	return jobList
}

func (mj *MultiTenantHandleT) getUnionQuerystring(workspaceCount map[string]int, workspacePayloadLimitMap map[string]int64, ds dataSetT, conditions QueryConditions) (string, []string) {
	var queries, workspacesToQuery []string
	queryInitial := mj.getInitialSingleWorkspaceQueryString(ds, conditions, workspaceCount)

	for workspace, count := range workspaceCount {
		if mj.isEmptyResult(ds, workspace, conditions.StateFilters, conditions.CustomValFilters, conditions.ParameterFilters) {
			continue
		}
		if count <= 0 {
			mj.logger.Errorf("workspaceCount <= 0 (%d) for workspace: %s. Limiting at 0 jobs for this workspace.", count, workspace)
			continue
		}
		queries = append(queries, mj.getSingleWorkspaceQueryString(workspace, count, workspacePayloadLimitMap[workspace]))
		workspacesToQuery = append(workspacesToQuery, workspace)
	}

	return queryInitial + `(` + strings.Join(queries, `) UNION (`) + `)`, workspacesToQuery
}

func (mj *MultiTenantHandleT) getInitialSingleWorkspaceQueryString(ds dataSetT, conditions QueryConditions, workspaceCount map[string]int) string {
	customValFilters := conditions.CustomValFilters
	parameterFilters := conditions.ParameterFilters
	stateFilters := conditions.StateFilters
	var sqlStatement string

	//some stats
	workspaceArray := make([]string, 0)
	for workspace := range workspaceCount {
		workspaceArray = append(workspaceArray, "'"+workspace+"'")
	}
	workspaceString := "(" + strings.Join(workspaceArray, ", ") + ")"

	var stateQuery, customValQuery, limitQuery, sourceQuery string

	if len(stateFilters) > 0 {
		stateQuery = "AND (" + constructStateQuery("job_latest_state", "job_state", stateFilters, "OR") + ")"
	} else {
		stateQuery = ""
	}

	if len(customValFilters) > 0 && !conditions.IgnoreCustomValFiltersInQuery {
		// mj.assert(!getAll, "getAll is true")
		customValQuery = " AND " +
			constructQuery(mj, "jobs.custom_val", customValFilters, "OR")
	} else {
		customValQuery = ""
	}

	if len(parameterFilters) > 0 {
		// mj.assert(!getAll, "getAll is true")
		sourceQuery += " AND " + constructParameterJSONQuery("jobs", parameterFilters)
	} else {
		sourceQuery = ""
	}

	sqlStatement = fmt.Sprintf(
		`with rt_jobs_view AS (
			SELECT
				jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val,
				jobs.event_payload, jobs.event_count, jobs.created_at,
				jobs.expire_at, jobs.workspace_id,
				pg_column_size(jobs.event_payload) as payload_size,
				job_latest_state.job_state, job_latest_state.attempt,
				job_latest_state.exec_time, job_latest_state.retry_time,
				job_latest_state.error_code, job_latest_state.error_response,
				job_latest_state.parameters as status_parameters
			FROM
				%[1]s AS jobs
				LEFT JOIN (
					SELECT
						job_id, job_state, attempt, exec_time, retry_time,
						error_code, error_response, parameters
					FROM %[2]s
					WHERE
						id IN (
						SELECT MAX(id)
						from %[2]s
						GROUP BY job_id
						)
				) AS job_latest_state ON jobs.job_id = job_latest_state.job_id
			WHERE
				jobs.workspace_id IN %[7]s %[3]s %[4]s %[5]s %[6]s`,
		ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, limitQuery, workspaceString)
	return sqlStatement + ")"
}
