//go:generate mockgen -destination=../mocks/jobsdb/mock_unionQuery.go -package=mocks_jobsdb github.com/rudderlabs/rudder-server/jobsdb MultiTenantJobsDB

package jobsdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats"
)

type MultiTenantHandleT struct {
	*HandleT
}

type workspacePickup struct {
	Limit        int
	PayloadLimit int64
	AfterJobID   *int64
}

type (
	MoreToken interface{}
	moreToken struct {
		afterJobIDs map[string]*int64
	}
)

type GetAllJobsResult struct {
	Jobs []*JobT
	More MoreToken
}

type MultiTenantJobsDB interface {
	GetAllJobs(context.Context, map[string]int, GetQueryParamsT, int, MoreToken) (*GetAllJobsResult, error)

	WithUpdateSafeTx(context.Context, func(tx UpdateSafeTx) error) error
	UpdateJobStatusInTx(ctx context.Context, tx UpdateSafeTx, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error
	UpdateJobStatus(ctx context.Context, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error

	DeleteExecuting()
	FailExecuting()

	GetJournalEntries(opType string) (entries []JournalEntryT)
	JournalMarkStart(opType string, opPayload json.RawMessage) int64
	JournalDeleteEntry(opID int64)
	GetPileUpCounts(context.Context) (map[string]map[string]int, error)
}

func (*MultiTenantHandleT) getSingleWorkspaceQueryString(workspace string, jobsLimit int, payloadLimit int64, afterJobID *int64) string {
	var sqlStatement string

	// some stats
	orderQuery := " ORDER BY jobs.job_id"
	limitQuery := fmt.Sprintf(" LIMIT %d ", jobsLimit)
	var jobIDQuery string
	if afterJobID != nil {
		jobIDQuery = fmt.Sprintf(" AND jobs.job_id > %d ", *afterJobID)
	}

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
		WHERE jobs.workspace_id='%[3]s' %[5]s %[4]s %[2]s`,
		"rt_jobs_view", limitQuery, workspace, orderQuery, jobIDQuery)

	if payloadLimit > 0 {
		return `select * from (` + sqlStatement + fmt.Sprintf(`) subquery where subquery.running_payload_size - subquery.payload_size <= %d`, payloadLimit)
	}
	return sqlStatement
}

// GetAllJobs gets jobs from all workspaces according to the pickup map
func (mj *MultiTenantHandleT) GetAllJobs(ctx context.Context, pickup map[string]int, params GetQueryParamsT, maxDSQuerySize int, more MoreToken) (*GetAllJobsResult, error) { // skipcq: CRT-P0003

	mtoken := &moreToken{}
	if more != nil {
		var ok bool
		if mtoken, ok = more.(*moreToken); !ok {
			return nil, fmt.Errorf("invalid token: %v", more)
		}
	}
	wsPickup := make(map[string]*workspacePickup, len(pickup))
	for workspace, limit := range pickup {
		payloadPercentage := math.Max(float64(limit), 0) / float64(params.JobsLimit)
		payloadLimit := int64(payloadPercentage * float64(params.PayloadSizeLimit))
		wsPickup[workspace] = &workspacePickup{
			Limit:        limit,
			PayloadLimit: payloadLimit,
			AfterJobID:   mtoken.afterJobIDs[workspace],
		}
	}
	// The order of lock is very important. The migrateDSLoop
	// takes lock in this order so reversing this will cause
	// deadlocks
	if !mj.dsMigrationLock.RTryLockWithCtx(ctx) {
		return nil, fmt.Errorf("could not acquire a migration read lock: %w", ctx.Err())
	}
	defer mj.dsMigrationLock.RUnlock()

	if !mj.dsListLock.RTryLockWithCtx(ctx) {
		return nil, fmt.Errorf("could not acquire a dslist read lock: %w", ctx.Err())
	}
	defer mj.dsListLock.RUnlock()

	dsList := mj.getDSList()
	outJobs := make([]*JobT, 0)

	var tablesQueried int
	params.StateFilters = []string{NotProcessed.State, Waiting.State, Failed.State}
	conditions := QueryConditions{
		IgnoreCustomValFiltersInQuery: params.IgnoreCustomValFiltersInQuery,
		CustomValFilters:              params.CustomValFilters,
		ParameterFilters:              params.ParameterFilters,
		StateFilters:                  params.StateFilters,
	}
	mToken := &moreToken{
		afterJobIDs: make(map[string]*int64),
	}
	start := time.Now()
	for _, ds := range dsList {
		jobs, err := mj.getUnionDS(ctx, ds, wsPickup, conditions)
		if err != nil {
			return nil, err
		}
		outJobs = append(outJobs, jobs...)
		for i := range jobs {
			job := jobs[i]
			jobID := job.JobID
			if _, ok := mToken.afterJobIDs[job.WorkspaceId]; !ok || jobID > *mToken.afterJobIDs[job.WorkspaceId] {
				mToken.afterJobIDs[job.WorkspaceId] = &jobID
			}
		}
		if len(jobs) > 0 {
			tablesQueried++
		}
		if len(wsPickup) == 0 {
			break
		}
		if tablesQueried >= maxDSQuerySize {
			break
		}
	}

	mj.unionQueryTime.SendTiming(time.Since(start))

	stats.Default.NewTaggedStat("tables_queried_gauge", stats.GaugeType, stats.Tags{
		"state":     "nonterminal",
		"query":     "union",
		"customVal": mj.tablePrefix,
	}).Gauge(tablesQueried)

	return &GetAllJobsResult{Jobs: outJobs, More: mToken}, nil
}

func (mj *MultiTenantHandleT) getUnionDS(ctx context.Context, ds dataSetT, pickup map[string]*workspacePickup, conditions QueryConditions) ([]*JobT, error) { // skipcq: CRT-P0003

	skipCache := map[string]struct{}{}
	for workspace, wp := range pickup {
		if wp.AfterJobID != nil {
			skipCache[workspace] = struct{}{}
		}
	}

	var jobList []*JobT
	queryString, workspacesToQuery := mj.getUnionQuerystring(pickup, ds, conditions)

	if len(workspacesToQuery) == 0 {
		return jobList, nil
	}
	for _, workspace := range workspacesToQuery {
		if _, ok := skipCache[workspace]; !ok {
			mj.markClearEmptyResult(ds, workspace, conditions.StateFilters, conditions.CustomValFilters, conditions.ParameterFilters,
				willTryToSet, nil)
		}
	}

	cacheUpdateByWorkspace := make(map[string]string)
	for _, workspace := range workspacesToQuery {
		cacheUpdateByWorkspace[workspace] = string(noJobs)
	}

	var rows *sql.Rows
	var err error

	stmt, err := mj.dbHandle.PrepareContext(ctx, queryString)
	mj.logger.Debug(queryString)
	if err != nil {
		return jobList, err
	}
	defer func(stmt *sql.Stmt) {
		err := stmt.Close()
		if err != nil {
			mj.logger.Errorf("failed to closed the sql statement: %s", err.Error())
		}
	}(stmt)

	rows, err = stmt.QueryContext(ctx, getTimeNowFunc())
	if err != nil {
		return jobList, err
	}
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
		// So look for the possible scenarios
		var _nullER sql.NullString
		var _nullSP sql.NullString
		err = rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.EventCount, &job.CreatedAt, &job.ExpireAt, &job.WorkspaceId, &job.PayloadSize, &_null,
			&_nullJS, &_nullA, &_nullET, &_nullRT, &_nullEC, &_nullER, &_nullSP)
		if err != nil {
			return jobList, err
		}

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
		if wsPickup := pickup[job.WorkspaceId]; wsPickup != nil {
			wsPickup.Limit -= 1
			if wsPickup.Limit == 0 {
				delete(pickup, job.WorkspaceId)
			}

			// payload limit is enabled only if > 0
			if wsPickup.PayloadLimit > 0 {
				wsPickup.PayloadLimit -= job.PayloadSize // decrement
				if wsPickup.PayloadLimit <= 0 {          // there is a possibility for an overflow
					delete(pickup, job.WorkspaceId)
				}
			}
		}
		cacheUpdateByWorkspace[job.WorkspaceId] = string(hasJobs)
	}
	if err = rows.Err(); err != nil {
		return jobList, err
	}

	// do cache stuff here
	_willTryToSet := willTryToSet
	for workspace, cacheUpdate := range cacheUpdateByWorkspace {
		if _, ok := skipCache[workspace]; !ok {
			mj.markClearEmptyResult(ds, workspace, conditions.StateFilters, conditions.CustomValFilters, conditions.ParameterFilters,
				cacheValue(cacheUpdate), &_willTryToSet)
		}
	}

	return jobList, err
}

func (mj *MultiTenantHandleT) getUnionQuerystring(pickup map[string]*workspacePickup, ds dataSetT, conditions QueryConditions) (string, []string) {
	var queries, workspacesToQuery []string
	queryInitial := mj.getInitialSingleWorkspaceQueryString(ds, conditions, pickup)

	for workspace, wp := range pickup {
		count := wp.Limit
		if mj.isEmptyResult(ds, workspace, conditions.StateFilters, conditions.CustomValFilters, conditions.ParameterFilters) {
			continue
		}
		if count <= 0 {
			mj.logger.Errorf("workspaceCount <= 0 (%d) for workspace: %s. Limiting at 0 jobs for this workspace.", wp, workspace)
			continue
		}
		queries = append(queries, mj.getSingleWorkspaceQueryString(workspace, wp.Limit, wp.PayloadLimit, wp.AfterJobID))
		workspacesToQuery = append(workspacesToQuery, workspace)
	}

	return queryInitial + `(` + strings.Join(queries, `) UNION (`) + `)`, workspacesToQuery
}

func (*MultiTenantHandleT) getInitialSingleWorkspaceQueryString(ds dataSetT, conditions QueryConditions, pickup map[string]*workspacePickup) string {
	customValFilters := conditions.CustomValFilters
	parameterFilters := conditions.ParameterFilters
	stateFilters := conditions.StateFilters
	var sqlStatement string

	// some stats
	workspaceArray := make([]string, 0)
	for workspace := range pickup {
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
		customValQuery = " AND " +
			constructQueryOR("jobs.custom_val", customValFilters)
	} else {
		customValQuery = ""
	}

	if len(parameterFilters) > 0 {
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
				"%[1]s" AS jobs
				LEFT JOIN "v_last_%[2]s" job_latest_state ON jobs.job_id = job_latest_state.job_id
			WHERE
				jobs.workspace_id IN %[7]s %[3]s %[4]s %[5]s %[6]s`,
		ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, limitQuery, workspaceString)
	return sqlStatement + ")"
}
