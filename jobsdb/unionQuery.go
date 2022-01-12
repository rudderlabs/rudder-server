package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats"
)

const (
	selectQuery = `SELECT jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count, jobs.created_at, jobs.expire_at, jobs.workspaceid,jobs.running_event_counts `
)

type MultiTenantHandleT struct {
	HandleT
}

type MultiTenantJobsDB interface {
	GetToRetry(params GetQueryParamsT) []*JobT
	GetUnprocessed(params GetQueryParamsT) []*JobT

	GetUnprocessedUnion(map[string]int, GetQueryParamsT, int) []*JobT
	GetProcessedUnion(map[string]int, GetQueryParamsT, int) []*JobT
	GetUnion(map[string]int, GetQueryParamsT, int) []*JobT
	GetCustomerCounts(int) map[string]int

	GetImportingList(params GetQueryParamsT) []*JobT

	BeginGlobalTransaction() *sql.Tx
	CommitTransaction(*sql.Tx)
	AcquireUpdateJobStatusLocks()
	ReleaseUpdateJobStatusLocks()

	UpdateJobStatusInTxn(txHandler *sql.Tx, statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error
	UpdateJobStatus(statusList []*JobStatusT, customValFilters []string, parameterFilters []ParameterFilterT) error

	DeleteExecuting(params GetQueryParamsT)

	GetJournalEntries(opType string) (entries []JournalEntryT)
	JournalMarkStart(opType string, opPayload json.RawMessage) int64
	JournalDeleteEntry(opID int64)
	GetPileUpCounts(map[string]map[string]int)
}

func (mj *MultiTenantHandleT) GetPileUpCounts(statMap map[string]map[string]int) {
	mj.dsMigrationLock.RLock()
	mj.dsListLock.RLock()
	defer mj.dsMigrationLock.RUnlock()
	defer mj.dsListLock.RUnlock()

	dsList := mj.getDSList(true)
	for _, ds := range dsList {
		queryString := fmt.Sprintf(`with joined as (
			select
			  j.job_id as jobID,
			  j.custom_val as customVal,
			  s.id as statusID,
			  s.job_state as jobState,
			  j.workspaceid as customer
			from
			  %[1]s j
			  left join (
				select * from (select
					  *,
					  ROW_NUMBER() OVER(
						PARTITION BY rs.job_id
						ORDER BY
						  rs.id DESC
					  ) AS row_no
					FROM
					  %[2]s as rs) nq1
				  where
				  nq1.row_no = 1

			  ) s on j.job_id = s.job_id
			where
			  (
				s.job_state not in (
				  'executing', 'aborted', 'succeeded',
				  'migrated'
				)
				or s.job_id is null
			  )
		  )
		  select
			count(*),
			customVal,
			customer
		  from
			joined
		  group by
			customVal,
			customer;`, ds.JobTable, ds.JobStatusTable)
		rows, err := mj.dbHandle.Query(queryString)
		mj.assertError(err)

		for rows.Next() {
			var count sql.NullInt64
			var customVal string
			var customer string
			err := rows.Scan(&count, &customVal, &customer)
			mj.assertError(err)
			if _, ok := statMap[customer]; !ok {
				statMap[customer] = make(map[string]int)
			}
			statMap[customer][customVal] += int(count.Int64)
		}
		if err = rows.Err(); err != nil {
			mj.assertError(err)
		}
	}
}

//used to get pickup-counts during server start-up
func (mj *MultiTenantHandleT) GetCustomerCounts(defaultBatchSize int) map[string]int {
	customerCount := make(map[string]int)
	//just using first DS here
	//get all jobs all over DSs and then calculate
	//use DSListLock also

	mj.dsMigrationLock.RLock()
	mj.dsListLock.RLock()
	defer mj.dsMigrationLock.RUnlock()
	defer mj.dsListLock.RUnlock()
	rows, err := mj.dbHandle.Query(fmt.Sprintf(`select workspaceid, count(job_id) from %s group by workspaceid;`, mj.getDSList(false)[0].JobTable))
	mj.assertError(err)

	for rows.Next() {
		var customer string
		var count int
		err := rows.Scan(&customer, &count)
		mj.assertError(err)
		customerCount[customer] = count
	}
	if err = rows.Err(); err != nil {
		mj.assertError(err)
	}
	return customerCount
}

//Unprocessed

func (mj *MultiTenantHandleT) getUnprocessedUnionQuerystring(customerCount map[string]int, ds dataSetT, params GetQueryParamsT) (string, []string) {
	var queries, customersToQuery []string
	queryInitial := mj.getInitialSingleCustomerUnprocessedQueryString(ds, params, true, customerCount)
	for customer, count := range customerCount {
		//do cache stuff here
		if mj.isEmptyResult(ds, customer, []string{NotProcessed.State}, params.CustomValFilters, params.ParameterFilters) {
			continue
		}
		if count <= 0 {
			mj.logger.Errorf("customerCount < 0 (%d) for customer: %s. Limiting at 0 unprocessed jobs for this customer.", count, customer)
			continue
		}
		queries = append(queries, mj.getSingleCustomerUnprocessedQueryString(customer, count, ds, params, true))
		customersToQuery = append(customersToQuery, customer)
	}

	return queryInitial + `(` + strings.Join(queries, `) UNION (`) + `)`, customersToQuery
}

func (mj *MultiTenantHandleT) getInitialSingleCustomerUnprocessedQueryString(ds dataSetT, params GetQueryParamsT, order bool, customerCount map[string]int) string {
	customValFilters := params.CustomValFilters
	parameterFilters := params.ParameterFilters
	var sqlStatement string
	customerArray := make([]string, 0)
	for customer := range customerCount {
		customerArray = append(customerArray, "'"+customer+"'")
	}
	customerString := "(" + strings.Join(customerArray, ", ") + ")"
	sqlStatement = fmt.Sprintf(
		`with rt_jobs_view AS (
				SELECT
				  jobs.job_id,
				  jobs.uuid,
				  jobs.user_id,
				  jobs.parameters,
				  jobs.custom_val,
				  jobs.event_payload,
				  jobs.event_count,
				  jobs.created_at,
				  jobs.expire_at,
				  jobs.workspaceid ,
				  0 as running_event_counts

				FROM
				%[1]s jobs LEFT JOIN %[2]s AS job_status ON jobs.job_id = job_status.job_id
					WHERE
					job_status.job_id is NULL AND jobs.workspaceid IN %[3]s
			  `,
		ds.JobTable, ds.JobStatusTable, customerString)
	if len(customValFilters) > 0 && !params.IgnoreCustomValFiltersInQuery {
		sqlStatement += " AND " + constructQuery(mj, "jobs.custom_val", customValFilters, "OR")
	}

	if len(parameterFilters) > 0 {
		sqlStatement += " AND " + constructParameterJSONQuery("jobs", parameterFilters)
	}

	//avoinding AfterJobID for now

	if params.UseTimeFilter {
		sqlStatement += fmt.Sprintf(" AND created_at < %s", params.Before)
	}

	return sqlStatement + ")"
}

func (mj *MultiTenantHandleT) getSingleCustomerUnprocessedQueryString(customer string, count int, ds dataSetT, params GetQueryParamsT, order bool) string {
	var sqlStatement string
	// event_count default 1, number of items in payload
	sqlStatement = fmt.Sprintf(
		selectQuery+
			`FROM %[1]s AS jobs `+
			`WHERE jobs.workspaceid='%[2]s'`,
		"rt_jobs_view", customer)
	orderQuery := " ORDER BY jobs.job_id"
	sqlStatement += orderQuery
	if count >= 0 {
		sqlStatement += fmt.Sprintf(" LIMIT %d", count)
	}

	return sqlStatement
}

func (mj *MultiTenantHandleT) GetUnprocessedUnion(customerCount map[string]int, params GetQueryParamsT, maxDSQuerySize int) []*JobT {

	//add stats

	//The order of lock is very important. The migrateDSLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	mj.dsMigrationLock.RLock()
	mj.dsListLock.RLock()
	defer mj.dsMigrationLock.RUnlock()
	defer mj.dsListLock.RUnlock()

	dsList := mj.getDSList(false)
	outJobs := make([]*JobT, 0)

	//removed count assert, because that params.count is not used..?
	var tablesQueried int
	var tablesQueriedStat stats.RudderStats
	var queryTime stats.RudderStats
	queryTime = stats.NewTaggedStat("union_query_time", stats.TimerType, stats.Tags{
		"state":    "unprocessed",
		"module":   mj.tablePrefix,
		"destType": params.CustomValFilters[0],
	})

	start := time.Now()
	for _, ds := range dsList {
		jobs := mj.getUnprocessedUnionDS(ds, customerCount, params)
		outJobs = append(outJobs, jobs...)
		if len(jobs) != 0 {
			tablesQueried++
		}
		if len(customerCount) == 0 {
			break
		}
	}

	queryTime.SendTiming(time.Since(start))
	tablesQueriedStat = stats.NewTaggedStat("tables_queried_gauge", stats.GaugeType, stats.Tags{
		"state":    "unprocessed",
		"module":   mj.tablePrefix,
		"destType": params.CustomValFilters[0],
	})
	tablesQueriedStat.Gauge(tablesQueried)

	//PickUp stats
	var pickUpCountStat stats.RudderStats
	customerCountStat := make(map[string]int)

	for _, job := range outJobs {
		if _, ok := customerCountStat[job.WorkspaceId]; !ok {
			customerCountStat[job.WorkspaceId] = 0
		}
		customerCountStat[job.WorkspaceId] += 1
	}

	for customer, jobCount := range customerCountStat {
		pickUpCountStat = stats.NewTaggedStat("pick_up_count", stats.CountType, stats.Tags{
			"customer": customer,
			"state":    "unprocessed",
			"module":   mj.tablePrefix,
			"destType": params.CustomValFilters[0],
		})
		pickUpCountStat.Count(jobCount)
	}

	return outJobs
}

func (mj *MultiTenantHandleT) getUnprocessedUnionDS(ds dataSetT, customerCount map[string]int, params GetQueryParamsT) []*JobT {
	var jobList []*JobT
	queryString, customersToQuery := mj.getUnprocessedUnionQuerystring(customerCount, ds, params)
	if len(customersToQuery) == 0 {
		return jobList
	}

	for _, customer := range customersToQuery {
		mj.markClearEmptyResult(ds, customer, []string{NotProcessed.State}, params.CustomValFilters, params.ParameterFilters, willTryToSet, nil)
	}

	cacheUpdateByCustomer := make(map[string]string)
	for _, customer := range customersToQuery {
		cacheUpdateByCustomer[customer] = string(noJobs)
	}

	var rows *sql.Rows
	var err error

	mj.logger.Debug(queryString)
	rows, err = mj.dbHandle.Query(queryString)
	mj.assertError(err)

	defer rows.Close()

	for rows.Next() {
		var job JobT
		var _null int
		err := rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.EventCount, &job.CreatedAt, &job.ExpireAt, &job.WorkspaceId, &_null)
		mj.assertError(err)
		jobList = append(jobList, &job)

		customerCount[job.WorkspaceId] -= 1
		if customerCount[job.WorkspaceId] == 0 {
			delete(customerCount, job.WorkspaceId)
		}

		cacheUpdateByCustomer[job.WorkspaceId] = string(hasJobs)
	}
	if err = rows.Err(); err != nil {
		mj.assertError(err)
	}

	//do cache stuff here
	_willTryToSet := willTryToSet
	for customer, cacheUpdate := range cacheUpdateByCustomer {
		mj.markClearEmptyResult(ds, customer, []string{NotProcessed.State}, params.CustomValFilters, params.ParameterFilters, cacheValue(cacheUpdate), &_willTryToSet)
	}

	mj.printNumJobsByCustomer(jobList)
	return jobList
}

//Processed

func (mj *MultiTenantHandleT) GetProcessedUnion(customerCount map[string]int, params GetQueryParamsT, maxDSQuerySize int) []*JobT {

	//The order of lock is very important. The migrateDSLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	mj.dsMigrationLock.RLock()
	mj.dsListLock.RLock()
	defer mj.dsMigrationLock.RUnlock()
	defer mj.dsListLock.RUnlock()

	dsList := mj.getDSList(false)
	outJobs := make([]*JobT, 0)

	var tablesQueried int
	var tablesQueriedStat stats.RudderStats
	var queryTime stats.RudderStats
	queryTime = stats.NewTaggedStat("union_query_time", stats.TimerType, stats.Tags{
		"state":    "nonterminal",
		"module":   mj.tablePrefix,
		"destType": params.CustomValFilters[0],
	})

	start := time.Now()
	for _, ds := range dsList {
		jobs := mj.getProcessedUnionDS(ds, customerCount, params)
		outJobs = append(outJobs, jobs...)
		tablesQueried++
		if len(customerCount) == 0 {
			break
		}
	}

	queryTime.SendTiming(time.Since(start))
	tablesQueriedStat = stats.NewTaggedStat("tables_queried_gauge", stats.GaugeType, stats.Tags{
		"state":    "nonterminal",
		"module":   mj.tablePrefix,
		"destType": params.CustomValFilters[0],
	})
	tablesQueriedStat.Gauge(tablesQueried)

	//PickUp stats
	var pickUpCountStat stats.RudderStats
	customerCountStat := make(map[string]int)

	for _, job := range outJobs {
		if _, ok := customerCountStat[job.WorkspaceId]; !ok {
			customerCountStat[job.WorkspaceId] = 0
		}
		customerCountStat[job.WorkspaceId] += 1
	}

	for customer, jobCount := range customerCountStat {
		pickUpCountStat = stats.NewTaggedStat("pick_up_count", stats.CountType, stats.Tags{
			"customer": customer,
			"state":    params.StateFilters[0],
			"module":   mj.tablePrefix,
			"destType": params.CustomValFilters[0],
		})
		pickUpCountStat.Count(jobCount)
	}

	return outJobs
}

func (mj *MultiTenantHandleT) getProcessedUnionDS(ds dataSetT, customerCount map[string]int, params GetQueryParamsT) []*JobT {
	var jobList []*JobT
	queryString, customersToQuery := mj.getProcessedUnionQuerystring(customerCount, ds, params)

	if len(customersToQuery) == 0 {
		return jobList
	}

	for _, customer := range customersToQuery {
		mj.markClearEmptyResult(ds, customer, params.StateFilters, params.CustomValFilters, params.ParameterFilters, willTryToSet, nil)
	}

	cacheUpdateByCustomer := make(map[string]string)
	for _, customer := range customersToQuery {
		cacheUpdateByCustomer[customer] = string(noJobs)
	}

	var rows *sql.Rows
	var err error

	stmt, err := mj.dbHandle.Prepare(queryString)
	mj.logger.Debug(queryString)
	mj.assertError(err)
	defer stmt.Close()

	rows, err = stmt.Query(getTimeNowFunc())
	mj.assertError(err)
	defer rows.Close()

	for rows.Next() {
		var job JobT
		var _null int
		err := rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.EventCount, &job.CreatedAt, &job.ExpireAt, &job.WorkspaceId, &_null,
			&job.LastJobStatus.JobState, &job.LastJobStatus.AttemptNum,
			&job.LastJobStatus.ExecTime, &job.LastJobStatus.RetryTime,
			&job.LastJobStatus.ErrorCode, &job.LastJobStatus.ErrorResponse, &job.LastJobStatus.Parameters)
		mj.assertError(err)
		jobList = append(jobList, &job)

		customerCount[job.WorkspaceId] -= 1
		if customerCount[job.WorkspaceId] == 0 {
			delete(customerCount, job.WorkspaceId)
		}

		cacheUpdateByCustomer[job.WorkspaceId] = string(hasJobs)
	}
	if err = rows.Err(); err != nil {
		mj.assertError(err)
	}

	//do cache stuff here
	_willTryToSet := willTryToSet
	for customer, cacheUpdate := range cacheUpdateByCustomer {
		mj.markClearEmptyResult(ds, customer, params.StateFilters, params.CustomValFilters, params.ParameterFilters, cacheValue(cacheUpdate), &_willTryToSet)
	}

	mj.printNumJobsByCustomer(jobList)
	return jobList
}

func (mj *MultiTenantHandleT) getProcessedUnionQuerystring(customerCount map[string]int, ds dataSetT, params GetQueryParamsT) (string, []string) {
	var queries, customersToQuery []string
	queryInitial := mj.getInitialSingleCustomerProcessedQueryString(ds, params, true, customerCount)

	for customer, count := range customerCount {
		//do cache stuff here
		if mj.isEmptyResult(ds, customer, params.StateFilters, params.CustomValFilters, params.ParameterFilters) {
			continue
		}
		if count < 0 {
			mj.logger.Errorf("customerCount < 0 (%d) for customer: %s. Limiting at 0 %s jobs for this customer.", count, customer, params.StateFilters[0])
			continue
		}
		queries = append(queries, mj.getSingleCustomerProcessedQueryString(customer, count, ds, params, true))
		customersToQuery = append(customersToQuery, customer)
	}

	return queryInitial + `(` + strings.Join(queries, `) UNION (`) + `)`, customersToQuery
}

func (mj *MultiTenantHandleT) getInitialSingleCustomerProcessedQueryString(ds dataSetT, params GetQueryParamsT, order bool, customerCount map[string]int) string {
	stateFilters := params.StateFilters
	customValFilters := params.CustomValFilters
	parameterFilters := params.ParameterFilters
	var sqlStatement string

	//some stats
	customerArray := make([]string, 0)
	for customer := range customerCount {
		customerArray = append(customerArray, "'"+customer+"'")
	}
	customerString := "(" + strings.Join(customerArray, ", ") + ")"

	var stateQuery, customValQuery, limitQuery, sourceQuery string

	if len(stateFilters) > 0 {
		stateQuery = " AND " + constructQuery(mj, "job_state", stateFilters, "OR")
	} else {
		stateQuery = ""
	}

	if len(customValFilters) > 0 && !params.IgnoreCustomValFiltersInQuery {
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

	sqlStatement = fmt.Sprintf(`with rt_jobs_view AS (
										SELECT
                                               jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count,
                                               jobs.created_at, jobs.expire_at, jobs.workspaceid,
											   sum(jobs.event_count) over (order by jobs.job_id asc) as running_event_counts,
                                               job_latest_state.job_state, job_latest_state.attempt,
                                               job_latest_state.exec_time, job_latest_state.retry_time,
                                               job_latest_state.error_code, job_latest_state.error_response, job_latest_state.parameters as status_parameters
                                            FROM
                                               %[1]s AS jobs,
                                               (SELECT job_id, job_state, attempt, exec_time, retry_time,
                                                 error_code, error_response, parameters FROM %[2]s WHERE id IN
                                                   (SELECT MAX(id) from %[2]s GROUP BY job_id) %[3]s)
                                               AS job_latest_state
                                            WHERE jobs.job_id=job_latest_state.job_id AND jobs.workspaceid IN %[7]s
                                             %[4]s %[5]s
                                             AND job_latest_state.retry_time < $1 %[6]s`,
		ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, limitQuery, customerString)

	return sqlStatement + ")"
}

func (mj *MultiTenantHandleT) getSingleCustomerProcessedQueryString(customer string, count int, ds dataSetT, params GetQueryParamsT, order bool) string {
	stateFilters := params.StateFilters
	var sqlStatement string

	if count < 0 {
		mj.logger.Errorf("customerCount < 0 (%d) for customer: %s. Limiting at 0 %s jobs for this customer.", count, customer, stateFilters[0])
		count = 0
	}

	//some stats
	orderQuery := " ORDER BY jobs.job_id"
	limitQuery := fmt.Sprintf(" LIMIT %d ", count)

	sqlStatement = fmt.Sprintf(`SELECT
                                               jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count,
                                               jobs.created_at, jobs.expire_at, jobs.workspaceid,
											   jobs.running_event_counts,
                                               jobs.job_state, jobs.attempt,
                                               jobs.exec_time, jobs.retry_time,
                                               jobs.error_code, jobs.error_response, jobs.status_parameters
                                            FROM
                                               %[1]s
                                               AS jobs
                                            WHERE jobs.workspaceid='%[3]s' %[4]s %[2]s`,
		"rt_jobs_view", limitQuery, customer, orderQuery)

	return sqlStatement
}

func (mj *MultiTenantHandleT) printNumJobsByCustomer(jobs []*JobT) {
	if len(jobs) == 0 {
		mj.logger.Debug("No Jobs found for this query")
	}
	customerJobCountMap := make(map[string]int)
	for _, job := range jobs {
		if _, ok := customerJobCountMap[job.WorkspaceId]; !ok {
			customerJobCountMap[job.WorkspaceId] = 0
		}
		customerJobCountMap[job.WorkspaceId] += 1
	}
	for customer, count := range customerJobCountMap {
		mj.logger.Debug(customer, `: `, count)
	}
}

func (mj *MultiTenantHandleT) GetUnion(customerCount map[string]int, params GetQueryParamsT, maxDSQuerySize int) []*JobT {

	//The order of lock is very important. The migrateDSLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	mj.dsMigrationLock.RLock()
	mj.dsListLock.RLock()
	defer mj.dsMigrationLock.RUnlock()
	defer mj.dsListLock.RUnlock()

	dsList := mj.getDSList(false)
	outJobs := make([]*JobT, 0)

	var tablesQueried int
	var tablesQueriedStat stats.RudderStats
	var queryTime stats.RudderStats
	// TODO: Change stats
	queryTime = stats.NewTaggedStat("union_query_time", stats.TimerType, stats.Tags{
		"state":    "nonterminal",
		"module":   mj.tablePrefix,
		"destType": params.CustomValFilters[0],
	})

	start := time.Now()
	for _, ds := range dsList {
		jobs := mj.getUnionDS(ds, customerCount, params)
		outJobs = append(outJobs, jobs...)
		tablesQueried++
		if len(customerCount) == 0 {
			break
		}
	}

	queryTime.SendTiming(time.Since(start))
	// TODO: Change stats
	tablesQueriedStat = stats.NewTaggedStat("tables_queried_gauge", stats.GaugeType, stats.Tags{
		"state":    "nonterminal",
		"module":   mj.tablePrefix,
		"destType": params.CustomValFilters[0],
	})
	tablesQueriedStat.Gauge(tablesQueried)

	//PickUp stats
	var pickUpCountStat stats.RudderStats
	customerCountStat := make(map[string]int)

	for _, job := range outJobs {
		if _, ok := customerCountStat[job.WorkspaceId]; !ok {
			customerCountStat[job.WorkspaceId] = 0
		}
		customerCountStat[job.WorkspaceId] += 1
	}

	for customer, jobCount := range customerCountStat {
		pickUpCountStat = stats.NewTaggedStat("pick_up_count", stats.CountType, stats.Tags{
			"customer": customer,
			"state":    params.StateFilters[0],
			"module":   mj.tablePrefix,
			"destType": params.CustomValFilters[0],
		})
		pickUpCountStat.Count(jobCount)
	}

	return outJobs
}

func (mj *MultiTenantHandleT) getUnionDS(ds dataSetT, customerCount map[string]int, params GetQueryParamsT) []*JobT {
	var jobList []*JobT
	queryString, customersToQuery := mj.getUnionQuerystring(customerCount, ds, params)

	if len(customersToQuery) == 0 {
		return jobList
	}

	for _, customer := range customersToQuery {
		mj.markClearEmptyResult(ds, customer, params.StateFilters, params.CustomValFilters, params.ParameterFilters, willTryToSet, nil)
	}

	cacheUpdateByCustomer := make(map[string]string)
	for _, customer := range customersToQuery {
		cacheUpdateByCustomer[customer] = string(noJobs)
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

	rows, err = stmt.Query()
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
		columns, err := rows.Columns()
		ct, _ := rows.ColumnTypes()
		fmt.Println(ct)
		mj.assertError(err)
		processedJob := false
		for _, clm := range columns {
			if clm == "LastJobStatus" {
				processedJob = true
				break
			}
		}
		if processedJob {
			err = rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
				&job.EventPayload, &job.EventCount, &job.CreatedAt, &job.ExpireAt, &job.WorkspaceId, &_null,
				&job.LastJobStatus.JobState, &job.LastJobStatus.AttemptNum,
				&job.LastJobStatus.ExecTime, &job.LastJobStatus.RetryTime,
				&job.LastJobStatus.ErrorCode, &job.LastJobStatus.ErrorResponse, &job.LastJobStatus.Parameters)
		} else {
			var _nullJS sql.NullString
			var _nullA sql.NullInt16
			var _nullET sql.NullTime
			var _nullRT sql.NullTime
			var _nullEC sql.NullString
			// TODO: Last two columns in the data are not supposed to be of type string,
			// they are supposed to be of type RAWJSON but SQL does not give a nullable RAWJSON type.
			//So look for the possible scenarios
			var _nullER sql.NullString
			var _nullSP sql.NullString
			err = rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
				&job.EventPayload, &job.EventCount, &job.CreatedAt, &job.ExpireAt, &job.WorkspaceId, &_null, &_nullJS,
				&_nullA, &_nullET, &_nullRT, &_nullEC, &_nullER, &_nullSP)
		}
		mj.assertError(err)
		jobList = append(jobList, &job)

		customerCount[job.WorkspaceId] -= 1
		if customerCount[job.WorkspaceId] == 0 {
			delete(customerCount, job.WorkspaceId)
		}

		cacheUpdateByCustomer[job.WorkspaceId] = string(hasJobs)
	}
	if err = rows.Err(); err != nil {
		mj.assertError(err)
	}

	//do cache stuff here
	_willTryToSet := willTryToSet
	for customer, cacheUpdate := range cacheUpdateByCustomer {
		mj.markClearEmptyResult(ds, customer, params.StateFilters, params.CustomValFilters, params.ParameterFilters, cacheValue(cacheUpdate), &_willTryToSet)
	}

	mj.printNumJobsByCustomer(jobList)
	return jobList
}

func (mj *MultiTenantHandleT) getUnionQuerystring(customerCount map[string]int, ds dataSetT, params GetQueryParamsT) (string, []string) {
	var queries, customersToQuery []string
	queryInitial := mj.getInitialSingleCustomerQueryString(ds, params, true, customerCount)

	for customer, count := range customerCount {
		//do cache stuff here
		if mj.isEmptyResult(ds, customer, params.StateFilters, params.CustomValFilters, params.ParameterFilters) {
			continue
		}
		if count < 0 {
			mj.logger.Errorf("customerCount < 0 (%d) for customer: %s. Limiting at 0 %s jobs for this customer.", count, customer, params.StateFilters[0])
			continue
		}
		queries = append(queries, mj.getSingleCustomerProcessedQueryString(customer, count, ds, params, true))
		customersToQuery = append(customersToQuery, customer)
	}

	return queryInitial + `(` + strings.Join(queries, `) UNION (`) + `)`, customersToQuery
}

func (mj *MultiTenantHandleT) getInitialSingleCustomerQueryString(ds dataSetT, params GetQueryParamsT, order bool, customerCount map[string]int) string {
	stateFilters := params.StateFilters
	customValFilters := params.CustomValFilters
	var sqlStatement string

	//some stats
	customerArray := make([]string, 0)
	for customer := range customerCount {
		customerArray = append(customerArray, "'"+customer+"'")
	}
	customerString := "(" + strings.Join(customerArray, ", ") + ")"

	var stateQuery, customValQuery string

	if len(stateFilters) > 0 {
		stateQuery = " OR " + constructQuery(mj, "job_latest_state.job_state", stateFilters, "OR")
	} else {
		stateQuery = ""
	}

	if len(customValFilters) > 0 && !params.IgnoreCustomValFiltersInQuery {
		// mj.assert(!getAll, "getAll is true")
		customValQuery = " AND " +
			constructQuery(mj, "jobs.custom_val", customValFilters, "OR")
	} else {
		customValQuery = ""
	}

	sqlStatement = fmt.Sprintf(`with rt_jobs_view AS (
		                                    SELECT
                                                jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val,
                                                jobs.event_payload, jobs.event_count, jobs.created_at,
                                                jobs.expire_at, jobs.workspaceid,
                                                sum(jobs.event_count) over (
                                                    order by jobs.job_id asc
                                                ) as running_event_counts,
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
                                                (
                                                    job_latest_state.job_id IS NULL
                                                    %[3]s
                                                )
                                                AND jobs.workspaceid IN %[5]s
                                                %[4]s`,
		ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, customerString)
	return sqlStatement + ")"
}
