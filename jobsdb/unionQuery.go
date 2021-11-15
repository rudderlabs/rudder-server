package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	selectQuery = `SELECT jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count, jobs.created_at, jobs.expire_at, jobs.customer, sum(jobs.event_count) over (order by jobs.job_id asc) as running_event_counts `
)

type MultiTenantHandleT struct {
	HandleT
}

type MultiTenantJobsDB interface {
	GetToRetry(params GetQueryParamsT) []*JobT
	GetUnprocessed(params GetQueryParamsT) []*JobT

	GetUnprocessedUnion(map[string]int, GetQueryParamsT) []*JobT
	GetProcessedUnion(map[string]int, GetQueryParamsT) []*JobT
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
	rows, err := mj.dbHandle.Query(fmt.Sprintf(`select customer, count(job_id) from %s group by customer;`, mj.getDSList(false)[0].JobTable))
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

func (mj *MultiTenantHandleT) getUnprocessedUnionQuerystring(customerCount map[string]int, ds dataSetT, params GetQueryParamsT) string {
	var queries []string

	for customer, count := range customerCount {
		//do cache stuff here
		queries = append(queries, mj.getSingleCustomerUnprocessedQueryString(customer, count, ds, params, true))
	}

	return `(` + strings.Join(queries, `) UNION (`) + `)`
}

func (mj *MultiTenantHandleT) getSingleCustomerUnprocessedQueryString(customer string, count int, ds dataSetT, params GetQueryParamsT, order bool) string {
	customValFilters := params.CustomValFilters
	parameterFilters := params.ParameterFilters
	var sqlStatement string

	// event_count default 1, number of items in payload
	sqlStatement = fmt.Sprintf(
		selectQuery+
			`FROM %[1]s AS jobs `+
			`LEFT JOIN %[2]s AS job_status ON jobs.job_id=job_status.job_id `+
			`WHERE job_status.job_id is NULL AND customer='%[3]s'`,
		ds.JobTable, ds.JobStatusTable, customer)

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

	if order {
		sqlStatement += " ORDER BY jobs.job_id"
	}

	if count > 0 {
		sqlStatement += fmt.Sprintf(" LIMIT %d", count)
	}

	if params.EventCount > 0 {
		sqlStatement = fmt.Sprintf(`SELECT * FROM (`+sqlStatement+`) AS subquery WHERE running_event_counts - event_count + 1 <= %d;`, params.EventCount)
	}

	return sqlStatement
}

func (mj *MultiTenantHandleT) GetUnprocessedUnion(customerCount map[string]int, params GetQueryParamsT) []*JobT {

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

	for _, ds := range dsList {
		jobs := mj.getUnprocessedUnionDS(ds, customerCount, params)
		outJobs = append(outJobs, jobs...)
		if len(customerCount) == 0 {
			break
		}
	}

	return outJobs
}

func (mj *MultiTenantHandleT) getUnprocessedUnionDS(ds dataSetT, customerCount map[string]int, params GetQueryParamsT) []*JobT {
	queryString := mj.getUnprocessedUnionQuerystring(customerCount, ds, params)

	var rows *sql.Rows
	var err error

	rows, err = mj.dbHandle.Query(queryString)
	mj.assertError(err)

	defer rows.Close()

	var jobList []*JobT
	for rows.Next() {
		var job JobT
		var _null int
		err := rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.EventCount, &job.CreatedAt, &job.ExpireAt, &job.Customer, &_null)
		mj.assertError(err)
		jobList = append(jobList, &job)

		customerCount[job.Customer] -= 1
		if customerCount[job.Customer] == 0 {
			delete(customerCount, job.Customer)
		}
	}
	if err = rows.Err(); err != nil {
		mj.assertError(err)
	}

	//do cache stuff here

	return jobList
}

//Processed

func (mj *MultiTenantHandleT) GetProcessedUnion(customerCount map[string]int, params GetQueryParamsT) []*JobT {

	//The order of lock is very important. The migrateDSLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	mj.dsMigrationLock.RLock()
	mj.dsListLock.RLock()
	defer mj.dsMigrationLock.RUnlock()
	defer mj.dsListLock.RUnlock()

	dsList := mj.getDSList(false)
	outJobs := make([]*JobT, 0)

	for _, ds := range dsList {
		jobs := mj.getProcessedUnionDS(ds, customerCount, params)
		outJobs = append(outJobs, jobs...)
		if len(customerCount) == 0 {
			break
		}
	}

	return outJobs
}

func (mj *MultiTenantHandleT) getProcessedUnionDS(ds dataSetT, customerCount map[string]int, params GetQueryParamsT) []*JobT {
	queryString := mj.getProcessedUnionQuerystring(customerCount, ds, params)

	var rows *sql.Rows
	var err error

	stmt, err := mj.dbHandle.Prepare(queryString)
	mj.assertError(err)
	defer stmt.Close()

	rows, err = stmt.Query(getTimeNowFunc())
	mj.assertError(err)
	defer rows.Close()

	var jobList []*JobT
	for rows.Next() {
		var job JobT
		var _null int
		err := rows.Scan(&job.JobID, &job.UUID, &job.UserID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.EventCount, &job.CreatedAt, &job.ExpireAt, &job.Customer, &_null)
		mj.assertError(err)
		jobList = append(jobList, &job)

		customerCount[job.Customer] -= 1
		if customerCount[job.Customer] == 0 {
			delete(customerCount, job.Customer)
		}
	}
	if err = rows.Err(); err != nil {
		mj.assertError(err)
	}

	//do cache stuff here

	return jobList
}

func (mj *MultiTenantHandleT) getProcessedUnionQuerystring(customerCount map[string]int, ds dataSetT, params GetQueryParamsT) string {
	var queries []string

	for customer, count := range customerCount {
		//do cache stuff here
		queries = append(queries, mj.getSingleCustomerProcessedQueryString(customer, count, ds, params, true))
	}

	return `(` + strings.Join(queries, `) UNION (`) + `)`
}

func (mj *MultiTenantHandleT) getSingleCustomerProcessedQueryString(customer string, count int, ds dataSetT, params GetQueryParamsT, order bool) string {
	stateFilters := params.StateFilters
	customValFilters := params.CustomValFilters
	parameterFilters := params.ParameterFilters
	var sqlStatement string

	//some stats

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

	limitQuery = fmt.Sprintf(" LIMIT %d ", count)

	sqlStatement = fmt.Sprintf(`SELECT
                                               jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count,
                                               jobs.created_at, jobs.expire_at,
											   sum(jobs.event_count) over (order by jobs.job_id asc) as running_event_counts,
                                               job_latest_state.job_state, job_latest_state.attempt,
                                               job_latest_state.exec_time, job_latest_state.retry_time,
                                               job_latest_state.error_code, job_latest_state.error_response, job_latest_state.parameters
                                            FROM
                                               %[1]s AS jobs,
                                               (SELECT job_id, job_state, attempt, exec_time, retry_time,
                                                 error_code, error_response, parameters FROM %[2]s WHERE id IN
                                                   (SELECT MAX(id) from %[2]s GROUP BY job_id) %[3]s)
                                               AS job_latest_state
                                            WHERE jobs.job_id=job_latest_state.job_id
                                             %[4]s %[5]s
                                             AND job_latest_state.retry_time < $1 ORDER BY jobs.job_id %[6]s`,
		ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, limitQuery)

	return sqlStatement
}
