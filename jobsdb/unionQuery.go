//go:generate mockgen -destination=../mocks/jobsdb/mock_unionQuery.go -package=mocks_jobsdb github.com/rudderlabs/rudder-server/jobsdb MultiTenantJobsDB

package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats"
)

const (
	selectQuery = `SELECT jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val, jobs.event_payload, jobs.event_count, jobs.created_at, jobs.expire_at, jobs.workspace_id,jobs.running_event_counts `
)

type MultiTenantHandleT struct {
	*HandleT
	Cache CacheOperator
}

type CacheOperator interface {
	HaveEmptyResult(ds dataSetT, customer string, stateFilters []string, customValFilters []string, parameterFilters []ParameterFilterT) bool
	UpdateCache(ds dataSetT, customer string, stateFilters []string, customValFilters []string, parameterFilters []ParameterFilterT, value cacheValue, checkAndSet *cacheValue)
}

type JobsDBStatusCache struct {
	once sync.Once
	a    HandleT
}

func (c *JobsDBStatusCache) HaveEmptyResult(ds dataSetT, customer string, stateFilters []string, customValFilters []string,
	parameterFilters []ParameterFilterT) bool {
	c.initCache()
	return c.a.isEmptyResult(ds, customer, stateFilters, customValFilters, parameterFilters)
}

func (c *JobsDBStatusCache) UpdateCache(ds dataSetT, customer string, stateFilters []string, customValFilters []string,
	parameterFilters []ParameterFilterT, value cacheValue, checkAndSet *cacheValue) {
	c.initCache()
	c.a.markClearEmptyResult(ds, customer, stateFilters, customValFilters, parameterFilters, value, checkAndSet)
}

func (c *JobsDBStatusCache) initCache() {
	c.once.Do(func() {
		if c.a.dsEmptyResultCache == nil {
			c.a.dsEmptyResultCache = map[dataSetT]map[string]map[string]map[string]map[string]cacheEntry{}
		}
	})
}

type MultiTenantJobsDB interface {
	GetToRetry(params GetQueryParamsT) []*JobT
	GetUnprocessed(params GetQueryParamsT) []*JobT

	GetAllJobs(map[string]int, GetQueryParamsT, int) []*JobT
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
			  j.workspace_id as customer
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
	rows, err := mj.dbHandle.Query(fmt.Sprintf(`select workspace_id, count(job_id) from %s group by workspace_id;`, mj.getDSList(false)[0].JobTable))
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

func (mj *MultiTenantHandleT) getSingleCustomerQueryString(customer string, count int, ds dataSetT, params GetQueryParamsT, order bool) string {
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
                                              jobs.created_at, jobs.expire_at, jobs.workspace_id,
											   jobs.running_event_counts,
                                              jobs.job_state, jobs.attempt,
                                              jobs.exec_time, jobs.retry_time,
                                              jobs.error_code, jobs.error_response, jobs.status_parameters
                                           FROM
                                              %[1]s
                                              AS jobs
                                           WHERE jobs.workspace_id='%[3]s' %[4]s %[2]s`,
		"rt_jobs_view", limitQuery, customer, orderQuery)

	return sqlStatement
}
//
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

//All Jobs

func (mj *MultiTenantHandleT) GetAllJobs(customerCount map[string]int, params GetQueryParamsT, maxDSQuerySize int) []*JobT {

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
	queryTime := stats.NewTaggedStat("union_query_time", stats.TimerType, stats.Tags{
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
		if tablesQueried > maxDSQuerySize {
			break
		}
	}

	queryTime.SendTiming(time.Since(start))
	tablesQueriedStat := stats.NewTaggedStat("tables_queried_gauge", stats.GaugeType, stats.Tags{
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
		mj.markClearEmptyResult(ds, customer, params.StateFilters, params.CustomValFilters, params.ParameterFilters,
			willTryToSet, nil)
	}

	cacheUpdateByCustomer := make(map[string]string)
	for _, customer := range customersToQuery {
		cacheUpdateByCustomer[customer] = string(noJobs)
	}

	var rows *sql.Rows
	var err error

	stmt, err := mj.dbHandle.Prepare(queryString)
	mj.logger.Info(queryString)
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
		mj.markClearEmptyResult(ds, customer, params.StateFilters, params.CustomValFilters, params.ParameterFilters,
			cacheValue(cacheUpdate), &_willTryToSet)
	}

	mj.printNumJobsByCustomer(jobList)
	return jobList
}

func (mj *MultiTenantHandleT) getUnionQuerystring(customerCount map[string]int, ds dataSetT, params GetQueryParamsT) (string, []string) {
	var queries, customersToQuery []string
	queryInitial := mj.getInitialSingleCustomerQueryString(ds, params, true, customerCount)

	for customer, count := range customerCount {
		if mj.isEmptyResult(ds, customer, params.StateFilters, params.CustomValFilters, params.ParameterFilters) {
			continue
		}
		if count < 0 {
			mj.logger.Errorf("customerCount < 0 (%d) for customer: %s. Limiting at 0 %s jobs for this customer.", count, customer, params.StateFilters[0])
			continue
		}
		queries = append(queries, mj.getSingleCustomerQueryString(customer, count, ds, params, true))
		customersToQuery = append(customersToQuery, customer)
	}

	return queryInitial + `(` + strings.Join(queries, `) UNION (`) + `)`, customersToQuery
}

func (mj *MultiTenantHandleT) getInitialSingleCustomerQueryString(ds dataSetT, params GetQueryParamsT, order bool, customerCount map[string]int) string {
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

	stateQuery = "((job_latest_state.job_state not in ('executing','aborted', 'succeeded', 'migrated') and job_latest_state.retry_time < $1) or job_latest_state.job_id is null)"

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

	if len(parameterFilters) > 0 {
		// mj.assert(!getAll, "getAll is true")
		sourceQuery += " AND " + constructParameterJSONQuery("jobs", parameterFilters)
	} else {
		sourceQuery = ""
	}

	sqlStatement = fmt.Sprintf(`with rt_jobs_view AS (
		                                    SELECT
                                                jobs.job_id, jobs.uuid, jobs.user_id, jobs.parameters, jobs.custom_val,
                                                jobs.event_payload, jobs.event_count, jobs.created_at,
                                                jobs.expire_at, jobs.workspace_id,
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
                                                jobs.workspace_id IN %[7]s %[3]s %[4]s %[5]s %[6]s`,
		ds.JobTable, ds.JobStatusTable, stateQuery, customValQuery, sourceQuery, limitQuery, customerString)
	return sqlStatement + ")"
}
