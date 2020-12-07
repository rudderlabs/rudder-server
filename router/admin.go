package router

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

type RouterAdmin struct {
	handles map[string]*HandleT
}

var adminInstance *RouterAdmin

func init() {
	adminInstance = &RouterAdmin{
		handles: make(map[string]*HandleT),
	}
	admin.RegisterStatusHandler("routers", adminInstance)
	admin.RegisterAdminHandler("Router", &RouterRpcHandler{})
}

func (ra *RouterAdmin) registerRouter(name string, handle *HandleT) {
	ra.handles[name] = handle
}

// Status function is used for debug purposes by the admin interface
func (ra *RouterAdmin) Status() interface{} {
	statusList := make([]map[string]interface{}, 0)
	for name, router := range ra.handles {
		routerStatus := router.perfStats.Status()
		routerStatus["name"] = name
		routerStatus["success-count"] = router.successCount
		routerStatus["failure-count"] = router.failCount
		routerFailedList := make([]string, 0)
		router.failedEventsListMutex.RLock()
		for e := router.failedEventsList.Front(); e != nil; e = e.Next() {
			status, _ := json.Marshal(e.Value)
			routerFailedList = append(routerFailedList, string(status))
		}
		router.failedEventsListMutex.RUnlock()
		if len(routerFailedList) > 0 {
			routerStatus["recent-failedstatuses"] = routerFailedList
		}
		statusList = append(statusList, routerStatus)

	}
	return statusList
}

type RouterRpcHandler struct {
}

type JobCountsByStateAndDestination struct {
	Count       int
	State       string
	Destination string
}

type FailedStatusErrorCodeCountsByDestination struct {
	Count       int
	ErrorCode   string
	Destination string
}

type JobCountByConnections struct {
	Count         int
	SourceId      string
	DestinationId string
}

type JobStatusCountsByExecTime struct {
	Count int
	State string
	Rank  int
}

type DSStats struct {
	JobCountsByStateAndDestination           []JobCountsByStateAndDestination
	FailedStatusErrorCodeCountsByDestination []FailedStatusErrorCodeCountsByDestination
	JobCountByConnections                    []JobCountByConnections
	JobStatusCountsByExecTime                []JobStatusCountsByExecTime
	UnprocessedJobCounts                     int
	err                                      error `json:"-"`
}

// group_by job_status
// group by custom_val
// Get all errors = distinct (error), count(*) where state=failed
// Distinct (src_id, dst_id)
// Router jobs status flow ⇒ ordered by rank
// unprocessed_params ⇒ Num jobs not yet picked
func (r *RouterRpcHandler) GetDSStats(dsName string, result *string) error {
	dsStats := DSStats{make([]JobCountsByStateAndDestination, 0), make([]FailedStatusErrorCodeCountsByDestination, 0), make([]JobCountByConnections, 0),
		make([]JobStatusCountsByExecTime, 0), 0, nil}
	dbHandle, err := sql.Open("postgres", jobsdb.GetConnectionString())
	if err != nil {
		dsStats.err = err
	}
	defer dbHandle.Close()
	getJobCountsByStateAndDestination(dbHandle, dsName, &dsStats)
	getFailedStatusErrorCodeCountsByDestination(dbHandle, dsName, &dsStats)
	getJobCountByConnections(dbHandle, dsName, &dsStats)
	getJobStatusCountsByExecTime(dbHandle, dsName, &dsStats)
	getUnprocessedJobCounts(dbHandle, dsName, &dsStats)
	response, err := json.MarshalIndent(dsStats, "", " ")
	if err != nil {
		*result = ""
		dsStats.err = err
	} else {
		*result = string(response)
	}
	return dsStats.err
}

func getJobCountsByStateAndDestination(dbHandle *sql.DB, dsName string, dsStats *DSStats) {
	if dsStats.err != nil {
		return
	}
	routerTableName := "rt_jobs_" + dsName
	routerStatusTableNmae := "rt_job_status_" + dsName
	sqlStmt := fmt.Sprintf(`select count(*), st.job_state, rt.custom_val from  %[1]s rt inner join  %[2]s st
	                        on st.job_id=rt.job_id group by rt.custom_val, st.job_state order by rt.custom_val`, routerTableName, routerStatusTableNmae)
	var rows *sql.Rows
	rows, dsStats.err = dbHandle.Query(sqlStmt)
	if dsStats.err != nil {
		return
	}
	defer rows.Close()
	result := JobCountsByStateAndDestination{}
	for rows.Next() {
		dsStats.err = rows.Scan(&result.Count, &result.State, &result.Destination)
		if dsStats.err != nil {
			return
		}
		dsStats.JobCountsByStateAndDestination = append(dsStats.JobCountsByStateAndDestination, result)
	}

	dsStats.err = rows.Err()
	if dsStats.err != nil {
		return // we return whenever we get an error to stop processing further downstream db requests
	}
	dsStats.err = rows.Close()
	return
}

func getFailedStatusErrorCodeCountsByDestination(dbHandle *sql.DB, dsName string, dsStats *DSStats) {
	if dsStats.err != nil {
		return
	}
	routerTableName := "rt_jobs_" + dsName
	routerStatusTableNmae := "rt_job_status_" + dsName
	sqlStmt := fmt.Sprintf(`select count(*), a.error_code, a.custom_val from (select count(*), rt.job_id, st.error_code as error_code ,
							rt.custom_val as custom_val from %[1]s rt inner join %[2]s st  on st.job_id=rt.job_id where st.job_state in 
							('failed', 'aborted') group by rt.job_id, st.error_code, rt.custom_val) as  a group by a.custom_val, a.error_code order by a.custom_val;`, routerTableName, routerStatusTableNmae)
	var rows *sql.Rows
	rows, dsStats.err = dbHandle.Query(sqlStmt)
	if dsStats.err != nil {
		return
	}
	defer rows.Close()
	result := FailedStatusErrorCodeCountsByDestination{}
	for rows.Next() {
		dsStats.err = rows.Scan(&result.Count, &result.ErrorCode, &result.Destination)
		if dsStats.err != nil {
			return
		}
		dsStats.FailedStatusErrorCodeCountsByDestination = append(dsStats.FailedStatusErrorCodeCountsByDestination, result)
	}

	dsStats.err = rows.Err()
	if dsStats.err != nil {
		return
	}
	dsStats.err = rows.Close()
	return
}

func getJobCountByConnections(dbHandle *sql.DB, dsName string, dsStats *DSStats) {
	if dsStats.err != nil {
		return
	}
	routerTableName := "rt_jobs_" + dsName
	sqlStmt := fmt.Sprintf(`select count(*), parameters->'source_id' as s, parameters -> 'destination_id' as d 
							from %[1]s group by parameters->'source_id', parameters->'destination_id' order by parameters->'destination_id';`, routerTableName)
	var rows *sql.Rows
	rows, dsStats.err = dbHandle.Query(sqlStmt)
	if dsStats.err != nil {
		return
	}
	defer rows.Close()
	result := JobCountByConnections{}
	for rows.Next() {
		dsStats.err = rows.Scan(&result.Count, &result.SourceId, &result.DestinationId)
		if dsStats.err != nil {
			return
		}
		dsStats.JobCountByConnections = append(dsStats.JobCountByConnections, result)
	}

	dsStats.err = rows.Err()
	if dsStats.err != nil {
		return
	}
	dsStats.err = rows.Close()
	return
}

func getJobStatusCountsByExecTime(dbHandle *sql.DB, dsName string, dsStats *DSStats) {
	if dsStats.err != nil {
		return
	}
	routerStatusTableName := "rt_job_status_" + dsName
	sqlStmt := fmt.Sprintf(`SELECT COUNT(*), job_state, rank FROM (SELECT job_state, RANK() OVER(PARTITION BY job_id 
							ORDER BY exec_time DESC) as rank, job_id from %s) as inner_table GROUP BY rank, job_state order by rank, job_state`, routerStatusTableName)
	var rows *sql.Rows
	rows, dsStats.err = dbHandle.Query(sqlStmt)
	if dsStats.err != nil {
		return
	}
	defer rows.Close()
	result := JobStatusCountsByExecTime{}
	for rows.Next() {
		dsStats.err = rows.Scan(&result.Count, &result.State, &result.Rank)
		if dsStats.err != nil {
			return
		}
		dsStats.JobStatusCountsByExecTime = append(dsStats.JobStatusCountsByExecTime, result)
	}

	dsStats.err = rows.Err()
	if dsStats.err != nil {
		return
	}
	dsStats.err = rows.Close()
	return
}

func getUnprocessedJobCounts(dbHandle *sql.DB, dsName string, dsStats *DSStats) {
	routerTableName := "rt_jobs_" + dsName
	routerStatusTableNmae := "rt_job_status_" + dsName
	sqlStatement := fmt.Sprintf(`select count(*) from %[1]s rt inner join %[2]s st on st.job_id=rt.job_id where st.job_id is NULL;`, routerTableName, routerStatusTableNmae)
	row := dbHandle.QueryRow(sqlStatement)
	dsStats.err = row.Scan(&dsStats.UnprocessedJobCounts)
	return
}
