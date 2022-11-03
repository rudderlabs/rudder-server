package router

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func RegisterAdminHandlers(readonlyRouterDB, readonlyBatchRouterDB jobsdb.ReadonlyJobsDB) {
	admin.RegisterAdminHandler("Router", &RouterRpcHandler{jobsDBPrefix: "rt", readonlyRouterDB: readonlyRouterDB})
	admin.RegisterAdminHandler("BatchRouter", &RouterRpcHandler{jobsDBPrefix: "batch_rt", readonlyBatchRouterDB: readonlyBatchRouterDB})
}

type Admin struct {
	handles map[string]*HandleT
}

var (
	adminInstance                                 *Admin
	routerJobsTableName, routerJobStatusTableName string
)

func InitRouterAdmin() {
	adminInstance = &Admin{
		handles: make(map[string]*HandleT),
	}
	admin.RegisterStatusHandler("routers", adminInstance)
}

func (ra *Admin) registerRouter(name string, handle *HandleT) {
	ra.handles[name] = handle
}

// Status function is used for debug purposes by the admin interface
func (ra *Admin) Status() interface{} {
	statusList := make([]map[string]interface{}, 0)
	for name, router := range ra.handles {
		routerStatus := make(map[string]interface{})
		routerStatus["name"] = name
		barriersMap := make(map[string]string, 0)
		for i, worker := range router.workers {
			if worker.barrier.Size() > 0 {
				barriersMap[strconv.Itoa(i)] = worker.barrier.String()
			}
		}
		if len(barriersMap) > 0 {
			routerStatus["worker-barriers"] = barriersMap
		}

		statusList = append(statusList, routerStatus)
	}
	return statusList
}

type RouterRpcHandler struct {
	jobsDBPrefix          string
	readonlyRouterDB      jobsdb.ReadonlyJobsDB
	readonlyBatchRouterDB jobsdb.ReadonlyJobsDB
}

type JobCountsByStateAndDestination struct {
	Count       int
	State       string
	Destination string
}

type ErrorCodeCountsByDestination struct {
	Count         int
	ErrorCode     string
	Destination   string
	DestinationID string
}

type JobCountByConnections struct {
	Count         int
	SourceId      string
	DestinationId string
}

type LatestJobStatusCounts struct {
	Count int
	State string
	Rank  int
}

type DSStats struct {
	JobCountsByStateAndDestination []JobCountsByStateAndDestination
	ErrorCodeCountsByDestination   []ErrorCodeCountsByDestination
	JobCountByConnections          []JobCountByConnections
	LatestJobStatusCounts          []LatestJobStatusCounts
	UnprocessedJobCounts           int
}

// GetDSStats
// group_by job_status
// group by custom_val
// Get all errors = distinct (error), count(*) where state=failed
// Distinct (src_id, dst_id)
// Router jobs status flow ⇒ ordered by rank
// unprocessed_params ⇒ Num jobs not yet picked
func (r *RouterRpcHandler) GetDSStats(dsName string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	var completeErr error
	dsStats := DSStats{
		make([]JobCountsByStateAndDestination, 0), make([]ErrorCodeCountsByDestination, 0), make([]JobCountByConnections, 0),
		make([]LatestJobStatusCounts, 0), 0,
	}
	dbHandle, err := sql.Open("postgres", misc.GetConnectionString())
	if err != nil {
		return err
	}
	defer func() { _ = dbHandle.Close() }()
	// TODO:: seems like sqlx library will be better as it allows to map structs to rows
	// that way the repeated logic can be brought to a single method
	err = getJobCountsByStateAndDestination(dbHandle, dsName, r.jobsDBPrefix, &dsStats)
	if err != nil {
		misc.AppendError("getJobCountsByStateAndDestination", &completeErr, &err)
	}
	err = getFailedStatusErrorCodeCountsByDestination(dbHandle, dsName, r.jobsDBPrefix, &dsStats)
	if err != nil {
		misc.AppendError("getFailedStatusErrorCodeCountsByDestination", &completeErr, &err)
	}
	err = getJobCountByConnections(dbHandle, dsName, r.jobsDBPrefix, &dsStats)
	if err != nil {
		misc.AppendError("getJobCountByConnections", &completeErr, &err)
	}
	err = getLatestJobStatusCounts(dbHandle, dsName, r.jobsDBPrefix, &dsStats)
	if err != nil {
		misc.AppendError("getLatestJobStatusCounts", &completeErr, &err)
	}
	err = getUnprocessedJobCounts(dbHandle, dsName, r.jobsDBPrefix, &dsStats)
	if err != nil {
		misc.AppendError("getUnprocessedJobCounts", &completeErr, &err)
	}

	var response []byte
	response, err = json.MarshalIndent(dsStats, "", " ")
	if err != nil {
		*result = ""
		misc.AppendError("MarshalIndent", &completeErr, &err)
	} else {
		*result = string(response)
	}
	// Since we try to execute each query independently once we are connected to db
	// this tries to capture errors that happened on all the execution paths
	return completeErr
}

func (r *RouterRpcHandler) getReadOnlyJobsDB(prefix string) jobsdb.ReadonlyJobsDB {
	if prefix == "rt" {
		return r.readonlyRouterDB
	}
	return r.readonlyBatchRouterDB
}

func (r *RouterRpcHandler) GetDSJobCount(arg string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	readOnlyJobsDB := r.getReadOnlyJobsDB(r.jobsDBPrefix)
	response, err := readOnlyJobsDB.GetJobSummaryCount(arg, r.jobsDBPrefix)
	*result = response
	return nil
}

func (r *RouterRpcHandler) GetDSFailedJobs(arg string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	readOnlyJobsDB := r.getReadOnlyJobsDB(r.jobsDBPrefix)
	response, err := readOnlyJobsDB.GetLatestFailedJobs(arg, r.jobsDBPrefix)
	*result = response
	return nil
}

func (r *RouterRpcHandler) GetJobByID(arg string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	readOnlyJobsDB := r.getReadOnlyJobsDB(r.jobsDBPrefix)
	response, err := readOnlyJobsDB.GetJobByID(arg, r.jobsDBPrefix)
	*result = response
	return err
}

func (r *RouterRpcHandler) GetJobIDStatus(arg string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	readOnlyJobsDB := r.getReadOnlyJobsDB(r.jobsDBPrefix)
	response, err := readOnlyJobsDB.GetJobIDStatus(arg, r.jobsDBPrefix)
	*result = response
	return err
}

func (r *RouterRpcHandler) GetDSList(_ string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()

	readOnlyJobsDB := r.getReadOnlyJobsDB(r.jobsDBPrefix)
	response, err := readOnlyJobsDB.GetDSListString()
	*result = response
	return nil
}

/*
JobCountsByStateAndDestination
================================================================================
│─────────────│───────────│─────────────│
│ COUNT (10)  │ STATE     │ DESTINATION │
│─────────────│───────────│─────────────│
│         323 │ aborted   │ AM          │
│          68 │ waiting   │ AM          │
│         646 │ failed    │ AM          │
│        1.3K │ executing │ AM          │
│         323 │ executing │ GA          │
│         323 │ succeeded │ GA          │
│         577 │ executing │ KISSMETRICS │
│          51 │ waiting   │ KISSMETRICS │
│         203 │ failed    │ KISSMETRICS │
│         323 │ succeeded │ KISSMETRICS │
│─────────────│───────────│─────────────│
*/
func getJobCountsByStateAndDestination(dbHandle *sql.DB, dsName, jobsDBPrefix string, dsStats *DSStats) error {
	routerJobsTableName = jobsDBPrefix + "_jobs_" + dsName
	routerJobStatusTableName = jobsDBPrefix + "_job_status_" + dsName
	sqlStmt := fmt.Sprintf(`select count(*), st.job_state, rt.custom_val from  %[1]s rt inner join  %[2]s st
	                        on st.job_id=rt.job_id group by rt.custom_val, st.job_state order by rt.custom_val`, routerJobsTableName, routerJobStatusTableName)
	var rows *sql.Rows
	var err error
	rows, err = dbHandle.Query(sqlStmt)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	result := JobCountsByStateAndDestination{}
	for rows.Next() {
		err = rows.Scan(&result.Count, &result.State, &result.Destination)
		if err != nil {
			return err
		}
		dsStats.JobCountsByStateAndDestination = append(dsStats.JobCountsByStateAndDestination, result)
	}

	err = rows.Err()
	if err != nil {
		return err // we return whenever we get an error to stop processing further downstream db requests
	}
	err = rows.Close()
	return err
}

/*
ErrorCodeCountsByDestination
================================================================================
│───────│────────────│─────────────│──────────────────────────────│
│ COUNT │ ERROR CODE │ DESTINATION │            DESTINATIONID     │
│───────│────────────│─────────────│──────────────────────────────│
│    92 │ 504        │ AM          │"1mIdI8twOB4SGioUPTXDqc8lbSL" │
│   323 │ 400        │ AM          │"1mIdI823e23e233244XDqc8lbSL" │
│   190 │ 504        │ KISSMETRICS │"1mIdI122332343434TXDqc8lbSL" │
│───────│────────────│─────────────│──────────────────────────────│
*/
func getFailedStatusErrorCodeCountsByDestination(dbHandle *sql.DB, dsName, jobsDBPrefix string, dsStats *DSStats) error {
	routerJobsTableName = jobsDBPrefix + "_jobs_" + dsName
	routerJobStatusTableName = jobsDBPrefix + "_job_status_" + dsName
	sqlStmt := fmt.Sprintf(`select count(*), a.error_code, a.custom_val, a.d from
							(select count(*), rt.job_id, st.error_code as error_code, rt.custom_val as custom_val,
								rt.parameters -> 'destination_id' as d from %[1]s rt inner join %[2]s st
								on st.job_id=rt.job_id where st.job_state in ('failed', 'aborted')
								group by rt.job_id, st.error_code, rt.custom_val, rt.parameters -> 'destination_id')
							as  a group by a.custom_val, a.error_code, a.d order by a.custom_val;`, routerJobsTableName, routerJobStatusTableName)
	var rows *sql.Rows
	var err error
	rows, err = dbHandle.Query(sqlStmt)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	result := ErrorCodeCountsByDestination{}
	for rows.Next() {
		err = rows.Scan(&result.Count, &result.ErrorCode, &result.Destination, &result.DestinationID)
		if err != nil {
			return err
		}
		dsStats.ErrorCodeCountsByDestination = append(dsStats.ErrorCodeCountsByDestination, result)
	}

	if err = rows.Err(); err != nil {
		return err
	}
	err = rows.Close()
	return err
}

/*
JobCountByConnections
================================================================================
│───────│───────────────────────────────│───────────────────────────────│
│ COUNT │ SOURCEID                      │ DESTINATIONID                 │
│───────│───────────────────────────────│───────────────────────────────│
│   323 │ "1kXnQTrRjEmjU2wH8KjRR8EJ3gm" │ "1kXo508bX4OAynyYkEBpH6aQYHP" │
│   323 │ "1kXnQTrRjEmjU2wH8KjRR8EJ3gm" │ "1kYW7q5ApiMkIG9TGsSZb7PIlrf" │
│   323 │ "1kXnQTrRjEmjU2wH8KjRR8EJ3gm" │ "1kgadfXiXiZPM8oKAtkPFxFjm0P" │
│───────│───────────────────────────────│───────────────────────────────│
*/
func getJobCountByConnections(dbHandle *sql.DB, dsName, jobsDBPrefix string, dsStats *DSStats) error {
	routerJobsTableName = jobsDBPrefix + "_jobs_" + dsName
	sqlStmt := fmt.Sprintf(`select count(*), parameters->'source_id' as s, parameters -> 'destination_id' as d from %[1]s
							group by parameters->'source_id', parameters->'destination_id'
							order by parameters->'destination_id';`, routerJobsTableName)
	var rows *sql.Rows
	var err error
	rows, err = dbHandle.Query(sqlStmt)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	result := JobCountByConnections{}
	for rows.Next() {
		err = rows.Scan(&result.Count, &result.SourceId, &result.DestinationId)
		if err != nil {
			return err
		}
		dsStats.JobCountByConnections = append(dsStats.JobCountByConnections, result)
	}

	err = rows.Err()
	if err != nil {
		return err
	}
	err = rows.Close()
	return err
}

/*
LatestJobStatusCounts
================================================================================
│─────────────│───────────│──────│
│ COUNT (10)  │ STATE     │ RANK │
│─────────────│───────────│──────│
│         323 │ aborted   │ 1    │
│         646 │ succeeded │ 1    │
│         969 │ executing │ 2    │
│         513 │ failed    │ 3    │
│          51 │ waiting   │ 3    │
│         564 │ executing │ 4    │
│         336 │ failed    │ 5    │
│         336 │ executing │ 6    │
│          68 │ waiting   │ 7    │
│          68 │ executing │ 8    │
│─────────────│───────────│──────│
*/
func getLatestJobStatusCounts(dbHandle *sql.DB, dsName, jobsDBPrefix string, dsStats *DSStats) error {
	routerJobStatusTableName = jobsDBPrefix + "_job_status_" + dsName
	sqlStmt := fmt.Sprintf(`SELECT COUNT(*), job_state, rank FROM
							(SELECT job_state, RANK() OVER(PARTITION BY job_id ORDER BY exec_time DESC) as rank, job_id from %s)
							as inner_table GROUP BY rank, job_state order by rank, job_state`, routerJobStatusTableName)
	var rows *sql.Rows
	var err error
	rows, err = dbHandle.Query(sqlStmt)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	result := LatestJobStatusCounts{}
	for rows.Next() {
		err = rows.Scan(&result.Count, &result.State, &result.Rank)
		if err != nil {
			return err
		}
		dsStats.LatestJobStatusCounts = append(dsStats.LatestJobStatusCounts, result)
	}

	err = rows.Err()
	if err != nil {
		return err
	}
	err = rows.Close()
	return err
}

func getUnprocessedJobCounts(dbHandle *sql.DB, dsName, jobsDBPrefix string, dsStats *DSStats) error {
	routerJobsTableName = jobsDBPrefix + "_jobs_" + dsName
	routerJobStatusTableName = jobsDBPrefix + "_job_status_" + dsName
	sqlStatement := fmt.Sprintf(`select count(*) from %[1]s rt inner join %[2]s st
								on st.job_id=rt.job_id where st.job_id is NULL;`, routerJobsTableName, routerJobStatusTableName)
	row := dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&dsStats.UnprocessedJobCounts)
	return err
}
