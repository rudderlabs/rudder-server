package jobsdb

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

func (jd *HandleT) getLastDsForExport(dsList []dataSetT) dataSetT {
	dsListLen := len(dsList)
	var ds dataSetT
	if !jd.isEmpty(dsList[dsListLen-1]) {
		ds = dsList[dsListLen-1]
	} else if dsListLen > 1 {
		ds = dsList[dsListLen-2]
	}

	return ds
}

//GetNonMigrated all jobs with no filters
func (jd *HandleT) GetNonMigrated(count int) []*JobT {

	//The order of lock is very important. The mainCheckLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	jd.dsMigrationLock.RLock()
	jd.dsListLock.RLock()
	defer jd.dsMigrationLock.RUnlock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList(false)
	outJobs := make([]*JobT, 0)
	jd.assert(count >= 0, fmt.Sprintf("count:%d received is less than 0", count))
	if count == 0 {
		return outJobs
	}

	for _, ds := range dsList {
		jd.assert(count > 0, fmt.Sprintf("count:%d is less than or equal to 0", count))
		jobs, err := jd.getNonMigratedJobsDS(ds, count)
		var statusList []*JobStatusT
		for _, job := range jobs {
			statusList = append(statusList, BuildStatus(job, MigratingState))
		}

		jd.UpdateJobStatus(statusList, []string{}, []ParameterFilterT{})
		jd.assertError(err)
		outJobs = append(outJobs, jobs...)
		count -= len(jobs)
		jd.assert(count >= 0, fmt.Sprintf("count:%d received is less than 0", count))
		if count == 0 {
			break
		}

		//Instead of full dsList, it needs to do only till the dataset before import and newEvent datasets
		if ds.Index == jd.migrationState.LastDsForExport.Index {
			break
		}
	}
	//Release lock
	return outJobs
}

func BuildStatus(job *JobT, jobState string) *JobStatusT {
	newStatus := JobStatusT{
		JobID:         job.JobID,
		JobState:      jobState,
		AttemptNum:    1,
		ExecTime:      time.Now(),
		RetryTime:     time.Now(),
		ErrorCode:     "200",
		ErrorResponse: []byte(`{"success":"OK"}`),
	}
	return &newStatus
}

//SQLJobStatusT is a temporary struct to handle nulls from postgres query
type SQLJobStatusT struct {
	JobID         sql.NullInt64
	JobState      sql.NullString //ENUM waiting, executing, succeeded, waiting_retry,  failed, aborted, migrated
	AttemptNum    sql.NullInt64
	ExecTime      sql.NullTime
	RetryTime     sql.NullTime
	ErrorCode     sql.NullString
	ErrorResponse sql.NullString
}

func (jd *HandleT) getNonMigratedJobsDS(ds dataSetT, count int) ([]*JobT, error) {
	var rows *sql.Rows
	var err error

	// What does isEmptyResult do?
	// TODO: Understand this and check if this should be uncommented or removed
	// if jd.isEmptyResult(ds, []string{"NP"}, customValFilters, parameterFilters) {
	// 	logger.Debugf("[getUnprocessedJobsDS] Empty cache hit for ds: %v, stateFilters: NP, customValFilters: %v, parameterFilters: %v", ds, customValFilters, parameterFilters)
	// 	return []*JobT{}, nil
	// }

	var sqlStatement string

	sqlStatement = fmt.Sprintf(`
		SELECT * FROM (
			SELECT DISTINCT ON (%[1]s.job_id)
				%[1]s.job_id, %[1]s.uuid, %[1]s.parameters, %[1]s.custom_val,
				%[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at,
				%[2]s.job_state, %[2]s.attempt, %[2]s.exec_time,
				%[2]s.retry_time, %[2]s.error_code, %[2]s.error_response
			FROM %[1]s LEFT JOIN %[2]s
				ON %[1]s.job_id = %[2]s.job_id
			order by %[1]s.job_id asc, %[2]s.id desc
		) as temp WHERE job_state IS NULL OR (job_state != 'migrating' AND job_state != 'migrated' AND job_state != 'wont_migrate')`, ds.JobTable, ds.JobStatusTable)

	if count > 0 {
		sqlStatement += fmt.Sprintf(" LIMIT %d", count)
	}

	logger.Info(sqlStatement)
	rows, err = jd.dbHandle.Query(sqlStatement)
	jd.assertError(err)
	defer rows.Close()

	var jobList []*JobT
	sqlJobStatusT := SQLJobStatusT{}
	for rows.Next() {
		var job JobT
		err := rows.Scan(&job.JobID, &job.UUID, &job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.CreatedAt, &job.ExpireAt,
			&sqlJobStatusT.JobState, &sqlJobStatusT.AttemptNum,
			&sqlJobStatusT.ExecTime, &sqlJobStatusT.RetryTime,
			&sqlJobStatusT.ErrorCode, &sqlJobStatusT.ErrorResponse)
		if err != nil {
			logger.Info(err)
		}
		jd.assertError(err)
		if sqlJobStatusT.JobState.Valid {
			rows.Scan(&job.JobID, &job.UUID, &job.Parameters, &job.CustomVal,
				&job.EventPayload, &job.CreatedAt, &job.ExpireAt,
				&job.LastJobStatus.JobState, &job.LastJobStatus.AttemptNum,
				&job.LastJobStatus.ExecTime, &job.LastJobStatus.RetryTime,
				&job.LastJobStatus.ErrorCode, &job.LastJobStatus.ErrorResponse)
			job.LastJobStatus.JobID = job.JobID
		}
		jobList = append(jobList, &job)
	}

	return jobList, nil
}

//PreExportCleanup removes all the entries from job_status_tables that are of state 'migrating'
func (jd *HandleT) PreExportCleanup() {
	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList(false)

	for _, ds := range dsList {
		jd.deleteMigratingJobStatusDS(ds)
	}
}

//PostExportCleanup removes all the entries from job_status_tables that are of state 'wont_migrate' or 'migrating'
func (jd *HandleT) PostExportCleanup() {
	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList(false)

	for _, ds := range dsList {
		jd.deleteWontMigrateJobStatusDS(ds)
		jd.deleteMigratingJobStatusDS(ds)
	}
}

func (jd *HandleT) deleteWontMigrateJobStatusDS(ds dataSetT) {
	sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE job_state='wont_migrate'`, ds.JobStatusTable)
	logger.Info(sqlStatement)
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
}

func (jd *HandleT) deleteMigratingJobStatusDS(ds dataSetT) {
	sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE job_state='migrating'`, ds.JobStatusTable)
	logger.Info(sqlStatement)
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
}

//GetUserID from job
func (jd *HandleT) GetUserID(job *JobT) string {
	return job.UserID
}

//ShouldExport tells if export should happen in migration
func (jd *HandleT) ShouldExport() bool {
	migrationStates := jd.GetCheckpoints(ExportOp)
	if len(migrationStates) > 1 {
		lastExportMigrationState := migrationStates[len(migrationStates)-1]
		if lastExportMigrationState.ToNode == "All" && lastExportMigrationState.Status == Exported {
			return false
		}
	}
	return true
}
