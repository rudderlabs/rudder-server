package jobsdb

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

//SetupForExport is used to setup jobsdb for export or for import or for both
func (jd *HandleT) SetupForExport() {
	jd.migrationState.lastDsForExport = jd.findOrCreateDsFromSetupCheckpoint(ExportOp)
	logger.Infof("[[ %s-JobsDB Export ]] Last ds for export : %v", jd.GetTablePrefix(), jd.migrationState.lastDsForExport)
}

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

//GetNonMigratedAndMarkMigrating all jobs with no filters
func (jd *HandleT) GetNonMigratedAndMarkMigrating(count int) []*JobT {
	logger.Debugf("[[ %s-JobsDB export ]] Inside GetNonMigrated waiting for locks", jd.GetTablePrefix())
	//The order of lock is very important. The mainCheckLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	jd.dsMigrationLock.RLock()
	jd.dsListLock.RLock()
	defer jd.dsMigrationLock.RUnlock()
	defer jd.dsListLock.RUnlock()
	logger.Debugf("[[ %s-JobsDB export ]] Inside GetNonMigrated and got locks", jd.GetTablePrefix())

	dsList := jd.getDSList(false)
	outJobs := make([]*JobT, 0)
	jd.assert(count >= 0, fmt.Sprintf("count:%d received is less than 0", count))
	if count == 0 {
		return outJobs
	}

	//Nothing to export in this case
	if jd.migrationState.lastDsForExport.Index == "" {
		return outJobs
	}

	for _, ds := range dsList {
		jd.assert(count > 0, fmt.Sprintf("count:%d is less than or equal to 0", count))
		jobs, err := jd.getNonMigratedJobsAndMarkMigratingDS(ds, count)
		jd.assertError(err)
		outJobs = append(outJobs, jobs...)
		count -= len(jobs)
		jd.assert(count >= 0, fmt.Sprintf("count:%d received is less than 0", count))
		if count == 0 {
			break
		}

		//Instead of full dsList, it needs to do only till the dataset before import and newEvent datasets
		if ds.Index == jd.migrationState.lastDsForExport.Index {
			break
		}
	}
	//Release lock
	return outJobs
}

//BuildStatus generates a struct of type JobStatusT for a given job and jobState
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

func (jd *HandleT) getNonMigratedJobsAndMarkMigratingDS(ds dataSetT, count int) ([]*JobT, error) {
	var rows *sql.Rows
	var err error

	var sqlStatement string

	sqlStatement = fmt.Sprintf(`
		SELECT * FROM (
			SELECT DISTINCT ON (%[1]s.job_id)
				%[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val,
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
		err := rows.Scan(&job.JobID, &job.UUID, &job.UserID,
			&job.Parameters, &job.CustomVal,
			&job.EventPayload, &job.CreatedAt, &job.ExpireAt,
			&sqlJobStatusT.JobState, &sqlJobStatusT.AttemptNum,
			&sqlJobStatusT.ExecTime, &sqlJobStatusT.RetryTime,
			&sqlJobStatusT.ErrorCode, &sqlJobStatusT.ErrorResponse)
		if err != nil {
			logger.Info(err)
		}
		jd.assertError(err)
		if sqlJobStatusT.JobState.Valid {
			err = rows.Scan(&job.JobID, &job.UUID, &job.UserID,
				&job.Parameters, &job.CustomVal,
				&job.EventPayload, &job.CreatedAt, &job.ExpireAt,
				&job.LastJobStatus.JobState, &job.LastJobStatus.AttemptNum,
				&job.LastJobStatus.ExecTime, &job.LastJobStatus.RetryTime,
				&job.LastJobStatus.ErrorCode, &job.LastJobStatus.ErrorResponse)
			job.LastJobStatus.JobID = job.JobID
			jd.assertError(err)
		}
		jobList = append(jobList, &job)
	}

	var statusList []*JobStatusT
	for _, job := range jobList {
		statusList = append(statusList, BuildStatus(job, MigratingState))
	}

	jd.updateJobStatusDS(ds, statusList, []string{}, []ParameterFilterT{})

	return jobList, nil
}

//UpdateJobStatusAndCheckpoint does update job status and checkpoint in a single transaction
func (jd *HandleT) UpdateJobStatusAndCheckpoint(statusList []*JobStatusT, fromNodeID string, toNodeID string, uploadLocation string) {
	txn, err := jd.dbHandle.Begin()
	jd.assertError(err)
	//TODO REMOVE
	logger.Debug("[DEFER UpdateJobStatusAndCheckpoint] rolling back transaction")
	defer txn.Rollback() //TODO: Review this. In a successful case rollback will be called after commit. In a failure case there will be a panic and a dangling db connection may be left
	jd.UpdateJobStatusInTxn(txn, statusList, []string{}, []ParameterFilterT{})
	migrationEvent := NewMigrationEvent(ExportOp, fromNodeID, toNodeID, uploadLocation, Exported, 0)
	migrationEvent.ID = jd.CheckpointInTxn(txn, &migrationEvent)
	txn.Commit()
}

//IsMigrating returns true if there are non zero jobs with status = 'migrating'
func (jd *HandleT) IsMigrating() bool {

	//The order of lock is very important. The mainCheckLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	jd.dsMigrationLock.RLock()
	jd.dsListLock.RLock()
	defer jd.dsMigrationLock.RUnlock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList(false)

	if jd.migrationState.lastDsForExport.Index == "" {
		return false
	}

	//This can be optimized by keeping 0 count ds in memory and avoid querying on them.
	for _, ds := range dsList {
		nonExportedCount := jd.getNonExportedJobsCountDS(ds)
		if nonExportedCount > 0 {
			return true
		}
		if ds.Index == jd.migrationState.lastDsForExport.Index {
			break
		}
	}
	return false
}

func (jd *HandleT) getNonExportedJobsCountDS(ds dataSetT) int64 {
	var sqlStatement string

	sqlStatement = fmt.Sprintf(`
		SELECT count(*) FROM (
			SELECT DISTINCT ON (%[1]s.job_id)
				%[1]s.job_id, %[1]s.uuid, %[1]s.user_id, %[1]s.parameters, %[1]s.custom_val,
				%[1]s.event_payload, %[1]s.created_at, %[1]s.expire_at,
				%[2]s.job_state, %[2]s.attempt, %[2]s.exec_time,
				%[2]s.retry_time, %[2]s.error_code, %[2]s.error_response
			FROM %[1]s LEFT JOIN %[2]s
				ON %[1]s.job_id = %[2]s.job_id
			order by %[1]s.job_id asc, %[2]s.id desc
		) as temp WHERE job_state IS NULL OR (job_state != 'migrated' AND job_state != 'wont_migrate')`, ds.JobTable, ds.JobStatusTable)

	logger.Info(sqlStatement)

	row := jd.dbHandle.QueryRow(sqlStatement)
	var count sql.NullInt64
	err := row.Scan(&count)
	jd.assertError(err)
	if count.Valid {
		return int64(count.Int64)
	}
	return int64(0)
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
