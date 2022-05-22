package jobsdb

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//SetupForExport is used to setup jobsdb for export or for import or for both
func (jd *HandleT) SetupForExport() {
	jd.migrationState.lastDsForExport = jd.findOrCreateDsFromSetupCheckpoint(ExportOp)
	jd.migrationState.nonExportedJobsCountByDS = make(map[string]int64)
	jd.migrationState.doesDSHaveJobsToMigrateMap = make(map[string]bool)
	jd.logger.Infof("[[ %s-JobsDB Export ]] Last ds for export : %v", jd.GetTablePrefix(), jd.migrationState.lastDsForExport)
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
	queryStat := stats.NewTaggedStat("get_for_export_and_update_status", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	jd.logger.Debugf("[[ %s-JobsDB export ]] Inside GetNonMigrated waiting for locks", jd.GetTablePrefix())
	//The order of lock is very important. The mainCheckLoop
	//takes lock in this order so reversing this will cause
	//deadlocks
	jd.dsMigrationLock.RLock()
	jd.dsListLock.RLock()
	defer jd.dsMigrationLock.RUnlock()
	defer jd.dsListLock.RUnlock()
	jd.logger.Debugf("[[ %s-JobsDB export ]] Inside GetNonMigrated and got locks", jd.GetTablePrefix())

	dsList := jd.getDSList(false)
	pkgLogger.Debugf("[[ %s-JobsDB export ]] Inside GetNonMigrated - dsList: %+v", jd.tablePrefix, dsList)
	outJobs := make([]*JobT, 0)
	jd.assert(count >= 0, fmt.Sprintf("count:%d received is less than 0", count))
	if count == 0 {
		return outJobs
	}

	//Nothing to export in this case
	if jd.migrationState.lastDsForExport.Index == "" {
		return outJobs
	}

	txn, txErr := jd.dbHandle.Begin()
	jd.assertError(txErr)
	var err error
	updatedStatesByDS := make(map[dataSetT]map[string][]string)
	for _, ds := range dsList {
		jd.assert(count > 0, fmt.Sprintf("count:%d is less than or equal to 0", count))

		pkgLogger.Debugf("[[ %s-JobsDB export ]] Inside GetNonMigrated - doesDSHaveJobsToMigrateMap: %+v", jd.tablePrefix, jd.migrationState.doesDSHaveJobsToMigrateMap)
		doesDSHaveJobsToMigrate, found := jd.migrationState.doesDSHaveJobsToMigrateMap[ds.Index]
		if found && !doesDSHaveJobsToMigrate {
			continue
		}

		var jobs []*JobT
		jobs, err = jd.getNonMigratedJobsFromDS(ds, count)
		if err != nil {
			break
		}
		pkgLogger.Debugf("[[ %s-JobsDB export ]] Inside GetNonMigrated - len(jobs): %d", jd.tablePrefix, len(jobs))

		var statusList []*JobStatusT
		for _, job := range jobs {
			statusList = append(statusList, BuildStatus(job, Migrating.State))
		}

		var updatedStates map[string][]string
		updatedStates, txErr = jd.updateJobStatusDSInTx(txn, ds, statusList, statTags{StateFilters: []string{Migrating.State}})
		if txErr != nil {
			break
		}
		updatedStatesByDS[ds] = updatedStates

		if len(jobs) == 0 {
			jd.migrationState.doesDSHaveJobsToMigrateMap[ds.Index] = false
		}

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
	jd.assertErrorAndRollbackTx(err, txn)
	jd.assertErrorAndRollbackTx(txErr, txn)

	err = txn.Commit()
	jd.assertError(err)

	for ds, updatedStatesByWorkspace := range updatedStatesByDS {
		allUpdatedStates := make([]string, 0)
		for workspace, updatedStates := range updatedStatesByWorkspace {
			jd.markClearEmptyResult(ds, workspace, updatedStates, []string{}, []ParameterFilterT{}, hasJobs, nil)
			allUpdatedStates = append(allUpdatedStates, updatedStates...)
		}
		//NOTE: Along with clearing cache for a particular workspace key, we also have to clear for allWorkspaces key
		jd.markClearEmptyResult(ds, allWorkspaces, misc.Unique(allUpdatedStates), []string{}, []ParameterFilterT{}, hasJobs, nil)
	}
	jd.assertError(err)

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
		Parameters:    []byte(`{}`),
		WorkspaceId:   job.WorkspaceId,
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

func (jd *HandleT) getNonMigratedJobsFromDS(ds dataSetT, count int) ([]*JobT, error) {
	queryStat := stats.NewTaggedStat("get_for_export_and_update_status_ds", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

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

	jd.assert(count > 0, fmt.Sprintf("count should be greater than 0, but count = %d", count))
	sqlStatement += fmt.Sprintf(" LIMIT %d", count)

	jd.logger.Info(sqlStatement)
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
			jd.logger.Info(err)
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

	return jobList, nil
}

//UpdateJobStatusAndCheckpoint does update job status and checkpoint in a single transaction
func (jd *HandleT) UpdateJobStatusAndCheckpoint(statusList []*JobStatusT, fromNodeID string, toNodeID string, jobsCount int64, uploadLocation string) {
	queryStat := stats.NewTaggedStat("update_status_and_checkpoint", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()
	txn, err := jd.dbHandle.Begin()
	jd.assertError(err)

	var updatedStatesMap map[dataSetT]map[string][]string
	updatedStatesMap, err = jd.doUpdateJobStatusInTx(txn, statusList, statTags{})
	jd.assertErrorAndRollbackTx(err, txn)

	migrationCheckpoint := NewMigrationCheckpoint(ExportOp, fromNodeID, toNodeID, jobsCount, uploadLocation, Exported, 0)
	migrationCheckpoint.ID, err = jd.CheckpointInTxn(txn, migrationCheckpoint)
	jd.assertErrorAndRollbackTx(err, txn)

	err = txn.Commit()
	jd.assertError(err)
	for ds, updatedStatesByWorkspace := range updatedStatesMap {
		allUpdatedStates := make([]string, 0)
		for workspace, updatedStates := range updatedStatesByWorkspace {
			jd.markClearEmptyResult(ds, workspace, updatedStates, []string{}, []ParameterFilterT{}, hasJobs, nil)
			allUpdatedStates = append(allUpdatedStates, updatedStates...)
		}
		//NOTE: Along with clearing cache for a particular workspace key, we also have to clear for allWorkspaces key
		jd.markClearEmptyResult(ds, allWorkspaces, misc.Unique(allUpdatedStates), []string{}, []ParameterFilterT{}, hasJobs, nil)
	}
}

//IsMigrating returns true if there are non zero jobs with status = 'migrating'
func (jd *HandleT) IsMigrating() bool {
	queryStat := stats.NewTaggedStat("is_migrating_check", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

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

	for _, ds := range dsList {
		nonExportedCount, found := jd.migrationState.nonExportedJobsCountByDS[ds.Index]
		if !found || nonExportedCount > 0 {
			nonExportedCount = jd.getNonExportedJobsCountDS(ds)
			jd.migrationState.nonExportedJobsCountByDS[ds.Index] = nonExportedCount
		}
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
	queryStat := stats.NewTaggedStat("get_non_exported_job_count", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	sqlStatement := fmt.Sprintf(`
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

	jd.logger.Info(sqlStatement)

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
	queryStat := stats.NewTaggedStat("pre_export_cleanup", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()
	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList(false)

	for _, ds := range dsList {
		jd.deleteMigratingJobStatusDS(ds)
	}
}

//PostExportCleanup removes all the entries from job_status_tables that are of state 'wont_migrate' or 'migrating'
func (jd *HandleT) PostExportCleanup() {
	queryStat := stats.NewTaggedStat("post_export_cleanup", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()
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
	jd.logger.Info(sqlStatement)
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
}

func (jd *HandleT) deleteMigratingJobStatusDS(ds dataSetT) {
	sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE job_state='migrating'`, ds.JobStatusTable)
	jd.logger.Info(sqlStatement)
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
}

//GetUserID from job
func (*HandleT) GetUserID(job *JobT) string {
	return job.UserID
}
