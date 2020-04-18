package jobsdb

import (
	"database/sql"
	"fmt"
	"math"
	"sync"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

//MigrationState maintains the state required during the migration process
type MigrationState struct {
	sequenceProvider        SequenceProvider
	dsForNewEvents          dataSetT
	isDsForMigrationCreated bool
	migrationDSCreationLock sync.RWMutex
}

/*
SetupForImportAndAcceptNewEvents is used to initialize the HandleT structure.
clearAll = True means it will remove all existing tables
tablePrefix must be unique and is used to separate
multiple users of JobsDB
dsRetentionPeriod = A DS is not deleted if it has some activity
in the retention time
*/
func (jd *HandleT) SetupForImportAndAcceptNewEvents(version int, isNew bool) {
	jd.dsListLock.Lock()
	defer jd.dsListLock.Unlock()
	dsList := jd.getDSList(true)
	newDSMin := int64(0)

	if !isNew {
		var minID, maxID sql.NullInt64
		sqlStatement := fmt.Sprintf(`SELECT MIN(job_id), MAX(job_id) FROM %s`, dsList[len(dsList)-1].JobTable)
		row := jd.dbHandle.QueryRow(sqlStatement)
		err := row.Scan(&minID, &maxID)
		jd.assertError(err)
		if maxID.Valid {
			newDSMin = int64(maxID.Int64)
		} else {
			panic("Unable to get max")
		}
		jd.migrationState.dsForNewEvents = jd.addNewDS(true, dataSetT{})
	} else {
		jd.migrationState.dsForNewEvents = dsList[0]
	}
	jd.updateSequenceNumber(jd.migrationState.dsForNewEvents, int64(version)*int64(math.Pow10(13)), jd.tablePrefix)
	jd.migrationState.sequenceProvider = NewSequenceProvider(newDSMin + 1)
}

func (jd *HandleT) updateSequenceNumber(ds dataSetT, sequenceNumber int64, tablePrefix string) {
	sqlStatement := fmt.Sprintf(`SELECT setval('%s_jobs_%s_job_id_seq', %d)`,
		tablePrefix, ds.Index, sequenceNumber)
	_, err := jd.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic("Unable to set sequence number")
	}
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
		jd.assertError(err)
		outJobs = append(outJobs, jobs...)
		count -= len(jobs)
		jd.assert(count >= 0, fmt.Sprintf("count:%d received is less than 0", count))
		if count == 0 {
			break
		}
	}
	//Release lock
	return outJobs
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
		) as temp WHERE job_state IS NULL OR (job_state != 'migrated' AND job_state != 'wont_migrate')`, ds.JobTable, ds.JobStatusTable)

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

//PostMigrationCleanup removes all the entries from job_status_tables that are of state 'wont_migrate'
func (jd *HandleT) PostMigrationCleanup() {
	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList(false)

	for _, ds := range dsList {
		jd.deleteWontMigrateJobStatusDS(ds)
	}
}

func (jd *HandleT) deleteWontMigrateJobStatusDS(ds dataSetT) {
	sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE job_state='wont_migrate'`, ds.JobStatusTable)
	logger.Info(sqlStatement)
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
}

//StoreImportedJobsAndJobStatuses is used to write the jobs to _tables
func (jd *HandleT) StoreImportedJobsAndJobStatuses(jobList []*JobT) {
	startJobID := jd.migrationState.sequenceProvider.ReserveIds(len(jobList))

	statusList := []*JobStatusT{}

	for idx, job := range jobList {
		jobID := startJobID + int64(idx)
		job.JobID = jobID
		job.LastJobStatus.JobID = jobID
		if job.LastJobStatus.JobState != "" {
			statusList = append(statusList, &job.LastJobStatus)
		}
	}

	jd.dsListLock.Lock()
	dsList := jd.getDSList(true)

	targetDS := dataSetT{"", "", "Unset"}

	jd.migrationState.migrationDSCreationLock.Lock()
	if !jd.migrationState.isDsForMigrationCreated {
		targetDS = jd.addNewDS(false, jd.migrationState.dsForNewEvents)
		jd.migrationState.isDsForMigrationCreated = true
	}
	jd.migrationState.migrationDSCreationLock.Unlock()

	if targetDS.Index == "Unset" {
		for idx, ds := range dsList {
			if ds.Index == jd.migrationState.dsForNewEvents.Index {
				targetDS = dsList[idx-1] //before this assert idx > 0
			}
		}
	}

	if targetDS.Index == "Unset" {
		panic("No ds found to migrate to")
	}

	jd.dsListLock.Unlock()

	//Take proper locks(may be not required) and move the two lines below into a single transaction
	jd.storeJobsDS(targetDS, true, true, jobList) //what is retry each expected to do?
	jd.updateJobStatusDS(targetDS, statusList, []string{}, []ParameterFilterT{})
}
