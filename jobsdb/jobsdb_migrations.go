package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sync"

	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//MigrationState maintains the state required during the migration process
type MigrationState struct {
	sequenceProvider     SequenceProvider
	DsForNewEvents       dataSetT `json:"dsForNewEvents"`
	DsForImport          dataSetT `json:"dsForImport"`
	LastDsForExport      dataSetT `json:"lastDsForExport"`
	importDSCreationLock sync.RWMutex
}

/*
SetupForMigrateAndAcceptNewEvents is used to initialize the MigrationState structure.
It creates a new ds if an empty ds is not already present for incoming events
It sets up sequence provider to last id in the previous table
It marks the DsForNewEvents(create if not exists), the ds to which new events can go to
It also marks the last populated dsIndex before migration start
version = used to set the id space of new incoming events to version*10^13
*/
func (jd *HandleT) SetupForMigrateAndAcceptNewEvents(version int) {
	jd.dsListLock.Lock()
	defer jd.dsListLock.Unlock()
	dsList := jd.getDSList(true)
	importDSMin := int64(0)

	migrationState := MigrationState{}
	migrationEvents := jd.GetCheckpoints(ExportOp)
	if len(migrationEvents) > 0 {
		json.Unmarshal([]byte(migrationEvents[0].Payload), &migrationState)
	}

	//TODO: Need to improvise this
	var shouldSetSequenceForDsForNewEvents bool
	if migrationState.DsForNewEvents.Index == "" {
		shouldSetSequenceForDsForNewEvents = true
	}

	if !jd.isEmpty(dsList[len(dsList)-1]) {
		importDSMin = jd.getMaxIDForDs(dsList[len(dsList)-1])
		if migrationState.DsForNewEvents.Index == "" {
			migrationState.DsForNewEvents = jd.addNewDS(true, dataSetT{})
		}
	} else {
		if len(dsList) > 1 {
			importDSMin = jd.getMaxIDForDs(dsList[len(dsList)-2])
		}
		if migrationState.DsForNewEvents.Index == "" {
			migrationState.DsForNewEvents = dsList[len(dsList)-1]
		}
	}

	dsList = jd.getDSList(true)

	if migrationState.LastDsForExport.Index == "" {
		for idx, ds := range dsList {
			if jd.migrationState.DsForNewEvents.Index == ds.Index {
				if idx > 0 {
					migrationState.LastDsForExport = dsList[idx-1]
				}
			}
		}
	}

	if shouldSetSequenceForDsForNewEvents {
		seqNoForNewDS := int64(version) * int64(math.Pow10(13))
		jd.updateSequenceNumber(migrationState.DsForNewEvents, seqNoForNewDS)
		logger.Infof("Jobsdb: New dataSet %s is prepared with start sequence : %d", jd.migrationState.DsForNewEvents, seqNoForNewDS)
	}

	jd.migrationState.DsForNewEvents = migrationState.DsForNewEvents
	jd.migrationState.LastDsForExport = migrationState.LastDsForExport

	jd.migrationState.sequenceProvider = NewSequenceProvider(importDSMin + 1)
	migrationEvent := NewMigrationEvent(ExportOp, misc.GetNodeID(), "All", "", PreparedForExport, 0)
	payloadBytes, err := json.Marshal(migrationState)
	if err != nil {
		panic("Unable to Marshal")
	}
	migrationEvent.Payload = string(payloadBytes)
	jd.Checkpoint(&migrationEvent)
}

func (jd *HandleT) getMaxIDForDs(ds dataSetT) int64 {
	var maxID sql.NullInt64
	sqlStatement := fmt.Sprintf(`SELECT MAX(job_id) FROM %s`, ds.JobTable)
	row := jd.dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&maxID)
	jd.assertError(err)
	var max int64
	if maxID.Valid {
		max = int64(maxID.Int64)
	} else {
		panic("Unable to get max")
	}
	return max
}

func (jd *HandleT) isEmpty(ds dataSetT) bool {
	var count sql.NullInt64
	sqlStatement := fmt.Sprintf(`SELECT count(*) from %s`, ds.JobTable)
	row := jd.dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&count)
	jd.assertError(err)
	if !count.Valid {
		panic("Unable to get count on this dataset")
	} else {
		return count.Int64 == 0
	}
}

func (jd *HandleT) updateSequenceNumber(ds dataSetT, sequenceNumber int64) {
	sqlStatement := fmt.Sprintf(`SELECT setval('%s_jobs_%s_job_id_seq', %d)`,
		jd.tablePrefix, ds.Index, sequenceNumber)
	_, err := jd.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err.Error())
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

		//Instead of full dsList, it needs to do only till the dataset before import and newEvent datasets
		if ds.Index == jd.migrationState.LastDsForExport.Index {
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

func (jd *HandleT) getStartJobID(count int, migrationEvent MigrationEvent) int64 {
	var sequenceNumber int64
	sequenceNumber = 0
	sequenceNumber = jd.getSeqNoForFileFromDB(migrationEvent.FileLocation, ImportOp)
	if sequenceNumber == 0 {
		sequenceNumber = jd.migrationState.sequenceProvider.ReserveIds(count)
	}
	migrationEvent.StartSeq = sequenceNumber
	jd.Checkpoint(&migrationEvent)
	return sequenceNumber
}

//StoreImportedJobsAndJobStatuses is used to write the jobs to _tables
func (jd *HandleT) StoreImportedJobsAndJobStatuses(jobList []*JobT, fileName string, migrationEvent MigrationEvent) {
	startJobID := jd.getStartJobID(len(jobList), migrationEvent)
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

	migrationState := MigrationState{}
	migrationEvents := jd.GetCheckpoints(ExportOp)
	if len(migrationEvents) > 0 {
		json.Unmarshal([]byte(migrationEvents[0].Payload), &migrationState)
	}

	targetDS := dataSetT{"", "", "Unset"}

	jd.migrationState.importDSCreationLock.Lock()
	if migrationState.DsForImport.Index == "" {
		migrationState.DsForImport = jd.addNewDS(false, jd.migrationState.DsForNewEvents)
		payloadBytes, err := json.Marshal(migrationState)
		if err != nil {
			panic("Unable to Marshal")
		}
		migrationEvent.Payload = string(payloadBytes)
		jd.Checkpoint(&migrationEvent)
	}

	jd.migrationState.importDSCreationLock.Unlock()

	if targetDS.Index == "Unset" {
		for idx, ds := range dsList {
			if ds.Index == jd.migrationState.DsForNewEvents.Index {
				targetDS = dsList[idx-1] //before this assert idx > 0
			}
		}
	}

	if targetDS.Index == "Unset" {
		panic("No ds found to migrate to")
	}

	jd.dsListLock.Unlock()

	//Take proper locks(may be not required) and move the two lines below into a single transaction
	jd.storeJobsDS(targetDS, true, true, jobList)
	//what is retry each expected to do?
	//TODO: Verify what is going to happen if the same jobs are imported again

	jd.updateJobStatusDS(targetDS, statusList, []string{}, []ParameterFilterT{})
}

//GetTablePrefix returns the table prefix of the jobsdb
func (jd *HandleT) GetTablePrefix() string {
	return jd.tablePrefix
}

//GetUserID from job
func (jd *HandleT) GetUserID(job *JobT) string {
	var userID string
	switch jd.GetTablePrefix() {
	case "gw":
		eventList, ok := misc.ParseRudderEventBatch(job.EventPayload)
		if !ok {
			//TODO: This can't be happening. This is done only to get userId/anonId. There should be a more reliable way.
			panic("Migrator: This can't be happening. This is done only to get userId/anonId. There should be a more reliable way.")
		}
		userID, ok = misc.GetAnonymousID(eventList[0])
	case "rt":
		userID = integrations.GetUserIDFromTransformerResponse(job.EventPayload)
	case "batch_rt":
		parsed, status := misc.ParseBatchRouterJob(job.EventPayload)
		if status {
			userID = fmt.Sprintf("%v", parsed["anonymousId"])
		} else {
			panic("Not able to get userId/AnonymousId from batch job")
		}
	}
	return userID
}

//ShouldExport tells if export should happen in migration
func (jd *HandleT) ShouldExport() bool {
	migrationStates := jd.GetCheckpoints(ExportOp)
	if len(migrationStates) > 1 {
		if migrationStates[len(migrationStates)-1].ToNode == "All" {
			return false
		}
	}
	return true
}
