package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/stats"
)

//SetupForImport is used to setup jobsdb for export or for import or for both
func (jd *HandleT) SetupForImport() {
	jd.migrationState.dsForNewEvents = jd.findOrCreateDsFromSetupCheckpoint(AcceptNewEventsOp)
	jd.logger.Infof("[[ %s-JobsDB Import ]] Ds for new events :%v", jd.GetTablePrefix(), jd.migrationState.dsForNewEvents)
}

func (jd *HandleT) getDsForImport(dsList []dataSetT) dataSetT {
	ds := newDataSet(jd.tablePrefix, jd.computeNewIdxForInterNodeMigration(jd.migrationState.dsForNewEvents))
	jd.addDS(ds)
	jd.logger.Infof("[[ %s-JobsDB Import ]] Should Checkpoint Import Setup event for the new ds : %v", jd.GetTablePrefix(), ds)
	return ds
}

//GetLastJobIDBeforeImport should return the largest job id stored so far
func (jd *HandleT) GetLastJobIDBeforeImport() int64 {
	jd.assert(jd.migrationState.dsForNewEvents.Index != "", "dsForNewEvents must be setup before calling this")
	jd.dsListLock.Lock()
	defer jd.dsListLock.Unlock()

	dsList := jd.getDSList(true)
	var lastJobIDBeforeNewImports int64
	for idx, dataSet := range dsList {
		if dataSet.Index == jd.migrationState.dsForNewEvents.Index {
			if idx > 0 {
				lastJobIDBeforeNewImports = jd.GetMaxIDForDs(dsList[idx-1])
			}
		}
	}

	return lastJobIDBeforeNewImports
}

//getDsForNewEvents always returns the jobs_1 table to which all the new events are written.
//In place migration is not supported anymore, so this should suffice.
func (jd *HandleT) getDsForNewEvents(dsList []dataSetT) dataSetT {
	return dataSetT{JobTable: fmt.Sprintf("%s_jobs_1", jd.tablePrefix), JobStatusTable: fmt.Sprintf("%s_job_status_1", jd.tablePrefix), Index: "1"}
}

//StoreJobsAndCheckpoint is used to write the jobs to _tables
func (jd *HandleT) StoreJobsAndCheckpoint(jobList []*JobT, migrationCheckpoint MigrationCheckpointT) {
	queryStat := stats.NewTaggedStat("store_imported_jobs_and_statuses", stats.TimerType, stats.Tags{"customVal": jd.tablePrefix})
	queryStat.Start()
	defer queryStat.End()

	var found bool
	var opID int64
	jd.migrationState.importLock.Lock()
	defer jd.migrationState.importLock.Unlock()

	// This if block should be idempotent. It is currently good. But if it changes we need to add separate locks outside
	if jd.migrationState.dsForImport.Index == "" {
		jd.migrationState.dsForImport, found = jd.findDsFromSetupCheckpoint(ImportOp)
	} else {
		found = true
	}

	if !found {
		jd.migrationState.dsForImport = jd.createSetupCheckpointAndGetDs(ImportOp)
		opPayload, err := json.Marshal(&jd.migrationState.dsForImport)
		jd.assertError(err)
		opID = jd.JournalMarkStart(migrateImportOperation, opPayload)
	} else if jd.checkIfFullDS(jd.migrationState.dsForImport) {
		jd.dsListLock.Lock()
		jd.migrationState.dsForImport = newDataSet(jd.tablePrefix, jd.computeNewIdxForInterNodeMigration(jd.migrationState.dsForNewEvents))
		jd.addDS(jd.migrationState.dsForImport)
		setupCheckpoint, found := jd.GetSetupCheckpoint(ImportOp)
		jd.assert(found, "There should be a setup checkpoint at this point. If not something went wrong. Go debug")
		setupCheckpoint.Payload, _ = json.Marshal(jd.migrationState.dsForImport)
		jd.Checkpoint(setupCheckpoint)
		jd.dsListLock.Unlock()
		opPayload, err := json.Marshal(&jd.migrationState.dsForImport)
		jd.assertError(err)
		opID = jd.JournalMarkStart(migrateImportOperation, opPayload)
	}

	jd.assert(jd.migrationState.dsForImport.Index != "", "dsForImportEvents was not setup. Go debug")

	jd.assert(migrationCheckpoint.StartSeq != 0, "Start sequence should have been assigned by importer before giving to jobsdb_import")
	statusList := []*JobStatusT{}
	for idx, job := range jobList {
		jobID := migrationCheckpoint.StartSeq + int64(idx)
		job.JobID = jobID
		job.LastJobStatus.JobID = jobID
		if job.LastJobStatus.JobState != "" {
			statusList = append(statusList, &job.LastJobStatus)
		}
	}

	txn, err := jd.dbHandle.Begin()
	jd.assertError(err)

	jd.logger.Debugf("[[ %s-JobsDB Import ]] %d jobs found in file:%s. Writing to db", jd.GetTablePrefix(), len(jobList), migrationCheckpoint.FileLocation)
	err = jd.copyJobsDSInTx(txn, jd.migrationState.dsForImport, jobList)
	jd.assertErrorAndRollbackTx(err, txn)

	jd.logger.Debugf("[[ %s-JobsDB Import ]] %d job_statuses found in file:%s. Writing to db", jd.GetTablePrefix(), len(statusList), migrationCheckpoint.FileLocation)
	_, err = jd.updateJobStatusDSInTx(txn, jd.migrationState.dsForImport, statusList, statTags{}) //Not collecting updatedStates here because the entire ds is un-marked for empty result after commit below
	jd.assertErrorAndRollbackTx(err, txn)

	migrationCheckpoint.Status = Imported
	_, err = jd.CheckpointInTxn(txn, migrationCheckpoint)
	jd.assertErrorAndRollbackTx(err, txn)

	if opID != 0 {
		err = jd.journalMarkDoneInTx(txn, opID)
		jd.assertErrorAndRollbackTx(err, txn)
	}

	err = txn.Commit()
	jd.assertError(err)

	//Clear ds from cache
	jd.dropDSFromCache(jd.migrationState.dsForImport)
}

func (jd *HandleT) updateSequenceNumber(ds dataSetT, sequenceNumber int64) {
	sqlStatement := fmt.Sprintf(`SELECT setval('%s_jobs_%s_job_id_seq', %d)`,
		jd.tablePrefix, ds.Index, sequenceNumber)
	_, err := jd.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err.Error())
	}
}

func (jd *HandleT) GetMaxIDForDs(ds dataSetT) int64 {
	var maxID sql.NullInt64
	sqlStatement := fmt.Sprintf(`SELECT MAX(job_id) FROM %s`, ds.JobTable)
	row := jd.dbHandle.QueryRow(sqlStatement)
	row.Scan(&maxID)

	if maxID.Valid {
		return int64(maxID.Int64)
	}
	return int64(0)

}

//UpdateSequenceNumberOfLatestDS updates (if not already updated) the sequence number of the right most dataset to the seq no provided.
func (jd *HandleT) UpdateSequenceNumberOfLatestDS(seqNoForNewDS int64) {
	jd.dsListLock.RLock()
	defer jd.dsListLock.RUnlock()

	dsList := jd.getDSList(false)
	dsListLen := len(dsList)
	var ds dataSetT
	if jd.isEmpty(dsList[dsListLen-1]) {
		ds = dsList[dsListLen-1]
	} else {
		ds = newDataSet(jd.tablePrefix, jd.computeNewIdxForAppend())
		jd.addNewDS(ds)
	}

	var serialInt sql.NullInt64
	sqlStatement := fmt.Sprintf(`SELECT nextval(pg_get_serial_sequence('%s', 'job_id'))`, ds.JobTable)
	row := jd.dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&serialInt)
	jd.assertError(err)

	if serialInt.Int64 < seqNoForNewDS {
		jd.updateSequenceNumber(ds, seqNoForNewDS)
		jd.logger.Infof("DataSet(%s)'s sequence number updated to : %d", ds, seqNoForNewDS)
	}
}
