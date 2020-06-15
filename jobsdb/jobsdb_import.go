package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"

	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

//SetupForImport is used to setup jobsdb for export or for import or for both
func (jd *HandleT) SetupForImport() {
	jd.migrationState.dsForNewEvents = jd.findOrCreateDsFromSetupCheckpoint(AcceptNewEventsOp)
	logger.Infof("[[ %s-JobsDB Import ]] Ds for new events :%v", jd.GetTablePrefix(), jd.migrationState.dsForNewEvents)
}

func (jd *HandleT) getDsForImport(dsList []dataSetT) dataSetT {
	ds := jd.addNewDS(insertForImport, jd.migrationState.dsForNewEvents)
	logger.Infof("[[ %s-JobsDB Import ]] Should Checkpoint Import Setup event for the new ds : %v", jd.GetTablePrefix(), ds)
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
				jd.assert(idx != len(dsList)-1, "Shouldn't be the last ds")
				lastJobIDBeforeNewImports = jd.getMaxIDForDs(dsList[idx-1])
			}
		}
	}

	return lastJobIDBeforeNewImports
}

func (jd *HandleT) getDsForNewEvents(dsList []dataSetT) dataSetT {
	dsListLen := len(dsList)
	var ds dataSetT
	if jd.isEmpty(dsList[dsListLen-1]) {
		ds = dsList[dsListLen-1]
	} else {
		ds = jd.addNewDS(appendToDsList, dataSetT{})
	}

	seqNoForNewDS := int64(jd.migrationState.toVersion)*int64(math.Pow10(13)) + 1
	jd.updateSequenceNumber(ds, seqNoForNewDS)
	logger.Infof("[[ %sJobsDB Import ]] New dataSet %s is prepared with start sequence : %d", jd.GetTablePrefix(), ds, seqNoForNewDS)
	return ds
}

//StoreJobsAndCheckpoint is used to write the jobs to _tables
func (jd *HandleT) StoreJobsAndCheckpoint(jobList []*JobT, migrationCheckpoint MigrationCheckpointT) {
	queryStat := stats.NewJobsDBStat("store_imported_jobs_and_statuses", stats.TimerType, jd.tablePrefix)
	queryStat.Start()
	defer queryStat.End()

	var found bool
	var opID int64
	jd.migrationState.importLock.Lock()

	// This if block should be idempotent. It is currently good. But if it changes we need to add separate locks outside
	if jd.migrationState.dsForImport.Index == "" {
		jd.migrationState.dsForImport, found = jd.findDsFromSetupCheckpoint(ImportOp)
	} else {
		found = true
	}

	if !found {
		defer jd.migrationState.importLock.Unlock()
		jd.migrationState.dsForImport = jd.createSetupCheckpointAndGetDs(ImportOp)
		opPayload, err := json.Marshal(&jd.migrationState.dsForImport)
		jd.assertError(err)
		opID = jd.JournalMarkStart(migrateImportOperation, opPayload)
	} else if jd.checkIfFullDS(jd.migrationState.dsForImport) {
		defer jd.migrationState.importLock.Unlock()
		jd.dsListLock.Lock()
		jd.migrationState.dsForImport = jd.addNewDS(insertForImport, jd.migrationState.dsForNewEvents)
		setupCheckpoint, found := jd.GetSetupCheckpoint(ImportOp)
		jd.assert(found, "There should be a setup checkpoint at this point. If not something went wrong. Go debug")
		setupCheckpoint.Payload, _ = json.Marshal(jd.migrationState.dsForImport)
		jd.Checkpoint(setupCheckpoint)
		jd.dsListLock.Unlock()
		opPayload, err := json.Marshal(&jd.migrationState.dsForImport)
		jd.assertError(err)
		opID = jd.JournalMarkStart(migrateImportOperation, opPayload)
	} else {
		jd.migrationState.importLock.Unlock()
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

	logger.Debugf("[[ %s-JobsDB Import ]] %d jobs found in file:%s. Writing to db", jd.GetTablePrefix(), len(jobList), migrationCheckpoint.FileLocation)
	err = jd.storeJobsDSInTxn(txn, jd.migrationState.dsForImport, true, jobList)
	jd.assertErrorAndRollbackTx(err, txn)

	logger.Debugf("[[ %s-JobsDB Import ]] %d job_statuses found in file:%s. Writing to db", jd.GetTablePrefix(), len(statusList), migrationCheckpoint.FileLocation)
	_, err = jd.updateJobStatusDSInTxn(txn, jd.migrationState.dsForImport, statusList) //Not collecting updatedStates here because the entire ds is un-marked for empty result after commit below
	jd.assertErrorAndRollbackTx(err, txn)

	migrationCheckpoint.Status = Imported
	_, err = jd.CheckpointInTxn(txn, migrationCheckpoint)
	jd.assertErrorAndRollbackTx(err, txn)

	if opID != 0 {
		err = jd.journalMarkDoneInTxn(txn, opID)
		jd.assertErrorAndRollbackTx(err, txn)
	}

	err = txn.Commit()
	jd.assertError(err)

	//Empty customValFilters means we want to clear for all
	jd.markClearEmptyResult(jd.migrationState.dsForImport, []string{}, []string{}, nil, false)
	// fmt.Println("Bursting CACHE")

}

func (jd *HandleT) updateSequenceNumber(ds dataSetT, sequenceNumber int64) {
	sqlStatement := fmt.Sprintf(`SELECT setval('%s_jobs_%s_job_id_seq', %d)`,
		jd.tablePrefix, ds.Index, sequenceNumber)
	_, err := jd.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err.Error())
	}
}

func (jd *HandleT) getMaxIDForDs(ds dataSetT) int64 {
	var maxID sql.NullInt64
	sqlStatement := fmt.Sprintf(`SELECT MAX(job_id) FROM %s`, ds.JobTable)
	row := jd.dbHandle.QueryRow(sqlStatement)
	row.Scan(&maxID)

	if maxID.Valid {
		return int64(maxID.Int64)
	}
	return int64(0)

}
