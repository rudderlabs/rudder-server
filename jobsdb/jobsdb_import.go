package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
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

func (jd *HandleT) setupSequenceProvider(ds dataSetT) {
	jd.dsListLock.Lock()
	defer jd.dsListLock.Unlock()

	if jd.migrationState.sequenceProvider == nil || !jd.migrationState.sequenceProvider.IsInitialized() {
		dsList := jd.getDSList(true)
		importDSMin := jd.getMaxIDForDs(ds)

		if importDSMin == 0 {
			for idx, dataSet := range dsList {
				if dataSet.Index == ds.Index {
					if idx > 0 {
						importDSMin = jd.getMaxIDForDs(dsList[idx-1])
					}
				}
			}
		}

		importCheckpoints := jd.GetCheckpoints(ImportOp, PreparedForImport)
		for _, checkpoint := range importCheckpoints {
			lastJobIDForCheckpoint := checkpoint.getLastJobID()
			if lastJobIDForCheckpoint > importDSMin {
				importDSMin = lastJobIDForCheckpoint
			}
		}

		jd.migrationState.sequenceProvider = NewSequenceProvider(importDSMin + 1)
	}
}

func (jd *HandleT) getDsForNewEvents(dsList []dataSetT) dataSetT {
	dsListLen := len(dsList)
	var ds dataSetT
	if jd.isEmpty(dsList[dsListLen-1]) {
		ds = dsList[dsListLen-1]
	} else {
		ds = jd.addNewDS(appendToDsList, dataSetT{})
	}

	seqNoForNewDS := int64(misc.GetMigratingToVersion())*int64(math.Pow10(13)) + 1
	jd.updateSequenceNumber(ds, seqNoForNewDS)
	logger.Infof("[[ %sJobsDB Import ]] New dataSet %s is prepared with start sequence : %d", jd.GetTablePrefix(), ds, seqNoForNewDS)
	return ds
}

//StoreImportedJobsAndJobStatuses is used to write the jobs to _tables
func (jd *HandleT) StoreImportedJobsAndJobStatuses(jobList []*JobT, fileName string, migrationEvent *MigrationEvent) {
	// This if block should be idempotent. It is currently good. But if it changes we need to add separate locks outside
	var found bool
	var opID int64
	jd.migrationState.importLock.Lock()

	//Also need to factor in the jd.migrationState.dsForImport == "" condition
	jd.migrationState.dsForImport, found = jd.findDsFromSetupCheckpoint(ImportOp)
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
		setupCheckpoint := jd.GetSetupCheckpoint(ImportOp)
		setupCheckpoint.Payload, _ = json.Marshal(jd.migrationState.dsForImport)
		jd.Checkpoint(setupCheckpoint)
		jd.dsListLock.Unlock()
		opPayload, err := json.Marshal(&jd.migrationState.dsForImport)
		jd.assertError(err)
		opID = jd.JournalMarkStart(migrateImportOperation, opPayload)
	} else {
		jd.migrationState.importLock.Unlock()
	}

	jd.setupSequenceProvider(jd.migrationState.dsForImport)

	if jd.migrationState.dsForImport.Index == "" {
		panic("dsForImportEvents was not setup. Go debug")
	}

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

	logger.Infof("[[ %s-JobsDB Import ]] Writing jobs from file:%s to db", jd.GetTablePrefix(), fileName)
	logger.Infof("[[ %s-JobsDB Import ]] %d jobs found in file:%s. Writing to db", jd.GetTablePrefix(), len(jobList), fileName)
	logger.Infof("[[ %s-JobsDB Import ]] %d job_statuses found in file:%s. Writing to db", jd.GetTablePrefix(), len(statusList), fileName)

	txn, err := jd.dbHandle.Begin()
	jd.assertError(err)
	//TODO REMOVE
	logger.Debug("[DEFER StoreImportedJobsAndJobStatuses] rolling back transaction")
	defer txn.Rollback() //TODO: Review this. In a successful case rollback will be called after commit. In a failure case there will be a panic and a dangling db connection may be left
	jd.storeJobsDSInTxn(txn, jd.migrationState.dsForImport, true, false, jobList)
	jd.updateJobStatusDSInTxn(txn, jd.migrationState.dsForImport, statusList, []string{}, []ParameterFilterT{})
	migrationEvent.Status = Imported
	jd.CheckpointInTxn(txn, migrationEvent)
	txn.Commit()

	//TODO what happens if it crashes here? Data loss?
	if opID != 0 {
		jd.JournalMarkDone(opID)
	}
}

func (jd *HandleT) getStartJobID(count int, migrationEvent *MigrationEvent) int64 {
	var sequenceNumber int64
	sequenceNumber = migrationEvent.StartSeq
	if sequenceNumber == 0 {
		sequenceNumber = jd.migrationState.sequenceProvider.ReserveIdsAndProvideStartSequence(count)
		migrationEvent.StartSeq = sequenceNumber
		jd.Checkpoint(migrationEvent)
	}
	return sequenceNumber
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
