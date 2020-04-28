package jobsdb

import (
	"database/sql"
	"fmt"
	"math"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

func (jd *HandleT) getDsForImport(dsList []dataSetT) dataSetT {
	ds := jd.addNewDS(false, jd.migrationState.DsForNewEvents)
	logger.Infof("Should Checkpoint Import Setup event for the new ds : %v", ds)
	return ds
}

func (jd *HandleT) setupSequenceProvider(ds dataSetT) {
	if jd.migrationState.sequenceProvider == nil || !jd.migrationState.sequenceProvider.IsInitialized() {
		jd.dsListLock.Lock()
		defer jd.dsListLock.Unlock()

		dsList := jd.getDSList(true)
		importDSMin := jd.getMaxIDForDs(ds)
		//TODO: Get sequence number from checkpoints and pick the greatest for importDSMin

		if importDSMin == 0 {
			for idx, dataSet := range dsList {
				if dataSet.Index == ds.Index {
					if idx > 0 {
						importDSMin = jd.getMaxIDForDs(dsList[idx-1])
					}
				}
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
		dsForNewEvents := jd.addNewDS(true, dataSetT{})
		ds = dsForNewEvents
	}

	seqNoForNewDS := int64(getNewVersion())*int64(math.Pow10(13)) + 1
	jd.updateSequenceNumber(ds, seqNoForNewDS)
	logger.Infof("Jobsdb: New dataSet %s is prepared with start sequence : %d", ds, seqNoForNewDS)
	return ds
}

func getNewVersion() int {
	//TODO: revisit this
	return config.GetRequiredEnvAsInt("CLUSTER_VERSION")
}

//StoreImportedJobsAndJobStatuses is used to write the jobs to _tables
func (jd *HandleT) StoreImportedJobsAndJobStatuses(jobList []*JobT, fileName string, migrationEvent *MigrationEvent) {
	if jd.migrationState.DsForImport.Index == "" {
		jd.migrationState.DsForImport = jd.findOrCreateDsFromSetupCheckpoint(ImportOp, jd.getDsForImport)
		jd.setupSequenceProvider(jd.migrationState.DsForImport)
	}

	if jd.migrationState.DsForImport.Index == "" {
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

	//TODO: modify storeJobsDS and updateJobStatusDS to accept an additional bool to support "on conflict do nothing"

	//TODO: get minimal functions for the below and put them both in a transaction
	jd.storeJobsDS(jd.migrationState.DsForImport, true, false, jobList)
	jd.updateJobStatusDS(jd.migrationState.DsForImport, statusList, []string{}, []ParameterFilterT{})
}

func (jd *HandleT) getStartJobID(count int, migrationEvent *MigrationEvent) int64 {
	var sequenceNumber int64
	sequenceNumber = 0
	sequenceNumber = jd.getSeqNoForFileFromDB(migrationEvent.FileLocation, ImportOp)
	if sequenceNumber == 0 {
		sequenceNumber = jd.migrationState.sequenceProvider.ReserveIds(count)
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
	err := row.Scan(&maxID)
	jd.assertError(err)

	if maxID.Valid {
		return int64(maxID.Int64)
	}
	return int64(0)

}
