package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//MigrationState maintains the state required during the migration process
type MigrationState struct {
	sequenceProvider *SequenceProvider
	DsForNewEvents   dataSetT `json:"dsForNewEvents"`
	DsForImport      dataSetT `json:"dsForImport"`
	LastDsForExport  dataSetT `json:"lastDsForExport"`
}

//setupFor is boiler plate code for setting up for different scenarios
func (jd *HandleT) setupFor(migrationType string, ds dataSetT, handler func([]dataSetT, dataSetT) (dataSetT, bool)) dataSetT {
	jd.dsListLock.Lock()
	defer jd.dsListLock.Unlock()

	setupEvent := jd.GetSetupCheckpoint(migrationType)
	if setupEvent == nil {
		me := NewSetupCheckpointEvent(migrationType, misc.GetNodeID())
		me.Payload = "{}"
		setupEvent = &me
	}
	json.Unmarshal([]byte(setupEvent.Payload), ds)

	dsList := jd.getDSList(true)
	ds, shouldCheckpoint := handler(dsList, ds)

	if shouldCheckpoint {
		payloadBytes, err := json.Marshal(ds)
		if err != nil {
			panic("Unable to Marshal")
		}
		setupEvent.Payload = string(payloadBytes)
		jd.Checkpoint(setupEvent)
	}
	return ds
}

//SetupForMigrations is used to setup jobsdb for export or for import or for both
func (jd *HandleT) SetupForMigrations(forExport bool, forImport bool) {
	if forExport {
		jd.migrationState.LastDsForExport = jd.setupFor(ExportOp, jd.migrationState.LastDsForExport, jd.exportSetup)
	}
	if forImport {
		jd.migrationState.DsForNewEvents = jd.setupFor(AcceptNewEventsOp, jd.migrationState.DsForNewEvents, jd.dsForNewEventsSetup)
	}

	logger.Infof("Last ds for export : %v || Ds for new events :%v", jd.migrationState.LastDsForExport, jd.migrationState.DsForNewEvents)
}

func (jd *HandleT) isEmpty(ds dataSetT) bool {
	var count sql.NullInt64
	sqlStatement := fmt.Sprintf(`SELECT count(*) from %s`, ds.JobTable)
	row := jd.dbHandle.QueryRow(sqlStatement)
	err := row.Scan(&count)
	jd.assertError(err)
	if count.Valid {
		return int64(count.Int64) == int64(0)
	}
	panic("Unable to get count on this dataset")
}

//GetTablePrefix returns the table prefix of the jobsdb
func (jd *HandleT) GetTablePrefix() string {
	return jd.tablePrefix
}
