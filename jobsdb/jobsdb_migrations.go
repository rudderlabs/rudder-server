package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

//MigrationState maintains the state required during the migration process
type MigrationState struct {
	sequenceProvider     *SequenceProvider
	DsForNewEvents       dataSetT `json:"dsForNewEvents"`
	DsForImport          dataSetT `json:"dsForImport"`
	LastDsForExport      dataSetT `json:"lastDsForExport"`
	importDSCreationLock sync.RWMutex
}

//setupFor is boiler plate code for setting up for different scenarios
func (jd *HandleT) setupFor(migrationType string, ds *dataSetT, handler func([]dataSetT, *MigrationEvent, *dataSetT) bool) {
	jd.dsListLock.Lock()
	defer jd.dsListLock.Unlock()

	migrationEvents := jd.GetCheckpoints(migrationType)
	var setupEvent *MigrationEvent
	if len(migrationEvents) > 0 {
		setupEvent = migrationEvents[0]
	} else {
		//TODO:
		//export: fromNodeId = misc.getNodeId, toNodeId=All, payload = LastDsForExport, status=PreparedForExport
		//import: fromNodeId = All, toNodeId=All, payload = DsForImport, status=PreparedForImport
		//prepareforEvents: fromNodeId = All, toNodeId=misc.getNodeId, payload = DsForNewEvents, status=PreparedForNew

		me := NewSetupCheckpointEvent(migrationType, misc.GetNodeID())
		me.Payload = "{}"
		setupEvent = &me
	}
	json.Unmarshal([]byte(setupEvent.Payload), ds)

	dsList := jd.getDSList(true)
	shouldCheckpoint := handler(dsList, setupEvent, ds)

	if shouldCheckpoint {
		payloadBytes, err := json.Marshal(ds)
		if err != nil {
			panic("Unable to Marshal")
		}
		setupEvent.Payload = string(payloadBytes)
		jd.Checkpoint(setupEvent)
	}

}

//SetupForMigrations is used to setup jobsdb for export or for import or for both
func (jd *HandleT) SetupForMigrations(forExport bool, forImport bool) {
	if forExport {
		jd.setupFor(ExportOp, &jd.migrationState.LastDsForExport, jd.exportSetup)
	}
	if forImport {
		jd.setupFor(AcceptNewEventsOp, &jd.migrationState.DsForNewEvents, jd.dsForNewEventsSetup)
	}
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
