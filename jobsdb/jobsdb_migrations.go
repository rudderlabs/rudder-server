package jobsdb

import (
	"database/sql"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

//MigrationState maintains the state required during the migration process
type MigrationState struct {
	sequenceProvider *SequenceProvider
	DsForNewEvents   dataSetT `json:"dsForNewEvents"`
	DsForImport      dataSetT `json:"dsForImport"`
	LastDsForExport  dataSetT `json:"lastDsForExport"`
}

//SetupForMigrations is used to setup jobsdb for export or for import or for both
func (jd *HandleT) SetupForMigrations(forExport bool, forImport bool) {
	if forExport {
		jd.migrationState.LastDsForExport = jd.findOrCreateDsFromSetupCheckpoint(ExportOp, jd.getLastDsForExport)
	}
	if forImport {
		jd.migrationState.DsForNewEvents = jd.findOrCreateDsFromSetupCheckpoint(AcceptNewEventsOp, jd.getDsForNewEvents)
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
