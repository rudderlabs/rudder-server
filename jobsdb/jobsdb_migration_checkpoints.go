package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

//MigrationCheckpointT captures an event of export/import to recover from incase of a crash during migration
type MigrationCheckpointT struct {
	ID            int64           `json:"ID"`
	MigrationType MigrationOp     `json:"MigrationType"` //ENUM : export, import, acceptNewEvents
	FromNode      string          `json:"FromNode"`
	ToNode        string          `json:"ToNode"`
	JobsCount     int64           `json:"JobsCount"`
	FileLocation  string          `json:"FileLocation"`
	Status        string          `json:"Status"` //ENUM : Look up 'Values for Status'
	StartSeq      int64           `json:"StartSeq"`
	Payload       json.RawMessage `json:"Payload"`
	TimeStamp     time.Time       `json:"TimeStamp"`
}

//MigrationOp is a custom type for supported types in migrationCheckpoint
type MigrationOp string

//ENUM Values for MigrationOp
const (
	ExportOp          = "export"
	ImportOp          = "import"
	AcceptNewEventsOp = "acceptNewEvents"
)

//ENUM Values for Status
const (
	SetupForExport = "setup_for_export"
	Exported       = "exported"
	Notified       = "notified"
	Completed      = "completed"

	SetupToAcceptNewEvents = "setup_to_accept_new_events"
	SetupForImport         = "setup_for_import"
	PreparedForImport      = "prepared_for_import"
	Imported               = "imported"
)

//MigrationCheckpointSuffix : Suffix for checkpoints table
const (
	MigrationCheckpointSuffix = "migration_checkpoints"
	UniqueConstraintSuffix    = "unique_checkpoint"
)

func (jd *HandleT) deleteSetupCheckpoint(operationType MigrationOp) {
	migrationCheckpoint, found := jd.GetSetupCheckpoint(operationType)

	if found {
		sqlStatement := fmt.Sprintf(`DELETE FROM %s WHERE id = %d`, jd.getCheckpointTableName(), migrationCheckpoint.ID)
		stmt, err := jd.dbHandle.Prepare(sqlStatement)
		jd.assertError(err)
		_, err = stmt.Exec()
		jd.assertError(err)
	}
}

//Checkpoint writes a migration event if id is passed as 0. Else it will update status and start_sequence
func (jd *HandleT) Checkpoint(migrationCheckpoint MigrationCheckpointT) int64 {
	id, err := jd.CheckpointInTxn(jd.dbHandle, migrationCheckpoint)
	jd.assertError(err)
	return id
}

//CheckpointInTxn writes a migration event if id is passed as 0. Else it will update status and start_sequence
// If txn is passed, it will run the statement in that txn, otherwise it will execute without a transaction
func (jd *HandleT) CheckpointInTxn(txHandler transactionHandler, migrationCheckpoint MigrationCheckpointT) (int64, error) {
	jd.assert(migrationCheckpoint.MigrationType == ExportOp ||
		migrationCheckpoint.MigrationType == ImportOp ||
		migrationCheckpoint.MigrationType == AcceptNewEventsOp,
		fmt.Sprintf("MigrationType: %s is not a supported operation. Should be %s or %s",
			migrationCheckpoint.MigrationType, ExportOp, ImportOp))

	var sqlStatement string
	var checkpointType string
	if migrationCheckpoint.ID > 0 {
		sqlStatement = fmt.Sprintf(`UPDATE %s SET status = $1, start_sequence = $2, payload = $3 WHERE id = $4 RETURNING id`, jd.getCheckpointTableName())
		checkpointType = "update"
	} else {
		sqlStatement = fmt.Sprintf(`INSERT INTO %s (migration_type, from_node, to_node, jobs_count, file_location, status, start_sequence, payload, time_stamp)
									VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT ON CONSTRAINT %s DO UPDATE SET status=EXCLUDED.status RETURNING id`, jd.getCheckpointTableName(), jd.getUniqueConstraintName())
		checkpointType = "insert"
	}

	var (
		stmt *sql.Stmt
		err  error
	)
	jd.logger.Debugf("Checkpointing with query : %s with migrationCheckpoint %+v", sqlStatement, migrationCheckpoint)
	var mcID int64

	stmt, err = txHandler.Prepare(sqlStatement)
	//skipcq: SCC-SA5001
	defer stmt.Close()
	if err != nil {
		return mcID, err
	}

	if migrationCheckpoint.ID > 0 {
		err = stmt.QueryRow(migrationCheckpoint.Status, migrationCheckpoint.StartSeq, migrationCheckpoint.Payload, migrationCheckpoint.ID).Scan(&mcID)
	} else {
		err = stmt.QueryRow(migrationCheckpoint.MigrationType,
			migrationCheckpoint.FromNode,
			migrationCheckpoint.ToNode,
			migrationCheckpoint.JobsCount,
			migrationCheckpoint.FileLocation,
			migrationCheckpoint.Status,
			migrationCheckpoint.StartSeq,
			migrationCheckpoint.Payload,
			time.Now()).Scan(&mcID)
	}
	if err != nil {
		return mcID, err
	}

	jd.logger.Infof("%s-Migration: %s checkpoint %s from %s to %s. file: %s containing %d jobs, status: %s for checkpointId: %d",
		jd.tablePrefix,
		migrationCheckpoint.MigrationType,
		checkpointType,
		migrationCheckpoint.FromNode,
		migrationCheckpoint.ToNode,
		migrationCheckpoint.FileLocation,
		migrationCheckpoint.JobsCount,
		migrationCheckpoint.Status,
		migrationCheckpoint.ID)
	return mcID, nil
}

//NewSetupCheckpointEvent returns a new migration event that captures setup for export, import of new event acceptance
func NewSetupCheckpointEvent(migrationType MigrationOp, node string) MigrationCheckpointT {
	switch migrationType {
	case ExportOp:
		return NewMigrationCheckpoint(migrationType, node, "All", int64(0), "", SetupForExport, 0)
	case AcceptNewEventsOp:
		return NewMigrationCheckpoint(migrationType, "All", node, int64(0), "", SetupToAcceptNewEvents, 0)
	case ImportOp:
		return NewMigrationCheckpoint(migrationType, "All", node, int64(0), "", SetupForImport, 0)
	default:
		panic("Illegal usage")
	}
}

//NewMigrationCheckpoint is a constructor for MigrationCheckpoint struct
func NewMigrationCheckpoint(migrationType MigrationOp, fromNode string, toNode string, jobsCount int64, fileLocation string, status string, startSeq int64) MigrationCheckpointT {
	return MigrationCheckpointT{0, migrationType, fromNode, toNode, jobsCount, fileLocation, status, startSeq, []byte("{}"), time.Now()}
}

//SetupForMigration prepares jobsdb to start migrations
func (jd *HandleT) SetupForMigration(fromVersion int, toVersion int) {
	jd.migrationState.fromVersion = fromVersion
	jd.migrationState.toVersion = toVersion
	jd.SetupCheckpointTable()
}

//SetupCheckpointTable creates a table
func (jd *HandleT) SetupCheckpointTable() {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGSERIAL PRIMARY KEY,
		migration_type TEXT NOT NULL,
		from_node TEXT NOT NULL,
		to_node TEXT NOT NULL,
		jobs_count BIGINT NOT NULL,
		file_location TEXT,
		status TEXT,
		start_sequence BIGINT,
		payload JSONB,
		time_stamp TIMESTAMP NOT NULL DEFAULT NOW(),
		CONSTRAINT %s UNIQUE(migration_type, from_node, to_node, file_location)
		);`, jd.getCheckpointTableName(), jd.getUniqueConstraintName())

	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
	jd.logger.Infof("%s-Migration: %s table created", jd.GetTablePrefix(), jd.getCheckpointTableName())
}

func (jd *HandleT) getCheckpointTableName() string {
	return fmt.Sprintf("%s_%d_%d_%s", jd.GetTablePrefix(), jd.migrationState.fromVersion, jd.migrationState.toVersion, MigrationCheckpointSuffix)
}

func (jd *HandleT) getUniqueConstraintName() string {
	return fmt.Sprintf("%s_%d_%d_%s", jd.GetTablePrefix(), jd.migrationState.fromVersion, jd.migrationState.toVersion, UniqueConstraintSuffix)
}

func (jd *HandleT) findDsFromSetupCheckpoint(migrationType MigrationOp) (dataSetT, bool) {
	setupEvent, found := jd.GetSetupCheckpoint(migrationType)
	if !found {
		return dataSetT{}, false
	}
	ds := dataSetT{}
	err := json.Unmarshal(setupEvent.Payload, &ds)
	jd.assertError(err)
	return ds, true
}

func (jd *HandleT) createSetupCheckpointAndGetDs(migrationType MigrationOp) dataSetT {
	jd.dsListLock.Lock()
	defer jd.dsListLock.Unlock()

	dsList := jd.getDSList(true)
	checkpoint := NewSetupCheckpointEvent(migrationType, misc.GetNodeID())

	var ds dataSetT
	switch migrationType {
	case ExportOp:
		ds = jd.getLastDsForExport(dsList)
	case AcceptNewEventsOp:
		ds = jd.getDsForNewEvents(dsList)
	case ImportOp:
		ds = jd.getDsForImport(dsList)
	}

	var err error
	checkpoint.Payload, err = json.Marshal(ds)
	if err != nil {
		panic("Unable to Marshal")
	}
	jd.Checkpoint(checkpoint)
	return ds
}

func (jd *HandleT) findOrCreateDsFromSetupCheckpoint(migrationType MigrationOp) dataSetT {
	ds, found := jd.findDsFromSetupCheckpoint(migrationType)
	if !found {
		ds = jd.createSetupCheckpointAndGetDs(migrationType)
	}
	return ds
}

//GetSetupCheckpoint gets all checkpoints and picks out the setup event for that type
func (jd *HandleT) GetSetupCheckpoint(migrationType MigrationOp) (MigrationCheckpointT, bool) {
	var setupStatus string
	switch migrationType {
	case ExportOp:
		setupStatus = SetupForExport
	case AcceptNewEventsOp:
		setupStatus = SetupToAcceptNewEvents
	case ImportOp:
		setupStatus = SetupForImport
	}
	setupEvents := jd.GetCheckpoints(migrationType, setupStatus)

	switch len(setupEvents) {
	case 0:
		return MigrationCheckpointT{}, false
	case 1:
		return setupEvents[0], true
	default:
		panic("More than 1 setup event found. This should not happen")
	}

}

//GetCheckpoints gets all checkpoints and
func (jd *HandleT) GetCheckpoints(migrationType MigrationOp, status string) []MigrationCheckpointT {
	queryString := fmt.Sprintf(`SELECT * from %s WHERE migration_type = $1`, jd.getCheckpointTableName())
	var statusFilter string
	if status != "" {
		statusFilter = fmt.Sprintf(` AND status = '%s'`, status)
	}
	orderByString := " ORDER BY ID ASC"

	sqlStatement := fmt.Sprintf("%s%s%s", queryString, statusFilter, orderByString)

	stmt, err := jd.dbHandle.Prepare(sqlStatement)
	jd.assertError(err)
	defer stmt.Close()

	rows, err := stmt.Query(migrationType)
	if err != nil {
		panic("Unable to query")
	}
	defer rows.Close()

	migrationCheckpoints := []MigrationCheckpointT{}
	for rows.Next() {
		migrationCheckpoint := MigrationCheckpointT{}

		err = rows.Scan(&migrationCheckpoint.ID, &migrationCheckpoint.MigrationType, &migrationCheckpoint.FromNode,
			&migrationCheckpoint.ToNode, &migrationCheckpoint.JobsCount, &migrationCheckpoint.FileLocation, &migrationCheckpoint.Status,
			&migrationCheckpoint.StartSeq, &migrationCheckpoint.Payload, &migrationCheckpoint.TimeStamp)
		if err != nil {
			panic(fmt.Sprintf("query result parse issue : %s", err.Error()))
		}
		migrationCheckpoints = append(migrationCheckpoints, migrationCheckpoint)
	}
	return migrationCheckpoints
}

//skipcq: SCC-U1000
func (migrationCheckpoint MigrationCheckpointT) getLastJobID() int64 {
	if migrationCheckpoint.StartSeq == 0 {
		return int64(0)
	}
	return migrationCheckpoint.StartSeq + migrationCheckpoint.JobsCount - 1
}
