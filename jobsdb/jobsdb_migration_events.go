package jobsdb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//MigrationEvent captures an event of export/import to recover from incase of a crash during migration
type MigrationEvent struct {
	ID            int64     `json:"ID"`
	MigrationType string    `json:"MigrationType"` //ENUM : export, import, acceptNewEvents
	FromNode      string    `json:"FromNode"`
	ToNode        string    `json:"ToNode"`
	FileLocation  string    `json:"FileLocation"`
	Status        string    `json:"Status"` //ENUM : Look up 'Values for Status'
	StartSeq      int64     `json:"StartSeq"`
	Payload       string    `json:"Payload"`
	TimeStamp     time.Time `json:"TimeStamp"`
}

//ENUM Values for MigrationType
const (
	ExportOp          = "export"
	ImportOp          = "import"
	AcceptNewEventsOp = "acceptNewEvents"
)

//ENUM Values for Status
const (
	Exported               = "exported"
	Notified               = "notified"
	Acknowledged           = "acknowledged"
	Imported               = "imported"
	PreparedForImport      = "prepared_for_import"
	PreparedForExport      = "prepared_for_export"
	SetupForImport         = "setup_for_import"
	SetupForExport         = "setup_for_export"
	SetupToAcceptNewEvents = "setup_to_accept_new_events"
)

//Checkpoint writes a migration event if id is passed as 0. Else it will update status and start_sequence
func (jd *HandleT) Checkpoint(migrationEvent *MigrationEvent) int64 {
	jd.assert(migrationEvent.MigrationType == ExportOp ||
		migrationEvent.MigrationType == ImportOp ||
		migrationEvent.MigrationType == AcceptNewEventsOp,
		fmt.Sprintf("MigrationType: %s is not a supported operation. Should be %s or %s",
			migrationEvent.MigrationType, ExportOp, ImportOp))

	var sqlStatement string
	if migrationEvent.ID > 0 {
		sqlStatement = fmt.Sprintf(`UPDATE %s_migration_checkpoints SET status = $1, start_sequence = $2 WHERE id = $3 RETURNING id`, jd.GetTablePrefix())
	} else {
		sqlStatement = fmt.Sprintf(`INSERT INTO %s_migration_checkpoints (migration_type, from_node, to_node, file_location, status, start_sequence, payload, time_stamp)
									VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (file_location) DO NOTHING RETURNING id`, jd.GetTablePrefix())
	}

	stmt, err := jd.dbHandle.Prepare(sqlStatement)
	jd.assertError(err)
	defer stmt.Close()

	var meID int64
	if migrationEvent.ID > 0 {
		err = stmt.QueryRow(migrationEvent.Status, migrationEvent.StartSeq, migrationEvent.ID).Scan(&meID)
	} else {
		err = stmt.QueryRow(migrationEvent.MigrationType,
			migrationEvent.FromNode,
			migrationEvent.ToNode,
			migrationEvent.FileLocation,
			migrationEvent.Status,
			migrationEvent.StartSeq,
			migrationEvent.Payload,
			time.Now()).Scan(&meID)
	}
	if err != nil {
		panic(err)
	}
	logger.Infof("%s-Migration: %s checkpoint from %s to %s. file: %s, status: %s",
		jd.tablePrefix,
		migrationEvent.MigrationType,
		migrationEvent.FromNode,
		migrationEvent.ToNode,
		migrationEvent.FileLocation,
		migrationEvent.Status)
	return meID
}

//NewSetupCheckpointEvent returns a new migration event that captures setup for export, import of new event acceptance
func NewSetupCheckpointEvent(migrationType string, node string) MigrationEvent {
	switch migrationType {
	case ExportOp:
		return NewMigrationEvent(migrationType, node, "All", SetupForExport, SetupForExport, 0)
	case AcceptNewEventsOp:
		return NewMigrationEvent(migrationType, "All", node, SetupToAcceptNewEvents, SetupToAcceptNewEvents, 0)
	case ImportOp:
		return NewMigrationEvent(migrationType, "All", node, SetupForImport, SetupForImport, 0)
	default:
		panic("Illegal usage")
	}
}

//NewMigrationEvent is a constructor for MigrationEvent struct
func NewMigrationEvent(migrationType string, fromNode string, toNode string, fileLocation string, status string, startSeq int64) MigrationEvent {
	return MigrationEvent{0, migrationType, fromNode, toNode, fileLocation, status, startSeq, "{}", time.Now()}
}

//SetupCheckpointTable creates a table
func (jd *HandleT) SetupCheckpointTable() {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_migration_checkpoints (
		id BIGSERIAL PRIMARY KEY,
		migration_type varchar(20) NOT NULL,
		from_node varchar(64) NOT NULL,
		to_node VARCHAR(64) NOT NULL,
		file_location TEXT UNIQUE,
		status varchar(64),
		start_sequence BIGINT,
		payload TEXT,
		time_stamp TIMESTAMP NOT NULL DEFAULT NOW());`, jd.GetTablePrefix())

	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
	logger.Infof("Migration: %s_migration_checkpoints table created", jd.GetTablePrefix())
}

//findOrCreateDsFromSetupCheckpoint is boiler plate code for setting up for different scenarios
func (jd *HandleT) findOrCreateDsFromSetupCheckpoint(migrationType string, findOrCreateDs func([]dataSetT) dataSetT) dataSetT {
	jd.dsListLock.Lock()
	defer jd.dsListLock.Unlock()

	var ds dataSetT
	setupEvent := jd.GetSetupCheckpoint(migrationType)
	if setupEvent == nil {
		me := NewSetupCheckpointEvent(migrationType, misc.GetNodeID())
		me.Payload = "{}"
		setupEvent = &me
	}
	json.Unmarshal([]byte(setupEvent.Payload), &ds)

	dsList := jd.getDSList(true)
	if setupEvent.Payload == "{}" {
		ds = findOrCreateDs(dsList)
		payloadBytes, err := json.Marshal(ds)
		if err != nil {
			panic("Unable to Marshal")
		}
		setupEvent.Payload = string(payloadBytes)
		jd.Checkpoint(setupEvent)
	}
	return ds
}

func (jd *HandleT) getSeqNoForFileFromDB(fileLocation string, migrationType string) int64 {
	jd.assert(migrationType == ExportOp ||
		migrationType == ImportOp,
		fmt.Sprintf("MigrationType: %s is not a supported operation. Should be %s or %s",
			migrationType, ExportOp, ImportOp))

	sqlStatement := fmt.Sprintf(`SELECT start_sequence from %s_migration_checkpoints WHERE file_location = $1 AND migration_type = $2 ORDER BY id DESC`, jd.GetTablePrefix())
	stmt, err := jd.dbHandle.Prepare(sqlStatement)
	defer stmt.Close()
	jd.assertError(err)

	rows, err := stmt.Query(fileLocation, migrationType)
	defer rows.Close()
	if err != nil {
		panic("Unable to query")
	}
	rows.Next()

	var sequenceNumber int64
	sequenceNumber = 0
	err = rows.Scan(&sequenceNumber)
	if err != nil && err.Error() != "sql: Rows are closed" {
		panic("query result pares issue")
	}
	return sequenceNumber
}

//GetSetupCheckpoint gets all checkpoints and picks out the setup event for that type
func (jd *HandleT) GetSetupCheckpoint(migrationType string) *MigrationEvent {
	var setupStatus string
	switch migrationType {
	case ExportOp:
		setupStatus = SetupForExport
	case AcceptNewEventsOp:
		setupStatus = SetupToAcceptNewEvents
	case ImportOp:
		setupStatus = SetupForImport
	}
	setupEvents := jd.getCheckpoints(migrationType, fmt.Sprintf(`SELECT * FROM %s_migration_checkpoints WHERE migration_type = $1 AND status = '%s' ORDER BY ID ASC`, jd.GetTablePrefix(), setupStatus))

	switch len(setupEvents) {
	case 0:
		return nil
	case 1:
		return setupEvents[0]
	default:
		panic("More than 1 setup event found. This should not happen")
	}

}

//GetCheckpoints gets all checkpoints and
func (jd *HandleT) GetCheckpoints(migrationType string) []*MigrationEvent {
	return jd.getCheckpoints(migrationType, fmt.Sprintf(`SELECT * from %s_migration_checkpoints WHERE migration_type = $1 ORDER BY ID ASC`, jd.GetTablePrefix()))
}

func (jd *HandleT) getCheckpoints(migrationType string, query string) []*MigrationEvent {
	sqlStatement := query
	stmt, err := jd.dbHandle.Prepare(sqlStatement)
	jd.assertError(err)
	defer stmt.Close()

	rows, err := stmt.Query(migrationType)
	if err != nil {
		panic("Unable to query")
	}
	defer rows.Close()

	migrationEvents := []*MigrationEvent{}
	for rows.Next() {
		migrationEvent := MigrationEvent{}

		var payload sql.NullString
		err = rows.Scan(&migrationEvent.ID, &migrationEvent.MigrationType, &migrationEvent.FromNode,
			&migrationEvent.ToNode, &migrationEvent.FileLocation, &migrationEvent.Status,
			&migrationEvent.StartSeq, &payload, &migrationEvent.TimeStamp)
		if err != nil {
			panic(fmt.Sprintf("query result pares issue : %s", err.Error()))
		}
		if payload.Valid {
			migrationEvent.Payload = payload.String
		} else {
			migrationEvent.Payload = "{}"
		}
		migrationEvents = append(migrationEvents, &migrationEvent)
	}
	return migrationEvents
}
