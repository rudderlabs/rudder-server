package jobsdb

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"
)

//MigrationEvent captures an event of export/import to recover from incase of a crash during migration
type MigrationEvent struct {
	ID            int64     `json:"ID"`
	MigrationType string    `json:"MigrationType"` //ENUM : export, import
	FromNode      string    `json:"FromNode"`
	ToNode        string    `json:"ToNode"`
	FileLocation  string    `json:"FileLocation"`
	Status        string    `json:"Status"` //ENUM : exported, imported, prepared_for_import
	StartSeq      int64     `json:"StartSeq"`
	TimeStamp     time.Time `json:"TimeStamp"`
}

//ENUM Values for MigrationType
const (
	ExportOp = "export"
	ImportOp = "import"
)

//ENUM Values for Status
const (
	Exported = "exported"
	Imported = "imported"
	Prepared = "prepared_for_import"
)

//Checkpoint writes a migration event
func (jd *HandleT) Checkpoint(migrationEvent *MigrationEvent) int64 {
	jd.assert(migrationEvent.MigrationType == ExportOp ||
		migrationEvent.MigrationType == ImportOp,
		fmt.Sprintf("MigrationType: %s is not a supported operation. Should be %s or %s",
			migrationEvent.MigrationType, ExportOp, ImportOp))

	var sqlStatement string
	if migrationEvent.ID > 0 {
		sqlStatement = fmt.Sprintf(`UPDATE %s_migration_checkpoints SET status = $1, start_sequence = $2 WHERE id = $3 RETURNING id`, jd.GetTablePrefix())
	} else {
		sqlStatement = fmt.Sprintf(`INSERT INTO %s_migration_checkpoints (migration_type, from_node, to_node, file_location, status, start_sequence, time_stamp)
									VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`, jd.GetTablePrefix())
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
			time.Now()).Scan(&meID)
	}
	if err != nil {
		panic("Failed to checkpoint")
	}
	logger.Info("Migration: %s checkpoint from %s to %s. file: %s, status: %s",
		migrationEvent.MigrationType,
		migrationEvent.FromNode,
		migrationEvent.ToNode,
		migrationEvent.FileLocation,
		migrationEvent.Status)
	return meID
}

//NewMigrationEvent is a constructor for MigrationEvent struct
func NewMigrationEvent(migrationType string, fromNode string, toNode string, fileLocation string, status string, startSeq int64) MigrationEvent {
	return MigrationEvent{0, migrationType, fromNode, toNode, fileLocation, status, startSeq, time.Now()}
}

//SetupCheckpointDBTable creates a table
func (jd *HandleT) SetupCheckpointDBTable() {
	sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_migration_checkpoints (
		id BIGSERIAL PRIMARY KEY,
		migration_type varchar(20) NOT NULL,
		from_node varchar(64) NOT NULL,
		to_node VARCHAR(64) NOT NULL,
		file_location varchar(256),
		status varchar(64),
		start_sequence BIGINT,
		time_stamp TIMESTAMP NOT NULL DEFAULT NOW());`, jd.GetTablePrefix())

	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
	logger.Info("Migration: %s_migration_checkpoints table created", jd.GetTablePrefix())
}
