package jobsdb

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
)

// journalOpPayloadT is the struct which is written to the journal
type journalOpPayloadT struct {
	From []dataSetT `json:"from"`
	To   dataSetT   `json:"to"`
}

const (
	addDSOperation             = "ADD_DS"
	migrateCopyOperation       = "MIGRATE_COPY"
	postMigrateDSOperation     = "POST_MIGRATE_DS_OP"
	dropDSOperation            = "DROP_DS"
	RawDataDestUploadOperation = "S3_DEST_UPLOAD"
)

type JournalEntryT struct {
	OpID      int64
	OpType    string
	OpDone    bool
	OpPayload json.RawMessage
}

func (jd *Handle) dropJournal() {
	sqlStatement := fmt.Sprintf(`DROP TABLE IF EXISTS %s_journal`, jd.tablePrefix)
	_, err := jd.dbHandle.Exec(sqlStatement)
	jd.assertError(err)
}

func (jd *Handle) JournalMarkStart(opType string, opPayload json.RawMessage) (int64, error) {
	var opID int64
	return opID, jd.WithTx(context.Background(), func(tx *Tx) error {
		var err error
		opID, err = jd.JournalMarkStartInTx(tx, opType, opPayload)
		return err
	})
}

func (jd *Handle) JournalMarkStartInTx(tx *Tx, opType string, opPayload json.RawMessage) (int64, error) {
	var opID int64
	jd.assert(opType == addDSOperation ||
		opType == migrateCopyOperation ||
		opType == postMigrateDSOperation ||
		opType == dropDSOperation ||
		opType == RawDataDestUploadOperation, fmt.Sprintf("opType: %s is not a supported op", opType))

	sqlStatement := fmt.Sprintf(`INSERT INTO %s_journal (operation, done, operation_payload, start_time, owner)
                                       VALUES ($1, $2, $3, $4, $5) RETURNING id`, jd.tablePrefix)
	err := tx.QueryRow(sqlStatement, opType, false, opPayload, time.Now(), jd.ownerType).Scan(&opID)
	return opID, err
}

// JournalMarkDone marks the end of a journal action
func (jd *Handle) JournalMarkDone(opID int64) error {
	return jd.WithTx(context.Background(), func(tx *Tx) error {
		return jd.journalMarkDoneInTx(tx, opID)
	})
}

// journalMarkDoneInTx marks the end of a journal action in a transaction
func (jd *Handle) journalMarkDoneInTx(tx *Tx, opID int64) error {
	sqlStatement := fmt.Sprintf(`UPDATE %s_journal SET done=$2, end_time=$3 WHERE id=$1 AND owner=$4`, jd.tablePrefix)
	_, err := tx.Exec(sqlStatement, opID, true, time.Now(), jd.ownerType)
	return err
}

func (jd *Handle) JournalDeleteEntry(opID int64) {
	sqlStatement := fmt.Sprintf(`DELETE from "%s_journal" WHERE id=$1 AND owner=$2`, jd.tablePrefix)
	_, err := jd.dbHandle.Exec(sqlStatement, opID, jd.ownerType)
	jd.assertError(err)
}

func (jd *Handle) GetJournalEntries(opType string) (entries []JournalEntryT) {
	sqlStatement := fmt.Sprintf(`SELECT id, operation, done, operation_payload
                                	from "%s_journal"
                                	WHERE
									done=False
									AND
									operation = '%s'
									AND
									owner='%s'
									ORDER BY id`, jd.tablePrefix, opType, jd.ownerType)
	rows, err := jd.dbHandle.Query(sqlStatement)
	jd.assertError(err)
	defer func() { _ = rows.Close() }()

	count := 0
	for rows.Next() {
		entries = append(entries, JournalEntryT{})
		err = rows.Scan(&entries[count].OpID, &entries[count].OpType, &entries[count].OpDone, &entries[count].OpPayload)
		jd.assertError(err)
		count++
	}
	jd.assertError(rows.Err())
	return entries
}

func (jd *Handle) recoverFromCrash(owner OwnerType, goRoutineType string) {
	var opTypes []string
	switch goRoutineType {
	case addDSGoRoutine:
		opTypes = []string{addDSOperation}
	case mainGoRoutine:
		opTypes = []string{migrateCopyOperation, postMigrateDSOperation, dropDSOperation}
	}

	sqlStatement := fmt.Sprintf(`SELECT id, operation, done, operation_payload
                                	from %s_journal
                                	WHERE
									done=False
									AND
									operation = ANY($1)
									AND
									owner = '%s'
                                	ORDER BY id`, jd.tablePrefix, owner)

	rows, err := jd.maintenanceDB().Query(sqlStatement, pq.Array(opTypes))
	jd.assertError(err)
	defer func() { _ = rows.Close() }()

	var opID int64
	var opType string
	var opDone bool
	var opPayload json.RawMessage
	var opPayloadJSON journalOpPayloadT
	undoOp := false
	var count int

	for rows.Next() {
		err = rows.Scan(&opID, &opType, &opDone, &opPayload)
		jd.assertError(err)
		jd.assert(!opDone, "opDone is true")
		count++
	}
	jd.assertError(rows.Err())
	jd.assert(count <= 1, fmt.Sprintf("count:%d > 1", count))
	_ = rows.Close()

	if count == 0 {
		// Nothing to recover
		return
	}

	// Need to recover the last failed operation
	// Get the payload and undo
	err = jsonrs.Unmarshal(opPayload, &opPayloadJSON)
	jd.assertError(err)

	switch opType {
	case addDSOperation:
		newDS := opPayloadJSON.To
		undoOp = true
		// Drop the table we were trying to create
		jd.logger.Infon("Recovering new DS operation", logger.NewStringField("newDS", newDS.String()))
		jd.dropDSForRecovery(newDS)
	case migrateCopyOperation:
		migrateDest := opPayloadJSON.To
		// Delete the destination of the interrupted
		// migration. After we start, code should
		// redo the migration
		jd.logger.Infon("Recovering migrateCopy operation", logger.NewStringField("migrateDest", migrateDest.String()))
		jd.dropDSForRecovery(migrateDest)
		undoOp = true
	case postMigrateDSOperation:
		var migrateSrc dataSetTList = opPayloadJSON.From
		for _, ds := range migrateSrc {
			jd.dropDSForRecovery(ds)
		}
		jd.logger.Infon("Recovering migrateDel operation", logger.NewStringField("migrateSrc", migrateSrc.String()))
		undoOp = false
	}

	if undoOp {
		sqlStatement = fmt.Sprintf(`DELETE from "%s_journal" WHERE id=$1`, jd.tablePrefix)
	} else {
		sqlStatement = fmt.Sprintf(`UPDATE "%s_journal" SET done=True WHERE id=$1`, jd.tablePrefix)
	}

	_, err = jd.maintenanceDB().Exec(sqlStatement, opID)
	jd.assertError(err)
}

const (
	addDSGoRoutine = "addDS"
	mainGoRoutine  = "main"
)

func (jd *Handle) recoverFromJournal(owner OwnerType) {
	jd.recoverFromCrash(owner, addDSGoRoutine)
	jd.recoverFromCrash(owner, mainGoRoutine)
}
