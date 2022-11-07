package router

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

var failedEventsManager FailedEventsManagerI

type FailedEventRowT struct {
	DestinationID string          `json:"destination_id"`
	RecordID      json.RawMessage `json:"record_id"`
}

var (
	failedKeysTablePrefix  = "failed_keys"
	failedKeysExpire       time.Duration
	failedKeysCleanUpSleep time.Duration
	failedKeysEnabled      bool
)

type FailedEventsManagerI interface {
	SaveFailedRecordIDs(map[string][]*FailedEventRowT, *sql.Tx)
	DropFailedRecordIDs(jobRunID string)
	FetchFailedRecordIDs(jobRunID string) []*FailedEventRowT
	GetDBHandle() *sql.DB
}

type FailedEventsManagerT struct {
	dbHandle *sql.DB
}

func GetFailedEventsManager() FailedEventsManagerI {
	if failedEventsManager == nil {
		fem := new(FailedEventsManagerT)
		dbHandle, err := sql.Open("postgres", misc.GetConnectionString())
		if err != nil {
			panic(err)
		}
		fem.dbHandle = dbHandle
		failedEventsManager = fem
	}

	return failedEventsManager
}

func (*FailedEventsManagerT) SaveFailedRecordIDs(taskRunIDFailedEventsMap map[string][]*FailedEventRowT, txn *sql.Tx) {
	if !failedKeysEnabled {
		return
	}

	for taskRunID, failedEvents := range taskRunIDFailedEventsMap {
		table := `"` + strings.ReplaceAll(fmt.Sprintf(`%s_%s`, failedKeysTablePrefix, taskRunID), `"`, `""`) + `"`
		sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		destination_id TEXT NOT NULL,
		record_id JSONB NOT NULL,
		created_at TIMESTAMP NOT NULL);`, table)
		_, err := txn.Exec(sqlStatement)
		if err != nil {
			_ = txn.Rollback()
			panic(err)
		}
		insertQuery := fmt.Sprintf(`INSERT INTO %s VALUES($1, $2, $3);`, table)
		stmt, err := txn.Prepare(insertQuery)
		if err != nil {
			_ = txn.Rollback()
			panic(err)
		}
		createdAt := time.Now()
		for _, failedEvent := range failedEvents {
			if len(failedEvent.RecordID) == 0 || !json.Valid(failedEvent.RecordID) {
				pkgLogger.Infof("skipped adding invalid recordId: %s, to failed keys table: %s", failedEvent.RecordID, table)
				continue
			}
			_, err = stmt.Exec(failedEvent.DestinationID, failedEvent.RecordID, createdAt)
			if err != nil {
				panic(err)
			}
		}

		stmt.Close()
	}
}

func (fem *FailedEventsManagerT) DropFailedRecordIDs(taskRunID string) {
	if !failedKeysEnabled {
		return
	}

	// Drop table
	table := fmt.Sprintf(`%s_%s`, failedKeysTablePrefix, taskRunID)
	sqlStatement := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table)
	_, err := fem.dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("Failed to drop table %s with error: %v", taskRunID, err)
	}
}

func (fem *FailedEventsManagerT) FetchFailedRecordIDs(taskRunID string) []*FailedEventRowT {
	if !failedKeysEnabled {
		return []*FailedEventRowT{}
	}

	failedEvents := make([]*FailedEventRowT, 0)

	var rows *sql.Rows
	var err error
	table := `"` + strings.ReplaceAll(fmt.Sprintf(`%s_%s`, failedKeysTablePrefix, taskRunID), `"`, `""`) + `"`
	sqlStatement := fmt.Sprintf(`SELECT %[1]s.destination_id, %[1]s.record_id
                                             FROM %[1]s `, table)
	rows, err = fem.dbHandle.Query(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("Failed to fetch from table %s with error: %v", taskRunID, err)
		return failedEvents
	}
	defer rows.Close()

	for rows.Next() {
		var failedEvent FailedEventRowT
		err := rows.Scan(&failedEvent.DestinationID, &failedEvent.RecordID)
		if err != nil {
			panic(err)
		}
		failedEvents = append(failedEvents, &failedEvent)
	}

	return failedEvents
}

func CleanFailedRecordsTableProcess(ctx context.Context) {
	if !failedKeysEnabled {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(failedKeysCleanUpSleep):
			dbHandle, err := sql.Open("postgres", misc.GetConnectionString())
			if err != nil {
				panic(err)
			}
			failedKeysLike := failedKeysTablePrefix + "%"
			failedKeysTableQuery := fmt.Sprintf(`SELECT table_name
													FROM information_schema.tables
													WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name ilike '%s'`, failedKeysLike)
			rows, err := dbHandle.Query(failedKeysTableQuery)
			if err != nil {
				panic(err)
			}
			for rows.Next() {
				var table string
				err = rows.Scan(&table)
				if err != nil {
					pkgLogger.Errorf("Failed to scan failed keys table %s with error: %v", table, err)
					return
				}
				latestCreatedAtQuery := fmt.Sprintf(`SELECT created_at from %s order by created_at desc limit 1`, table)
				row := dbHandle.QueryRow(latestCreatedAtQuery)
				var latestCreatedAt time.Time
				err = row.Scan(&latestCreatedAt)
				if err != nil && err != sql.ErrNoRows {
					pkgLogger.Errorf("Failed to fetch records from failed keys table %s with error: %v", table, err)
					continue
				}
				currentTime := time.Now()
				diff := currentTime.Sub(latestCreatedAt)
				if diff > failedKeysExpire {
					dropQuery := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, table)
					rows, err = dbHandle.Query(dropQuery)
					if err != nil {
						pkgLogger.Errorf("Failed to drop table %s with error: %v", table, err)
					}
				}
			}
			dbHandle.Close()
		}
	}
}

func (fem *FailedEventsManagerT) GetDBHandle() *sql.DB {
	return fem.dbHandle
}
