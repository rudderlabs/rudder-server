package router

import (
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

var (
	failedEventsManager FailedEventsManagerI
)

type FailedEventRowT struct {
	DestinationID string
	RecordID      string
}

type FailedEventsManagerI interface {
	SaveFailedRecordIDs(map[string][]*FailedEventRowT, *sql.Tx)
	DropFailedRecordIDs(jobRunID string)
	FetchFailedRecordIDs(jobRunID string) []*FailedEventRowT
}

type FailedEventsManagerT struct {
	dbHandle *sql.DB
}

func GetFailedEventsManager() FailedEventsManagerI {
	if failedEventsManager == nil {
		fem := new(FailedEventsManagerT)
		dbHandle, err := sql.Open("postgres", jobsdb.GetConnectionString())
		if err != nil {
			panic(err)
		}
		fem.dbHandle = dbHandle
		failedEventsManager = fem
	}

	return failedEventsManager
}

func (fem *FailedEventsManagerT) SaveFailedRecordIDs(jobRunIDFailedEventsMap map[string][]*FailedEventRowT, txn *sql.Tx) {
	for jobRunID, failedEvents := range jobRunIDFailedEventsMap {
		sqlStatement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		destination_id TEXT NOT NULL,
		record_id TEXT NOT NULL);`, jobRunID)

		_, err := txn.Exec(sqlStatement)
		if err != nil {
			panic(err)
		}

		stmt, err := txn.Prepare(pq.CopyIn(jobRunID, "destination_id", "record_id"))
		if err != nil {
			panic(err)
		}
		for _, failedEvent := range failedEvents {
			_, err = stmt.Exec(failedEvent.DestinationID, failedEvent.RecordID)
			if err != nil {
				panic(err)
			}
		}
		_, err = stmt.Exec()
		if err != nil {
			panic(err)
		}
		stmt.Close()
	}
}

func (fem *FailedEventsManagerT) DropFailedRecordIDs(jobRunID string) {
	//Drop table
	sqlStatement := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, jobRunID)
	_, err := fem.dbHandle.Exec(sqlStatement)
	if err != nil {
		pkgLogger.Errorf("Failed to drop table %s with error: %v", jobRunID, err)
	}
}

func (fem *FailedEventsManagerT) FetchFailedRecordIDs(taskRunID string) []*FailedEventRowT {
	failedEvents := make([]*FailedEventRowT, 0)

	var rows *sql.Rows
	var err error
	sqlStatement := fmt.Sprintf(`SELECT %[1]s.destination_id, %[1]s.record_id
                                             FROM %[1]s `, taskRunID)
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
