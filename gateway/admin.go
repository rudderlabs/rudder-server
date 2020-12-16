package gateway

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type GatewayAdmin struct {
	handle *HandleT
}

var prefix = "gw_jobs_"

// Status function is used for debug purposes by the admin interface
func (g *GatewayAdmin) Status() interface{} {
	configSubscriberLock.RLock()
	defer configSubscriberLock.RUnlock()
	writeKeys := make([]string, 0, len(enabledWriteKeysSourceMap))
	for k := range enabledWriteKeysSourceMap {
		writeKeys = append(writeKeys, k)
	}

	return map[string]interface{}{
		"ack-count":          g.handle.ackCount,
		"recv-count":         g.handle.recvCount,
		"enabled-write-keys": writeKeys,
		"jobsdb":             g.handle.jobsDB.Status(),
	}

}

type GatewayRPCHandler struct {
	jobsDB jobsdb.JobsDB
}

type SqlRunner struct {
	dbHandle     *sql.DB
	jobTableName string
}

type SourceEvents struct {
	Count  int
	Source string
}

func (r *SqlRunner) getUniqueSources() ([]SourceEvents, error) {
	var rows *sql.Rows
	var err error
	sources := make([]SourceEvents, 0)
	uniqueSourceValsStmt := fmt.Sprintf(`select count(*) as count, parameters -> 'source_id' as source from %s group by parameters -> 'source_id';`, r.jobTableName)
	rows, err = r.dbHandle.Query(uniqueSourceValsStmt)
	if err != nil {
		return sources, err
	}
	defer rows.Close()
	sourceEvent := SourceEvents{}
	for rows.Next() {
		err = rows.Scan(&sourceEvent.Count, &sourceEvent.Source)
		if err != nil {
			return sources, err // defer closing of rows, so return will not memory leak
		}
		sources = append(sources, sourceEvent)
	}

	err = rows.Err()
	if err != nil {
		return sources, err
	}

	err = rows.Close() // rows.close will be called in defer too, but it should be harmless to call multiple times
	return sources, err
}

func (r *SqlRunner) getNumUniqueUsers() (int, error) {
	var numUsers int
	var err error
	numUniqueUsersStmt := fmt.Sprintf(`select count(*) from (select  distinct user_id from %s) as t`, r.jobTableName)
	err = runSQL(r, numUniqueUsersStmt, &numUsers)
	return numUsers, err
}

func (r *SqlRunner) getAvgBatchSize() (float64, error) {
	var batchSize sql.NullFloat64
	var avgBatchSize float64
	var err error
	avgBatchSizeStmt := fmt.Sprintf(`select avg(jsonb_array_length(batch)) from (select event_payload->'batch' as batch from %s) t`, r.jobTableName)
	err = runSQL(r, avgBatchSizeStmt, &batchSize)
	if batchSize.Valid {
		avgBatchSize = batchSize.Float64
	}
	return avgBatchSize, err
}

func (r *SqlRunner) getTableSize() (int64, error) {
	var tableSize int64
	var err error
	tableSizeStmt := fmt.Sprintf(`select pg_total_relation_size('%s')`, r.jobTableName)
	err = runSQL(r, tableSizeStmt, &tableSize)
	return tableSize, err
}

func (r *SqlRunner) getTableRowCount() (int, error) {
	var numRows int
	var err error
	totalRowsStmt := fmt.Sprintf(`select count(*) from %s`, r.jobTableName)
	err = runSQL(r, totalRowsStmt, &numRows)
	return numRows, err
}

type DSStats struct {
	SourceNums   []SourceEvents
	NumUsers     int
	AvgBatchSize float64
	TableSize    int64
	NumRows      int
}

// first_event, last_event min--maxid to event: available in dsrange
// Average batch size ⇒ num_events we want per ds
// writeKey, count(*)  we want source name to count per ds
// Num Distinct users per ds
// Avg Event size = Table_size / (avg Batch size * Total rows) is Table_size correct measure?
// add job status group by
/*
EventsBySource
================================================================================
│───────│────────│
│ COUNT │ SOURCE │
│───────│────────│
│   448 │ tr-2   │
│───────│────────│
================================================================================
NumUsers :  228
NumUsers :  1.2589285714285714
NumUsers :  1351680
NumUsers :  448
*/
func (g *GatewayRPCHandler) GetDSStats(dsName string, result *string) error {
	var completeErr error
	jobTableName := prefix + dsName
	dbHandle, err := sql.Open("postgres", jobsdb.GetConnectionString())
	if err != nil {
		return err
	}
	defer dbHandle.Close() // since this also returns an error, we can explicitly close but not doing
	runner := &SqlRunner{dbHandle: dbHandle, jobTableName: jobTableName}
	sources, serr := runner.getUniqueSources()
	if serr != nil {
		misc.AppendError("getUniqueSources", &completeErr, &serr)
	}
	numUsers, uerr := runner.getNumUniqueUsers()
	if serr != nil {
		misc.AppendError("getNumUniqueUsers", &completeErr, &uerr)
	}
	avgBatchSize, berr := runner.getAvgBatchSize()
	if serr != nil {
		misc.AppendError("getAvgBatchSize", &completeErr, &berr)
	}
	tableSize, tserr := runner.getTableSize()
	if serr != nil {
		misc.AppendError("getTableSize", &completeErr, &tserr)
	}
	numRows, rerr := runner.getTableRowCount()
	if serr != nil {
		misc.AppendError("getTableRowCount", &completeErr, &rerr)
	}

	configSubscriberLock.RLock()
	sourcesEventToCounts := make([]SourceEvents, 0)
	for _, sourceEvent := range sources {
		name, found := sourceIDToNameMap[sourceEvent.Source[1:len(sourceEvent.Source)-1]]
		if found {
			sourcesEventToCounts = append(sourcesEventToCounts, SourceEvents{sourceEvent.Count, name})
		}
	}
	configSubscriberLock.RUnlock()
	response, err := json.MarshalIndent(DSStats{sourcesEventToCounts, numUsers, avgBatchSize, tableSize, numRows}, "", " ")
	if err != nil {
		*result = ""
		misc.AppendError("MarshalIndent", &completeErr, &err)
	} else {
		*result = string(response)
	}

	return err
}

func runSQL(runner *SqlRunner, query string, reciever interface{}) error {
	row := runner.dbHandle.QueryRow(query)
	err := row.Scan(reciever)
	if err != nil {
		if err == sql.ErrNoRows {
			fmt.Println("Zero rows found")
		}
	}
	return err
}
