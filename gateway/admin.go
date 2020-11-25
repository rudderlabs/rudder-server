package gateway

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/jobsdb"
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
	err          error
}

func (r *SqlRunner) getUniqueSources() []string {
	sources := make([]string, 0)
	if r.err != nil {
		return sources
	}
	uniqueSourceValsStmt := fmt.Sprintf(`select  distinct parameters->'source_id' from %s`, r.jobTableName)
	var rows *sql.Rows
	rows, r.err = r.dbHandle.Query(uniqueSourceValsStmt)
	defer rows.Close() // don't need to check error as we are firing read ops
	if r.err != nil {
		return sources
	}
	var sourceID string
	for rows.Next() {
		if r.err = rows.Scan(&sourceID); r.err != nil {
			return sources
		}
		sources = append(sources, sourceID)
	}

	if r.err = rows.Err(); r.err != nil {
		return sources
	}

	return sources
}

func (r *SqlRunner) getNumUniqueUsers() int {
	var numUsers int
	if r.err != nil {
		return 0
	}
	numUniqueUsersStmt := fmt.Sprintf(`select count(*) from (select  distinct user_id from %s) as t`, r.jobTableName)
	r.err = runSQL(r, numUniqueUsersStmt, &numUsers)
	return numUsers
}

func (r *SqlRunner) getAvgBatchSize() float64 {
	var batchSize sql.NullFloat64
	var avgBatchSize float64
	if r.err != nil {
		return 0
	}
	avgBatchSizeStmt := fmt.Sprintf(`select avg(jsonb_array_length(batch)) from (select event_payload->'batch' as batch from %s) t`, r.jobTableName)
	r.err = runSQL(r, avgBatchSizeStmt, &batchSize)
	if batchSize.Valid {
		avgBatchSize = batchSize.Float64
	}
	return avgBatchSize
}

func (r *SqlRunner) getTableSize() int64 {
	var tableSize int64
	if r.err != nil {
		return 0
	}
	tableSizeStmt := fmt.Sprintf(`select pg_total_relation_size('%s')`, r.jobTableName)
	r.err = runSQL(r, tableSizeStmt, &tableSize)
	return tableSize
}

func (r *SqlRunner) getTableRowCount() int {
	var numRows int
	if r.err != nil {
		return 0
	}
	totalRowsStmt := fmt.Sprintf(`select count(*) from %s`, r.jobTableName)
	r.err = runSQL(r, totalRowsStmt, &numRows)
	return numRows
}

type DSStats struct {
	Sources      []string
	NumUsers     int
	AvgBatchSize float64
	TableSize    int64
	NumRows      int
}

// first_event, last_event min--maxid to event?
// Average batch size ⇒ num_events we want per ds ?
// writeKey, count(*)  we want source name...per ds?
// Num Distinct users per ds?
// Avg Event size = Table_size / (avg Batch size * Total rows)
func (g *GatewayRPCHandler) GetDSStats(dsName string, result *string) error {
	jobTableName := prefix + dsName
	dbHandle, err := sql.Open("postgres", g.jobsDB.GetConnectionStringPresent())
	defer dbHandle.Close()
	runner := &SqlRunner{dbHandle: dbHandle, jobTableName: jobTableName, err: err}

	// TODO: Check for the case when table is not oresent
	sources := runner.getUniqueSources()
	numUsers := runner.getNumUniqueUsers()
	avgBatchSize := runner.getAvgBatchSize()
	tableSize := runner.getTableSize()
	numRows := runner.getTableRowCount()

	//fmt.Println(sources, numUsers, avgBatchSize, tableSize, numRows)
	response, err := json.MarshalIndent(DSStats{sources, numUsers, avgBatchSize, tableSize, numRows}, "", " ")
	if err != nil {
		*result = ""
	} else {
		*result = string(response)
	}

	return runner.err
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
