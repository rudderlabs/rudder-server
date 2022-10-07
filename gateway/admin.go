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
	writeKeys := make([]string, 0, len(writeKeysSourceMap))
	for k := range writeKeysSourceMap {
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
	jobsDB         jobsdb.JobsDB
	readOnlyJobsDB jobsdb.ReadonlyJobsDB
}

type SqlRunner struct {
	dbHandle     *sql.DB
	jobTableName string
}

type SourceEvents struct {
	Count int
	Name  string
	ID    string
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
	defer func() { _ = rows.Close() }()
	sourceEvent := SourceEvents{}
	for rows.Next() {
		err = rows.Scan(&sourceEvent.Count, &sourceEvent.ID)
		if err != nil {
			return sources, err // defer closing of rows, so return will not memory leak
		}
		sources = append(sources, sourceEvent)
	}

	if err = rows.Err(); err != nil {
		return sources, err
	}

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

// GetDSStats TODO :
// first_event, last_event min--maxid to event: available in dsrange
// Average batch size ⇒ num_events we want per ds
// writeKey, count(*)  we want source name to count per ds
// Num Distinct users per ds
// Avg Event size = Table_size / (avg Batch size * Total rows) is Table_size correct measure?
// add job status group by
/*
EventsBySource
================================================================================
│───────│───────────│───────────────────────────────│
│ COUNT │ NAME      │ ID                            │
│───────│───────────│───────────────────────────────│
│     7 │ test-dev  │ "1jEZBT9aChBgbVkfKBjtLau8XAM" │
│     1 │ and-raid  │ "1lBkol38t4m5Xz3zZAeSZ0P26QU" │
│───────│───────────│───────────────────────────────│
================================================================================
NumUsers :  2
AvgBatchSize :  1
TableSize :  65536
NumRows :  8
*/
func (*GatewayRPCHandler) GetDSStats(dsName string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()

	var completeErr error
	jobTableName := prefix + dsName
	dbHandle, err := sql.Open("postgres", misc.GetConnectionString())
	if err != nil {
		return err
	}
	defer func() { _ = dbHandle.Close() }() // since this also returns an error, we can explicitly close but not doing
	runner := &SqlRunner{dbHandle: dbHandle, jobTableName: jobTableName}
	sources, err := runner.getUniqueSources()
	if err != nil {
		misc.AppendError("getUniqueSources", &completeErr, &err)
	}
	numUsers, err := runner.getNumUniqueUsers()
	if err != nil {
		misc.AppendError("getNumUniqueUsers", &completeErr, &err)
	}
	avgBatchSize, err := runner.getAvgBatchSize()
	if err != nil {
		misc.AppendError("getAvgBatchSize", &completeErr, &err)
	}
	tableSize, err := runner.getTableSize()
	if err != nil {
		misc.AppendError("getTableSize", &completeErr, &err)
	}
	numRows, err := runner.getTableRowCount()
	if err != nil {
		misc.AppendError("getTableRowCount", &completeErr, &err)
	}

	configSubscriberLock.RLock()
	sourcesEventToCounts := make([]SourceEvents, 0)
	for _, sourceEvent := range sources {
		name, found := sourceIDToNameMap[sourceEvent.ID[1:len(sourceEvent.ID)-1]]
		if found {
			sourcesEventToCounts = append(sourcesEventToCounts, SourceEvents{sourceEvent.Count, name, sourceEvent.ID})
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

	return completeErr
}

func (g *GatewayRPCHandler) GetDSList(_ string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	response, err := g.readOnlyJobsDB.GetDSListString()
	*result = response
	return nil
}

func (g *GatewayRPCHandler) GetDSJobCount(arg string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	response, err := g.readOnlyJobsDB.GetJobSummaryCount(arg, prefix)
	*result = response
	return err
}

func (g *GatewayRPCHandler) GetDSFailedJobs(arg string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	response, err := g.readOnlyJobsDB.GetLatestFailedJobs(arg, prefix)
	*result = response
	return err
}

func (g *GatewayRPCHandler) GetJobByID(arg string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	response, err := g.readOnlyJobsDB.GetJobByID(arg, prefix)
	*result = response
	return err
}

func (g *GatewayRPCHandler) GetJobIDStatus(arg string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	response, err := g.readOnlyJobsDB.GetJobIDStatus(arg, prefix)
	*result = response
	return err
}

func runSQL(runner *SqlRunner, query string, receiver interface{}) error {
	row := runner.dbHandle.QueryRow(query)
	err := row.Scan(receiver)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil //"Zero rows found"
		}
	}
	return err
}
