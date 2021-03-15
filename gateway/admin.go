package gateway

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

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
	jobsDB         jobsdb.JobsDB
	readOnlyJobsDB jobsdb.ReadonlyHandleT
}

type SqlRunner struct {
	dbHandle           *sql.DB
	jobTableName       string
	jobStatusTableName string
}

type DSPair struct {
	jobTableName       string
	jobStatusTableName string
}

type SourceEvents struct {
	Count int
	Name  string
	ID    string
}

type EventStatusDetailed struct {
	Status        string
	SourceID      string
	DestinationID string
	CustomVal     string
	Count         int
}

type EventStatusStats struct {
	StatsNums []EventStatusDetailed
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

// TODO :
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
func (g *GatewayRPCHandler) GetDSStats(dsName string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("Internal Rudder Server Error. Error: %w", r)
		}
	}()

	var completeErr error
	jobTableName := prefix + dsName
	dbHandle, err := sql.Open("postgres", jobsdb.GetConnectionString())
	if err != nil {
		return err
	}
	defer dbHandle.Close() // since this also returns an error, we can explicitly close but not doing
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

func (g *GatewayRPCHandler) GetDSList(emptyInput string, result *string) (err error) {
	dsList := g.readOnlyJobsDB.GetDSList()
	for _, ds := range dsList {
		*result = *result + ds.JobTable + "\n"
	}
	return nil
}

func (g *GatewayRPCHandler) GetDSJobCount(arg string, result *string) (err error) {
	dbHandle := g.readOnlyJobsDB.DbHandle
	dsListArr := make([]DSPair, 0)
	argList := strings.Split(arg, ":")
	if argList[0] != "" {
		dsListArr = append(dsListArr, DSPair{jobTableName: prefix + argList[0], jobStatusTableName: "gw_job_" + "status_" + argList[0]})
	} else if argList[1] != "" {
		maxCount, err := strconv.Atoi(argList[1])
		if err != nil {
			return err
		}
		dsList := g.readOnlyJobsDB.GetDSList()
		for index, ds := range dsList {
			if index < maxCount {
				dsListArr = append(dsListArr, DSPair{jobTableName: ds.JobTable, jobStatusTableName: ds.JobStatusTable})
			}
		}
	} else {
		dsList := g.readOnlyJobsDB.GetDSList()
		dsListArr = append(dsListArr, DSPair{jobTableName: dsList[0].JobTable, jobStatusTableName: dsList[0].JobStatusTable})
	}
	eventStatusDetailed := make([]EventStatusDetailed, 0)
	for _, dsPair := range dsListArr {
		runner := &SqlRunner{dbHandle: dbHandle, jobTableName: dsPair.jobTableName, jobStatusTableName: dsPair.jobStatusTableName}
		sqlStatement := fmt.Sprintf(`SELECT COUNT(*) as count, %[2]s.job_state,%[1]s.parameters->'source_id' as source,%[1]s.custom_val,
									%[1]s.parameters->'destination_id' as destination FROM %[1]s LEFT JOIN %[2]s ON %[1]s.job_id=%[2]s.job_id 
									GROUP BY %[2]s.job_state,%[1]s.parameters->'source_id',%[1]s.custom_val,%[1]s.parameters->'destination_id';`, dsPair.jobTableName, dsPair.jobStatusTableName)

		row, _ := runner.dbHandle.Query(sqlStatement)
		defer row.Close()
		for row.Next() {
			event := EventStatusDetailed{}
			_ = row.Scan(&event.Count, &event.Status, &event.SourceID, &event.CustomVal, &event.DestinationID)
			eventStatusDetailed = append(eventStatusDetailed, event)
		}
	}
	response, err := json.MarshalIndent(EventStatusStats{eventStatusDetailed}, "", " ")
	if err != nil {
		*result = ""
		return err
	}
	*result = string(response)
	return nil
}

func (g *GatewayRPCHandler) GetDSFailedJobs(dsName string, result *string) (err error) {
	dbHandle := g.readOnlyJobsDB.DbHandle
	dsListArr := make([]string, 0)
	if dsName != "" {
		dsListArr = append(dsListArr, prefix+"status_"+dsName)
	} else {
		dsList := g.readOnlyJobsDB.GetDSList()
		for _, ds := range dsList {
			dsListArr = append(dsListArr, ds.JobStatusTable)
		}
	}
	for _, tableName := range dsListArr {
		runner := &SqlRunner{dbHandle: dbHandle, jobTableName: tableName}
		sqlStatement := ""
		rows, err := runner.dbHandle.Query(sqlStatement)
		defer rows.Close()
		if err == nil {
			var jobId string
			_ = rows.Scan(&jobId)
			*result = *result + jobId
		}
	}
	return nil
}

func runSQL(runner *SqlRunner, query string, reciever interface{}) error {
	row := runner.dbHandle.QueryRow(query)
	err := row.Scan(reciever)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil //"Zero rows found"
		}
	}
	return err
}
