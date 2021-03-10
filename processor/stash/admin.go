package stash

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

type StashRpcHandler struct {
	jobsDB         jobsdb.JobsDB
	readOnlyJobsDB jobsdb.ReadonlyHandleT
}

type SqlRunner struct {
	dbHandle     *sql.DB
	jobTableName string
}

var prefix = "proc_error_jobs_"

/*
ProcErrorsByDestinationCount
================================================================================
│───────│─────────────│──────────────────────────────────│
│ COUNT │ DESTINATION │ ERROR                            │
│───────│─────────────│──────────────────────────────────│
│     3 │ GA          │ "server side identify is not on" │
│───────│─────────────│──────────────────────────────────│
*/
func (s *StashRpcHandler) GetDSStats(dsName string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("Internal Rudder Server Error. Error: %w", r)
		}
	}()
	jobTableName := prefix + dsName
	dbHandle, err := sql.Open("postgres", jobsdb.GetConnectionString())
	//skipcq: SCC-SA5001
	defer dbHandle.Close()
	if err != nil {
		return err
	}
	results, execErr := s.getErrorCountByDest(dbHandle, jobTableName)
	if execErr != nil {
		return execErr
	}

	response, marshalErr := json.MarshalIndent(results, "", " ")
	if marshalErr != nil {
		*result = ""
	} else {
		*result = string(response)
	}
	return marshalErr
}

func (s *StashRpcHandler) GetDSList(dsName string, result *string) (err error) {
	dsList := s.readOnlyJobsDB.GetDSList()
	for _, ds := range dsList {
		*result = *result + ds.JobTable + "\n"
	}
	return nil
}

func (s *StashRpcHandler) GetDSJobCount(dsName string, result *string) (err error) {
	dbHandle := s.readOnlyJobsDB.DbHandle
	dsListArr := make([]string, 0)
	var totalCount int
	if dsName != "" {
		dsListArr = append(dsListArr, prefix+dsName)
	} else {
		dsList := s.readOnlyJobsDB.GetDSList()
		for _, ds := range dsList {
			dsListArr = append(dsListArr, ds.JobTable)
		}
	}
	for _, tableName := range dsListArr {
		runner := &SqlRunner{dbHandle: dbHandle, jobTableName: tableName}
		count, err := runner.getTableRowCount()
		if err == nil {
			totalCount = totalCount + int(count)
		}
	}
	*result = strconv.Itoa(totalCount)
	return nil
}

func (r *SqlRunner) getTableRowCount() (int, error) {
	var numRows int
	var err error
	totalRowsStmt := fmt.Sprintf(`select count(*) from %s`, r.jobTableName)
	err = runSQL(r, totalRowsStmt, &numRows)
	return numRows, err
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

type DestinationCountResult struct {
	Count    int
	DestName string
	Error    string
}

func (s *StashRpcHandler) getErrorCountByDest(dbHandle *sql.DB, jobTableName string) ([]DestinationCountResult, error) {
	results := make([]DestinationCountResult, 0)
	uniqueSourceValsStmt := fmt.Sprintf(`select count(*) as count, custom_val as dest, parameters -> 'error' as error from %s group by custom_val, parameters -> 'error'`, jobTableName)
	var rows *sql.Rows
	var err error
	rows, err = dbHandle.Query(uniqueSourceValsStmt)
	if err != nil {
		return results, err
	}
	defer rows.Close()
	singleResult := DestinationCountResult{}
	for rows.Next() {
		err = rows.Scan(&singleResult.Count, &singleResult.DestName, &singleResult.Error)
		if err != nil {
			return results, err
		}
		results = append(results, singleResult)
	}

	if err = rows.Err(); err != nil {
		return results, err
	}

	if err = rows.Close(); err != nil {
		return results, err
	}

	return results, nil
}
