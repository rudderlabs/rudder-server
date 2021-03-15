package stash

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

type StashRpcHandler struct {
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

func (s *StashRpcHandler) GetDSFailedJobs(dsName string, result *string) (err error) {
	dbHandle := s.readOnlyJobsDB.DbHandle
	dsListArr := make([]string, 0)
	if dsName != "" {
		dsListArr = append(dsListArr, prefix+"status_"+dsName)
	} else {
		dsList := s.readOnlyJobsDB.GetDSList()
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

func (s *StashRpcHandler) GetDSJobCount(arg string, result *string) (err error) {
	dbHandle := s.readOnlyJobsDB.DbHandle
	dsListArr := make([]DSPair, 0)
	argList := strings.Split(arg, ":")
	if argList[0] != "" {
		dsListArr = append(dsListArr, DSPair{jobTableName: prefix + argList[0], jobStatusTableName: "proc_error_job_" + "status_" + argList[0]})
	} else if argList[1] != "" {
		maxCount, err := strconv.Atoi(argList[1])
		if err != nil {
			return err
		}
		dsList := s.readOnlyJobsDB.GetDSList()
		for index, ds := range dsList {
			if index < maxCount {
				dsListArr = append(dsListArr, DSPair{jobTableName: ds.JobTable, jobStatusTableName: ds.JobStatusTable})
			}
		}
	} else {
		dsList := s.readOnlyJobsDB.GetDSList()
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
