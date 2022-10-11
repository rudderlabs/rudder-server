package stash

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type StashRpcHandler struct {
	ReadOnlyJobsDB jobsdb.ReadonlyJobsDB
}

var prefix = "proc_error_jobs_"

// GetDSStats
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
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	jobTableName := prefix + dsName
	dbHandle, err := sql.Open("postgres", misc.GetConnectionString())
	// skipcq: SCC-SA5001
	defer func() { _ = dbHandle.Close() }()
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

func (s *StashRpcHandler) GetDSList(_ string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	response, err := s.ReadOnlyJobsDB.GetDSListString()
	*result = response
	return nil
}

func (s *StashRpcHandler) GetDSFailedJobs(arg string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	response, err := s.ReadOnlyJobsDB.GetLatestFailedJobs(arg, prefix)
	*result = response
	return err
}

func (s *StashRpcHandler) GetJobByID(arg string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	response, err := s.ReadOnlyJobsDB.GetJobByID(arg, prefix)
	*result = response
	return err
}

func (s *StashRpcHandler) GetJobIDStatus(arg string, result *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	response, err := s.ReadOnlyJobsDB.GetJobIDStatus(arg, prefix)
	*result = response
	return err
}

type DestinationCountResult struct {
	Count    int
	DestName string
	Error    string
}

func (*StashRpcHandler) getErrorCountByDest(dbHandle *sql.DB, jobTableName string) ([]DestinationCountResult, error) {
	results := make([]DestinationCountResult, 0)
	uniqueSourceValsStmt := fmt.Sprintf(`select count(*) as count, custom_val as dest, parameters -> 'error' as error from %s group by custom_val, parameters -> 'error'`, jobTableName)
	var rows *sql.Rows
	var err error
	rows, err = dbHandle.Query(uniqueSourceValsStmt)
	if err != nil {
		return results, err
	}
	defer func() { _ = rows.Close() }()
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
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()
	response, err := s.ReadOnlyJobsDB.GetJobSummaryCount(arg, prefix)
	*result = response
	return nil
}
