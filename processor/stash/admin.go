package stash

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

type StashRpcHandler struct {
	jobsDB jobsdb.JobsDB
}

var prefix = "proc_error_jobs_"

func (s *StashRpcHandler) GetDSStats(dsName string, result *string) error {
	jobTableName := prefix + dsName
	dbHandle, err := sql.Open("postgres", jobsdb.GetConnectionString())
	defer dbHandle.Close()
	if err != nil {
		return err
	}
	results, execErr := s.getErrorCountByDest(dbHandle, jobTableName)
	response, marshalErr := json.MarshalIndent(results, "", " ")
	if marshalErr != nil {
		*result = ""
	} else {
		*result = string(response)
	}
	return execErr
}

type DestinationCountResult struct {
	Count    int
	DestName string
}

func (s *StashRpcHandler) getErrorCountByDest(dbHandle *sql.DB, jobTableName string) ([]DestinationCountResult, error) {
	results := make([]DestinationCountResult, 0)
	uniqueSourceValsStmt := fmt.Sprintf(`select count(*) as count, custom_val as dest from %s group by custom_val`, jobTableName)
	var rows *sql.Rows
	var err error
	rows, err = dbHandle.Query(uniqueSourceValsStmt)
	if err != nil {
		return results, err
	}
	defer rows.Close()
	singleResult := DestinationCountResult{}
	for rows.Next() {
		err = rows.Scan(&singleResult.Count, &singleResult.DestName)
		if err != nil {
			return results, err
		}
		results = append(results, singleResult)
	}

	err = rows.Err()
	if err != nil {
		return results, err
	}

	err = rows.Close()
	if err != nil {
		return results, err
	}

	return results, nil
}
