package jobsdb

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb/internal/dsindex"
	"github.com/samber/lo"
)

type sqlDbOrTx interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

/*
Function to return an ordered list of datasets and datasetRanges
Most callers use the in-memory list of dataset and datasetRanges
*/
func (jd *HandleT) getDSListFromDB() []dataSetT {
	var datasetList []dataSetT
	tableNames := jd.mustGetAllTableNames()

	jobNameMap := map[string]string{}
	jobStatusNameMap := map[string]string{}
	var dnumList []string

	for _, t := range tableNames {
		if strings.HasPrefix(t, jd.tablePrefix+"_jobs_") {
			dnum := t[len(jd.tablePrefix+"_jobs_"):]
			jobNameMap[dnum] = t
			dnumList = append(dnumList, dnum)
			continue
		}
		if strings.HasPrefix(t, jd.tablePrefix+"_job_status_") {
			dnum := t[len(jd.tablePrefix+"_job_status_"):]
			jobStatusNameMap[dnum] = t
			continue
		}
	}

	sortDnumList(dnumList)

	for _, dnum := range dnumList {
		jobName, ok := jobNameMap[dnum]
		jd.assert(ok, fmt.Sprintf("dnum %s is not found in jobNameMap", dnum))
		jobStatusName, ok := jobStatusNameMap[dnum]
		jd.assert(ok, fmt.Sprintf("dnum %s is not found in jobStatusNameMap", dnum))
		datasetList = append(datasetList,
			dataSetT{
				JobTable:       jobName,
				JobStatusTable: jobStatusName,
				Index:          dnum,
			})
	}

	return datasetList
}

/*
sortDnumList Function to sort table suffixes. We should not have any use case
for having > 2 len suffixes (e.g. 1_1_1 - see comment below)
but this sort handles the general case
*/
func sortDnumList(dnumList []string) {
	sort.Slice(dnumList, func(i, j int) bool {
		return dsindex.MustParse(dnumList[i]).Less(dsindex.MustParse(dnumList[j]))
	})
}

// mustGetAllTableNames gets all table names from Postgres and panics in case of an error
func (jd *HandleT) mustGetAllTableNames() []string {
	var tableNames []string
	var err error

	err = WithExponentialBackoffRetry(context.TODO(), func(context.Context) error {
		tableNames, err = getAllTableNames(jd.dbHandle)
		return err
	}, jd.maxQueryRetryTime)

	jd.assertError(err)
	return tableNames
}

// getAllTableNames gets all table names from Postgres
func getAllTableNames(dbHandle sqlDbOrTx) ([]string, error) {
	var tableNames []string
	rows, err := dbHandle.Query(`SELECT tablename
									FROM pg_catalog.pg_tables
									WHERE schemaname != 'pg_catalog' AND
									schemaname != 'information_schema'`)
	if err != nil {
		return tableNames, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var tbName string
		err = rows.Scan(&tbName)
		if err != nil {
			return tableNames, err
		}
		tableNames = append(tableNames, tbName)
	}
	return tableNames, nil
}

// checkValidJobState Function to check validity of states
func checkValidJobState(jd assertInterface, stateFilters []string) {
	jobStateMap := make(map[string]jobStateT)
	for _, js := range jobStates {
		jobStateMap[js.State] = js
	}
	for _, st := range stateFilters {
		js, ok := jobStateMap[st]
		jd.assert(ok, fmt.Sprintf("state %s is not found in jobStates: %v", st, jobStates))
		jd.assert(js.isValid, fmt.Sprintf("jobState : %v is not valid", js))
	}
}

// constructQueryOR construct a query were paramKey is any of the values in paramValues
func constructQueryOR(paramKey string, paramList []string) string {
	var queryList []string
	for _, p := range paramList {
		queryList = append(queryList, "("+paramKey+"='"+p+"')")
	}
	return "(" + strings.Join(queryList, " OR ") + ")"
}

// constructParameterJSONQuery construct and return query
func constructParameterJSONQuery(alias string, parameterFilters []ParameterFilterT) string {
	// eg. query with optional destination_id (batch_rt_jobs_1.parameters @> '{"source_id":"<source_id>","destination_id":"<destination_id>"}'  OR (batch_rt_jobs_1.parameters @> '{"source_id":"<source_id>"}' AND batch_rt_jobs_1.parameters -> 'destination_id' IS NULL))
	conditions := lo.Map(parameterFilters, func(parameter ParameterFilterT, _ int) string {
		return fmt.Sprintf(`%s.parameters->>'%s'='%s'`, alias, parameter.Name, parameter.Value)
	})

	return "(" + strings.Join(conditions, " OR ") + ")"
}

// statTags is a struct to hold tags for stats
type statTags struct {
	CustomValFilters []string
	ParameterFilters []ParameterFilterT
	StateFilters     []string
	WorkspaceID      string
}

func (jd *HandleT) getTimerStat(stat string, tags *statTags) stats.Measurement {
	return stats.Default.NewTaggedStat(
		stat,
		stats.TimerType,
		tags.getStatsTags(jd.tablePrefix),
	)
}

func (tags *statTags) getStatsTags(tablePrefix string) stats.Tags {
	statTagsMap := map[string]string{
		"tablePrefix": tablePrefix,
	}
	if tags != nil {
		customValTag := strings.Join(tags.CustomValFilters, "_")
		stateFiltersTag := strings.Join(tags.StateFilters, "_")

		if customValTag != "" {
			statTagsMap["customVal"] = customValTag
		}

		if stateFiltersTag != "" {
			statTagsMap["stateFilters"] = stateFiltersTag
		}

		if tags.WorkspaceID != "" && tags.WorkspaceID != "*" {
			statTagsMap["workspaceId"] = tags.WorkspaceID
		}

		for _, paramTag := range tags.ParameterFilters {
			statTagsMap[paramTag.Name] = paramTag.Value
		}
	}

	return statTagsMap
}
