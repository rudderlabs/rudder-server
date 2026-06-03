package jobsdb

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/dsindex"
)

type sqlDbOrTx interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

const preDropTableComment = "rudder:pre_drop:v1"

/*
Function to return an ordered list of datasets and datasetRanges
Most callers use the in-memory list of dataset and datasetRanges
*/
func getDSList(jd asserter, dbHandle sqlDbOrTx, tablePrefix string) ([]dataSetT, error) {
	var datasetList []dataSetT

	// Read the table names from PG
	tableNames, err := getAllTableNames(dbHandle)
	if err != nil {
		return nil, fmt.Errorf("getAllTableNames: %w", err)
	}
	// Tables are of form jobs_ and job_status_. Iterate
	// through them and sort them to produce and
	// ordered list of datasets

	jobNameMap := map[string]string{}
	jobStatusNameMap := map[string]string{}
	var dnumList []string

	for _, t := range tableNames {
		if strings.HasPrefix(t, tablePrefix+"_jobs_") {
			dnum := t[len(tablePrefix+"_jobs_"):]
			jobNameMap[dnum] = t
			dnumList = append(dnumList, dnum)
			continue
		}
		if strings.HasPrefix(t, tablePrefix+"_job_status_") {
			dnum := t[len(tablePrefix+"_job_status_"):]
			jobStatusNameMap[dnum] = t
			continue
		}
	}

	sortDnumList(dnumList)

	// Create the structure
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

	return datasetList, nil
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

// getAllTableNames gets all table names from Postgres, excluding tables marked as pre-drop.
func getAllTableNames(dbHandle sqlDbOrTx) ([]string, error) {
	type tableInfo struct {
		name        string
		description sql.NullString
	}
	var tables []tableInfo
	rows, err := dbHandle.Query(`SELECT c.relname, d.description
									FROM pg_catalog.pg_class c
									JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
									LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid AND d.objsubid = 0
									WHERE n.nspname != 'pg_catalog'
										AND n.nspname != 'information_schema'
										AND c.relkind = 'r'`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var table tableInfo
		if err = rows.Scan(&table.name, &table.description); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	preDropTables := make(map[string]struct{})
	for _, table := range tables {
		if !table.description.Valid || !strings.HasPrefix(table.description.String, "rudder:pre_drop:") {
			continue
		}
		jobTable, statusTable, ok := preDropDatasetTables(table.name)
		if !ok {
			preDropTables[table.name] = struct{}{}
			continue
		}
		preDropTables[jobTable] = struct{}{}
		preDropTables[statusTable] = struct{}{}
	}

	tableNames := make([]string, 0, len(tables))
	for _, table := range tables {
		if _, ok := preDropTables[table.name]; ok {
			continue
		}
		tableNames = append(tableNames, table.name)
	}
	return tableNames, nil
}

func preDropDatasetTables(tableName string) (jobTable, statusTable string, ok bool) {
	if prefix, index, found := strings.Cut(tableName, "_job_status_"); found && prefix != "" && index != "" {
		return prefix + "_jobs_" + index, tableName, true
	}
	if prefix, index, found := strings.Cut(tableName, "_jobs_"); found && prefix != "" && index != "" {
		return tableName, prefix + "_job_status_" + index, true
	}
	return "", "", false
}

// checkValidJobState Function to check validity of states
func checkValidJobState(jd asserter, stateFilters []string) {
	jobStateMap := lo.SliceToMap(jobStates, func(js jobStateT) (string, struct{}) { return js.State, struct{}{} })
	for _, st := range stateFilters {
		if _, ok := jobStateMap[st]; !ok {
			jd.assert(false, fmt.Sprintf("state %s is not found in jobStates: %v", st, jobStates))
		}
	}
}

// constructQueryOR construct a query were paramKey is any of the values in paramValues
func constructQueryOR(paramKey string, paramList []string, additionalPredicates ...string) string {
	var queryList []string
	for _, p := range paramList {
		queryList = append(queryList, "("+paramKey+"='"+p+"')")
	}
	queryList = append(queryList, additionalPredicates...)
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
	MoreToken        bool
}

func (jd *Handle) getTimerStat(stat string, tags *statTags) stats.Measurement {
	return jd.stats.NewTaggedStat(
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
		if tags.MoreToken {
			statTagsMap["moreToken"] = "true"
		}
	}

	return statTagsMap
}
