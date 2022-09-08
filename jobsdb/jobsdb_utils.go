package jobsdb

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/rudderlabs/rudder-server/admin"
	"github.com/rudderlabs/rudder-server/services/stats"
)

type sqlDbOrTx interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

/*
Function to return an ordered list of datasets and datasetRanges
Most callers use the in-memory list of dataset and datasetRanges
*/
func getDSList(jd assertInterface, dbHandle sqlDbOrTx, tablePrefix string) []dataSetT {
	datasetList := []dataSetT{}

	// Read the table names from PG
	tableNames := mustGetAllTableNames(jd, dbHandle)

	// Tables are of form jobs_ and job_status_. Iterate
	// through them and sort them to produce and
	// ordered list of datasets

	jobNameMap := map[string]string{}
	jobStatusNameMap := map[string]string{}
	dnumList := []string{}

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

	sortDnumList(jd, dnumList)

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

	return datasetList
}

/*
sortDnumList Function to sort table suffixes. We should not have any use case
for having > 2 len suffixes (e.g. 1_1_1 - see comment below)
but this sort handles the general case
*/
func sortDnumList(jd assertInterface, dnumList []string) {
	sort.Slice(dnumList, func(i, j int) bool {
		src := strings.Split(dnumList[i], "_")
		dst := strings.Split(dnumList[j], "_")
		comparison, err := dsComparitor(src, dst)
		jd.assertError(err)
		return comparison
	})
}

// returns true, nil if src is less than dst
// returns false, nil if src is greater than dst
// returns false, someError if src is equal to dst
var dsComparitor = func(src, dst []string) (bool, error) {
	k := 0
	for {
		if k >= len(src) {
			// src has same prefix but is shorter
			// For example, src=1.1 while dest=1.1.1
			if k >= len(dst) {
				return false, fmt.Errorf("k:%d >= len(dst):%d", k, len(dst))
			}
			if k <= 0 {
				return false, fmt.Errorf("k:%d <= 0", k)
			}
			return true, nil
		}
		if k >= len(dst) {
			// Opposite of case above
			if k <= 0 {
				return false, fmt.Errorf("k:%d <= 0", k)
			}
			if k >= len(src) {
				return false, fmt.Errorf("k:%d >= len(src):%d", k, len(src))
			}
			return false, nil
		}
		if src[k] == dst[k] {
			// Loop
			k++
			continue
		}
		// Strictly ordered. Return
		srcInt, err := strconv.Atoi(src[k])
		if err != nil {
			return false, fmt.Errorf("string to int conversion failed for source %v. with error %w", src, err)
		}
		dstInt, err := strconv.Atoi(dst[k])
		if err != nil {
			return false, fmt.Errorf("string to int conversion failed for destination %v. with error %w", dst, err)
		}
		return srcInt < dstInt, nil
	}
}

// mustGetAllTableNames gets all table names from Postgres and panics in case of an error
func mustGetAllTableNames(jd assertInterface, dbHandle sqlDbOrTx) []string {
	tableNames, err := getAllTableNames(dbHandle)
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
	defer rows.Close()
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

// constructStateQuery construct query from provided state filters
func constructStateQuery(alias, paramKey string, paramList []string, queryType string) string {
	var queryList []string
	// Building state query for non executing states
	for _, p := range paramList {
		if p != NotProcessed.State {
			queryList = append(queryList, "("+alias+"."+paramKey+"='"+p+"')")
		}
	}
	temp := "((" + strings.Join(queryList, " "+queryType+" ") + ")" + " and " + alias + ".retry_time < $1)"

	// Building state query for executing states
	for _, p := range paramList {
		if p == NotProcessed.State {
			temp = temp + " " + queryType + " " + alias + ".job_id is null"
			break
		}
	}

	return temp
}

// constructParameterJSONQuery construct and return query
func constructParameterJSONQuery(table string, parameterFilters []ParameterFilterT) string {
	// eg. query with optional destination_id (batch_rt_jobs_1.parameters @> '{"source_id":"<source_id>","destination_id":"<destination_id>"}'  OR (batch_rt_jobs_1.parameters @> '{"source_id":"<source_id>"}' AND batch_rt_jobs_1.parameters -> 'destination_id' IS NULL))
	var allKeyValues, mandatoryKeyValues, opNullConditions []string
	for _, parameter := range parameterFilters {
		allKeyValues = append(allKeyValues, fmt.Sprintf(`%q:%q`, parameter.Name, parameter.Value))
		if parameter.Optional {
			opNullConditions = append(opNullConditions, fmt.Sprintf(`%q.parameters -> '%s' IS NULL`, table, parameter.Name))
		} else {
			mandatoryKeyValues = append(mandatoryKeyValues, fmt.Sprintf(`%q:%q`, parameter.Name, parameter.Value))
		}
	}
	opQuery := ""
	if len(opNullConditions) > 0 {
		opQuery += fmt.Sprintf(` OR (%q.parameters @> '{%s}' AND %s)`, table, strings.Join(mandatoryKeyValues, ","), strings.Join(opNullConditions, " AND "))
	}
	return fmt.Sprintf(`(%s.parameters @> '{%s}' %s)`, table, strings.Join(allKeyValues, ","), opQuery)
}

// Admin Handlers
type JobsdbUtilsHandler struct{}

var jobsdbUtilsHandler *JobsdbUtilsHandler

func Init3() {
	jobsdbUtilsHandler = &JobsdbUtilsHandler{}
	admin.RegisterAdminHandler("JobsdbUtilsHandler", jobsdbUtilsHandler)
}

func (*JobsdbUtilsHandler) RunSQLQuery(argString string, reply *string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			pkgLogger.Error(r)
			err = fmt.Errorf("internal Rudder server error: %v", r)
		}
	}()

	args := strings.Split(argString, ":")
	var response string
	var readOnlyJobsDB ReadonlyHandleT
	if args[0] == "brt" {
		args[0] = "batch_rt"
	}

	readOnlyJobsDB.Setup(args[0])

	switch args[1] {
	case "Jobs between JobID's of a User":
		response, err = readOnlyJobsDB.GetJobIDsForUser(args)
	case "Error Code Count By Destination":
		response, err = readOnlyJobsDB.GetFailedStatusErrorCodeCountsByDestination(args)
	}
	*reply = response
	return err
}

func (jd *HandleT) getTimerStat(stat string, tags *statTags) stats.RudderStats {
	timingTags := map[string]string{
		"tablePrefix": jd.tablePrefix,
	}
	if tags != nil {
		customValTag := strings.Join(tags.CustomValFilters, "_")
		stateFiltersTag := strings.Join(tags.StateFilters, "_")

		if customValTag != "" {
			timingTags["customVal"] = customValTag
		}

		if stateFiltersTag != "" {
			timingTags["stateFilters"] = stateFiltersTag
		}

		for _, paramTag := range tags.ParameterFilters {
			timingTags[paramTag.Name] = paramTag.Value
		}
	}

	return stats.NewTaggedStat(stat, stats.TimerType, timingTags)
}
