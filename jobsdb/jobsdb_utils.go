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

/*
Function to return an ordered list of datasets and datasetRanges
Most callers use the in-memory list of dataset and datasetRanges
*/
func getDSList(jd assertInterface, dbHandle *sql.DB, tablePrefix string) []dataSetT {
	datasetList := []dataSetT{}

	//Read the table names from PG
	tableNames := getAllTableNames(jd, dbHandle)

	//Tables are of form jobs_ and job_status_. Iterate
	//through them and sort them to produce and
	//ordered list of datasets

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

	//If any service has crashed while creating DS, this may happen. Handling such case gracefully.
	if len(jobNameMap) != len(jobStatusNameMap) {
		jd.assert(len(jobNameMap) == len(jobStatusNameMap)+1, fmt.Sprintf("Length of jobNameMap(%d) - length of jobStatusNameMap(%d) is more than 1", len(jobNameMap), len(jobStatusNameMap)))
		deletedDNum := removeExtraKey(jobNameMap, jobStatusNameMap)
		//remove deletedDNum from dnumList
		var idx int
		var dnum string
		var foundDeletedDNum bool
		for idx, dnum = range dnumList {
			if dnum == deletedDNum {
				foundDeletedDNum = true
				break
			}
		}
		if foundDeletedDNum {
			dnumList = remove(dnumList, idx)
		}
	}

	//Create the structure
	for _, dnum := range dnumList {
		jobName, ok := jobNameMap[dnum]
		jd.assert(ok, fmt.Sprintf("dnum %s is not found in jobNameMap", dnum))
		jobStatusName, ok := jobStatusNameMap[dnum]
		jd.assert(ok, fmt.Sprintf("dnum %s is not found in jobStatusNameMap", dnum))
		datasetList = append(datasetList,
			dataSetT{JobTable: jobName,
				JobStatusTable: jobStatusName, Index: dnum})
	}

	return datasetList
}

/*sortDnumList Function to sort table suffixes. We should not have any use case
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
			//src has same prefix but is shorter
			//For example, src=1.1 while dest=1.1.1
			if k >= len(dst) {
				return false, fmt.Errorf("k:%d >= len(dst):%d", k, len(dst))
			}
			if k <= 0 {
				return false, fmt.Errorf("k:%d <= 0", k)
			}
			return true, nil
		}
		if k >= len(dst) {
			//Opposite of case above
			if k <= 0 {
				return false, fmt.Errorf("k:%d <= 0", k)
			}
			if k >= len(src) {
				return false, fmt.Errorf("k:%d >= len(src):%d", k, len(src))
			}
			return false, nil
		}
		if src[k] == dst[k] {
			//Loop
			k++
			continue
		}
		//Strictly ordered. Return
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

//getAllTableNames Function to get all table names form Postgres
func getAllTableNames(jd assertInterface, dbHandle *sql.DB) []string {
	//Read the table names from PG
	stmt, err := dbHandle.Prepare(`SELECT tablename
                                        FROM pg_catalog.pg_tables
                                        WHERE schemaname != 'pg_catalog' AND
                                        schemaname != 'information_schema'`)
	jd.assertError(err)
	defer stmt.Close()

	rows, err := stmt.Query()
	jd.assertError(err)
	defer rows.Close()

	tableNames := []string{}
	for rows.Next() {
		var tbName string
		err = rows.Scan(&tbName)
		jd.assertError(err)
		tableNames = append(tableNames, tbName)
	}

	return tableNames
}

//checkValidJobState Function to check validity of states
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

//constructQuery construct and return query
func constructQuery(jd assertInterface, paramKey string, paramList []string, queryType string) string {
	jd.assert(queryType == "OR" || queryType == "AND", fmt.Sprintf("queryType:%s is neither OR nor AND", queryType))
	var queryList []string
	for _, p := range paramList {
		queryList = append(queryList, "("+paramKey+"='"+p+"')")
	}
	return "(" + strings.Join(queryList, " "+queryType+" ") + ")"
}

//constructStateQuery construct query from provided state filters
func constructStateQuery(alias, paramKey string, paramList []string, queryType string) string {
	var queryList []string
	//Building state query for non executing states
	for _, p := range paramList {
		if p != NotProcessed.State {
			queryList = append(queryList, "("+alias+"."+paramKey+"='"+p+"')")
		}
	}
	temp := "((" + strings.Join(queryList, " "+queryType+" ") + ")" + " and " + alias + ".retry_time < $1)"

	//Building state query for executing states
	for _, p := range paramList {
		if p == NotProcessed.State {
			temp = temp + " " + queryType + " " + alias + ".job_id is null"
			break
		}
	}

	return temp
}

//constructParameterJSONQuery construct and return query
func constructParameterJSONQuery(table string, parameterFilters []ParameterFilterT) string {
	// eg. query with optional destination_id (batch_rt_jobs_1.parameters @> '{"source_id":"<source_id>","destination_id":"<destination_id>"}'  OR (batch_rt_jobs_1.parameters @> '{"source_id":"<source_id>"}' AND batch_rt_jobs_1.parameters -> 'destination_id' IS NULL))
	var allKeyValues, mandatoryKeyValues, opNullConditions []string
	for _, parameter := range parameterFilters {
		allKeyValues = append(allKeyValues, fmt.Sprintf(`"%s":"%s"`, parameter.Name, parameter.Value))
		if parameter.Optional {
			opNullConditions = append(opNullConditions, fmt.Sprintf(`"%s".parameters -> '%s' IS NULL`, table, parameter.Name))
		} else {
			mandatoryKeyValues = append(mandatoryKeyValues, fmt.Sprintf(`"%s":"%s"`, parameter.Name, parameter.Value))
		}
	}
	opQuery := ""
	if len(opNullConditions) > 0 {
		opQuery += fmt.Sprintf(` OR ("%s".parameters @> '{%s}' AND %s)`, table, strings.Join(mandatoryKeyValues, ","), strings.Join(opNullConditions, " AND "))
	}
	return fmt.Sprintf(`(%s.parameters @> '{%s}' %s)`, table, strings.Join(allKeyValues, ","), opQuery)
}

//Admin Handlers
type JobsdbUtilsHandler struct {
}

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
	*reply = string(response)
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
