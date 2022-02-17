package jobsdb

type MultiTenantLegacy struct {
	*HandleT
}

func (mj *MultiTenantLegacy) GetAllJobs(workspaceCount map[string]int, params GetQueryParamsT, maxDSQuerySize int) []*JobT {
	toQuery := workspaceCount["0"]
	retryList := mj.GetToRetry(GetQueryParamsT{CustomValFilters: params.CustomValFilters, JobCount: toQuery})
	toQuery -= len(retryList)
	waitList := mj.GetWaiting(GetQueryParamsT{CustomValFilters: params.CustomValFilters, JobCount: toQuery})
	toQuery -= len(waitList)
	unprocessedList := mj.GetUnprocessed(GetQueryParamsT{CustomValFilters: params.CustomValFilters, JobCount: toQuery})

	var list []*JobT
	list = append(list, retryList...)
	list = append(list, waitList...)
	list = append(list, unprocessedList...)

	return list
}

func (mj *MultiTenantLegacy) GetPileUpCounts(statMap map[string]map[string]int) {
}

func (mj *MultiTenantLegacy) GetUnprocessedUnion(workspaceCount map[string]int, params GetQueryParamsT, maxDSQuerySize int) []*JobT {
	return []*JobT{}
}

func (mj *MultiTenantLegacy) GetProcessedUnion(workspaceCount map[string]int, params GetQueryParamsT, maxDSQuerySize int) []*JobT {
	return []*JobT{}
}
