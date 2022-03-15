package jobsdb

type MultiTenantLegacy struct {
	*HandleT
}

func (mj *MultiTenantLegacy) GetAllJobs(workspaceCount map[string]int, params GetQueryParamsT, _ int) []*JobT {
	toQuery := 0
	for workspace := range workspaceCount {
		toQuery += workspaceCount[workspace]
	}
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
