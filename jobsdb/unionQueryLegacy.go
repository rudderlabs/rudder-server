package jobsdb

type MultiTenantLegacy struct {
	*HandleT
}

func (mj *MultiTenantLegacy) GetAllJobs(_ map[string]int, params GetQueryParamsT, maxDSQuerySize int) []*JobT {
	toQuery := maxDSQuerySize
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
