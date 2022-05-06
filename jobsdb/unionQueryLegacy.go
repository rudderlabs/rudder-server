package jobsdb

type MultiTenantLegacy struct {
	*HandleT
}

func (mj *MultiTenantLegacy) GetAllJobs(workspaceCount map[string]int, params GetQueryParamsT, _ int) []*JobT {
	toQuery := 0
	for workspace := range workspaceCount {
		toQuery += workspaceCount[workspace]
	}
	params.JobsLimit = toQuery

	retryList := mj.GetToRetry(params)
	params.JobsLimit -= len(retryList)
	waitList := mj.GetWaiting(params)
	params.JobsLimit -= len(waitList)
	unprocessedList := mj.GetUnprocessed(params)

	var list []*JobT
	list = append(list, retryList...)
	list = append(list, waitList...)
	list = append(list, unprocessedList...)

	return list
}
