package jobsdb

type MultiTenantLegacy struct {
	*HandleT
}

func (mj *MultiTenantLegacy) GetAllJobs(workspaceCount map[string]int, params GetQueryParamsT, _ int) []*JobT {
	var list []*JobT
	toQuery := 0
	for workspace := range workspaceCount {
		toQuery += workspaceCount[workspace]
	}
	params.JobsLimit = toQuery
	payloadLimitEnabled := params.PayloadSizeLimit > 0

	retryList := mj.GetToRetry(params)
	list = append(list, retryList...)
	updateParams(&params, retryList)
	if limitsReached(payloadLimitEnabled, params) {
		return list
	}

	waitList := mj.GetWaiting(params)
	list = append(list, waitList...)
	updateParams(&params, waitList)
	if limitsReached(payloadLimitEnabled, params) {
		return list
	}

	unprocessedList := mj.GetUnprocessed(params)
	list = append(list, unprocessedList...)
	return list
}

func updateParams(params *GetQueryParamsT, jobs []*JobT) {
	params.JobsLimit -= len(jobs)
	params.PayloadSizeLimit -= getTotalPayload(jobs)
}

func getTotalPayload(jobs []*JobT) int64 {
	var total int64
	for i := range jobs {
		total += jobs[i].PayloadSize
	}
	return total
}

func limitsReached(payloadLimitEnabled bool, params GetQueryParamsT) bool {
	return params.JobsLimit <= 0 || (payloadLimitEnabled && params.PayloadSizeLimit <= 0)
}
