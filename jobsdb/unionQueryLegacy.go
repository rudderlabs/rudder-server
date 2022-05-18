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

	toRetry := mj.GetToRetry(params)
	list = append(list, toRetry.Jobs...)
	if toRetry.LimitsReached {
		return list
	}
	updateParams(&params, toRetry)

	waiting := mj.GetWaiting(params)
	list = append(list, waiting.Jobs...)
	if waiting.LimitsReached {
		return list
	}
	updateParams(&params, waiting)

	unprocessed := mj.GetUnprocessed(params)
	list = append(list, unprocessed.Jobs...)
	return list
}

func updateParams(params *GetQueryParamsT, jobs JobsResult) {
	params.JobsLimit -= len(jobs.Jobs)
	if params.EventsLimit > 0 {
		params.EventsLimit -= jobs.EventsCount
	}
	if params.PayloadSizeLimit > 0 {
		params.PayloadSizeLimit -= jobs.PayloadSize
	}
}
