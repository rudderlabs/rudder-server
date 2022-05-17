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
	eventsLimitEnabled := params.EventsLimit > 0
	payloadLimitEnabled := params.PayloadSizeLimit > 0

	retryList := mj.GetToRetry(params)
	list = append(list, retryList...)
	updateParams(eventsLimitEnabled, payloadLimitEnabled, &params, retryList)
	if limitsReached(eventsLimitEnabled, payloadLimitEnabled, params) {
		return list
	}

	waitList := mj.GetWaiting(params)
	list = append(list, waitList...)
	updateParams(eventsLimitEnabled, payloadLimitEnabled, &params, waitList)
	if limitsReached(eventsLimitEnabled, payloadLimitEnabled, params) {
		return list
	}

	unprocessedList := mj.GetUnprocessed(params)
	list = append(list, unprocessedList...)
	return list
}

func updateParams(eventsLimitEnabled, payloadLimitEnabled bool, params *GetQueryParamsT, jobs []*JobT) {
	params.JobsLimit -= len(jobs)
	if eventsLimitEnabled {
		params.EventsLimit -= getTotalEvents(jobs)
	}
	if payloadLimitEnabled {
		params.PayloadSizeLimit -= getTotalPayload(jobs)
	}
}

func limitsReached(eventsLimitEnabled, payloadLimitEnabled bool, params GetQueryParamsT) bool {
	return params.JobsLimit <= 0 ||
		(eventsLimitEnabled && params.EventsLimit <= 0) ||
		(payloadLimitEnabled && params.PayloadSizeLimit <= 0)
}

func getTotalEvents(jobs []*JobT) int {
	var total int
	for i := range jobs {
		total += jobs[i].EventCount
	}
	return total
}

func getTotalPayload(jobs []*JobT) int64 {
	var total int64
	for i := range jobs {
		total += jobs[i].PayloadSize
	}
	return total
}
