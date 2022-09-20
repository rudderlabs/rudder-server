package jobsdb

import "context"

type MultiTenantLegacy struct {
	*HandleT
}

func (mj *MultiTenantLegacy) GetAllJobs(ctx context.Context, workspaceCount map[string]int, params GetQueryParamsT, _ int) ([]*JobT, error) { // skipcq: CRT-P0003
	var list []*JobT
	toQuery := 0
	for workspace := range workspaceCount {
		toQuery += workspaceCount[workspace]
	}
	params.JobsLimit = toQuery

	toRetry, err := mj.GetToRetry(ctx, params)
	if err != nil {
		return nil, err
	}
	list = append(list, toRetry.Jobs...)
	if toRetry.LimitsReached {
		return list, nil
	}
	updateParams(&params, toRetry)

	waiting, err := mj.GetWaiting(ctx, params)
	if err != nil {
		return nil, err
	}
	list = append(list, waiting.Jobs...)
	if waiting.LimitsReached {
		return list, nil
	}
	updateParams(&params, waiting)

	unprocessed, err := mj.GetUnprocessed(ctx, params)
	if err != nil {
		return nil, err
	}
	list = append(list, unprocessed.Jobs...)
	return list, nil
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
