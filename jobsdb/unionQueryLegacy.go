package jobsdb

import (
	"context"
	"fmt"
)

type MultiTenantLegacy struct {
	*HandleT
}

type legacyMoreToken struct {
	retryAfterJobID       *int64
	waitingAfterJobID     *int64
	unprocessedAfterJobID *int64
}

func (mj *MultiTenantLegacy) GetAllJobs(ctx context.Context, pickup map[string]int, params GetQueryParamsT, _ int, more MoreToken) (*GetAllJobsResult, error) { // skipcq: CRT-P0003
	mtoken := &legacyMoreToken{}
	if more != nil {
		var ok bool
		if mtoken, ok = more.(*legacyMoreToken); !ok {
			return nil, fmt.Errorf("invalid token: %+v", more)
		}
	}

	var list []*JobT
	toQuery := 0
	for _, limit := range pickup {
		toQuery += limit
	}
	params.JobsLimit = toQuery
	params.AfterJobID = mtoken.retryAfterJobID
	toRetry, err := mj.GetToRetry(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(toRetry.Jobs) > 0 {
		retryAfterJobID := toRetry.Jobs[len(toRetry.Jobs)-1].JobID
		mtoken.retryAfterJobID = &retryAfterJobID
	}

	list = append(list, toRetry.Jobs...)
	if toRetry.LimitsReached {
		return &GetAllJobsResult{Jobs: list, More: mtoken}, nil
	}
	updateParams(&params, toRetry, mtoken.waitingAfterJobID)

	waiting, err := mj.GetWaiting(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(waiting.Jobs) > 0 {
		waitingAfterJobID := waiting.Jobs[len(waiting.Jobs)-1].JobID
		mtoken.waitingAfterJobID = &waitingAfterJobID
	}
	list = append(list, waiting.Jobs...)
	if waiting.LimitsReached {
		return &GetAllJobsResult{Jobs: list, More: mtoken}, nil
	}
	updateParams(&params, waiting, mtoken.unprocessedAfterJobID)

	unprocessed, err := mj.GetUnprocessed(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(unprocessed.Jobs) > 0 {
		unprocessedAfterJobID := unprocessed.Jobs[len(unprocessed.Jobs)-1].JobID
		mtoken.unprocessedAfterJobID = &unprocessedAfterJobID
	}
	list = append(list, unprocessed.Jobs...)
	return &GetAllJobsResult{Jobs: list, More: mtoken}, nil
}

func updateParams(params *GetQueryParamsT, jobs JobsResult, nextAfterJobID *int64) {
	params.JobsLimit -= len(jobs.Jobs)
	if params.EventsLimit > 0 {
		params.EventsLimit -= jobs.EventsCount
	}
	if params.PayloadSizeLimit > 0 {
		params.PayloadSizeLimit -= jobs.PayloadSize
	}
	params.AfterJobID = nextAfterJobID
}
