package jobsdb

import (
	"context"
	"fmt"
)

type MultiTenantLegacy struct {
	*HandleT
}

type legacyMoreToken struct {
	pendingAfterJobID *int64
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
	var workspaceID string
	for wsID, limit := range pickup {
		toQuery += limit
		workspaceID = wsID
	}
	params.JobsLimit = toQuery
	params.WorkspaceFilter = []string{workspaceID}
	pending, err := mj.GetPending(ctx, params)
	if err != nil {
		return nil, err
	}
	if len(pending.Jobs) > 0 {
		unprocessedAfterJobID := pending.Jobs[len(pending.Jobs)-1].JobID
		mtoken.pendingAfterJobID = &unprocessedAfterJobID
	}
	list = append(list, pending.Jobs...)
	return &GetAllJobsResult{Jobs: list, More: mtoken}, nil
}
