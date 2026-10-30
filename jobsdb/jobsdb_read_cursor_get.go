package jobsdb

import (
	"context"
	"fmt"
	"slices"

	"github.com/samber/lo"
)

func (rc *readCursor) GetJobs(ctx context.Context, states []string, params GetQueryParams) (JobsResult, error) {
	p, firstLiveJobID := rc.startRead(states, &params, true)
	res, err := rc.JobsDB.GetJobs(ctx, states, params)
	if err == nil {
		rc.endRead(p, firstLiveJobID, res)
	}
	return res, err
}

func (rc *readCursor) GetUnprocessed(ctx context.Context, params GetQueryParams) (JobsResult, error) {
	p, firstLiveJobID := rc.startRead([]string{Unprocessed.State}, &params, true)
	res, err := rc.JobsDB.GetUnprocessed(ctx, params)
	if err == nil {
		rc.endRead(p, firstLiveJobID, res)
	}
	return res, err
}

func (rc *readCursor) GetToProcess(ctx context.Context, params GetQueryParams, more MoreToken) (*MoreJobsResult, error) {
	p, firstLiveJobID := rc.startRead([]string{Failed.State, Waiting.State, Unprocessed.State}, &params, more == nil)
	res, err := rc.JobsDB.GetToProcess(ctx, params, more)
	if err == nil && res != nil {
		rc.endRead(p, firstLiveJobID, res.JobsResult)
	}
	return res, err
}

func (rc *readCursor) GetImporting(ctx context.Context, params GetQueryParams, more MoreToken) (*MoreJobsResult, error) {
	p, firstLiveJobID := rc.startRead([]string{Importing.State}, &params, more == nil)
	res, err := rc.JobsDB.GetImporting(ctx, params, more)
	if err == nil && res != nil {
		rc.endRead(p, firstLiveJobID, res.JobsResult)
	}
	return res, err
}

// pipelineKeyOf returns the key of the pipeline that params read. It fails if they filter on something that a
// key does not support.
func (rc *readCursor) pipelineKeyOf(params *GetQueryParams) (key pipelineKey, err error) {
	unsupported := func() (pipelineKey, error) {
		return pipelineKey{}, fmt.Errorf("read filters that no pipeline key supports: custom values %q, workspace %q, parameters %v, partitions %q, consumer %q",
			params.CustomValFilters, params.WorkspaceID, params.ParameterFilters, params.PartitionFilters, params.Consumer)
	}
	if len(params.CustomValFilters) > 1 || len(params.PartitionFilters) > 0 || len(params.ParameterFilters) > 1 || params.WorkspaceID != "" && len(params.ParameterFilters) > 0 {
		return unsupported()
	}
	if len(params.CustomValFilters) == 1 {
		key.CustomVal = params.CustomValFilters[0]
	}
	switch {
	case params.WorkspaceID != "":
		key.FilterType, key.FilterValue = workspaceFilterType, params.WorkspaceID
	case len(params.ParameterFilters) == 0:
		key.FilterType = noneFilterType
	case params.ParameterFilters[0].Name == "source_id":
		key.FilterType, key.FilterValue = sourceFilterType, params.ParameterFilters[0].Value
	case params.ParameterFilters[0].Name == "destination_id":
		key.FilterType, key.FilterValue = destinationFilterType, params.ParameterFilters[0].Value
	default:
		return unsupported()
	}
	if rc.h.conf.multiConsumer {
		key.Consumer = params.Consumer
	}
	return key, nil
}

// startRead skips the datasets below the applied watermark of the read's pipeline. If the read includes
// unprocessed, it returns the pipeline, so that the read can advance its watermark, and the first job id of
// the live dataset. A read of a later page or of a terminal state skips nothing.
func (rc *readCursor) startRead(states []string, params *GetQueryParams, firstPage bool) (p *pipeline, firstLiveJobID int64) {
	if rc.disabled.Load() || !firstPage || !lo.EveryBy(states, func(s string) bool { return s == Unprocessed.State || slices.Contains(validNonTerminalStates, s) }) {
		return nil, 0
	}
	key, err := rc.pipelineKeyOf(params)
	if err != nil {
		rc.disable(err)
		return nil, 0
	}
	p = rc.getPipeline(key)
	params.skipBelowJobID = p.applied.Load()
	if !slices.Contains(states, Unprocessed.State) || params.JobsLimit <= 0 || params.PayloadSizeLimit < 0 { // the latter two query nothing
		return nil, 0
	}
	if _, dsRangeList := rc.h.dsList.snapshot(); len(dsRangeList) > 0 {
		firstLiveJobID = dsRangeList[len(dsRangeList)-1].maxJobID + 1
	}
	return p, firstLiveJobID
}

// endRead advances the watermark of the read's pipeline to its first job, as reads return jobs in job id
// order, but not past firstLiveJobID. A read that returns no jobs from all the datasets it was allowed to query
// advances it to firstLiveJobID. A read that ends after the decorator got disabled may have missed the job of
// the status that disabled it.
func (rc *readCursor) endRead(p *pipeline, firstLiveJobID int64, res JobsResult) {
	if p == nil || rc.disabled.Load() {
		return
	}
	switch {
	case len(res.Jobs) > 0:
		p.advance(min(res.Jobs[0].JobID, firstLiveJobID))
	case !res.DSLimitsReached:
		p.advance(firstLiveJobID)
	}
}
