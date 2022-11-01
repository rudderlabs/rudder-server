package jobiterator

import (
	"context"
	"fmt"
	"sort"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

type IteratorOptFn func(*Iterator)

// WithMaxQueries sets the maximum number of queries that can be made to the jobsDB
// for fetching more jobs.
func WithMaxQueries(maxQueries int) IteratorOptFn {
	return func(ji *Iterator) {
		ji.maxQueries = maxQueries
	}
}

// WithDiscardedPercentageTolerance sets the discarded percentage tolerance,
// i.e. the maximum percentage of discarded jobs that can be tolerated without further querying jobsDB.
func WithDiscardedPercentageTolerance(discardedPercentageTolerance int) IteratorOptFn {
	return func(ji *Iterator) {
		ji.discardedPercentageTolerance = discardedPercentageTolerance
	}
}

// WithLegacyOrderGroupKeyFn if enabled, sets the order group key function to assume a legacy jobs pickup algorithm.
func WithLegacyOrderGroupKey(legacy bool) IteratorOptFn {
	return func(ji *Iterator) {
		if legacy {
			ji.orderGroupKeyFn = func(job *jobsdb.JobT) string { return job.LastJobStatus.JobState }
		}
	}
}

// Iterator is a job iterator with support for fetching more than the original set of jobs requested,
// in case some of these jobs get discarded, according to the configured discarded percentage tolerance.
type Iterator struct {
	params                       jobsdb.GetQueryParamsT
	maxQueries                   int
	discardedPercentageTolerance int
	orderGroupKeyFn              func(*jobsdb.JobT) string
	getJobsFn                    func(context.Context, map[string]int, jobsdb.GetQueryParamsT, jobsdb.MoreToken) (*jobsdb.GetAllJobsResult, error)
	state                        struct {
		// running iterator state
		jobs        []*jobsdb.JobT
		idx         int
		previousJob map[string]*jobsdb.JobT

		// closed indicates whether the iterator has reached the end or not
		closed bool

		stats IteratorStats

		// for the next query
		discarded         int
		pickupMap         map[string]int
		continuationToken jobsdb.MoreToken
	}
}

// IteratorStats holds statistics about an iterator
type IteratorStats struct {
	// QueryCount is the number of queries made to the jobsDB
	QueryCount int
	// TotalJobs is the total number of jobs queried
	TotalJobs int
	// DiscardedJobs is the number of jobs discarded
	DiscardedJobs int
}

// New returns a new job iterator
//   - pickupMap: a map of workspaceID to number of jobs to be fetched from that workspace
//   - params: jobsDB query parameters
//   - getJobsFn: the function to fetch jobs from jobsDB
//   - opts: optional iterator options
func New(pickupMap map[string]int, params jobsdb.GetQueryParamsT, getJobsFn func(context.Context, map[string]int, jobsdb.GetQueryParamsT, jobsdb.MoreToken) (*jobsdb.GetAllJobsResult, error), opts ...IteratorOptFn) *Iterator {
	ji := &Iterator{
		params:                       params,
		maxQueries:                   100,
		discardedPercentageTolerance: 0,
		getJobsFn:                    getJobsFn,
		orderGroupKeyFn:              func(job *jobsdb.JobT) string { return job.WorkspaceId },
	}
	ji.state.pickupMap = pickupMap
	ji.state.previousJob = map[string]*jobsdb.JobT{}
	for _, opt := range opts {
		opt(ji)
	}
	return ji
}

// HasNext returns true when there are more jobs to be fetched by Next(), false otherwise.
func (ji *Iterator) HasNext() bool {
	jobsLength := len(ji.state.jobs)
	if jobsLength > ji.state.idx { // we have more jobs in the current batch
		return true
	}
	if ji.state.closed {
		return false
	}
	if ji.state.stats.QueryCount >= ji.maxQueries { // iterator has reached the end
		ji.state.closed = true
		return false
	}
	if len(ji.state.pickupMap) == 0 { // nothing left to fetch
		ji.state.closed = true
		return false
	}
	if jobsLength > 0 {
		discardedPercentage := (ji.state.discarded * 100) / jobsLength
		// don't continue if discarded jobs are within tolerance limits
		if discardedPercentage <= ji.discardedPercentageTolerance {
			ji.state.closed = true
			return false
		}
	}

	// try to fetch some more jobs
	var err error
	var jobsLimit int
	for _, limit := range ji.state.pickupMap {
		if limit > 0 {
			jobsLimit += limit
		}
	}
	ji.params.JobsLimit = jobsLimit

	ji.state.stats.QueryCount++
	allJobsResult, err := ji.getJobsFn(context.Background(), ji.state.pickupMap, ji.params, ji.state.continuationToken)
	if err != nil {
		panic(err)
	}
	ji.state.jobs = allJobsResult.Jobs
	ji.state.continuationToken = allJobsResult.More
	ji.state.stats.TotalJobs += len(ji.state.jobs)

	// reset state
	ji.state.idx = 0
	ji.state.discarded = 0
	ji.state.pickupMap = map[string]int{}

	if len(ji.state.jobs) == 0 { // no more jobs, iterator has reached the end
		ji.state.closed = true
		return false
	}

	sort.Slice(ji.state.jobs, func(i, j int) bool {
		return ji.state.jobs[i].JobID < ji.state.jobs[j].JobID
	})

	return true
}

// Next returns the next job to be processed.
// Never call Next() without checking HasNext() first.
func (ji *Iterator) Next() *jobsdb.JobT {
	idx := ji.state.idx
	ji.state.idx++
	nextJob := ji.state.jobs[idx]
	orderGroupKey := ji.orderGroupKeyFn(nextJob)
	if previousJob, ok := ji.state.previousJob[orderGroupKey]; ok && previousJob.JobID > nextJob.JobID {
		panic(fmt.Errorf("job iterator encountered out of order jobs for group key %s: previousJobID: %d, nextJobID: %d", orderGroupKey, previousJob.JobID, nextJob.JobID))
	}
	ji.state.previousJob[orderGroupKey] = nextJob
	return nextJob
}

// Discard is called when a job is not processed.
// By discarding a job we are allowing the iterator to fetch more jobs from jobsDB.
func (ji *Iterator) Discard(job *jobsdb.JobT) {
	ji.state.stats.DiscardedJobs++
	ji.state.discarded++
	ji.state.pickupMap[job.WorkspaceId]++
}

func (ji *Iterator) Stats() IteratorStats {
	return ji.state.stats
}
