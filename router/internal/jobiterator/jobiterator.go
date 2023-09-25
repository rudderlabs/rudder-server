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

// Iterator is a job iterator with support for fetching more than the original set of jobs requested,
// in case some of these jobs get discarded, according to the configured discarded percentage tolerance.
type Iterator struct {
	params                       jobsdb.GetQueryParams
	maxQueries                   int
	discardedPercentageTolerance int
	getJobsFn                    func(context.Context, jobsdb.GetQueryParams, jobsdb.MoreToken) (*jobsdb.MoreJobsResult, error)
	state                        struct {
		// running iterator state
		jobs        []*jobsdb.JobT
		idx         int
		previousJob *jobsdb.JobT

		// closed indicates whether the iterator has reached the end or not
		closed bool

		stats IteratorStats

		// for the next query
		discarded         int
		jobsLimit         int
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
	// LimitsReached indicates whether the iterator reached the limits of the jobsDB while querying
	LimitsReached bool
}

// New returns a new job iterator
//   - params: jobsDB query parameters
//   - getJobsFn: the function to fetch jobs from jobsDB
//   - opts: optional iterator options
func New(params jobsdb.GetQueryParams, getJobsFn func(context.Context, jobsdb.GetQueryParams, jobsdb.MoreToken) (*jobsdb.MoreJobsResult, error), opts ...IteratorOptFn) *Iterator {
	ji := &Iterator{
		params:                       params,
		maxQueries:                   100,
		discardedPercentageTolerance: 0,
		getJobsFn:                    getJobsFn,
	}
	ji.state.jobsLimit = params.JobsLimit
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
	if ji.state.jobsLimit == 0 { // nothing left to fetch
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
	ji.params.JobsLimit = ji.state.jobsLimit

	ji.state.stats.QueryCount++
	allJobsResult, err := ji.getJobsFn(context.Background(), ji.params, ji.state.continuationToken)
	if err != nil {
		panic(err)
	}
	ji.state.jobs = allJobsResult.Jobs
	ji.state.continuationToken = allJobsResult.More
	jobCount := len(ji.state.jobs)
	ji.state.jobsLimit -= jobCount
	ji.state.stats.TotalJobs += jobCount
	if !ji.state.stats.LimitsReached {
		ji.state.stats.LimitsReached = allJobsResult.LimitsReached
	}

	// reset state
	ji.state.idx = 0
	ji.state.discarded = 0
	ji.state.jobsLimit = 0

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
	if ji.state.previousJob != nil && ji.state.previousJob.JobID > nextJob.JobID {
		panic(fmt.Errorf("job iterator encountered out of order jobs: previousJobID: %d, nextJobID: %d", ji.state.previousJob.JobID, nextJob.JobID))
	}
	ji.state.previousJob = nextJob
	return nextJob
}

// Discard is called when a job is not processed.
// By discarding a job we are allowing the iterator to fetch more jobs from jobsDB.
func (ji *Iterator) Discard(job *jobsdb.JobT) {
	ji.state.stats.DiscardedJobs++
	ji.state.discarded++
	ji.state.jobsLimit++
}

func (ji *Iterator) Stats() IteratorStats {
	return ji.state.stats
}
