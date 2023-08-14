package jobiterator

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

func TestJobIterator(t *testing.T) {
	t.Run("iterate without any jobs", func(t *testing.T) {
		m := &mockGetJobs{t: t}
		it := New(jobsdb.GetQueryParams{JobsLimit: 5}, m.GetJobs)
		// first batch will return no jobs
		m.jobs = []*jobsdb.JobT{}
		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 5}
		require.False(t, it.HasNext(), "it shouldn't have next")
		require.False(t, it.HasNext(), "it shouldn't have next for the second time")
		require.Equal(t, 1, m.count, "expected 1 call of get jobs to be performed")
		require.Equal(t, 1, it.Stats().QueryCount)
		require.Equal(t, 0, it.Stats().DiscardedJobs)
		require.Equal(t, 0, it.Stats().TotalJobs)
	})

	t.Run("iterate without discarding any job", func(t *testing.T) {
		m := &mockGetJobs{t: t}
		it := New(jobsdb.GetQueryParams{JobsLimit: 5}, m.GetJobs)
		// first batch will return 2 jobs from workspace A and 2 jobs from workspace B
		m.jobs = []*jobsdb.JobT{
			{JobID: 2, WorkspaceId: "A"},
			{JobID: 3, WorkspaceId: "B"},
			{JobID: 1, WorkspaceId: "A"},
			{JobID: 4, WorkspaceId: "B"},
		}
		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 5}
		require.True(t, it.HasNext(), "it should have next")

		var count int
		var previousJob *jobsdb.JobT
		for it.HasNext() {
			job := it.Next()
			count++
			if previousJob != nil {
				require.Greater(t, job.JobID, previousJob.JobID, "jobs should be iterated in order by JobID")
			}
			previousJob = job
		}
		require.Equal(t, 4, count, "expected 4 jobs to be returned")
		require.Equal(t, 1, m.count, "expected 1 call of get jobs to be performed")
		require.Equal(t, 1, it.Stats().QueryCount)
		require.Equal(t, 0, it.Stats().DiscardedJobs)
		require.Equal(t, 4, it.Stats().TotalJobs)
	})

	t.Run("iterate, discard one job and fetch one more", func(t *testing.T) {
		m := &mockGetJobs{t: t}
		it := New(jobsdb.GetQueryParams{JobsLimit: 5}, m.GetJobs)
		// first batch will return 2 jobs from workspace A and 2 jobs from workspace B
		m.jobs = []*jobsdb.JobT{
			{JobID: 2, WorkspaceId: "A"},
			{JobID: 3, WorkspaceId: "B"},
			{JobID: 1, WorkspaceId: "A"},
			{JobID: 4, WorkspaceId: "B"},
		}

		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 5}

		require.True(t, it.HasNext())

		// 2nd batch should return 1 job from workspace A
		m.jobs = []*jobsdb.JobT{
			{JobID: 5, WorkspaceId: "A"},
		}

		discardedJobID := int64(2)
		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 1}

		var count int
		var previousJob *jobsdb.JobT
		for it.HasNext() {
			job := it.Next()
			count++
			if previousJob != nil {
				require.Greater(t, job.JobID, previousJob.JobID, "jobs should be iterated in order by JobID")
			}
			previousJob = job
			if job.JobID == discardedJobID {
				it.Discard(job)
			}
		}
		require.Equal(t, 5, count, "expected 5 jobs to be returned")
		require.Equal(t, 2, m.count, "expected 2 calls of get jobs to be performed")
		require.Equal(t, 2, it.Stats().QueryCount)
		require.Equal(t, 1, it.Stats().DiscardedJobs)
		require.Equal(t, 5, it.Stats().TotalJobs)
	})

	t.Run("iterate, discard one job but nothing more to fetch", func(t *testing.T) {
		m := &mockGetJobs{t: t}
		it := New(jobsdb.GetQueryParams{JobsLimit: 5}, m.GetJobs)
		// first batch will return 2 jobs from workspace A and 2 jobs from workspace B
		m.jobs = []*jobsdb.JobT{
			{JobID: 2, WorkspaceId: "A"},
			{JobID: 3, WorkspaceId: "B"},
			{JobID: 1, WorkspaceId: "A"},
			{JobID: 4, WorkspaceId: "B"},
		}

		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 5}
		require.True(t, it.HasNext())

		// 2nd batch should return no jobs
		m.jobs = []*jobsdb.JobT{}

		discardedJobID := int64(2)
		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 1}

		var count int
		var previousJob *jobsdb.JobT
		for it.HasNext() {
			job := it.Next()
			count++
			if previousJob != nil {
				require.Greater(t, job.JobID, previousJob.JobID, "jobs should be iterated in order by JobID")
			}
			previousJob = job
			if job.JobID == discardedJobID {
				it.Discard(job)
			}
		}
		require.Equal(t, 4, count, "expected 4 jobs to be returned")
		require.Equal(t, 2, m.count, "expected 2 calls of get jobs to be performed")
	})

	t.Run("iterate, discard one job from each workspace and fetch one more from each", func(t *testing.T) {
		m := &mockGetJobs{t: t}
		it := New(jobsdb.GetQueryParams{JobsLimit: 5}, m.GetJobs)
		// first batch will return 2 jobs from workspace A and 2 jobs from workspace B
		m.jobs = []*jobsdb.JobT{
			{JobID: 2, WorkspaceId: "A"},
			{JobID: 3, WorkspaceId: "B"},
			{JobID: 1, WorkspaceId: "A"},
			{JobID: 4, WorkspaceId: "B"},
		}

		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 5}
		require.True(t, it.HasNext())

		// 2nd batch should return 1 job from workspace A and 1 job from workspace B
		m.jobs = []*jobsdb.JobT{
			{JobID: 6, WorkspaceId: "B"},
			{JobID: 5, WorkspaceId: "A"},
		}

		discardedJobIDA := int64(2)
		discardedJobIDB := int64(4)
		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 2}

		var count int
		var previousJob *jobsdb.JobT
		for it.HasNext() {
			job := it.Next()
			count++
			if previousJob != nil {
				require.Greater(t, job.JobID, previousJob.JobID, "jobs should be iterated in order by JobID")
			}
			previousJob = job
			if job.JobID == discardedJobIDA || job.JobID == discardedJobIDB {
				it.Discard(job)
			}
		}
		require.Equal(t, 6, count, "expected 6 jobs to be returned")
		require.Equal(t, 2, m.count, "expected 2 calls of get jobs to be performed")
	})

	t.Run("iterate, discard one job but don't fetch more due to maxQueries", func(t *testing.T) {
		m := &mockGetJobs{t: t}
		it := New(jobsdb.GetQueryParams{JobsLimit: 5}, m.GetJobs, WithMaxQueries(1))
		// first batch will return 2 jobs from workspace A
		m.jobs = []*jobsdb.JobT{
			{JobID: 1, WorkspaceId: "A"},
			{JobID: 2, WorkspaceId: "A"},
		}

		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 5}

		require.True(t, it.HasNext())

		// 2nd batch should return 1 job from workspace A
		m.jobs = []*jobsdb.JobT{
			{JobID: 3, WorkspaceId: "A"},
		}

		discardedJobID := int64(2)
		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 1}

		var count int
		var previousJob *jobsdb.JobT
		for it.HasNext() {
			job := it.Next()
			count++
			if previousJob != nil {
				require.Greater(t, job.JobID, previousJob.JobID, "jobs should be iterated in order by JobID")
			}
			previousJob = job
			if job.JobID == discardedJobID {
				it.Discard(job)
			}
		}
		require.Equal(t, 2, count, "expected 2 jobs to be returned")
		require.Equal(t, 1, m.count, "expected 1 calls of get jobs to be performed")
	})

	t.Run("iterate, discard one job but don't fetch more due to discardedPercentageTolerance", func(t *testing.T) {
		m := &mockGetJobs{t: t}
		it := New(jobsdb.GetQueryParams{JobsLimit: 5}, m.GetJobs, WithDiscardedPercentageTolerance(50))
		// first batch will return 2 jobs from workspace A
		m.jobs = []*jobsdb.JobT{
			{JobID: 1, WorkspaceId: "A"},
			{JobID: 2, WorkspaceId: "A"},
		}

		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 5}

		require.True(t, it.HasNext())

		// 2nd batch should return 1 job from workspace A
		m.jobs = []*jobsdb.JobT{
			{JobID: 3, WorkspaceId: "A"},
		}

		discardedJobID := int64(2)
		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 1}

		var count int
		var previousJob *jobsdb.JobT
		for it.HasNext() {
			job := it.Next()
			count++
			if previousJob != nil {
				require.Greater(t, job.JobID, previousJob.JobID, "jobs should be iterated in order by JobID")
			}
			previousJob = job
			if job.JobID == discardedJobID {
				it.Discard(job)
			}
		}
		require.Equal(t, 2, count, "expected 2 jobs to be returned")
		require.Equal(t, 1, m.count, "expected 1 calls of get jobs to be performed")
	})

	t.Run("error during query causes a panic", func(t *testing.T) {
		m := &mockGetJobs{t: t}
		it := New(jobsdb.GetQueryParams{JobsLimit: 5}, m.GetJobs)
		// first batch will fail
		m.fail = true
		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 5}

		require.Panics(t, func() { it.HasNext() })
	})

	t.Run("out-of-order events during iterator's next causes panic", func(t *testing.T) {
		m := &mockGetJobs{t: t}
		it := New(jobsdb.GetQueryParams{JobsLimit: 5}, m.GetJobs)
		// first batch will return 2 jobs from workspace A
		m.jobs = []*jobsdb.JobT{
			{JobID: 1, WorkspaceId: "A", LastJobStatus: jobsdb.JobStatusT{}},
			{JobID: 3, WorkspaceId: "A", LastJobStatus: jobsdb.JobStatusT{JobState: jobsdb.Waiting.State}},
		}

		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 5}

		require.True(t, it.HasNext())

		// 2nd batch should return 1 job from workspace A, but it's out of order
		m.jobs = []*jobsdb.JobT{
			{JobID: 2, WorkspaceId: "A", LastJobStatus: jobsdb.JobStatusT{JobState: jobsdb.Waiting.State}},
		}

		discardedJobID := int64(3)
		m.expectedParams = &jobsdb.GetQueryParams{JobsLimit: 1}

		var count int
		var previousJob *jobsdb.JobT
		require.Panics(t, func() {
			for it.HasNext() {
				job := it.Next()
				count++
				if previousJob != nil {
					require.Greater(t, job.JobID, previousJob.JobID, "jobs should be iterated in order by JobID")
				}
				previousJob = job
				if job.JobID == discardedJobID {
					it.Discard(job)
				}
			}
		})

		require.Equal(t, 2, count, "expected 2 jobs to be returned")
		require.Equal(t, 2, m.count, "expected 2 calls of get jobs to be performed")
	})
}

type mockGetJobs struct {
	t              *testing.T
	count          int
	expectedParams *jobsdb.GetQueryParams
	jobs           []*jobsdb.JobT
	fail           bool
}

func (m *mockGetJobs) GetJobs(_ context.Context, params jobsdb.GetQueryParams, resumeFrom jobsdb.MoreToken) (*jobsdb.MoreJobsResult, error) {
	m.count++
	if m.expectedParams != nil {
		require.Equalf(m.t, *m.expectedParams, params, "expect call %d to get jobs to be performed with the expected params", m.count)
	}
	if m.count > 1 {
		require.Equalf(m.t, m.count-1, resumeFrom, "expect call %d to get jobs to be performed with the expected resumeFrom", m.count)
	}
	if m.fail {
		return nil, errors.New("failed to get jobs")
	}
	return &jobsdb.MoreJobsResult{
		JobsResult: jobsdb.JobsResult{
			Jobs: m.jobs,
		},
		More: m.count,
	}, nil
}
