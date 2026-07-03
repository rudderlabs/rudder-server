package jobsdb

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
)

func TestGetPendingConsumerJobs(t *testing.T) {
	_ = startPostgres(t)
	ctx := context.Background()

	newJobDB := func(t *testing.T, multiConsumer bool) *Handle {
		t.Helper()
		c := config.New()
		c.Set("JobsDB.maxDSSize", 100000)
		jd := &Handle{
			TriggerAddNewDS:   func() <-chan time.Time { return make(chan time.Time) },
			TriggerCompaction: func() <-chan time.Time { return make(chan time.Time) },
			config:            c,
		}
		jd.conf.multiConsumer = multiConsumer
		require.NoError(t, jd.Setup(ReadWrite, true, strings.ToLower(rand.String(5))))
		t.Cleanup(jd.TearDown)
		return jd
	}

	mcStatus := func(job *JobT, consumer, state string) *JobStatusT {
		return &JobStatusT{
			JobID: job.JobID, JobState: state, AttemptNum: 1,
			ExecTime: time.Now(), RetryTime: time.Now(), ErrorCode: "200",
			ErrorResponse: []byte(`{}`), Parameters: []byte(`{}`),
			WorkspaceId: job.WorkspaceId, CustomVal: job.CustomVal, Consumer: consumer,
		}
	}

	pendingStates := []string{Failed.State, Waiting.State, Unprocessed.State}

	consumersOf := func(jobs []*JobT) map[int64][]string {
		m := make(map[int64][]string, len(jobs))
		for _, j := range jobs {
			c := append([]string(nil), j.Consumers...)
			sort.Strings(c)
			m[j.JobID] = c
		}
		return m
	}

	t.Run("single-consumer: returns failed + unprocessed, excludes terminal", func(t *testing.T) {
		jd := newJobDB(t, false)
		jobs := genJobs(defaultWorkspaceID, "mc", 4, 1)
		for _, j := range jobs {
			j.PartitionID = "p1"
		}
		require.NoError(t, jd.Store(ctx, jobs))
		require.NoError(t, jd.UpdateJobStatus(ctx, []*JobStatusT{
			mcStatus(jobs[0], "", Succeeded.State), // terminal → excluded
			mcStatus(jobs[1], "", Failed.State),    // pending
			// jobs[2] no status → unprocessed → pending
			mcStatus(jobs[3], "", Aborted.State), // terminal → excluded
		}))

		res, err := jd.GetPendingConsumerJobs(ctx, pendingStates, GetQueryParams{PartitionFilters: []string{"p1"}, JobsLimit: 100})
		require.NoError(t, err)
		ids := lo_jobIDs(res.Jobs)
		require.ElementsMatch(t, []int64{jobs[1].JobID, jobs[2].JobID}, ids)
		for _, j := range res.Jobs {
			require.Equal(t, []string{""}, j.Consumers)
		}
	})

	t.Run("multi-consumer: trims Consumers to pending set", func(t *testing.T) {
		jd := newJobDB(t, true)
		jobs := genJobs(defaultWorkspaceID, "mc", 4, 1)
		for _, j := range jobs {
			j.PartitionID = "p1"
		}
		jobs[0].Consumers = []string{"A", "B"}
		jobs[1].Consumers = []string{"A", "B"}
		jobs[2].Consumers = []string{"A", "B"}
		jobs[3].Consumers = []string{"A"}
		require.NoError(t, jd.Store(ctx, jobs))
		require.NoError(t, jd.UpdateJobStatus(ctx, []*JobStatusT{
			mcStatus(jobs[0], "A", Succeeded.State), // job0 fully terminal → excluded
			mcStatus(jobs[0], "B", Succeeded.State),
			mcStatus(jobs[1], "A", Succeeded.State), // job1: A done, B pending → {B}
			mcStatus(jobs[1], "B", Failed.State),
			// job2: no status → {A,B}
			mcStatus(jobs[3], "A", Failed.State), // job3: A pending → {A}
		}))

		res, err := jd.GetPendingConsumerJobs(ctx, pendingStates, GetQueryParams{PartitionFilters: []string{"p1"}, JobsLimit: 100})
		require.NoError(t, err)
		got := consumersOf(res.Jobs)
		require.Len(t, got, 3)
		require.Equal(t, []string{"B"}, got[jobs[1].JobID])
		require.Equal(t, []string{"A", "B"}, got[jobs[2].JobID])
		require.Equal(t, []string{"A"}, got[jobs[3].JobID])
		require.NotContains(t, got, jobs[0].JobID)
	})

	t.Run("multi-consumer: executing-state recovery returns executing consumers", func(t *testing.T) {
		jd := newJobDB(t, true)
		jobs := genJobs(defaultWorkspaceID, "mc", 1, 1)
		jobs[0].Consumers = []string{"A", "B"}
		require.NoError(t, jd.Store(ctx, jobs))
		require.NoError(t, jd.UpdateJobStatus(ctx, []*JobStatusT{
			mcStatus(jobs[0], "A", Executing.State),
			mcStatus(jobs[0], "B", Failed.State),
		}))

		res, err := jd.GetPendingConsumerJobs(ctx, []string{Executing.State}, GetQueryParams{JobsLimit: 100})
		require.NoError(t, err)
		require.Len(t, res.Jobs, 1)
		require.Equal(t, []string{"A"}, res.Jobs[0].Consumers)
	})

	t.Run("partition filter excludes other partitions", func(t *testing.T) {
		jd := newJobDB(t, true)
		jobs := genJobs(defaultWorkspaceID, "mc", 2, 1)
		jobs[0].PartitionID = "p1"
		jobs[0].Consumers = []string{"A"}
		jobs[1].PartitionID = "p2"
		jobs[1].Consumers = []string{"A"}
		require.NoError(t, jd.Store(ctx, jobs))

		res, err := jd.GetPendingConsumerJobs(ctx, pendingStates, GetQueryParams{PartitionFilters: []string{"p1"}, JobsLimit: 100})
		require.NoError(t, err)
		require.Equal(t, []int64{jobs[0].JobID}, lo_jobIDs(res.Jobs))
	})

	t.Run("JobsLimit caps the batch and signals LimitsReached", func(t *testing.T) {
		jd := newJobDB(t, true)
		jobs := genJobs(defaultWorkspaceID, "mc", 5, 1)
		for _, j := range jobs {
			j.Consumers = []string{"A"}
		}
		require.NoError(t, jd.Store(ctx, jobs))

		res, err := jd.GetPendingConsumerJobs(ctx, pendingStates, GetQueryParams{JobsLimit: 3})
		require.NoError(t, err)
		require.Len(t, res.Jobs, 3)
		require.True(t, res.LimitsReached)
	})
}

func lo_jobIDs(jobs []*JobT) []int64 {
	ids := make([]int64, len(jobs))
	for i, j := range jobs {
		ids[i] = j.JobID
	}
	return ids
}
