package jobsdb

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
)

func TestJobsdbPileupCount(t *testing.T) {
	_ = startPostgres(t)

	const (
		TablePrefix           = "prefix"
		CustomVal             = "CUSTOMVAL"
		WorkspaceID           = "workspaceID"
		DestinationID         = "destinationID"
		OriginalPendingEvents = 50
	)
	generateJobs := func(numOfJob int) []*JobT {
		js := make([]*JobT, numOfJob)
		for i := range numOfJob {
			js[i] = &JobT{
				WorkspaceId:  WorkspaceID,
				Parameters:   []byte(`{"source_id":"sourceID","destination_id":"destinationID"}`),
				EventPayload: []byte(`{"testKey":"testValue"}`),
				UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
				UUID:         uuid.New(),
				CustomVal:    CustomVal,
				EventCount:   1,
			}
		}
		return js
	}

	c := config.New()
	c.Set("JobsDB.maxDSSize", 1)              // create 1 dataset per event
	c.Set("JobsDB.pileupCountConcurrency", 1) // 1 goroutine to get pileup counts
	statsStore, err := memstats.New()
	require.NoError(t, err)

	addDS := make(chan time.Time)
	compactDS := make(chan time.Time)
	jdb := NewForReadWrite(TablePrefix, WithConfig(c), WithStats(statsStore))
	jdb.TriggerAddNewDS = func() <-chan time.Time {
		return addDS
	}
	jdb.TriggerCompaction = func() <-chan time.Time {
		return compactDS
	}

	require.NoError(t, jdb.Start())
	defer jdb.TearDown()

	for range OriginalPendingEvents {
		require.NoError(t, jdb.Store(context.Background(), generateJobs(1)))
		require.NoError(t, err)
		addDS <- time.Now()
		addDS <- time.Now()
	}

	// get all jobs
	res, err := jdb.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{CustomVal}, JobsLimit: 100})
	require.NoError(t, err)
	require.Equal(t, OriginalPendingEvents, len(res.Jobs))

	var pendingEventsCount int
	increasePendingEvents := func(tablePrefix, workspaceID, destType, destinationID string, value float64) {
		require.Equal(t, TablePrefix, tablePrefix)
		require.Equal(t, WorkspaceID, workspaceID)
		require.Equal(t, CustomVal, destType)
		require.Equal(t, DestinationID, destinationID)
		pendingEventsCount += int(value)
	}
	require.NoError(t, jdb.GetPileUpCounts(context.Background(), time.Now(), increasePendingEvents))
	require.EqualValues(t, OriginalPendingEvents, pendingEventsCount)

	actualPendingEvents := pendingEventsCount
	pendingEventsCount = 0

	beforeUpdating := time.Now()
	for i, state := range jobStates {
		if !state.isValid {
			continue
		}
		require.NoError(t, jdb.UpdateJobStatus(context.Background(), []*JobStatusT{{
			JobID:         res.Jobs[i].JobID,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			JobState:      state.State,
			WorkspaceId:   WorkspaceID,
			Parameters:    []byte(`{"source_id":"sourceID","destination_id":"destinationID"}`),
			ErrorResponse: []byte(`{}`),
			AttemptNum:    1,
			ErrorCode:     "999",
			CustomVal:     res.Jobs[i].CustomVal,
		}}))
		if state.isTerminal {
			actualPendingEvents -= 1
		}
		require.NoError(t, jdb.GetPileUpCounts(context.Background(), time.Now(), increasePendingEvents))
		require.EqualValues(t, actualPendingEvents, pendingEventsCount)
		pendingEventsCount = 0
	}
	require.NoError(t, jdb.GetPileUpCounts(context.Background(), beforeUpdating, increasePendingEvents))
	require.EqualValues(t, OriginalPendingEvents, pendingEventsCount, "Getting pileup counts for the past should get the original count")

	toProcess, err := jdb.GetJobs(context.Background(), lo.FilterMap(jobStates, func(s jobStateT, _ int) (string, bool) {
		return s.State, !s.isTerminal
	}), GetQueryParams{JobsLimit: OriginalPendingEvents})
	require.NoError(t, err)
	require.Equal(t, OriginalPendingEvents, len(res.Jobs))

	// Start 4 goroutines
	//
	//   - one will get pileup counts
	//   - another one will update job statuses as terminal
	//   - the third one will try to get unprocessed jobs
	//   - the last one will try to trigger compaction
	//
	// Finally, the pileup count should be 0
	start := time.Now()
	var pileupCount atomic.Int64
	var queries int
	var migrations int
	g1, ctx1 := errgroup.WithContext(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	g2, ctx2 := errgroup.WithContext(ctx2)

	g1.Go(func() error {
		for {
			select {
			case <-ctx1.Done():
				return nil
			case <-time.After(1 * time.Millisecond):
				err := jdb.GetPileUpCounts(ctx1, start, func(tablePrefix, workspaceID, destType, destinationID string, value float64) {
					pileupCount.Add(int64(value))
				})
				cancel2() // stop goroutines 3 and 4 after getting pileup counts
				return err
			}
		}
	})
	g1.Go(func() error {
		for _, job := range toProcess.Jobs {
			if err := jdb.UpdateJobStatus(ctx1, []*JobStatusT{{
				JobID:         job.JobID,
				ExecTime:      time.Now(),
				RetryTime:     time.Now(),
				JobState:      Succeeded.State,
				WorkspaceId:   WorkspaceID,
				Parameters:    []byte(`{"source_id":"sourceID","destination_id":"destinationID"}`),
				ErrorResponse: []byte(`{}`),
				AttemptNum:    1,
				ErrorCode:     "200",
				CustomVal:     job.CustomVal,
			}}); err != nil {
				return err
			}
			pileupCount.Add(-1)
		}
		return nil
	})

	g2.Go(func() error {
		for {
			select {
			case <-ctx2.Done():
				return nil
			case <-time.After(time.Microsecond):
				_, err := jdb.GetUnprocessed(ctx2, GetQueryParams{CustomValFilters: []string{CustomVal}, JobsLimit: 1})
				if err != nil && ctx2.Err() == nil {
					return err
				}
				queries++
			}
		}
	})
	g2.Go(func() error {
		for {
			select {
			case <-ctx2.Done():
				return nil
			case <-time.After(10 * time.Millisecond): // we are assuming that 10ms is the least time that a query can take
				compactDS <- time.Now()
				migrations++
			}
		}
	})

	require.NoError(t, g1.Wait(), "goroutines 1 and 2 should not return an error")
	require.NoError(t, g2.Wait(), "goroutines 3 and 4 should not return an error")
	require.EqualValues(t, 0, pileupCount.Load())
	t.Logf("Queries: %d, Migrations: %d", queries, migrations)
}

// TestJobsdbPileupCountMultiConsumer verifies that GetPileUpCounts on a multi-consumer handle
// reports pending events per consumer (consumer = destination_id): a job pending for N consumers
// contributes N pending events, and a per-consumer terminal status drops exactly that consumer.
func TestJobsdbPileupCountMultiConsumer(t *testing.T) {
	_ = startPostgres(t)

	const (
		customVal   = "MCPILE"
		workspaceID = "ws"
	)

	c := config.New()
	c.Set("JobsDB.maxDSSize", 100)

	jdb := Handle{config: c}
	jdb.conf.multiConsumer = true
	require.NoError(t, jdb.Setup(ReadWrite, true, strings.ToLower(rand.String(5))))
	defer jdb.TearDown()

	// job0 pending for A and B; job1 pending for A only
	jobs := []*JobT{
		{UUID: uuid.New(), UserID: "u", CustomVal: customVal, Parameters: []byte(`{}`), EventPayload: []byte(`{}`), EventCount: 1, WorkspaceId: workspaceID, Consumers: []string{"A", "B"}},
		{UUID: uuid.New(), UserID: "u", CustomVal: customVal, Parameters: []byte(`{}`), EventPayload: []byte(`{}`), EventCount: 1, WorkspaceId: workspaceID, Consumers: []string{"A"}},
	}
	require.NoError(t, jdb.Store(context.Background(), jobs))

	pileup := func() map[string]int {
		counts := map[string]int{}
		require.NoError(t, jdb.GetPileUpCounts(context.Background(), time.Now(), func(tablePrefix, ws, destType, destinationID string, value float64) {
			require.Equal(t, customVal, destType)
			require.Equal(t, workspaceID, ws)
			counts[destinationID] += int(value)
		}))
		return counts
	}

	// Both jobs pending for A, one for B.
	require.Equal(t, map[string]int{"A": 2, "B": 1}, pileup())

	// Store is COPY-based and does not populate JobID; consumer "B" uniquely identifies job0.
	job0, err := jdb.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 10, Consumer: "B"})
	require.NoError(t, err)
	require.Len(t, job0.Jobs, 1)

	// Consumer A finishes job0 → A drops to 1, B unchanged.
	require.NoError(t, jdb.UpdateJobStatus(context.Background(), []*JobStatusT{{
		JobID: job0.Jobs[0].JobID, JobState: Succeeded.State, AttemptNum: 1,
		ExecTime: time.Now(), RetryTime: time.Now(), ErrorCode: "200",
		ErrorResponse: []byte(`{}`), Parameters: []byte(`{}`),
		WorkspaceId: workspaceID, CustomVal: customVal, Consumer: "A",
	}}))
	require.Equal(t, map[string]int{"A": 1, "B": 1}, pileup())
}

// TestJobsdbPileupCountCutoff verifies that the cutoff bounds both sides of the count: jobs created
// after it are not counted, no matter whether they are pending or not. Counting them would make the
// caller, which accounts for jobs stored after the cutoff on its own, count them twice.
func TestJobsdbPileupCountCutoff(t *testing.T) {
	_ = startPostgres(t)

	const (
		customVal   = "CUTOFF"
		workspaceID = "ws"
	)
	generateJobs := func(numOfJob int, consumers []string) []*JobT {
		js := make([]*JobT, numOfJob)
		for i := range numOfJob {
			js[i] = &JobT{
				WorkspaceId:  workspaceID,
				Parameters:   []byte(`{"source_id":"sourceID","destination_id":"destinationID"}`),
				EventPayload: []byte(`{"testKey":"testValue"}`),
				UserID:       "a-292e-4e79-9880-f8009e0ae4a3",
				UUID:         uuid.New(),
				CustomVal:    customVal,
				EventCount:   1,
				Consumers:    consumers,
			}
		}
		return js
	}

	for _, multiConsumer := range []bool{false, true} {
		t.Run("multiConsumer="+strconv.FormatBool(multiConsumer), func(t *testing.T) {
			c := config.New()
			c.Set("JobsDB.maxDSSize", 1) // create 1 dataset per event, so that the cutoff is applied across datasets

			jdb := Handle{config: c}
			jdb.conf.multiConsumer = multiConsumer
			var consumers []string
			if multiConsumer {
				consumers = []string{"destinationID"}
			}
			require.NoError(t, jdb.Setup(ReadWrite, true, strings.ToLower(rand.String(5))))
			defer jdb.TearDown()

			pileup := func(cutoff time.Time) int {
				var count int
				require.NoError(t, jdb.GetPileUpCounts(context.Background(), cutoff, func(_, _, _, _ string, value float64) {
					count += int(value)
				}))
				return count
			}
			// dbNow returns the database's current time, the same clock that stamps created_at.
			dbNow := func() time.Time {
				var now time.Time
				require.NoError(t, jdb.dbHandle.QueryRow("SELECT NOW()").Scan(&now))
				return now
			}

			require.NoError(t, jdb.Store(context.Background(), generateJobs(5, consumers)))
			cutoff := dbNow()
			require.NoError(t, jdb.Store(context.Background(), generateJobs(3, consumers)))

			require.Equal(t, 5, pileup(cutoff), "jobs stored after the cutoff should not be counted")
			require.Equal(t, 8, pileup(dbNow()), "all jobs should be counted with a cutoff past the last store")

			// A job stored after the cutoff stays uncounted even once it is terminal.
			stored, err := jdb.GetUnprocessed(context.Background(), GetQueryParams{CustomValFilters: []string{customVal}, JobsLimit: 10, Consumer: lo.Ternary(multiConsumer, "destinationID", "")})
			require.NoError(t, err)
			require.Len(t, stored.Jobs, 8)
			last := stored.Jobs[len(stored.Jobs)-1]
			require.NoError(t, jdb.UpdateJobStatus(context.Background(), []*JobStatusT{{
				JobID: last.JobID, JobState: Succeeded.State, AttemptNum: 1,
				ExecTime: time.Now(), RetryTime: time.Now(), ErrorCode: "200",
				ErrorResponse: []byte(`{}`), Parameters: []byte(`{}`),
				WorkspaceId: workspaceID, CustomVal: customVal, Consumer: lo.Ternary(multiConsumer, "destinationID", ""),
			}}))
			require.Equal(t, 5, pileup(cutoff))
			require.Equal(t, 7, pileup(dbNow()))
		})
	}
}

// TestJobsdbPileupCountCompactionBarrier verifies that pileup counting and compaction are mutually
// exclusive: the count waits for an in-flight compaction to finish, and a compaction triggered while
// a count is in progress skips its round instead of moving datasets under it.
func TestJobsdbPileupCountCompactionBarrier(t *testing.T) {
	_ = startPostgres(t)

	const customVal = "BARRIER"

	c := config.New()
	c.Set("JobsDB.maxDSSize", 1)
	jdb := Handle{config: c}
	require.NoError(t, jdb.Setup(ReadWrite, true, strings.ToLower(rand.String(5))))
	defer jdb.TearDown()

	require.NoError(t, jdb.Store(context.Background(), []*JobT{{
		UUID: uuid.New(), UserID: "u", CustomVal: customVal, Parameters: []byte(`{}`),
		EventPayload: []byte(`{}`), EventCount: 1, WorkspaceId: "ws",
	}}))

	t.Run("a count waits for an in-flight compaction", func(t *testing.T) {
		require.True(t, jdb.compactionLock.TryLock(), "no one else should be holding the compaction lock")
		defer jdb.compactionLock.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		err := jdb.GetPileUpCounts(ctx, time.Now(), func(_, _, _, _ string, _ float64) {
			t.Error("no counting should happen while compaction is in progress")
		})
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("a compaction skips its round during a count", func(t *testing.T) {
		counting := make(chan struct{})
		release := make(chan struct{})
		var count atomic.Int64
		done := make(chan error, 1)
		go func() {
			done <- jdb.GetPileUpCounts(context.Background(), time.Now(), func(_, _, _, _ string, value float64) {
				if count.Add(int64(value)) == 1 {
					close(counting)
					<-release
				}
			})
		}()

		<-counting // the count is in progress and holding the compaction lock
		require.False(t, jdb.compactionLock.TryLock(), "compaction should skip its round while a count is in progress")
		close(release)

		require.NoError(t, <-done)
		require.EqualValues(t, 1, count.Load())
		require.True(t, jdb.compactionLock.TryLock(), "compaction should be able to run once the count is over")
		jdb.compactionLock.Unlock()
	})
}
