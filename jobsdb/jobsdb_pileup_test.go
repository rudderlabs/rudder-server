package jobsdb

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
)

func TestJobsdbPileupCount(t *testing.T) {
	_ = startPostgres(t)

	const (
		TablePrefix           = "prefix"
		CustomVal             = "CUSTOMVAL"
		WorkspaceID           = "workspaceID"
		OriginalPendingEvents = 50
	)
	generateJobs := func(numOfJob int) []*JobT {
		js := make([]*JobT, numOfJob)
		for i := 0; i < numOfJob; i++ {
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
	migrateDS := make(chan time.Time)
	jdb := NewForReadWrite(TablePrefix, WithConfig(c), WithStats(statsStore))
	jdb.TriggerAddNewDS = func() <-chan time.Time {
		return addDS
	}
	jdb.TriggerMigrateDS = func() <-chan time.Time {
		return migrateDS
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
	increasePendingEvents := func(tablePrefix, workspace, destType string, value float64) {
		require.Equal(t, TablePrefix, tablePrefix)
		require.Equal(t, WorkspaceID, workspace)
		require.Equal(t, CustomVal, destType)
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
		}}, nil, nil))
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
	//   - the last one will try to trigger migrations
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
				err := jdb.GetPileUpCounts(ctx1, start, func(tablePrefix, workspace, destType string, value float64) {
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
			}}, nil, nil); err != nil {
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
				migrateDS <- time.Now()
				migrations++
			}
		}
	})

	require.NoError(t, g1.Wait(), "goroutines 1 and 2 should not return an error")
	require.NoError(t, g2.Wait(), "goroutines 3 and 4 should not return an error")
	require.EqualValues(t, 0, pileupCount.Load())
	t.Logf("Queries: %d, Migrations: %d", queries, migrations)
	// not reliable to uncomment this assertion
	// require.Greaterf(t, queries, migrations, "migrations should not be pausing queries")
}
