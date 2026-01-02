package jobsdb

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

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
	increasePendingEvents := func(tablePrefix, workspaceID, destType, destinationID string, value float64) {
		require.Equal(t, TablePrefix, tablePrefix)
		require.Equal(t, WorkspaceID, workspaceID)
		require.Equal(t, CustomVal, destType)
		pendingEventsCount += int(value)
	}
	require.NoError(t, jdb.GetPileUpCounts(context.Background(), increasePendingEvents))
	require.EqualValues(t, OriginalPendingEvents, pendingEventsCount)

	actualPendingEvents := pendingEventsCount
	pendingEventsCount = 0

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
		require.NoError(t, jdb.GetPileUpCounts(context.Background(), increasePendingEvents))
		require.EqualValues(t, actualPendingEvents, pendingEventsCount)
		pendingEventsCount = 0
	}
}
