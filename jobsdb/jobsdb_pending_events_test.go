package jobsdb

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"
)

func TestPendingEventsJobsDB(t *testing.T) {
	const (
		workspaceID   = "test-workspace"
		customVal     = "WEBHOOK"
		destinationID = "dest-1"
	)

	genJobsWithDestination := func(count int) []*JobT {
		jobs := make([]*JobT, count)
		for i := range jobs {
			jobs[i] = &JobT{
				Parameters:   []byte(`{"batch_id":1,"source_id":"sourceID","destination_id":"` + destinationID + `"}`),
				EventPayload: []byte(`{"key":"value"}`),
				UserID:       "user-1",
				UUID:         uuid.New(),
				CustomVal:    customVal,
				EventCount:   1,
				WorkspaceId:  workspaceID,
			}
		}
		return jobs
	}

	genJobStatuses := func(jobs []*JobT, state string) []*JobStatusT {
		statuses := make([]*JobStatusT, len(jobs))
		for i, job := range jobs {
			statuses[i] = &JobStatusT{
				JobID:         job.JobID,
				JobState:      state,
				AttemptNum:    1,
				ErrorCode:     "200",
				ErrorResponse: []byte(`{}`),
				Parameters:    []byte(``),
				WorkspaceId:   job.WorkspaceId,
				CustomVal:     job.CustomVal,
				JobParameters: job.Parameters,
			}
		}
		return statuses
	}

	t.Run("successful transactions update pending events", func(t *testing.T) {
		_ = startPostgres(t)
		c := config.New()
		c.Set("jobsdb.maxDSSize", 100)

		jobDB := Handle{config: c}
		tablePrefix := strings.ToLower(rand.String(5))
		err := jobDB.Setup(ReadWrite, false, tablePrefix)
		require.NoError(t, err)
		defer jobDB.TearDown()

		registry := &mockPendingEventsRegistry{}
		decoratedDB := NewPendingEventsJobsDB(&jobDB, registry)

		// Store 5 jobs
		jobs := genJobsWithDestination(5)
		err = decoratedDB.Store(context.Background(), jobs)
		require.NoError(t, err)

		// After commit, pending events should be increased
		require.Equal(t, 1, registry.increaseCallCount, "IncreasePendingEvents should be called once")
		require.Equal(t, float64(5), registry.totalIncreased, "Should have increased by 5")
		require.Equal(t, tablePrefix, registry.lastTablePrefix)
		require.Equal(t, workspaceID, registry.lastWorkspaceID)
		require.Equal(t, customVal, registry.lastDestType)
		require.Equal(t, destinationID, registry.lastDestinationID)

		// Fetch the stored jobs to get their IDs
		storedJobs, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        10,
		})
		require.NoError(t, err)
		require.Len(t, storedJobs.Jobs, 5)

		// Update job statuses to succeeded (terminal state)
		statuses := genJobStatuses(storedJobs.Jobs, Succeeded.State)
		err = decoratedDB.UpdateJobStatus(context.Background(), statuses)
		require.NoError(t, err)

		// After commit, pending events should be decreased
		require.Equal(t, 1, registry.decreaseCallCount, "DecreasePendingEvents should be called once")
		require.Equal(t, float64(5), registry.totalDecreased, "Should have decreased by 5")
	})

	t.Run("non-terminal status does not decrease pending events", func(t *testing.T) {
		_ = startPostgres(t)
		c := config.New()
		c.Set("jobsdb.maxDSSize", 100)

		jobDB := Handle{config: c}
		tablePrefix := strings.ToLower(rand.String(5))
		err := jobDB.Setup(ReadWrite, false, tablePrefix)
		require.NoError(t, err)
		defer jobDB.TearDown()

		registry := &mockPendingEventsRegistry{}
		decoratedDB := NewPendingEventsJobsDB(&jobDB, registry)

		// Store 3 jobs
		jobs := genJobsWithDestination(3)
		err = decoratedDB.Store(context.Background(), jobs)
		require.NoError(t, err)

		require.Equal(t, float64(3), registry.totalIncreased)

		// Fetch the stored jobs
		storedJobs, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        10,
		})
		require.NoError(t, err)

		// Update job statuses to executing (non-terminal state)
		statuses := genJobStatuses(storedJobs.Jobs, Executing.State)
		err = decoratedDB.UpdateJobStatus(context.Background(), statuses)
		require.NoError(t, err)

		// Non-terminal status should not decrease pending events
		require.Equal(t, 0, registry.decreaseCallCount, "DecreasePendingEvents should not be called for non-terminal states")
		require.Equal(t, float64(0), registry.totalDecreased)
	})

	t.Run("failed store transaction does not update pending events", func(t *testing.T) {
		_ = startPostgres(t)
		c := config.New()
		c.Set("jobsdb.maxDSSize", 100)

		jobDB := Handle{config: c}
		tablePrefix := strings.ToLower(rand.String(5))
		err := jobDB.Setup(ReadWrite, false, tablePrefix)
		require.NoError(t, err)
		defer jobDB.TearDown()

		registry := &mockPendingEventsRegistry{}
		decoratedDB := NewPendingEventsJobsDB(&jobDB, registry)

		// Use WithStoreSafeTx to control the transaction and simulate failure
		jobs := genJobsWithDestination(3)
		expectedErr := errors.New("simulated transaction failure")

		err = decoratedDB.WithStoreSafeTx(context.Background(), func(tx StoreSafeTx) error {
			if err := decoratedDB.StoreInTx(context.Background(), tx, jobs); err != nil {
				return err
			}
			// Return error to abort the transaction
			return expectedErr
		})
		require.Error(t, err)
		require.Equal(t, expectedErr, err)

		// Transaction was rolled back, so pending events should not be updated
		require.Equal(t, 0, registry.increaseCallCount, "IncreasePendingEvents should not be called on failed transaction")
		require.Equal(t, float64(0), registry.totalIncreased)

		// Now store successfully
		err = decoratedDB.Store(context.Background(), jobs)
		require.NoError(t, err)

		// After successful commit, pending events should be increased
		require.Equal(t, 1, registry.increaseCallCount)
		require.Equal(t, float64(3), registry.totalIncreased)
	})

	t.Run("failed update status transaction does not update pending events", func(t *testing.T) {
		_ = startPostgres(t)
		c := config.New()
		c.Set("jobsdb.maxDSSize", 100)

		jobDB := Handle{config: c}
		tablePrefix := strings.ToLower(rand.String(5))
		err := jobDB.Setup(ReadWrite, false, tablePrefix)
		require.NoError(t, err)
		defer jobDB.TearDown()

		registry := &mockPendingEventsRegistry{}
		decoratedDB := NewPendingEventsJobsDB(&jobDB, registry)

		// Store jobs successfully first
		jobs := genJobsWithDestination(3)
		err = decoratedDB.Store(context.Background(), jobs)
		require.NoError(t, err)
		require.Equal(t, float64(3), registry.totalIncreased)

		// Fetch the stored jobs
		storedJobs, err := jobDB.GetUnprocessed(context.Background(), GetQueryParams{
			CustomValFilters: []string{customVal},
			JobsLimit:        10,
		})
		require.NoError(t, err)

		// Try to update status but abort the transaction
		statuses := genJobStatuses(storedJobs.Jobs, Succeeded.State)
		expectedErr := errors.New("simulated update failure")

		err = decoratedDB.WithUpdateSafeTx(context.Background(), func(tx UpdateSafeTx) error {
			if err := decoratedDB.UpdateJobStatusInTx(context.Background(), tx, statuses); err != nil {
				return err
			}
			// Return error to abort the transaction
			return expectedErr
		})
		require.Error(t, err)
		require.Equal(t, expectedErr, err)

		// Transaction was rolled back, so pending events should not be decreased
		require.Equal(t, 0, registry.decreaseCallCount, "DecreasePendingEvents should not be called on failed transaction")
		require.Equal(t, float64(0), registry.totalDecreased)

		// Now update successfully
		err = decoratedDB.UpdateJobStatus(context.Background(), statuses)
		require.NoError(t, err)

		// After successful commit, pending events should be decreased
		require.Equal(t, 1, registry.decreaseCallCount)
		require.Equal(t, float64(3), registry.totalDecreased)
	})

	t.Run("multiple workspaces and destinations are tracked separately", func(t *testing.T) {
		_ = startPostgres(t)
		c := config.New()
		c.Set("jobsdb.maxDSSize", 100)

		jobDB := Handle{config: c}
		tablePrefix := strings.ToLower(rand.String(5))
		err := jobDB.Setup(ReadWrite, false, tablePrefix)
		require.NoError(t, err)
		defer jobDB.TearDown()

		registry := &mockPendingEventsRegistry{}
		decoratedDB := NewPendingEventsJobsDB(&jobDB, registry)

		// Create jobs with different workspaces and destinations
		jobs := []*JobT{
			{
				Parameters:   []byte(`{"destination_id":"dest-1"}`),
				EventPayload: []byte(`{}`),
				UUID:         uuid.New(),
				CustomVal:    "WEBHOOK",
				EventCount:   1,
				WorkspaceId:  "workspace-1",
			},
			{
				Parameters:   []byte(`{"destination_id":"dest-1"}`),
				EventPayload: []byte(`{}`),
				UUID:         uuid.New(),
				CustomVal:    "WEBHOOK",
				EventCount:   1,
				WorkspaceId:  "workspace-1",
			},
			{
				Parameters:   []byte(`{"destination_id":"dest-2"}`),
				EventPayload: []byte(`{}`),
				UUID:         uuid.New(),
				CustomVal:    "S3",
				EventCount:   1,
				WorkspaceId:  "workspace-1",
			},
			{
				Parameters:   []byte(`{"destination_id":"dest-3"}`),
				EventPayload: []byte(`{}`),
				UUID:         uuid.New(),
				CustomVal:    "WEBHOOK",
				EventCount:   1,
				WorkspaceId:  "workspace-2",
			},
		}

		err = decoratedDB.Store(context.Background(), jobs)
		require.NoError(t, err)

		// Should have 3 separate calls (one for each unique workspace+destType+destinationID combination)
		require.Equal(t, 3, registry.increaseCallCount)
		require.Equal(t, float64(4), registry.totalIncreased)
	})
}

// mockPendingEventsRegistry tracks calls to the pending events registry
type mockPendingEventsRegistry struct {
	increaseCallCount int
	decreaseCallCount int
	totalIncreased    float64
	totalDecreased    float64
	lastTablePrefix   string
	lastWorkspaceID   string
	lastDestType      string
	lastDestinationID string
}

func (m *mockPendingEventsRegistry) IncreasePendingEvents(tablePrefix, workspaceID, destType, destinationID string, value float64) {
	m.increaseCallCount++
	m.totalIncreased += value
	m.lastTablePrefix = tablePrefix
	m.lastWorkspaceID = workspaceID
	m.lastDestType = destType
	m.lastDestinationID = destinationID
}

func (m *mockPendingEventsRegistry) DecreasePendingEvents(tablePrefix, workspaceID, destType, destinationID string, value float64) {
	m.decreaseCallCount++
	m.totalDecreased += value
	m.lastTablePrefix = tablePrefix
	m.lastWorkspaceID = workspaceID
	m.lastDestType = destType
	m.lastDestinationID = destinationID
}
