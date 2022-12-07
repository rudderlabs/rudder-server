package eventorder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/jobsdb"
)

func Test_Job_Failed_Scenario(t *testing.T) {
	barrier := NewBarrier(WithMetadata(map[string]string{"key1": "value1"}))

	require.Nil(t, barrier.Peek("user1"), "peek should return nil since no barrier exists")

	enter, previousFailedJobID := barrier.Enter("user1", 1)
	require.True(t, enter, "job 1 for user1 should be accepted since no barrier exists")
	require.Nil(t, previousFailedJobID)

	enter, previousFailedJobID = barrier.Enter("user1", 2)
	require.True(t, enter, "job 2 for user1 should be accepted since no barrier exists")
	require.Nil(t, previousFailedJobID)

	require.False(t, firstBool(barrier.Wait("user1", 1)), "job 1 for user1 shouldn't wait since no barrier exists")
	require.False(t, firstBool(barrier.Wait("user1", 2)), "job 2 for user1 shouldn't wait since no barrier exists")

	require.NoError(t, barrier.StateChanged("user1", 1, jobsdb.Failed.State))

	require.True(t, firstBool(barrier.Wait("user1", 2)), "job 2 for user1 should wait after job 1 has failed")
	require.Equal(t, 1, barrier.Size(), "barrier should have size of 1")
	require.Equal(t, `Barrier{map[key1:value1][{key: user1, failedJobID: 1, concurrentJobs: map[]}]}`, barrier.String(), "the barrier's string representation should be human readable")
	require.NoError(t, barrier.StateChanged("user1", 2, jobsdb.Waiting.State))

	barrier.Sync()

	require.EqualValues(t, 1, *barrier.Peek("user1"), "peek should return failed job id 1")
	enter, previousFailedJobID = barrier.Enter("user1", 1)
	require.True(t, enter, "job 1 for user1 should be accepted even if previously failed")
	require.NotNil(t, previousFailedJobID)
	require.EqualValues(t, 1, *previousFailedJobID, "previously failed job id should be 1")

	require.False(t, firstBool(barrier.Wait("user1", 1)), "job 1 for user1 shouldn't wait since it is the previously failed one")

	require.NoError(t, barrier.StateChanged("user1", 1, jobsdb.Succeeded.State))

	enter, previousFailedJobID = barrier.Enter("user1", 2)
	require.False(t, enter, "job 2 for user1 shouldn't be accepted even after job 1 has succeeded until barrier is synced")
	require.NotNil(t, previousFailedJobID)
	require.EqualValues(t, 1, *previousFailedJobID, "previously failed job id should be 1")

	barrier.Sync()
	enter, previousFailedJobID = barrier.Enter("user1", 2)
	require.True(t, enter, "job 2 for user1 should be accepted after barrier is synced")
	require.Nil(t, previousFailedJobID)
}

func Test_Job_Aborted_Scenario(t *testing.T) {
	barrier := NewBarrier(WithConcurrencyLimit(1))

	// Fail job 1 then enter again
	enter, previousFailedJobID := barrier.Enter("user1", 1)
	require.Nil(t, previousFailedJobID)
	require.True(t, enter, "job 1 for user1 should be accepted since no barrier exists")
	require.False(t, firstBool(barrier.Wait("user1", 1)), "job 1 for user1 shouldn't wait since no barrier exists")
	require.Equal(t, 0, barrier.Sync())
	require.NoError(t, barrier.StateChanged("user1", 1, jobsdb.Failed.State))
	enter, previousFailedJobID = barrier.Enter("user1", 1)
	require.True(t, enter, "job 1 for user1 should be accepted even if previously failed")
	require.NotNil(t, previousFailedJobID)
	require.EqualValues(t, 1, *previousFailedJobID, "previously failed job id should be 1")
	require.False(t, firstBool(barrier.Wait("user1", 1)), "job 1 for user1 shouldn't wait since it is the previously failed one")

	// Abort job 1
	enter, previousFailedJobID = barrier.Enter("user1", 1)
	require.True(t, enter, "job 1 for user1 should be accepted even if previously failed")
	require.NotNil(t, previousFailedJobID)
	require.EqualValues(t, 1, *previousFailedJobID, "previously failed job id should be 1")
	require.False(t, firstBool(barrier.Wait("user1", 1)), "job 1 for user1 shouldn't wait since it is the previously failed one")
	require.NoError(t, barrier.StateChanged("user1", 1, jobsdb.Aborted.State))

	// Try to enter job 2
	enter, previousFailedJobID = barrier.Enter("user1", 2)
	require.False(t, enter, "job 2 for user1 shouldn't be accepted before the barrier is synced")
	require.NotNil(t, previousFailedJobID)
	require.EqualValues(t, 1, *previousFailedJobID, "previously failed job id should be 1")

	// Try to enter job 2 after sync
	require.Equal(t, 1, barrier.Sync())
	enter, previousFailedJobID = barrier.Enter("user1", 2)
	require.True(t, enter, "job 2 for user1 should be accepted after job 1 has aborted")
	require.Nil(t, previousFailedJobID)

	// Try to enter job 3
	enter, previousFailedJobID = barrier.Enter("user1", 3)
	require.False(t, enter, "job 3 for user1 shouldn't be accepted since it is above the concurrency limit")
	require.Nil(t, previousFailedJobID)

	// Job 2 aborted
	require.False(t, firstBool(barrier.Wait("user1", 2)), "job 2 for user1 shouldn't wait")

	require.NoError(t, barrier.StateChanged("user1", 2, jobsdb.Aborted.State))

	enter, previousFailedJobID = barrier.Enter("user1", 3)
	require.False(t, enter, "job 3 for user1 shouldn't be accepted after job 2 aborted before the barrier is synced")
	require.Nil(t, previousFailedJobID)
	require.Equal(t, 1, barrier.Sync(), "barrier should sync 1 command")
	require.Equal(t, 0, barrier.Sync(), "barrier should empty the sync queue after syncing")

	enter, previousFailedJobID = barrier.Enter("user1", 3)
	require.True(t, enter, "job 3 for user1 should be accepted after job 2 aborted and barrier is synced: %v", barrier)
	require.Nil(t, previousFailedJobID)

	enter, previousFailedJobID = barrier.Enter("user1", 4)
	require.True(t, enter, "job 4 for user1 should be accepted after job 2 aborted and barrier is synced (no concurrency limit)")
	require.Nil(t, previousFailedJobID)
	enter, previousFailedJobID = barrier.Enter("user1", 5)
	require.True(t, enter, "job 5 for user1 should be accepted after job 2 aborted and barrier is synced (no concurrency limit)")
	require.Nil(t, previousFailedJobID)
	enter, previousFailedJobID = barrier.Enter("user1", 6)
	require.True(t, enter, "job 6 for user1 should be accepted after job 2 aborted and barrier is synced (no concurrency limit)")
	require.Nil(t, previousFailedJobID)
}

func Test_Job_Abort_then_Fail(t *testing.T) {
	barrier := NewBarrier(WithConcurrencyLimit(2))

	enter, previousFailedJobID := barrier.Enter("user1", 1)
	require.True(t, enter, "job 1 for user1 should be accepted since no barrier exists")
	require.Nil(t, previousFailedJobID)

	require.False(t, firstBool(barrier.Wait("user1", 1)), "job 1 for user1 shouldn't wait")
	require.NoError(t, barrier.StateChanged("user1", 1, jobsdb.Aborted.State))

	require.Equal(t, 0, barrier.Sync())

	enter, previousFailedJobID = barrier.Enter("user1", 2)
	require.True(t, enter, "job 2 for user1 should be accepted after job 1 has aborted (no barrier)")
	require.Nil(t, previousFailedJobID)
	enter, previousFailedJobID = barrier.Enter("user1", 3)
	require.True(t, enter, "job 3 for user1 should be accepted after job 1 has aborted (no barrier)")
	require.Nil(t, previousFailedJobID)
	enter, previousFailedJobID = barrier.Enter("user1", 4)
	require.True(t, enter, "job 4 for user1 should be accepted after job 1 has aborted (no barrier)")
	require.Nil(t, previousFailedJobID)

	require.False(t, firstBool(barrier.Wait("user1", 2)), "job 2 for user1 shouldn't wait")
	require.NoError(t, barrier.StateChanged("user1", 2, jobsdb.Failed.State))

	require.True(t, firstBool(barrier.Wait("user1", 3)), "job 3 for user1 should wait since job 2 has failed")
	require.NoError(t, barrier.StateChanged("user1", 3, jobsdb.Waiting.State))

	require.True(t, firstBool(barrier.Wait("user1", 4)), "job 4 for user1 should wait since job 2 has failed")
	require.NoError(t, barrier.StateChanged("user1", 4, jobsdb.Waiting.State))

	require.Equal(t, 2, barrier.Sync())
	enter, previousFailedJobID = barrier.Enter("user1", 2)
	require.True(t, enter, "job 2 for user1 should be accepted")
	require.NotNil(t, previousFailedJobID)
	require.EqualValues(t, 2, *previousFailedJobID, "previously failed job id should be 2")
}

func Test_Job_Fail_then_Abort(t *testing.T) {
	barrier := NewBarrier(WithConcurrencyLimit(2))

	enter, previousFailedJobID := barrier.Enter("user1", 1)
	require.True(t, enter, "job 1 for user1 should be accepted since no barrier exists")
	require.Nil(t, previousFailedJobID)

	require.False(t, firstBool(barrier.Wait("user1", 1)), "job 1 for user1 shouldn't wait")
	require.NoError(t, barrier.StateChanged("user1", 1, jobsdb.Failed.State))

	require.Equal(t, 0, barrier.Sync())

	enter, previousFailedJobID = barrier.Enter("user1", 1)
	require.True(t, enter, "job 1 for user1 should be accepted after job 1 has failed")
	require.EqualValues(t, 1, *previousFailedJobID, "previously failed job id should be 1")

	enter, previousFailedJobID = barrier.Enter("user1", 2)
	require.False(t, enter, "job 2 for user1 shouldn't be accepted after job 1 has failed")
	require.EqualValues(t, 1, *previousFailedJobID, "previously failed job id should be 1")

	require.False(t, firstBool(barrier.Wait("user1", 1)), "job 1 for user1 shouldn't wait")
	require.NoError(t, barrier.StateChanged("user1", 1, jobsdb.Aborted.State))

	enter, previousFailedJobID = barrier.Enter("user1", 2)
	require.False(t, enter, "job 2 for user1 shouldn't be accepted after job 1 has aborted until it is synced")
	require.NotNil(t, previousFailedJobID)
	require.EqualValues(t, 1, *previousFailedJobID)

	require.Equal(t, 1, barrier.Sync(), "barrier should sync 1 command")

	enter, previousFailedJobID = barrier.Enter("user1", 2)
	require.True(t, enter, "job 2 for user1 should be accepted after job 1 has aborted and it is synced")
	require.Nil(t, previousFailedJobID)

	enter, previousFailedJobID = barrier.Enter("user1", 3)
	require.True(t, enter, "job 3 for user1 should be accepted after job 1 has aborted and it is synced")
	require.Nil(t, previousFailedJobID)

	enter, previousFailedJobID = barrier.Enter("user1", 4)
	require.False(t, enter, "job 4 for user1 shouldn't be accepted since it violates the concurrency limit")
	require.Nil(t, previousFailedJobID)

	require.False(t, firstBool(barrier.Wait("user1", 2)), "job 2 for user1 shouldn't wait")
	require.NoError(t, barrier.StateChanged("user1", 2, jobsdb.Failed.State))

	require.True(t, firstBool(barrier.Wait("user1", 3)), "job 3 for user1 should wait after job 2 has failed")
	require.NoError(t, barrier.StateChanged("user1", 3, jobsdb.Waiting.State))
	require.Equal(t, 1, barrier.Sync(), "barrier should sync 1 command")
}

func Test_Panic_Scenarios(t *testing.T) {
	barrier := NewBarrier()

	enter, _ := barrier.Enter("user1", 2)
	require.True(t, enter, "job 2 for user1 should be accepted since no barrier exists")
	require.NoError(t, barrier.StateChanged("user1", 2, jobsdb.Failed.State))
	require.Error(t, barrier.StateChanged("user1", 2, "other state"))

	// panicking during wait
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err, "barrier should panic when asking it if you should wait for a job with a previous job id than the currently failed one")
		}()
		_, _ = barrier.Wait("user1", 1)
	}()

	// panicking during state changed
	func() {
		defer func() {
			err := recover()
			require.NotNil(t, err, "barrier should panic when posting a state for a job with a previous job id than the currently failed one")
		}()
		_ = barrier.StateChanged("user1", 1, jobsdb.Failed.State)
	}()
}

func TestBarrier_Leave(t *testing.T) {
	orderKey := "user1"
	barrier := NewBarrier(WithConcurrencyLimit(1))

	enter, _ := barrier.Enter(orderKey, 1)
	require.Truef(t, enter, "job 1 for %s should be accepted since no barrier exists", orderKey)

	require.NoError(t, barrier.StateChanged(orderKey, 1, jobsdb.Failed.State))
	require.EqualValues(t, 0, barrier.Sync())
	enter, _ = barrier.Enter(orderKey, 2)
	require.Falsef(t, enter, "job 2 for %s should not be accepted since job 1 has failed", orderKey)

	require.NoError(t, barrier.StateChanged(orderKey, 1, jobsdb.Aborted.State))
	require.EqualValues(t, 1, barrier.Sync())
	enter, _ = barrier.Enter(orderKey, 2)
	require.Truef(t, enter, "job 2 for %s should be accepted since job 1 was aborted", orderKey)

	enter, _ = barrier.Enter(orderKey, 3)
	require.Falsef(t, enter, "job 3 for %s should not be accepted since job 2 is running", orderKey)

	barrier.Leave(orderKey, 2)
	enter, _ = barrier.Enter(orderKey, 3)
	require.Truef(t, enter, "job 3 for %s should now be accepted since job 2 left", orderKey)
}

func firstBool(v bool, _ ...interface{}) bool {
	return v
}
