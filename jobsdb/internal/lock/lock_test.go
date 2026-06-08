package lock_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
)

func TestRLock(t *testing.T) {
	locker := lock.NewLocker("test", "ds", stats.NOP)
	locker.RLock()

	t.Run("Multiple read locks can be acquired", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()
		require.True(t, locker.RTryLockWithCtx(ctx))
		locker.RUnlock()
	})

	t.Run("A write lock cannot be acquired if a read lock is already acquired", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()
		require.False(t, locker.TryLockWithCtx(ctx))
	})

	t.Run("A write lock can be acquired after a read lock is released", func(t *testing.T) {
		locker.RUnlock()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		require.True(t, locker.TryLockWithCtx(ctx))
		locker.Unlock()
	})
}

func TestAsyncLock(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	locker := lock.NewLocker("test", "ds", stats.NOP)

	l, c, err := locker.AsyncLockWithCtx(context.Background())
	require.NoError(t, err)
	require.NotNil(t, l)

	secondLock := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		locker.WithLock(func(l lock.LockToken) {
			secondLock <- struct{}{}
		})
	}()
	wg.Wait()

	select {
	case <-secondLock:
		t.Fatal("second lock should not have been acquired")
	case <-time.After(100 * time.Millisecond):
		c <- l
	}

	select {
	case <-secondLock:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("second lock should have been acquired")
	}
}

func TestAsyncLockTimeout(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	locker := lock.NewLocker("test", "ds", stats.NOP)
	locker.RLock()
	defer locker.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	l, c, err := locker.AsyncLockWithCtx(ctx)
	require.Error(t, err)
	require.Nil(t, l)
	require.Nil(t, c)
}

func TestTimers(t *testing.T) {
	newStore := func(t *testing.T) (*memstats.Store, *lock.Locker) {
		t.Helper()
		store, err := memstats.New()
		require.NoError(t, err)
		return store, lock.NewLocker("test", "ds", store)
	}

	writeTags := func(async string) stats.Tags {
		return stats.Tags{"name": "test", "prefix": "ds", "lockType": "write", "async": async}
	}
	readTags := stats.Tags{"name": "test", "prefix": "ds", "lockType": "read", "async": "false"}

	hasDurations := func(t *testing.T, store *memstats.Store, metric string, tags stats.Tags) {
		t.Helper()
		m := store.Get(metric, tags)
		require.NotNil(t, m, "metric %q not recorded", metric)
		require.NotEmpty(t, m.Durations(), "metric %q has no durations", metric)
	}
	noDurations := func(t *testing.T, store *memstats.Store, metric string, tags stats.Tags) {
		t.Helper()
		m := store.Get(metric, tags)
		if m != nil {
			require.Empty(t, m.Durations(), "metric %q should not have been recorded", metric)
		}
	}

	t.Run("RTryLockWithCtx records read wait on success", func(t *testing.T) {
		store, locker := newStore(t)
		ctx := context.Background()
		require.True(t, locker.RTryLockWithCtx(ctx))
		locker.RUnlock()
		hasDurations(t, store, "jobsdb_lock_wait_time", readTags)
	})

	t.Run("RTryLockWithCtx records nothing on failure", func(t *testing.T) {
		store, locker := newStore(t)
		locker.RLock()
		defer locker.RUnlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		require.False(t, locker.TryLockWithCtx(ctx)) // block with a write attempt
		noDurations(t, store, "jobsdb_lock_wait_time", writeTags("false"))
	})

	t.Run("WithLock records wait, held, and total", func(t *testing.T) {
		store, locker := newStore(t)
		locker.WithLock(func(lock.LockToken) {})
		hasDurations(t, store, "jobsdb_lock_wait_time", writeTags("false"))
		hasDurations(t, store, "jobsdb_lock_time", writeTags("false"))
		hasDurations(t, store, "jobsdb_lock_total_time", writeTags("false"))
	})

	t.Run("WithLockInCtx records wait, held, and total", func(t *testing.T) {
		store, locker := newStore(t)
		err := locker.WithLockInCtx(context.Background(), func(lock.LockToken) error { return nil })
		require.NoError(t, err)
		hasDurations(t, store, "jobsdb_lock_wait_time", writeTags("false"))
		hasDurations(t, store, "jobsdb_lock_time", writeTags("false"))
		hasDurations(t, store, "jobsdb_lock_total_time", writeTags("false"))
	})

	t.Run("TryLockWithCtx+Unlock records wait, held, and total", func(t *testing.T) {
		store, locker := newStore(t)
		require.True(t, locker.TryLockWithCtx(context.Background()))
		hasDurations(t, store, "jobsdb_lock_wait_time", writeTags("false"))
		noDurations(t, store, "jobsdb_lock_time", writeTags("false"))
		noDurations(t, store, "jobsdb_lock_total_time", writeTags("false"))

		locker.Unlock()
		hasDurations(t, store, "jobsdb_lock_time", writeTags("false"))
		hasDurations(t, store, "jobsdb_lock_total_time", writeTags("false"))
	})

	t.Run("TryLockWithCtx records nothing on failure", func(t *testing.T) {
		store, locker := newStore(t)
		locker.RLock()
		defer locker.RUnlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		require.False(t, locker.TryLockWithCtx(ctx))
		noDurations(t, store, "jobsdb_lock_wait_time", writeTags("false"))
		noDurations(t, store, "jobsdb_lock_time", writeTags("false"))
		noDurations(t, store, "jobsdb_lock_total_time", writeTags("false"))
	})

	t.Run("AsyncLockWithCtx records async wait, held, and total", func(t *testing.T) {
		defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
		store, locker := newStore(t)
		token, release, err := locker.AsyncLockWithCtx(context.Background())
		require.NoError(t, err)
		hasDurations(t, store, "jobsdb_lock_wait_time", writeTags("true"))
		noDurations(t, store, "jobsdb_lock_time", writeTags("true"))
		noDurations(t, store, "jobsdb_lock_total_time", writeTags("true"))

		release <- token
		// give the goroutine time to record after unlock
		require.Eventually(t, func() bool {
			m := store.Get("jobsdb_lock_time", writeTags("true"))
			return m != nil && len(m.Durations()) > 0
		}, time.Second, time.Millisecond)
		hasDurations(t, store, "jobsdb_lock_total_time", writeTags("true"))
	})
}
