package lock_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
)

func TestRLock(t *testing.T) {
	locker := lock.NewLocker()
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
	locker := lock.NewLocker()

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
	locker := lock.NewLocker()
	locker.RLock()
	defer locker.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	l, c, err := locker.AsyncLockWithCtx(ctx)
	require.Error(t, err)
	require.Nil(t, l)
	require.Nil(t, c)
}
