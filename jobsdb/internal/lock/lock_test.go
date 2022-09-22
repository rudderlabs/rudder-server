package lock_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb/internal/lock"
	"github.com/stretchr/testify/require"
)

func TestAsyncLock(t *testing.T) {
	locker := &lock.DSListLocker{}

	l, c := locker.AsyncLock()
	require.NotNil(t, l)

	secondLock := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wg.Done()
		locker.WithLock(func(l lock.DSListLockToken) {
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

func TestWithCtxAwareLock(t *testing.T) {
	locker := &lock.DSListLocker{}
	syncChan := make(chan struct{})

	var wg sync.WaitGroup
	t.Run("context timeout should be triggered while acquiring lock.", func(t *testing.T) {
		var counter int
		wg.Add(1)
		go func() {
			wg.Done()
			locker.WithLock(func(l lock.DSListLockToken) {
				<-syncChan
			})
		}()
		wg.Wait()
		timeout := time.Second
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		lockAcquired := locker.WithCtxAwareLock(ctx, func(l lock.DSListLockToken) {
			counter++
			t.Fatal("lock should not have been acquired")
		})
		totalTime := time.Since(start)
		require.GreaterOrEqual(t, totalTime, timeout, "total time should be greater than or equal to timeout")
		require.Equal(t, 0, counter, "expected locker.WithTimeoutLock to timeout & not execute the function")
		require.False(t, lockAcquired)
		syncChan <- struct{}{}
	})

	t.Run("context timeout shouldn't be triggered, and should successfully acquire the lock.", func(t *testing.T) {
		var counter int
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		lockAcquired := locker.WithCtxAwareLock(ctx, func(l lock.DSListLockToken) {
			counter++
		})
		require.Equal(t, 1, counter, "expected locker.WithTimeoutLock to acquire the lock & execute the function")
		require.True(t, lockAcquired)
	})
}

func TestAsyncLockWithCtx(t *testing.T) {
	t.Run("context timeout should be triggered while acquiring lock.", func(t *testing.T) {
		locker := &lock.DSListLocker{}

		l1, c1 := locker.AsyncLock()
		require.NotNil(t, l1)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		start := time.Now()
		l2, c2 := locker.AsyncLockWithCtx(ctx)
		require.Nil(t, l2)
		require.Nil(t, c2)
		totalTime := time.Since(start)
		require.GreaterOrEqual(t, totalTime, time.Second, "total time should be greater than or equal to timeout")
		c1 <- l1
	})
	t.Run("context timeout shouldn't be triggered while acquiring lock.", func(t *testing.T) {
		locker := &lock.DSListLocker{}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		start := time.Now()
		l2, c2 := locker.AsyncLockWithCtx(ctx)
		require.NotNil(t, l2)
		require.NotNil(t, c2)
		totalTime := time.Since(start)
		require.LessOrEqual(t, totalTime, time.Second, "total time should be greater than or equal to timeout")
		c2 <- l2
	})
}
