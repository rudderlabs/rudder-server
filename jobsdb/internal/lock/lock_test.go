package lock_test

import (
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
