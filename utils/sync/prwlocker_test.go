package sync_test

import (
	gsync "sync"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/utils/sync"
	"github.com/stretchr/testify/require"
)

func TestPartitionRWLocker(t *testing.T) {
	t.Run("Lock and Unlock different partitions at the same time", func(t *testing.T) {
		locker := sync.NewPartitionRWLocker()
		locker.Lock("id1")
		locker.Lock("id2")

		locker.Unlock("id1")
		locker.Unlock("id2")
	})

	t.Run("RLock and RUnlock different partitions at the same time", func(t *testing.T) {
		locker := sync.NewPartitionRWLocker()
		locker.RLock("id1")
		locker.RLock("id2")

		locker.RUnlock("id1")
		locker.RUnlock("id2")
	})

	t.Run("Concurrent locks", func(t *testing.T) {
		locker := sync.NewPartitionRWLocker()
		mu := locker.RWMutexFor("id1")
		var wg gsync.WaitGroup
		var counter int
		goroutines := 1000
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				mu.Lock()
				counter = counter + 1
				time.Sleep(1 * time.Millisecond)
				mu.Unlock()
				mu.RLock()
				time.Sleep(1 * time.Millisecond)
				mu.RUnlock()
			}()
		}
		wg.Wait()
		require.Equalf(t, goroutines, counter, "it should have incremented the counter %d times", goroutines)
	})
}
