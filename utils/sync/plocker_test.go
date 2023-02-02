package sync_test

import (
	gsync "sync"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/utils/sync"
	"github.com/stretchr/testify/require"
)

func TestPartitionLocker(t *testing.T) {
	t.Run("Lock and Unlock different partitions at the same time", func(t *testing.T) {
		locker := sync.NewPartitionLocker()
		locker.Lock("id1")
		locker.Lock("id2")

		locker.Unlock("id1")
		locker.Unlock("id2")
	})

	t.Run("Concurrent locks", func(t *testing.T) {
		locker := sync.NewPartitionLocker()
		var wg gsync.WaitGroup
		var counter int
		goroutines := 1000
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				locker.Lock("id")
				counter = counter + 1
				time.Sleep(1 * time.Millisecond)
				locker.Unlock("id")
			}()
		}
		wg.Wait()
		require.Equalf(t, goroutines, counter, "it should have incremented the counter %d times", goroutines)
	})
}
