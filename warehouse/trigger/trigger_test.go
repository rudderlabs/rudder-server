package trigger_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/warehouse/trigger"
)

func TestTrigger(t *testing.T) {
	identifier := "test"

	t.Run("basic flow", func(t *testing.T) {
		store := trigger.NewStore()
		require.False(t, store.IsTriggered(identifier))

		store.Trigger(identifier)
		require.True(t, store.IsTriggered(identifier))

		store.ClearTrigger(identifier)
		require.False(t, store.IsTriggered(identifier))
	})

	t.Run("concurrent access", func(t *testing.T) {
		store := trigger.NewStore()
		const numGoroutines = 100
		const numIterations = 100

		var wg sync.WaitGroup
		wg.Add(3 * numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < numIterations; j++ {
					store.Trigger("concurrent")
				}
			}()
		}
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < numIterations; j++ {
					store.ClearTrigger("concurrent")
				}
			}()
		}

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < numIterations; j++ {
					_ = store.IsTriggered("concurrent")
				}
			}()
		}

		wg.Wait()
	})
}

func BenchmarkTrigger(b *testing.B) {
	store := trigger.NewStore()

	var wg sync.WaitGroup
	wg.Add(b.N * 3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func() {
			defer wg.Done()
			store.Trigger("concurrent")
		}()
		go func() {
			defer wg.Done()
			_ = store.IsTriggered("concurrent")
		}()
		go func() {
			defer wg.Done()
			store.ClearTrigger("concurrent")
		}()
	}
}
