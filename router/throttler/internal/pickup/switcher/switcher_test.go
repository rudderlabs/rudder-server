package switcher

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/router/throttler/internal/types"
)

func TestThrottlerSwitcher(t *testing.T) {
	t.Run("NewThrottlerSwitcher", func(t *testing.T) {
		valueLoader := &mockValueLoader{value: false}
		mainThrottler := newMockThrottler("main", 100)
		altThrottler := newMockThrottler("alt", 200)

		switcher := NewThrottlerSwitcher(valueLoader, mainThrottler, altThrottler)

		require.NotNil(t, switcher)

		// Verify it implements the Throttler interface
		var _ types.PickupThrottler = switcher // nolint: staticcheck
	})

	t.Run("CheckLimitReached_UsesMainThrottler", func(t *testing.T) {
		valueLoader := &mockValueLoader{value: false} // Use main throttler
		mainThrottler := newMockThrottler("main", 100)
		altThrottler := newMockThrottler("alt", 200)

		mainThrottler.setLimitReached(true)
		altThrottler.setLimitReached(false)

		switcher := NewThrottlerSwitcher(valueLoader, mainThrottler, altThrottler)

		ctx := context.Background()
		limited, err := switcher.CheckLimitReached(ctx, 50)

		require.True(t, limited)
		require.NoError(t, err)

		// Verify main throttler was called
		require.Len(t, mainThrottler.checkLimitReachedCalls, 1)
		require.Equal(t, ctx, mainThrottler.checkLimitReachedCalls[0].ctx)
		require.Equal(t, int64(50), mainThrottler.checkLimitReachedCalls[0].cost)

		// Verify alternative throttler was not called
		require.Len(t, altThrottler.checkLimitReachedCalls, 0)
	})

	t.Run("CheckLimitReached_UsesAlternativeThrottler", func(t *testing.T) {
		valueLoader := &mockValueLoader{value: true} // Use alternative throttler
		mainThrottler := newMockThrottler("main", 100)
		altThrottler := newMockThrottler("alt", 200)

		mainThrottler.setLimitReached(true)
		altThrottler.setLimitReached(false)

		switcher := NewThrottlerSwitcher(valueLoader, mainThrottler, altThrottler)

		ctx := context.Background()
		limited, err := switcher.CheckLimitReached(ctx, 75)

		require.False(t, limited)
		require.NoError(t, err)

		// Verify alternative throttler was called
		require.Len(t, altThrottler.checkLimitReachedCalls, 1)
		require.Equal(t, ctx, altThrottler.checkLimitReachedCalls[0].ctx)
		require.Equal(t, int64(75), altThrottler.checkLimitReachedCalls[0].cost)

		// Verify main throttler was not called
		require.Len(t, mainThrottler.checkLimitReachedCalls, 0)
	})

	t.Run("CheckLimitReached_SwitchingBehavior", func(t *testing.T) {
		valueLoader := &mockValueLoader{value: false}
		mainThrottler := newMockThrottler("main", 100)
		altThrottler := newMockThrottler("alt", 200)

		mainThrottler.setLimitReached(true)
		altThrottler.setLimitReached(false)

		switcher := NewThrottlerSwitcher(valueLoader, mainThrottler, altThrottler)

		ctx := context.Background()

		// First call - should use main throttler
		limited, err := switcher.CheckLimitReached(ctx, 30)
		require.True(t, limited)
		require.NoError(t, err)
		require.Len(t, mainThrottler.checkLimitReachedCalls, 1)
		require.Len(t, altThrottler.checkLimitReachedCalls, 0)

		// Switch to alternative
		valueLoader.setValue(true)

		// Second call - should use alternative throttler
		limited, err = switcher.CheckLimitReached(ctx, 40)
		require.False(t, limited)
		require.NoError(t, err)
		require.Len(t, mainThrottler.checkLimitReachedCalls, 1) // Still 1
		require.Len(t, altThrottler.checkLimitReachedCalls, 1)  // Now 1

		// Switch back to main
		valueLoader.setValue(false)

		// Third call - should use main throttler again
		limited, err = switcher.CheckLimitReached(ctx, 50)
		require.True(t, limited)
		require.NoError(t, err)
		require.Len(t, mainThrottler.checkLimitReachedCalls, 2) // Now 2
		require.Len(t, altThrottler.checkLimitReachedCalls, 1)  // Still 1
	})

	t.Run("CheckLimitReached_ErrorPropagation", func(t *testing.T) {
		valueLoader := &mockValueLoader{value: false}
		mainThrottler := newMockThrottler("main", 100)
		altThrottler := newMockThrottler("alt", 200)

		expectedError := errors.New("test error")
		mainThrottler.setCheckError(expectedError)

		switcher := NewThrottlerSwitcher(valueLoader, mainThrottler, altThrottler)

		ctx := context.Background()
		limited, err := switcher.CheckLimitReached(ctx, 25)

		require.False(t, limited)
		require.Equal(t, expectedError, err)
	})

	t.Run("ResponseCodeReceived_BothThrottlers", func(t *testing.T) {
		valueLoader := &mockValueLoader{value: false}
		mainThrottler := newMockThrottler("main", 100)
		altThrottler := newMockThrottler("alt", 200)

		switcher := NewThrottlerSwitcher(valueLoader, mainThrottler, altThrottler)

		// Send response codes
		switcher.ResponseCodeReceived(200)
		switcher.ResponseCodeReceived(429)
		switcher.ResponseCodeReceived(500)

		// Verify both throttlers received all codes
		require.Equal(t, []int{200, 429, 500}, mainThrottler.responseCodeReceivedCalls)
		require.Equal(t, []int{200, 429, 500}, altThrottler.responseCodeReceivedCalls)
	})

	t.Run("ResponseCodeReceived_IndependentOfSwitching", func(t *testing.T) {
		valueLoader := &mockValueLoader{value: false}
		mainThrottler := newMockThrottler("main", 100)
		altThrottler := newMockThrottler("alt", 200)

		switcher := NewThrottlerSwitcher(valueLoader, mainThrottler, altThrottler)

		// Send codes while using main
		switcher.ResponseCodeReceived(200)

		// Switch to alternative
		valueLoader.setValue(true)
		switcher.ResponseCodeReceived(429)

		// Switch back to main
		valueLoader.setValue(false)
		switcher.ResponseCodeReceived(500)

		// Both throttlers should have received all codes regardless of switching
		require.Equal(t, []int{200, 429, 500}, mainThrottler.responseCodeReceivedCalls)
		require.Equal(t, []int{200, 429, 500}, altThrottler.responseCodeReceivedCalls)
	})

	t.Run("Shutdown_BothThrottlers", func(t *testing.T) {
		valueLoader := &mockValueLoader{value: false}
		mainThrottler := newMockThrottler("main", 100)
		altThrottler := newMockThrottler("alt", 200)

		switcher := NewThrottlerSwitcher(valueLoader, mainThrottler, altThrottler)

		switcher.Shutdown()

		// Verify both throttlers were shut down
		require.True(t, mainThrottler.shutdownCalled)
		require.True(t, altThrottler.shutdownCalled)
	})

	t.Run("GetLimit_UsesMainThrottler", func(t *testing.T) {
		valueLoader := &mockValueLoader{value: false} // Use main throttler
		mainThrottler := newMockThrottler("main", 100)
		altThrottler := newMockThrottler("alt", 200)

		switcher := NewThrottlerSwitcher(valueLoader, mainThrottler, altThrottler)

		limit := switcher.GetLimit()

		require.Equal(t, int64(100), limit)
	})

	t.Run("GetLimit_UsesAlternativeThrottler", func(t *testing.T) {
		valueLoader := &mockValueLoader{value: true} // Use alternative throttler
		mainThrottler := newMockThrottler("main", 100)
		altThrottler := newMockThrottler("alt", 200)

		switcher := NewThrottlerSwitcher(valueLoader, mainThrottler, altThrottler)

		limit := switcher.GetLimit()

		require.Equal(t, int64(200), limit)
	})

	t.Run("GetLimit_SwitchingBehavior", func(t *testing.T) {
		valueLoader := &mockValueLoader{value: false}
		mainThrottler := newMockThrottler("main", 100)
		altThrottler := newMockThrottler("alt", 200)

		switcher := NewThrottlerSwitcher(valueLoader, mainThrottler, altThrottler)

		// Should use main throttler's limit
		limit := switcher.GetLimit()
		require.Equal(t, int64(100), limit)

		// Switch to alternative
		valueLoader.setValue(true)

		// Should use alternative throttler's limit
		limit = switcher.GetLimit()
		require.Equal(t, int64(200), limit)

		// Switch back to main
		valueLoader.setValue(false)

		// Should use main throttler's limit again
		limit = switcher.GetLimit()
		require.Equal(t, int64(100), limit)
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		valueLoader := &mockValueLoader{value: false}
		mainThrottler := newMockThrottler("main", 100)
		altThrottler := newMockThrottler("alt", 200)

		switcher := NewThrottlerSwitcher(valueLoader, mainThrottler, altThrottler)

		// Test concurrent access to different methods
		done := make(chan bool, 3)

		go func() {
			ctx := context.Background()
			_, _ = switcher.CheckLimitReached(ctx, 10)
			done <- true
		}()

		go func() {
			switcher.ResponseCodeReceived(200)
			done <- true
		}()

		go func() {
			_ = switcher.GetLimit()
			done <- true
		}()

		// Wait for all goroutines to complete
		for range 3 {
			<-done
		}

		// Verify operations completed without deadlock
		require.True(t, true) // If we reach here, no deadlock occurred
	})
}

// mockValueLoader implements config.ValueLoader[bool] for testing
type mockValueLoader struct {
	value bool
}

func (m *mockValueLoader) Load() bool {
	return m.value
}

func (m *mockValueLoader) setValue(value bool) {
	m.value = value
}

// mockThrottler implements types.Throttler for testing
type mockThrottler struct {
	name                      string
	checkLimitReachedCalls    []checkLimitCall
	responseCodeReceivedCalls []int
	shutdownCalled            bool
	limit                     int64
	limitReached              bool
	checkError                error
}

type checkLimitCall struct {
	ctx  context.Context
	cost int64
}

func newMockThrottler(name string, limit int64) *mockThrottler {
	return &mockThrottler{
		name:  name,
		limit: limit,
	}
}

func (m *mockThrottler) CheckLimitReached(ctx context.Context, cost int64) (limited bool, retErr error) {
	m.checkLimitReachedCalls = append(m.checkLimitReachedCalls, checkLimitCall{ctx: ctx, cost: cost})
	return m.limitReached, m.checkError
}

func (m *mockThrottler) ResponseCodeReceived(code int) {
	m.responseCodeReceivedCalls = append(m.responseCodeReceivedCalls, code)
}

func (m *mockThrottler) Shutdown() {
	m.shutdownCalled = true
}

func (m *mockThrottler) GetLimit() int64 {
	return m.limit
}

func (m *mockThrottler) setLimitReached(limitReached bool) {
	m.limitReached = limitReached
}

func (m *mockThrottler) setCheckError(err error) {
	m.checkError = err
}
