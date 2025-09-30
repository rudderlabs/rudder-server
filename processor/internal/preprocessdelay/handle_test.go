package preprocessdelay_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/processor/internal/preprocessdelay"
)

func TestHandle(t *testing.T) {
	t.Run("with delay", func(t *testing.T) {
		t.Run("no received at calls", func(t *testing.T) {
			sleeper := &mockSleeper{}
			handle := preprocessdelay.NewHandle(10*time.Second, sleeper.Sleep)
			err := handle.Sleep(context.Background())
			require.NoError(t, err)
			require.Len(t, sleeper.calls, 0)
		})

		t.Run("sleeps if latest received at is not met by delay", func(t *testing.T) {
			sleeper := &mockSleeper{}
			handle := preprocessdelay.NewHandle(10*time.Second, sleeper.Sleep)
			handle.JobReceivedAt(time.Now().Add(-1 * time.Second))
			handle.JobReceivedAt(time.Now().Add(-2 * time.Second))
			handle.JobReceivedAt(time.Now().Add(-3 * time.Second))
			handle.JobReceivedAt(time.Now().Add(-4 * time.Second))
			handle.JobReceivedAt(time.Now().Add(-5 * time.Second))
			err := handle.Sleep(context.Background())
			require.NoError(t, err)
			require.Len(t, sleeper.calls, 1)
			require.GreaterOrEqual(t, sleeper.calls[0], 8*time.Second)
			require.Less(t, sleeper.calls[0], 10*time.Second)
		})

		t.Run("doesn't sleep if latest received at is met by delay", func(t *testing.T) {
			sleeper := &mockSleeper{}
			handle := preprocessdelay.NewHandle(10*time.Second, sleeper.Sleep)
			handle.JobReceivedAt(time.Now().Add(-15 * time.Second))
			err := handle.Sleep(context.Background())
			require.NoError(t, err)
			require.Len(t, sleeper.calls, 0)
		})

		t.Run("cancelled context", func(t *testing.T) {
			sleeper := &mockSleeper{}
			handle := preprocessdelay.NewHandle(10*time.Second, sleeper.Sleep)
			handle.JobReceivedAt(time.Now().Add(-5 * time.Second))
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := handle.Sleep(ctx)
			require.Error(t, err)
			require.Len(t, sleeper.calls, 1)
		})

		t.Run("sleeper error", func(t *testing.T) {
			sleeper := &mockSleeper{err: errors.New("sleeper error")}
			handle := preprocessdelay.NewHandle(10*time.Second, sleeper.Sleep)
			handle.JobReceivedAt(time.Now().Add(-5 * time.Second))
			err := handle.Sleep(context.Background())
			require.Error(t, err)
			require.Len(t, sleeper.calls, 1)
		})

		t.Run("default sleeper", func(t *testing.T) {
			handle := preprocessdelay.NewHandle(1*time.Second, nil)
			handle.JobReceivedAt(time.Now().Add(-500 * time.Millisecond))
			start := time.Now()
			err := handle.Sleep(context.Background())
			require.NoError(t, err)
			require.GreaterOrEqual(t, time.Since(start), 500*time.Millisecond)

			t.Run("with cancelled context", func(t *testing.T) {
				handle := preprocessdelay.NewHandle(10*time.Second, nil)
				handle.JobReceivedAt(time.Now().Add(-5 * time.Second))
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				start := time.Now()
				err := handle.Sleep(ctx)
				require.Error(t, err)
				require.Less(t, time.Since(start), 1*time.Second)
			})
		})
	})

	t.Run("without delay", func(t *testing.T) {
		t.Run("doesn't sleep", func(t *testing.T) {
			sleeper := &mockSleeper{err: errors.New("should not be called")}
			handle := preprocessdelay.NewHandle(0, sleeper.Sleep)
			handle.JobReceivedAt(time.Now().Add(-10 * time.Second))
			err := handle.Sleep(context.Background())
			require.NoError(t, err)
		})
		t.Run("with cancelled context", func(t *testing.T) {
			sleeper := &mockSleeper{err: errors.New("should not be called")}
			handle := preprocessdelay.NewHandle(0, sleeper.Sleep)
			handle.JobReceivedAt(time.Now().Add(-10 * time.Second))
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := handle.Sleep(ctx)
			require.NoError(t, err)
		})
	})
}

type mockSleeper struct {
	calls []time.Duration
	err   error
}

func (m *mockSleeper) Sleep(ctx context.Context, d time.Duration) error {
	m.calls = append(m.calls, d)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return m.err
}
