package safeguard_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/safeguard"

	"github.com/stretchr/testify/require"
)

func TestMustStop(t *testing.T) {
	t.Run("no panic: if context not canceled", func(t *testing.T) {
		stop := safeguard.MustStop(context.Background(), 1*time.Millisecond)
		<-time.After(3 * time.Millisecond)
		stop()
	})

	t.Run("no panic: if within timeout", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		stop := safeguard.MustStop(ctx, 2*time.Millisecond)
		stop()
	})

	t.Run("panic: timeout before stop called", func(t *testing.T) {
		t.Helper()
		wg := sync.WaitGroup{}

		Go := func(fn func()) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.Panics(t, fn)
			}()
		}

		g := &safeguard.Guard{
			Go: Go,
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_ = g.MustStop(ctx, 1*time.Millisecond)

		wg.Wait()
	})
}
