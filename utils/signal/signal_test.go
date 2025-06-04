package signal_test

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	rsignal "github.com/rudderlabs/rudder-server/utils/signal"
)

func TestNotifyContextWithCallback(t *testing.T) {
	t.Run("receives a cancel signal and calls the callback", func(t *testing.T) {
		called := false
		ctx, cancel := rsignal.NotifyContextWithCallback(func() {
			called = true
		}, syscall.SIGINT)
		defer cancel()

		// Simulate a signal being sent
		p, err := os.FindProcess(os.Getpid())
		require.NoError(t, err, "it should be able to find the process")
		require.NoError(t, p.Signal(os.Interrupt))

		select {
		case <-ctx.Done():
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			require.True(t, called, "Callback should have been called")
		case <-time.After(1 * time.Second):
			t.Error("Expected context to be done within 1 second")
		}
	})

	t.Run("does not call the callback if context is canceled by us", func(t *testing.T) {
		called := false
		ctx, cancel := rsignal.NotifyContextWithCallback(func() {
			called = true
		}, syscall.SIGINT)
		cancel() // Cancel the context immediately

		select {
		case <-ctx.Done():
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			require.False(t, called, "Callback should not have been called")
		case <-time.After(1 * time.Second):
			t.Error("Expected context to be done within 1 second")
		}
	})
}
