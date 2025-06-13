package signal

import (
	"context"
	"os"
	"os/signal"
	"sync"
)

// NotifyContextWithCallback creates a context that listens for OS signals (e.g. SIGINT, SIGTERM)
// and calls the provided callback function when a signal is received, before canceling the context.
func NotifyContextWithCallback(fn func(), signals ...os.Signal) (ctx context.Context, stop context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())

	// Set up signal handling with direct channel for more control
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, signals...)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer signal.Stop(signalCh)

		select {
		case <-signalCh:
			// A signal was received
			fn()
			cancel()
		case <-ctx.Done():
			// Context was canceled by the user
			// Do nothing, just return
		}
	}()
	// If the signal context is not done, we can just wait for it to be done.

	return ctx, func() {
		cancel()
		wg.Wait() // Wait for the goroutine to finish before returning
	}
}
