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
	signalCtx, signalCancel := signal.NotifyContext(context.Background(), signals...)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-signalCtx.Done():
			// If the signal context is done, we log an event and cancel our context.
			fn()
			cancel()
		case <-ctx.Done():
			signalCancel() // If our context is done, we cancel the signal context.
		}
	}()
	// If the signal context is not done, we can just wait for it to be done.

	return ctx, func() {
		cancel()
		signalCancel()
		wg.Wait() // Wait for the goroutine to finish before returning
	}
}
