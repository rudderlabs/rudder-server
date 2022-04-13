package waitgroup

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWaitGroup(t *testing.T) {
	start := time.Now()
	gr1, gr2 := make(chan struct{}), make(chan struct{})

	wg := New()
	wg.Add(2)
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		close(gr1)
	}()
	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		close(gr2)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, wg.Wait(ctx))
	require.InDelta(t, 50*time.Millisecond, time.Since(start), float64(10*time.Millisecond))

	select {
	case <-gr1:
	default:
		t.Fatal("Go routine 1 was not executed")
	}
	select {
	case <-gr2:
	default:
		t.Fatal("Go routine 2 was not executed")
	}
}

func TestWaitGroupTimeout(t *testing.T) {
	done := make(chan struct{})

	wg := New()
	wg.Add(2)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
	}()
	go func() {
		defer wg.Done()
		// this will cause Wait() to error since the context will have a 50ms timeout and here we sleep for 100ms
		time.Sleep(100 * time.Millisecond)
		close(done)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, wg.Wait(ctx), context.DeadlineExceeded)
	<-done
}
