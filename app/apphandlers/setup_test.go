package apphandlers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestTerminalErrorFunction(t *testing.T) {
	t.Run("calling terminalErrFn", func(t *testing.T) {
		g, ctx := errgroup.WithContext(context.Background())
		terminalErrFn := terminalErrorFunction(ctx, g)

		g.Go(func() error { // this go routine will not return an error but call the terminalErrFn
			terminalErrFn(fmt.Errorf("terminal error"))
			return nil
		})

		g.Go(func() error { // this go routine will wait for the context to be cancelled and return nil
			<-ctx.Done()
			return nil
		})

		err := g.Wait() // the error returned by the first go routine shall be returned by g.Wait()
		require.Error(t, err)
		require.EqualError(t, err, "terminal error")
	})

	t.Run("context canceled without calling terminalErrFn", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		g, ctx := errgroup.WithContext(ctx)
		_ = terminalErrorFunction(ctx, g)

		g.Go(func() error { // this go routine will wait for the context to be cancelled and return nil
			<-ctx.Done()
			return nil
		})

		cancel()                     // cancel the context
		require.NoError(t, g.Wait()) // all go routines shall return nil
	})
}
