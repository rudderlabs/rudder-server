package misc_test

import (
	"context"
	"testing"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/require"
)

func TestAsyncInit(t *testing.T) {
	asyncInit := misc.NewAsyncInit(2)

	select {
	case <-asyncInit.Wait():
		require.Fail(t, "should not be done yet")
	default:
	}

	asyncInit.Done()
	select {
	case <-asyncInit.Wait():
		require.Fail(t, "should not be done yet")
	default:
	}

	asyncInit.Done()

	select {
	case <-asyncInit.Wait():
	default:
		require.Fail(t, "should be done already")
	}
}

func TestAsyncInitContext(t *testing.T) {
	asyncInit := misc.NewAsyncInit(1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.Error(t, asyncInit.WaitContext(ctx), "the context error should be returned")

	ctx, cancel = context.WithCancel(context.Background())
	asyncInit.Done()
	defer cancel()
	require.NoError(t, asyncInit.WaitContext(ctx), "no error should be returned")
}
