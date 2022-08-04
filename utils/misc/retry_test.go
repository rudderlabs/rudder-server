package misc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/require"
)

func TestRetryWith(t *testing.T) {
	t.Run("ideal case: no timeout", func(t *testing.T) {
		var op operation
		err := misc.RetryWith(context.Background(), time.Millisecond, 3, op.do)
		require.NoError(t, err)
	})

	t.Run("error returned after maximum retry i.e. 3", func(t *testing.T) {
		op := operation{
			timeout: time.Second,
		}
		err := misc.RetryWith(context.Background(), time.Millisecond, 3, op.do)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.DeadlineExceeded))
		require.Equal(t, 3, op.attempts)
	})

	t.Run("parent context canceled", func(t *testing.T) {
		op := operation{
			timeout: time.Second,
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := misc.RetryWith(ctx, time.Millisecond, 5, op.do)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
		require.Equal(t, 0, op.attempts)
	})

	t.Run("1st attempt failed & 2nd attempt succeeded", func(t *testing.T) {
		op := operation{
			timeout:             time.Second,
			timeoutAfterAttempt: 1,
		}
		err := misc.RetryWith(context.Background(), time.Millisecond, 3, op.do)
		require.Equal(t, 2, op.attempts)
		require.NoError(t, err)
	})
}

type operation struct {
	attempts            int
	timeout             time.Duration
	timeoutAfterAttempt int
}

func (o *operation) do(ctx context.Context) error {
	o.attempts++
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if o.timeoutAfterAttempt > 0 && o.attempts > o.timeoutAfterAttempt {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(o.timeout):
		return nil
	}
}
