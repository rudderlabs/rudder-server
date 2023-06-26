package misc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/misc"
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

func TestRetryWithNotify(t *testing.T) {
	t.Run("test if notify func is called", func(t *testing.T) {
		expectedAttempt := []int{1, 2}
		receivedAttempt := make([]int, 0)
		notify := func(attempt int) {
			receivedAttempt = append(receivedAttempt, attempt)
		}
		op := operation{
			timeout:             time.Second,
			timeoutAfterAttempt: 2,
		}
		err := misc.RetryWithNotify(context.Background(), time.Millisecond, 3, op.do, notify)
		require.NoError(t, err)
		require.Equal(t, expectedAttempt, receivedAttempt)
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

func TestQueryWithRetries(t *testing.T) {
	t.Run("ideal case: no timeout", func(t *testing.T) {
		var op operation
		_, err := misc.QueryWithRetries(context.Background(), time.Millisecond, 3, op.doAndReturn)
		require.NoError(t, err)
	})

	t.Run("error returned after maximum retry i.e. 3", func(t *testing.T) {
		op := operation{
			timeout: time.Second,
		}
		_, err := misc.QueryWithRetries(context.Background(), time.Millisecond, 3, op.doAndReturn)
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

		_, err := misc.QueryWithRetries(ctx, time.Millisecond, 5, op.doAndReturn)
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled))
		require.Equal(t, 0, op.attempts)
	})

	t.Run("1st attempt failed & 2nd attempt succeeded", func(t *testing.T) {
		op := operation{
			timeout:             time.Second,
			timeoutAfterAttempt: 1,
		}
		_, err := misc.QueryWithRetries(context.Background(), time.Millisecond, 3, op.doAndReturn)
		require.Equal(t, 2, op.attempts)
		require.NoError(t, err)
	})
}

func TestQueryWithRetriesNotify(t *testing.T) {
	t.Run("test if notify func is called", func(t *testing.T) {
		expectedAttempt := []int{1, 2}
		receivedAttempt := make([]int, 0)
		notify := func(attempt int) {
			receivedAttempt = append(receivedAttempt, attempt)
		}
		op := operation{
			timeout:             time.Second,
			timeoutAfterAttempt: 2,
		}
		_, err := misc.QueryWithRetriesAndNotify(context.Background(), time.Millisecond, 3, op.doAndReturn, notify)
		require.NoError(t, err)
		require.Equal(t, expectedAttempt, receivedAttempt)
	})
}

func (o *operation) doAndReturn(ctx context.Context) (interface{}, error) {
	o.attempts++
	var err error
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if o.timeoutAfterAttempt > 0 && o.attempts > o.timeoutAfterAttempt {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(o.timeout):
		return nil, err
	}
}
