package misc_test

import (
	"context"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/require"
)

func TestRetryWith(t *testing.T) {

	t.Run("ideal case: no timeout", func(t *testing.T) {
		err := misc.RetryWith(context.Background(), time.Second, 2, func(ctx context.Context) error {
			return nil
		})
		require.NoError(t, err)
	})

	t.Run("error returned after maximum retry i.e. 3", func(t *testing.T) {
		attempt := 0
		err := misc.RetryWith(context.Background(), time.Second, 3, func(ctx context.Context) error {
			attempt++
			return context.DeadlineExceeded
		})
		require.Equal(t, 3, attempt)
		require.Error(t, err)
	})

	t.Run("parents context canceled", func(t *testing.T) {
		attempt := 0
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		go func() {
			time.Sleep(time.Second * 1)
			cancel()
		}()
		err := misc.RetryWith(ctx, time.Second, 5, func(ctx context.Context) error {
			attempt++
			time.Sleep(time.Second * 1)
			return context.DeadlineExceeded
		})
		require.Equal(t, 1, attempt)
		require.Error(t, err)
	})

	t.Run("1st attempt failed & 2nd attempt succeeded", func(t *testing.T) {
		attempt := 0
		err := misc.RetryWith(context.Background(), time.Second, 5, func(ctx context.Context) error {

			attempt++
			if attempt == 1 {
				return context.DeadlineExceeded
			}
			return nil
		})
		require.Equal(t, 2, attempt)
		require.NoError(t, err)
	})

}
