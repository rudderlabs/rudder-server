package misc_test

import (
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/require"
)

func TestExponentialNumber(t *testing.T) {
	t.Run("exponential int", func(t *testing.T) {
		var exp misc.ExponentialNumber[int]
		require.Equal(t, 1, exp.Next(1, 10))
		require.Equal(t, 2, exp.Next(1, 10))
		require.Equal(t, 4, exp.Next(1, 10))
		require.Equal(t, 8, exp.Next(1, 10))
		require.Equal(t, 10, exp.Next(1, 10))
		require.Equal(t, 10, exp.Next(1, 10))
		exp.Reset()
		require.Equal(t, 1, exp.Next(1, 10))
	})

	t.Run("exponential duration", func(t *testing.T) {
		var exp misc.ExponentialNumber[time.Duration]
		require.Equal(t, 1*time.Second, exp.Next(time.Second, 10*time.Second))
		require.Equal(t, 2*time.Second, exp.Next(time.Second, 10*time.Second))
		require.Equal(t, 4*time.Second, exp.Next(time.Second, 10*time.Second))
		require.Equal(t, 8*time.Second, exp.Next(time.Second, 10*time.Second))
		require.Equal(t, 10*time.Second, exp.Next(time.Second, 10*time.Second))
		require.Equal(t, 10*time.Second, exp.Next(time.Second, 10*time.Second))
		exp.Reset()
		require.Equal(t, 1*time.Second, exp.Next(time.Second, 10*time.Second))
	})
}
