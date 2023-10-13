package batchrouter_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/router/batchrouter"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func TestJobParameters(t *testing.T) {
	t.Run("ParseReceivedAtTime", func(t *testing.T) {
		refTime := time.Now().UTC().Truncate(time.Millisecond)
		t.Run("invalid string", func(t *testing.T) {
			jp := batchrouter.JobParameters{
				ReceivedAt: refTime.Format(misc.RFC3339Milli),
			}
			require.Equal(t, refTime, jp.ParseReceivedAtTime())
		})

		t.Run("empty string", func(t *testing.T) {
			var jp batchrouter.JobParameters
			require.True(t, jp.ParseReceivedAtTime().IsZero(), "an empty ReceivedAt should return a zero value time")
		})

		t.Run("invalid string", func(t *testing.T) {
			jp := batchrouter.JobParameters{
				ReceivedAt: "invalid",
			}
			require.True(t, jp.ParseReceivedAtTime().IsZero(), "an invalid ReceivedAt should return a zero value time")
		})
	})
}
