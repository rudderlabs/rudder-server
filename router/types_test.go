package router_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func TestJobParameters(t *testing.T) {
	t.Run("ParseReceivedAtTime", func(t *testing.T) {
		refTime := time.Now().UTC().Truncate(time.Millisecond)
		t.Run("valid string", func(t *testing.T) {
			jp := routerutils.JobParameters{
				ReceivedAt: refTime.Format(misc.RFC3339Milli),
			}
			require.Equal(t, refTime, jp.ParseReceivedAtTime())
		})

		t.Run("empty string", func(t *testing.T) {
			var jp routerutils.JobParameters
			require.True(t, jp.ParseReceivedAtTime().IsZero(), "an empty ReceivedAt should return a zero value time")
		})

		t.Run("invalid string", func(t *testing.T) {
			jp := routerutils.JobParameters{
				ReceivedAt: "invalid",
			}
			require.True(t, jp.ParseReceivedAtTime().IsZero(), "an invalid ReceivedAt should return a zero value time")
		})
	})
}
