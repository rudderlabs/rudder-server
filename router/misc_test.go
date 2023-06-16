package router_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	kitsync "github.com/rudderlabs/rudder-go-kit/sync"
	"github.com/rudderlabs/rudder-server/router"
)

func TestLimiterPriorityValueFrom(t *testing.T) {
	require.Equal(t, kitsync.LimiterPriorityValueLow, router.LimiterPriorityValueFrom(-1, 100), "negative value should correspond to lowest priority")
	require.Equal(t, kitsync.LimiterPriorityValueHigh, router.LimiterPriorityValueFrom(2, 1), "value larger than the max should correspond to highest priority")

	require.Equal(t, kitsync.LimiterPriorityValueLow, router.LimiterPriorityValueFrom(0, 100))
	require.Equal(t, kitsync.LimiterPriorityValueLow, router.LimiterPriorityValueFrom(1, 100))
	require.Equal(t, kitsync.LimiterPriorityValueLow, router.LimiterPriorityValueFrom(25, 100))

	require.Equal(t, kitsync.LimiterPriorityValueMedium, router.LimiterPriorityValueFrom(26, 100))
	require.Equal(t, kitsync.LimiterPriorityValueMedium, router.LimiterPriorityValueFrom(50, 100))

	require.Equal(t, kitsync.LimiterPriorityValueMediumHigh, router.LimiterPriorityValueFrom(51, 100))
	require.Equal(t, kitsync.LimiterPriorityValueMediumHigh, router.LimiterPriorityValueFrom(75, 100))

	require.Equal(t, kitsync.LimiterPriorityValueHigh, router.LimiterPriorityValueFrom(76, 100))
	require.Equal(t, kitsync.LimiterPriorityValueHigh, router.LimiterPriorityValueFrom(100, 100))
}
