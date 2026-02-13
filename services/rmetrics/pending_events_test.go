package rmetrics_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats/metric"
	"github.com/rudderlabs/rudder-server/services/rmetrics"
)

func TestPendingEventsRegistry(t *testing.T) {
	mi := metric.Instance
	mi.Reset()
	defer mi.Reset()
	const (
		tablePrefix    = "tablePrefix"
		workspaceID    = "workspaceID"
		destType       = "destType"
		destinationID  = "destinationID"
		destinationID1 = "destinationID1"
	)
	t.Run("default", func(t *testing.T) {
		mi.Reset()
		r := rmetrics.NewPendingEventsRegistry()
		r.IncreasePendingEvents(tablePrefix, workspaceID, destType, destinationID, 2)
		require.EqualValues(t, 2, r.PendingEvents(tablePrefix, workspaceID, destType, destinationID).IntValue())
		r.IncreasePendingEvents(tablePrefix, workspaceID, destType, destinationID1, 1)
		require.EqualValues(t, 1, r.PendingEvents(tablePrefix, workspaceID, destType, destinationID1).IntValue())
		r.DecreasePendingEvents(tablePrefix, workspaceID, destType, destinationID, 1)
		require.EqualValues(t, 1, r.PendingEvents(tablePrefix, workspaceID, destType, destinationID).IntValue())

		mi.GetRegistry(metric.PublishedMetrics).Range(func(key, value any) bool {
			require.FailNow(t, "unexpected metric in published metrics")
			return false
		})
		r.Publish()
		var metricsCount int
		mi.GetRegistry(metric.PublishedMetrics).Range(func(key, value any) bool {
			metricsCount++
			return true
		})
		require.Equal(t, 6, metricsCount, "for each pending event, 3 gauges are created, plus 2 aggregate gauges")
		r.Publish() // should be a no-op

		r.Reset()
		mi.GetRegistry(metric.PublishedMetrics).Range(func(key, value any) bool {
			require.FailNow(t, "unexpected metric in published metrics")
			return false
		})

		r.IncreasePendingEvents(tablePrefix, workspaceID, destType, destinationID, 2)
		r.IncreasePendingEvents(tablePrefix, workspaceID, destType, destinationID1, 1)
		r.DecreasePendingEvents(tablePrefix, workspaceID, destType, destinationID, 1)
		mi.GetRegistry(metric.PublishedMetrics).Range(func(key, value any) bool {
			require.FailNow(t, "unexpected metric in published metrics")
			return false
		})
		r.Publish()
		metricsCount = 0
		mi.GetRegistry(metric.PublishedMetrics).Range(func(key, value any) bool {
			metricsCount++
			return true
		})
		require.Equal(t, 6, metricsCount, "a publish after a reset should publish any pending events recorded after reset")
	})

	t.Run("published", func(t *testing.T) {
		mi.Reset()
		r := rmetrics.NewPendingEventsRegistry(rmetrics.WithPublished())
		r.IncreasePendingEvents(tablePrefix, workspaceID, destType, destinationID, 1)
		r.DecreasePendingEvents(tablePrefix, workspaceID, destType, destinationID, 1)
		require.EqualValues(t, 0, r.PendingEvents(tablePrefix, workspaceID, destType, destinationID).IntValue())
		var metricsCount int
		mi.GetRegistry(metric.PublishedMetrics).Range(func(key, value any) bool {
			metricsCount++
			return true
		})
		require.Equal(t, 5, metricsCount, "for each pending event, 3 gauges are created, plus 2 aggregate gauges")
		r.Publish() // should be a no-op
		r.Reset()
		mi.GetRegistry(metric.PublishedMetrics).Range(func(key, value any) bool {
			require.FailNow(t, "unexpected metric in published metrics")
			return false
		})
		r.IncreasePendingEvents(tablePrefix, workspaceID, destType, destinationID, 1)
		mi.GetRegistry(metric.PublishedMetrics).Range(func(key, value any) bool {
			require.FailNow(t, "unexpected metric in published metrics")
			return false
		})
		r.Publish()
		metricsCount = 0
		mi.GetRegistry(metric.PublishedMetrics).Range(func(key, value any) bool {
			metricsCount++
			return true
		})
		require.Equal(t, 5, metricsCount, "a publish after a reset should publish any pending events recorded after reset")
	})
}
