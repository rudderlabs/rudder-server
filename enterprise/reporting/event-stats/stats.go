package event_stats

import (
	"strconv"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/types"
)

const (
	EventsDeliveredMetricName = "events_delivered_total"
	EventsAbortedMetricName   = "events_aborted_total"
)

func Report(workspaceId string, metric *types.PUReportedMetric) {
	// tracking delivery event stats which has state - succeeded, aborted
	if !(metric.StatusDetail.Status == "aborted" || metric.StatusDetail.Status == "succeeded") {
		return
	}

	tags := map[string]string{
		"workspaceId":    workspaceId,
		"sourceId":       metric.ConnectionDetails.SourceID,
		"destinationId":  metric.ConnectionDetails.DestinationID,
		"reportedBy":     metric.PUDetails.PU,
		"sourceCategory": metric.ConnectionDetails.SourceCategory,
		"terminal":       strconv.FormatBool(metric.PUDetails.TerminalPU),
	}

	metricName := EventsDeliveredMetricName
	if metric.StatusDetail.Status == "aborted" {
		metricName = EventsAbortedMetricName
	}
	stats.Default.NewTaggedStat(metricName, stats.CountType, tags).Count(int(metric.StatusDetail.Count))
}
