package reporting

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/utils/types"
)

const (
	EventsDeliveredMetricName = "events_delivered_total"
	EventsAbortedMetricName   = "events_aborted_total"
)

type EventStatsReporter struct {
	stats            stats.Stats
	configSubscriber *configSubscriber
}

func NewEventStatsReporter(configSubscriber *configSubscriber, stats stats.Stats) *EventStatsReporter {
	return &EventStatsReporter{
		stats:            stats,
		configSubscriber: configSubscriber,
	}
}

func (es *EventStatsReporter) Report(metrics []*types.PUReportedMetric, _ *sql.Tx) error {
	// tracking delivery event stats which has state - succeeded, aborted
	for _, metric := range metrics {
		metric := metric
		if !(metric.StatusDetail.Status == "aborted" || metric.StatusDetail.Status == "succeeded") {
			continue
		}
		tags := stats.Tags{
			"workspaceId":    es.configSubscriber.WorkspaceIDFromSource(metric.ConnectionDetails.SourceID),
			"sourceId":       metric.ConnectionDetails.SourceID,
			"destinationId":  metric.ConnectionDetails.DestinationID,
			"reportedBy":     metric.PUDetails.PU,
			"sourceCategory": metric.ConnectionDetails.SourceCategory,
			"terminal":       strconv.FormatBool(metric.PUDetails.TerminalPU),
		}
		fmt.Println(tags)
		metricName := EventsDeliveredMetricName
		if metric.StatusDetail.Status == "aborted" {
			metricName = EventsAbortedMetricName
		}
		es.stats.NewTaggedStat(metricName, stats.CountType, tags).Count(int(metric.StatusDetail.Count))
	}
	return nil
}

func (es *EventStatsReporter) DatabaseSyncer(types.SyncerConfig) types.ReportingSyncer {
	return func() {}
}
