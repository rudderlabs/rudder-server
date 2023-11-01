package reporting

import (
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

func (es *EventStatsReporter) Report(metrics []*types.PUReportedMetric) {
	// tracking delivery event stats which has state - succeeded, aborted
	for index := range metrics {
		if !(metrics[index].StatusDetail.Status == "aborted" || metrics[index].StatusDetail.Status == "succeeded") {
			continue
		}
		tags := stats.Tags{
			"workspaceId":     es.configSubscriber.WorkspaceIDFromSource(metrics[index].ConnectionDetails.SourceID),
			"sourceId":        metrics[index].ConnectionDetails.SourceID,
			"destinationId":   metrics[index].ConnectionDetails.DestinationID,
			"reportedBy":      metrics[index].PUDetails.PU,
			"sourceCategory":  metrics[index].ConnectionDetails.SourceCategory,
			"terminal":        strconv.FormatBool(metrics[index].PUDetails.TerminalPU),
			"status_code":     strconv.Itoa(metrics[index].StatusDetail.StatusCode),
			"destinationType": es.configSubscriber.GetDestDetail(metrics[index].ConnectionDetails.DestinationID).destType,
		}
		metricName := EventsDeliveredMetricName
		if metrics[index].StatusDetail.Status == "aborted" {
			metricName = EventsAbortedMetricName
		}
		es.stats.NewTaggedStat(metricName, stats.CountType, tags).Count(int(metrics[index].StatusDetail.Count))
	}
}
