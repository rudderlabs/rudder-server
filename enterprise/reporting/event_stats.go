package reporting

import (
	"strconv"

	"github.com/rudderlabs/rudder-go-kit/stats"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
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

const EventsProcessedMetricName = "events_processed_total"

func (es *EventStatsReporter) Record(metrics []*types.PUReportedMetric) {
	for index := range metrics {
		tags := stats.Tags{
			"workspaceId":     es.configSubscriber.WorkspaceIDFromSource(metrics[index].ConnectionDetails.SourceID),
			"sourceId":        metrics[index].ConnectionDetails.SourceID,
			"destinationId":   metrics[index].ConnectionDetails.DestinationID,
			"reportedBy":      metrics[index].PUDetails.PU,
			"sourceCategory":  metrics[index].ConnectionDetails.SourceCategory,
			"terminal":        strconv.FormatBool(metrics[index].PUDetails.TerminalPU),
			"status_code":     strconv.Itoa(metrics[index].StatusDetail.StatusCode),
			"destinationType": es.configSubscriber.GetDestDetail(metrics[index].ConnectionDetails.DestinationID).destType,
			"status":          metrics[index].StatusDetail.Status,
		}
		es.stats.NewTaggedStat(EventsProcessedMetricName, stats.CountType, tags).Count(int(metrics[index].StatusDetail.Count))
	}
}

func (es *EventStatsReporter) Report(metrics []*types.PUReportedMetric, tx *Tx) error {
	tx.AddSuccessListener(func() {
		es.Record(metrics)
	})
	return nil
}

func (es *EventStatsReporter) Stop() {
}

func (es *EventStatsReporter) DatabaseSyncer(c types.SyncerConfig) types.ReportingSyncer {
	return func() {}
}
