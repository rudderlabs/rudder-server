package reporting

import (
	"context"
	"strconv"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
)

type EventStatsReporter struct {
	stats            stats.Stats
	configSubscriber *configSubscriber
}

const EventStream = "event-stream"

func NewEventStatsReporter(configSubscriber *configSubscriber, stats stats.Stats) *EventStatsReporter {
	return &EventStatsReporter{
		stats:            stats,
		configSubscriber: configSubscriber,
	}
}

const EventsProcessedMetricName = "events_processed_total"

func (es *EventStatsReporter) Record(metrics []*types.PUReportedMetric) {
	for index := range metrics {
		sourceCategory := metrics[index].SourceCategory
		if sourceCategory == "" {
			sourceCategory = EventStream
		}
		terminal := strconv.FormatBool(metrics[index].TerminalPU)
		status := metrics[index].StatusDetail.Status
		if status == jobsdb.Aborted.State {
			terminal = "true"
		}
		tags := stats.Tags{
			"workspaceId":     es.configSubscriber.WorkspaceIDFromSource(metrics[index].SourceID),
			"sourceId":        metrics[index].SourceID,
			"destinationId":   metrics[index].DestinationID,
			"reportedBy":      metrics[index].PU,
			"sourceCategory":  sourceCategory,
			"statusCode":      strconv.Itoa(metrics[index].StatusDetail.StatusCode),
			"terminal":        terminal,
			"destinationType": es.configSubscriber.GetDestDetail(metrics[index].DestinationID).destType,
			"status":          status,
			"trackingPlanId":  metrics[index].TrackingPlanID,
		}
		es.stats.NewTaggedStat(EventsProcessedMetricName, stats.CountType, tags).Count(int(metrics[index].StatusDetail.Count))
	}
}

func (es *EventStatsReporter) Report(_ context.Context, metrics []*types.PUReportedMetric, tx *Tx) error {
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
