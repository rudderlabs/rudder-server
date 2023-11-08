package reporting

import (
	"strconv"

	"github.com/rudderlabs/rudder-go-kit/stats"
	. "github.com/rudderlabs/rudder-server/utils/tx" //nolint:staticcheck
	"github.com/rudderlabs/rudder-server/utils/types"
)

var measurementNames map[string]string = map[string]string{
	"succeeded": "events_delivered_total",
	"aborted":   "events_aborted_total",
}

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

func (es *EventStatsReporter) Record(metrics []*types.PUReportedMetric) {
	// tracking delivery event stats which has state - succeeded, aborted
	for index := range metrics {
		if measurement, ok := measurementNames[metrics[index].StatusDetail.Status]; ok {
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
			es.stats.NewTaggedStat(measurement, stats.CountType, tags).Count(int(metrics[index].StatusDetail.Count))
		}
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
