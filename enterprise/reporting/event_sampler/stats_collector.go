package event_sampler

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

const (
	StatReportingEventSamplerRequestsTotal   = "reporting_event_sampler_requests_total"
	StatReportingEventSamplerRequestDuration = "reporting_event_sampler_request_duration_seconds"
	StatReportingBadgerDBSize                = "reporting_badger_db_size_bytes"
)

type StatsCollector struct {
	module      string
	stats       stats.Stats
	getCounter  stats.Measurement
	putCounter  stats.Measurement
	getDuration stats.Measurement
	putDuration stats.Measurement
}

func NewStatsCollector(eventSamplerType, module string, statsFactory stats.Stats) *StatsCollector {
	getRequestTags := getTags(eventSamplerType, module, "get")
	putRequestTags := getTags(eventSamplerType, module, "put")

	return &StatsCollector{
		module:      module,
		stats:       statsFactory,
		getCounter:  statsFactory.NewTaggedStat(StatReportingEventSamplerRequestsTotal, stats.CountType, getRequestTags),
		putCounter:  statsFactory.NewTaggedStat(StatReportingEventSamplerRequestsTotal, stats.CountType, putRequestTags),
		getDuration: statsFactory.NewTaggedStat(StatReportingEventSamplerRequestDuration, stats.TimerType, getRequestTags),
		putDuration: statsFactory.NewTaggedStat(StatReportingEventSamplerRequestDuration, stats.TimerType, putRequestTags),
	}
}

func (sc *StatsCollector) RecordGet() {
	sc.getCounter.Increment()
}

func (sc *StatsCollector) RecordPut() {
	sc.putCounter.Increment()
}

func (sc *StatsCollector) RecordGetDuration(start time.Time) {
	sc.getDuration.SendTiming(time.Since(start))
}

func (sc *StatsCollector) RecordPutDuration(start time.Time) {
	sc.putDuration.SendTiming(time.Since(start))
}

func (sc *StatsCollector) RecordBadgerDBSize(usageType string, size int64) {
	sc.stats.NewTaggedStat(StatReportingBadgerDBSize, stats.GaugeType, stats.Tags{"module": sc.module, "usageType": usageType}).Gauge(size)
}

func getTags(eventSamplerType, module, operation string) stats.Tags {
	return stats.Tags{"type": eventSamplerType, "module": module, "operation": operation}
}
