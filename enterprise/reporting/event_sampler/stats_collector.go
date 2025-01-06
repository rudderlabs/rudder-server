package event_sampler

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

const (
	StatReportingEventSamplerPutTotal    = "reporting_event_sampler_put_total"
	StatReportingEventSamplerGetTotal    = "reporting_event_sampler_get_total"
	StatReportingEventSamplerPutDuration = "reporting_event_sampler_put_duration_seconds"
	StatReportingEventSamplerGetDuration = "reporting_event_sampler_get_duration_seconds"
)

type StatsCollector struct {
	stats       stats.Stats
	getCounter  stats.Measurement
	putCounter  stats.Measurement
	getDuration stats.Measurement
	putDuration stats.Measurement
}

func NewStatsCollector(eventSamplerType, module string, statsFactory stats.Stats) *StatsCollector {
	tags := getTags(eventSamplerType, module)

	return &StatsCollector{
		stats:       statsFactory,
		getCounter:  statsFactory.NewTaggedStat(StatReportingEventSamplerPutTotal, stats.CountType, tags),
		putCounter:  statsFactory.NewTaggedStat(StatReportingEventSamplerGetTotal, stats.CountType, tags),
		getDuration: statsFactory.NewTaggedStat(StatReportingEventSamplerGetDuration, stats.TimerType, tags),
		putDuration: statsFactory.NewTaggedStat(StatReportingEventSamplerPutDuration, stats.TimerType, tags),
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

func getTags(eventSamplerType, module string) stats.Tags {
	return stats.Tags{"type": eventSamplerType, "module": module}
}
