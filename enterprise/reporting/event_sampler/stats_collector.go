package event_sampler

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

const (
	StatReportingEventSamplerPutCount    = "reporting_event_sampler_put_count"
	StatReportingEventSamplerGetCount    = "reporting_event_sampler_get_count"
	StatReportingEventSamplerPutDuration = "reporting_event_sampler_put_duration"
	StatReportingEventSamplerGetDuration = "reporting_event_sampler_get_duration"
)

type StatsCollector struct {
	stats       stats.Stats
	getCounter  stats.Measurement
	putCounter  stats.Measurement
	getDuration stats.Measurement
	putDuration stats.Measurement
}

func NewStatsCollector(eventSamplerType string, module string, statsFactory stats.Stats) *StatsCollector {
	tags := getTags(eventSamplerType, module)

	return &StatsCollector{
		stats:       statsFactory,
		getCounter:  statsFactory.NewTaggedStat(StatReportingEventSamplerGetCount, stats.CountType, tags),
		putCounter:  statsFactory.NewTaggedStat(StatReportingEventSamplerPutCount, stats.CountType, tags),
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

func getTags(eventSamplerType string, module string) stats.Tags {
	return stats.Tags{"type": eventSamplerType, "module": module}
}
