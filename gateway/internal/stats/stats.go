package stats

import (
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

type SourceStat struct {
	Source string

	WriteKey    string
	ReqType     string
	SourceID    string
	WorkspaceID string
	SourceType  string
	Version     string

	reason string

	requests struct {
		total int

		succeeded  int
		failed     int
		dropped    int
		suppressed int
	}
	events struct {
		total int
		bot   int

		succeeded int
		failed    int
	}
}

// RequestSucceeded increments the requests total & succeeded counters by one
func (ss *SourceStat) RequestSucceeded() {
	ss.requests.total++
	ss.requests.succeeded++
}

// RequestDropped increments the requests total & dropped counters by one
func (ss *SourceStat) RequestDropped() {
	ss.requests.total++
	ss.requests.dropped++
}

// RequestSuppressed increments the requests total & suppressed counters by one
func (ss *SourceStat) RequestSuppressed() {
	ss.requests.total++
	ss.requests.suppressed++
}

// RequestFailed increments the requests total & failed counters by one
func (ss *SourceStat) RequestFailed(reason string) {
	ss.requests.total++
	ss.requests.failed++
	ss.reason = reason
}

// RequestEventsSucceeded increments the requests total & succeeded counters by one, and the events total & succeeded counters by num
func (ss *SourceStat) RequestEventsSucceeded(num int) {
	ss.events.succeeded += num
	ss.events.total += num
	ss.requests.total++
	ss.requests.succeeded++
}

// RequestEventsFailed increments the requests total & failed counters by one, and the events total & failed counters by num
func (ss *SourceStat) RequestEventsFailed(num int, reason string) {
	ss.requests.total++
	ss.requests.failed++
	ss.events.failed += num
	ss.events.total += num
	ss.reason = reason
}

// EventsSuccess increments the events total & succeeded counters by num
func (ss *SourceStat) EventsSuccess(num int) {
	ss.events.succeeded += num
	ss.events.total += num
}

// EventsFailed increments the events total & failed counters by num
func (ss *SourceStat) EventsFailed(num int, reason string) {
	ss.events.failed += num
	ss.events.total += num
	ss.reason = reason
}

func (ss *SourceStat) RequestEventsBot(num int) {
	ss.events.bot += num
}

// Report captured stats
func (ss *SourceStat) Report(s stats.Stats) {
	tags := stats.Tags{
		"source":      ss.Source,
		"writeKey":    ss.WriteKey,
		"reqType":     ss.ReqType,
		"workspaceId": ss.WorkspaceID,
		"sourceID":    ss.SourceID,
		"sourceType":  ss.SourceType,
		"sdkVersion":  ss.Version,
	}

	failedTags := lo.Assign(tags)
	if ss.reason != "" {
		failedTags["reason"] = ss.reason
	}
	if ss.requests.total > 0 {
		s.NewTaggedStat("gateway.write_key_requests", stats.CountType, tags).Count(ss.requests.total)
		s.NewTaggedStat("gateway.write_key_successful_requests", stats.CountType, tags).Count(ss.requests.succeeded)
		s.NewTaggedStat("gateway.write_key_failed_requests", stats.CountType, failedTags).Count(ss.requests.failed)
		s.NewTaggedStat("gateway.write_key_dropped_requests", stats.CountType, tags).Count(ss.requests.dropped)
		s.NewTaggedStat("gateway.write_key_suppressed_requests", stats.CountType, tags).Count(ss.requests.suppressed)
	}
	if ss.events.total > 0 {
		s.NewTaggedStat("gateway.write_key_events", stats.CountType, tags).Count(ss.events.total)
		s.NewTaggedStat("gateway.write_key_successful_events", stats.CountType, tags).Count(ss.events.succeeded)
		s.NewTaggedStat("gateway.write_key_failed_events", stats.CountType, failedTags).Count(ss.events.failed)

		if ss.events.bot > 0 {
			s.NewTaggedStat("gateway.write_key_bot_events", stats.CountType, tags).Count(ss.events.bot)
		}
	}
}
