package snowpipestreaming

import (
	"strconv"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

// hpStats are the metrics of [ManagerV2]. Metrics shared with the classic path carry a version tag.
type hpStats struct {
	factory stats.Stats
	tags    stats.Tags

	jobs struct {
		importing stats.Counter
		succeeded stats.Counter
		failed    stats.Counter
		aborted   stats.Counter
	}
	discards                   stats.Counter
	pollingInProgress          stats.Counter
	duplicateEventsInBatch     stats.Counter
	duplicateEventsDueToOffset stats.Counter
	requestBodySize            stats.Histogram
	// rowErrors counts rows Snowflake skipped: the only signal that it rejected rows.
	rowErrors stats.Counter
	// schemaCacheMissOnSend must stay at zero: non-zero means a batch had columns the table still lacked.
	schemaCacheMissOnSend stats.Counter
}

func newHPStats(factory stats.Stats, destination *backendconfig.DestinationT) hpStats {
	s := hpStats{
		factory: factory,
		tags: stats.Tags{
			"module":        "batch_router",
			"workspaceId":   destination.WorkspaceID,
			"destType":      destination.DestinationDefinition.Name,
			"destinationId": destination.ID,
			"version":       strconv.Itoa(versionHP),
		},
	}
	jobs := func(status string) stats.Counter {
		return factory.NewTaggedStat("snowpipe_streaming_jobs", stats.CountType, s.with(stats.Tags{"status": status}))
	}
	s.jobs.importing = jobs("importing")
	s.jobs.succeeded = jobs("succeeded")
	s.jobs.failed = jobs("failed")
	s.jobs.aborted = jobs("aborted")
	s.discards = factory.NewTaggedStat("snowpipe_streaming_discards", stats.CountType, s.tags)
	s.pollingInProgress = factory.NewTaggedStat("snowpipe_streaming_polling_in_progress", stats.CountType, s.tags)
	s.duplicateEventsInBatch = factory.NewTaggedStat("snowpipe_streaming_duplicate_events", stats.CountType, s.with(stats.Tags{"reason": "batch"}))
	s.duplicateEventsDueToOffset = factory.NewTaggedStat("snowpipe_streaming_duplicate_events", stats.CountType, s.with(stats.Tags{"reason": "offset"}))
	s.requestBodySize = factory.NewTaggedStat("snowpipe_streaming_request_body_size", stats.HistogramType, s.with(stats.Tags{"api": "append_rows"}))
	s.rowErrors = factory.NewTaggedStat("snowpipe_streaming_row_errors", stats.CountType, s.tags)
	s.schemaCacheMissOnSend = factory.NewTaggedStat("snowpipe_streaming_schema_cache_miss_on_send", stats.CountType, s.tags)
	return s
}

func (s *hpStats) with(extra stats.Tags) stats.Tags {
	return lo.Assign(s.tags, extra)
}

// schemaCacheRefresh counts schema reads, by what triggered them: a cold cache, an expired entry or an unknown column.
func (s *hpStats) schemaCacheRefresh(trigger string) stats.Counter {
	return s.factory.NewTaggedStat("snowpipe_streaming_schema_cache_refresh", stats.CountType, s.with(stats.Tags{"trigger": trigger}))
}

// nullOffsetRecovery counts failed imports by branch: an empty committed offset, rows lost, or a missing channel.
func (s *hpStats) nullOffsetRecovery(branch string) stats.Counter {
	return s.factory.NewTaggedStat("snowpipe_streaming_null_offset_recovery", stats.CountType, s.with(stats.Tags{"branch": branch}))
}

// apiTimer times a REST call. Use as defer s.apiTimer("name")().
func (s *hpStats) apiTimer(api string) func() {
	return s.factory.NewTaggedStat("snowpipe_streaming_api_response_time", stats.TimerType, s.with(stats.Tags{"api": api})).RecordDuration()
}
