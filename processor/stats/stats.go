package processorstats

import (
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type DelayedEventStats struct {
	Stats     stats.Stats
	Threshold time.Duration
}

func (s *DelayedEventStats) ObserveSourceEvents(source *backendconfig.SourceT, events []transformer.TransformerEvent) {
	statusCount := map[string]int{}

	for _, event := range events {
		originalTimestamp, ok := misc.GetParsedTimestamp(event.Message["originalTimestamp"])
		if !ok {
			statusCount["missing_original_timestamp"]++
			continue
		}

		sentAt, ok := misc.GetParsedTimestamp(event.Message["sentAt"])
		if !ok {
			statusCount["missing_sent_at"]++
			continue
		}

		if sentAt.Sub(originalTimestamp) > s.Threshold {
			statusCount["late"]++
		} else {
			statusCount["ok"]++
		}
	}

	for status, count := range statusCount {
		s.Stats.NewTaggedStat("processor.delayed_events", stats.CountType, stats.Tags{
			"sourceId":    source.ID,
			"sourceType":  source.SourceDefinition.Category,
			"workspaceId": source.WorkspaceID,
			"status":      status,
		}).Count(count)
	}
}
