package delayed

import (
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type eventStats struct {
	stats     stats.Stats
	threshold time.Duration
}

func NewEventStats(stats stats.Stats, config *config.Config) *eventStats {
	threshold := config.GetDuration("processor.delayed_events.threshold", 10*24, time.Hour)

	return &eventStats{
		stats:     stats,
		threshold: threshold,
	}
}

func (s *eventStats) ObserveSourceEvents(source *backendconfig.SourceT, events []transformer.TransformerEvent) {
	statusCount := map[string]map[string]int{
		"missing_original_timestamp": {},
		"missing_sent_at":            {},
		"late":                       {},
		"on-time":                    {},
	}

	for _, event := range events {
		sdkVersion := "unknown"

		sdkContext, err := misc.NestedMapLookup(event.Message, "context", "library")
		if err == nil {
			m, ok := sdkContext.(map[string]interface{})
			if ok {
				sdkLibVersion, _ := m["version"].(string)
				sdkLibName, _ := m["name"].(string)

				if sdkLibName != "" || sdkLibVersion != "" {
					sdkVersion = strings.Join([]string{sdkLibName, sdkLibVersion}, "/")
				}
			}
		}

		originalTimestamp, ok := misc.GetParsedTimestamp(event.Message["originalTimestamp"])
		if !ok {
			statusCount["missing_original_timestamp"][sdkVersion]++
			continue
		}

		sentAt, ok := misc.GetParsedTimestamp(event.Message["sentAt"])
		if !ok {
			statusCount["missing_sent_at"][sdkVersion]++
			continue
		}

		if sentAt.Sub(originalTimestamp) > s.threshold {
			statusCount["late"][sdkVersion]++
		} else {
			statusCount["on-time"][sdkVersion]++
		}
	}

	for status, versions := range statusCount {
		for version, count := range versions {
			s.stats.NewTaggedStat("processor.delayed_events", stats.CountType, stats.Tags{
				"sourceId":    source.ID,
				"sourceType":  source.SourceDefinition.Category,
				"workspaceId": source.WorkspaceID,
				"status":      status,
				"sdkVersion":  version,
			}).Count(count)
		}
	}
}
