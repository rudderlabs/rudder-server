package delayed_test

import (
	"testing"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/processor/delayed"
	"github.com/rudderlabs/rudder-server/processor/transformer"
)

func TestEventStats(t *testing.T) {
	s := memstats.New()
	c := config.New()

	c.Set("processor.delayed_events.threshold", "1h")

	es := delayed.NewEventStats(s, c)

	source := &backendconfig.SourceT{
		ID: "sourceID-1",
		SourceDefinition: backendconfig.SourceDefinitionT{
			Category: "sourceType-1",
		},
		WorkspaceID: "workspaceID-1",
	}

	t.Run("test batch", func(t *testing.T) {
		es.ObserveSourceEvents(source, []transformer.TransformerEvent{
			// missing originalTimestamp
			{
				Message: map[string]interface{}{
					"sentAt": "2020-01-01T00:00:00.000Z",
				},
			},
			// missing sentAt
			{
				Message: map[string]interface{}{
					"originalTimestamp": "2020-01-01T00:00:00.000Z",
				},
			},
			// missing both counts as missing_original_timestamp
			{
				Message: map[string]interface{}{},
			},
			// ok, 10 minutes late
			{
				Message: map[string]interface{}{
					"originalTimestamp": "2020-01-01T00:00:00.000Z",
					"sentAt":            "2020-01-01T00:10:00.000Z",
				},
			},
			// late, 1 month late
			{
				Message: map[string]interface{}{
					"originalTimestamp": "2020-01-01T00:00:00.000Z",
					"sentAt":            "2020-02-01T00:00:00.000Z",
				},
			},
			// late, 1 day late
			{
				Message: map[string]interface{}{
					"originalTimestamp": "2020-01-01T00:00:00.000Z",
					"sentAt":            "2020-01-02T00:00:00.000Z",
				},
			},
			// late, 1 hour and 10 seconds late
			{
				Message: map[string]interface{}{
					"originalTimestamp": "2020-01-01T00:00:00.000Z",
					"sentAt":            "2020-01-01T01:00:10.000Z",
				},
			},
			// ok
			{
				Message: map[string]interface{}{
					"originalTimestamp": "2020-01-01T00:00:00.000Z",
					"sentAt":            "2020-01-01T00:00:00.000Z",
				},
			},
		})

		require.Equal(t, float64(2), s.Get("processor.delayed_events", stats.Tags{
			"sourceId":    "sourceID-1",
			"sourceType":  "sourceType-1",
			"workspaceId": "workspaceID-1",
			"status":      "ok",
		}).LastValue())
		require.Equal(t, float64(3), s.Get("processor.delayed_events", stats.Tags{
			"sourceId":    "sourceID-1",
			"sourceType":  "sourceType-1",
			"workspaceId": "workspaceID-1",
			"status":      "late",
		}).LastValue())
		require.Equal(t, float64(2), s.Get("processor.delayed_events", stats.Tags{
			"sourceId":    "sourceID-1",
			"sourceType":  "sourceType-1",
			"workspaceId": "workspaceID-1",
			"status":      "missing_original_timestamp",
		}).LastValue())
		require.Equal(t, float64(1), s.Get("processor.delayed_events", stats.Tags{
			"sourceId":    "sourceID-1",
			"sourceType":  "sourceType-1",
			"workspaceId": "workspaceID-1",
			"status":      "missing_sent_at",
		}).LastValue())
	})

}
