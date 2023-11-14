package delayed_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"

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
		sdkLibrary := "rudder-go"
		sdkVersion := "1.0.0"

		es.ObserveSourceEvents(source, fromSDK(sdkLibrary, sdkVersion, []transformer.TransformerEvent{
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
			// on-time, 10 minutes late
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
			// on-time
			{
				Message: map[string]interface{}{
					"originalTimestamp": "2020-01-01T00:00:00.000Z",
					"sentAt":            "2020-01-01T00:00:00.000Z",
				},
			},
		}))

		require.Equal(t, float64(2), s.Get("processor.delayed_events", stats.Tags{
			"sourceId":    "sourceID-1",
			"sourceType":  "sourceType-1",
			"workspaceId": "workspaceID-1",
			"status":      "on-time",
			"sdkVersion":  "rudder-go/1.0.0",
		}).LastValue())
		require.Equal(t, float64(3), s.Get("processor.delayed_events", stats.Tags{
			"sourceId":    "sourceID-1",
			"sourceType":  "sourceType-1",
			"workspaceId": "workspaceID-1",
			"status":      "late",
			"sdkVersion":  "rudder-go/1.0.0",
		}).LastValue())
		require.Equal(t, float64(2), s.Get("processor.delayed_events", stats.Tags{
			"sourceId":    "sourceID-1",
			"sourceType":  "sourceType-1",
			"workspaceId": "workspaceID-1",
			"status":      "missing_original_timestamp",
			"sdkVersion":  "rudder-go/1.0.0",
		}).LastValue())
		require.Equal(t, float64(1), s.Get("processor.delayed_events", stats.Tags{
			"sourceId":    "sourceID-1",
			"sourceType":  "sourceType-1",
			"workspaceId": "workspaceID-1",
			"status":      "missing_sent_at",
			"sdkVersion":  "rudder-go/1.0.0",
		}).LastValue())
	})

	t.Run("extract SDK version", func(t *testing.T) {
		es.ObserveSourceEvents(source, []transformer.TransformerEvent{
			{
				Message: map[string]interface{}{
					"originalTimestamp": "2020-01-01T00:00:00.000Z",
					"sentAt":            "2020-01-01T00:10:00.000Z",
				},
			},
		})

		es.ObserveSourceEvents(source, []transformer.TransformerEvent{
			{
				Message: map[string]interface{}{
					"originalTimestamp": "2020-01-01T00:00:00.000Z",
					"sentAt":            "2020-01-01T00:10:00.000Z",
					"context":           map[string]interface{}{},
				},
			},
		})

		es.ObserveSourceEvents(source, []transformer.TransformerEvent{
			{
				Message: map[string]interface{}{
					"originalTimestamp": "2020-01-01T00:00:00.000Z",
					"sentAt":            "2020-01-01T00:10:00.000Z",
					"context": map[string]interface{}{
						"library": map[string]interface{}{},
					},
				},
			},
		})

		es.ObserveSourceEvents(source, []transformer.TransformerEvent{
			{
				Message: map[string]interface{}{
					"originalTimestamp": "2020-01-01T00:00:00.000Z",
					"sentAt":            "2020-01-01T00:10:00.000Z",
					"context": map[string]interface{}{
						"library": map[string]interface{}{
							"name": "rudder-go",
						},
					},
				},
			},
		})

		es.ObserveSourceEvents(source, []transformer.TransformerEvent{
			{
				Message: map[string]interface{}{
					"originalTimestamp": "2020-01-01T00:00:00.000Z",
					"sentAt":            "2020-01-01T00:10:00.000Z",
					"context": map[string]interface{}{
						"library": map[string]interface{}{
							"version": "1.0.0",
						},
					},
				},
			},
		})

		es.ObserveSourceEvents(source, []transformer.TransformerEvent{
			{
				Message: map[string]interface{}{
					"originalTimestamp": "2020-01-01T00:00:00.000Z",
					"sentAt":            "2020-01-01T00:10:00.000Z",
					"context": map[string]interface{}{
						"library": map[string]interface{}{
							"version": 1,
						},
					},
				},
			},
		})

	})
}

func fromSDK(lib, version string, events []transformer.TransformerEvent) []transformer.TransformerEvent {
	for i := range events {
		events[i].Message["context"] = map[string]interface{}{
			"library": map[string]interface{}{
				"name":    lib,
				"version": version,
			},
		}
	}

	return events
}
