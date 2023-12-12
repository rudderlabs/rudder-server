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
	s, err := memstats.New()
	require.NoError(t, err)

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

		require.ElementsMatch(t, []memstats.Metric{
			{
				Name: "processor.delayed_events",
				Tags: stats.Tags{
					"sourceId":    "sourceID-1",
					"sourceType":  "sourceType-1",
					"workspaceId": "workspaceID-1",
					"status":      "on-time",
					"sdkVersion":  "rudder-go/1.0.0",
				},
				Value: 2,
			},
			{
				Name: "processor.delayed_events",
				Tags: stats.Tags{
					"sourceId":    "sourceID-1",
					"sourceType":  "sourceType-1",
					"workspaceId": "workspaceID-1",
					"status":      "late",
					"sdkVersion":  "rudder-go/1.0.0",
				},
				Value: 3,
			},
			{
				Name: "processor.delayed_events",
				Tags: stats.Tags{
					"sourceId":    "sourceID-1",
					"sourceType":  "sourceType-1",
					"workspaceId": "workspaceID-1",
					"status":      "missing_original_timestamp",
					"sdkVersion":  "rudder-go/1.0.0",
				},
				Value: 2,
			},
			{
				Name: "processor.delayed_events",
				Tags: stats.Tags{
					"sourceId":    "sourceID-1",
					"sourceType":  "sourceType-1",
					"workspaceId": "workspaceID-1",
					"status":      "missing_sent_at",
					"sdkVersion":  "rudder-go/1.0.0",
				},
				Value: 1,
			},
		}, s.GetAll())
	})

	t.Run("extract SDK version", func(t *testing.T) {
		testCases := []struct {
			name               string
			message            map[string]any
			expectedSDKVersion string
		}{
			{
				name:               "missing context",
				message:            map[string]interface{}{},
				expectedSDKVersion: "unknown",
			},
			{
				name: "missing context.library",
				message: map[string]interface{}{
					"context": map[string]interface{}{},
				},
				expectedSDKVersion: "unknown",
			},
			{
				name: "missing context.library contents",
				message: map[string]interface{}{
					"context": map[string]interface{}{
						"library": map[string]interface{}{},
					},
				},
				expectedSDKVersion: "unknown",
			},
			{
				name: "missing context.library.name",
				message: map[string]interface{}{
					"context": map[string]interface{}{
						"library": map[string]interface{}{
							"version": "1.0.0",
						},
					},
				},
				expectedSDKVersion: "/1.0.0",
			},
			{
				name: "missing context.library.version",
				message: map[string]interface{}{
					"context": map[string]interface{}{
						"library": map[string]interface{}{
							"name": "rudder-go",
						},
					},
				},
				expectedSDKVersion: "rudder-go/",
			},
			{
				name: " context.library.version not a string",
				message: map[string]interface{}{
					"context": map[string]interface{}{
						"library": map[string]interface{}{
							"name":    "rudder-go",
							"version": 2,
						},
					},
				},
				expectedSDKVersion: "rudder-go/",
			},
			{
				name: "context.library.name not a string",
				message: map[string]interface{}{
					"context": map[string]interface{}{
						"library": map[string]interface{}{
							"name": []string{"rudder-go"},
						},
					},
				},
				expectedSDKVersion: "unknown",
			},
			{
				name: "parse context.library name and version",
				message: map[string]interface{}{
					"context": map[string]interface{}{
						"library": map[string]interface{}{
							"name":    "rudder-go",
							"version": "1.0.0",
						},
					},
				},
				expectedSDKVersion: "rudder-go/1.0.0",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				tc.message["originalTimestamp"] = "2020-01-01T00:00:00.000Z"
				tc.message["sentAt"] = "2020-01-01T00:10:00.000Z"
				events := []transformer.TransformerEvent{
					{
						Message: tc.message,
					},
				}

				s, err := memstats.New()
				require.NoError(t, err)

				es := delayed.NewEventStats(s, c)

				es.ObserveSourceEvents(source, events)

				require.Equal(t, []memstats.Metric{
					{
						Name: "processor.delayed_events",
						Tags: stats.Tags{
							"sourceId":    "sourceID-1",
							"sourceType":  "sourceType-1",
							"workspaceId": "workspaceID-1",
							"status":      "on-time",
							"sdkVersion":  tc.expectedSDKVersion,
						},
						Value: 1,
					},
				}, s.GetAll())
			})
		}
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
