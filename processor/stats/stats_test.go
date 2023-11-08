package processorstats_test

import (
	"testing"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/stretchr/testify/require"

	processorstats "github.com/rudderlabs/rudder-server/processor/stats"
	"github.com/rudderlabs/rudder-server/processor/transformer"
)

func TestEventStats(t *testing.T) {
	ms := memstats.New()

	s := &processorstats.DelayedEventStats{
		Stats:     ms,
		Threshold: time.Hour,
	}

	source := &backendconfig.SourceT{
		ID: "sourceID-1",
		SourceDefinition: backendconfig.SourceDefinitionT{
			Category: "sourceType-1",
		},
		WorkspaceID: "workspaceID-1",
	}

	s.ObserveSourceEvents(source, []transformer.TransformerEvent{
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
		// ok
		{
			Message: map[string]interface{}{
				"originalTimestamp": "2020-01-01T00:00:00.000Z",
				"sentAt":            "2020-01-01T00:00:00.000Z",
			},
		},
		// late
		{
			Message: map[string]interface{}{
				"originalTimestamp": "2020-01-01T00:00:00.000Z",
				"sentAt":            "2020-02-01T00:00:00.000Z",
			},
		},
		// late
		{
			Message: map[string]interface{}{
				"originalTimestamp": "2020-01-01T00:00:00.000Z",
				"sentAt":            "2020-02-01T00:00:00.000Z",
			},
		},
		// late
		{
			Message: map[string]interface{}{
				"originalTimestamp": "2020-01-01T00:00:00.000Z",
				"sentAt":            "2020-02-01T00:00:00.000Z",
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

	require.Equal(t, float64(2), ms.Get("processor.delayed_events", stats.Tags{
		"sourceId":    "sourceID-1",
		"sourceType":  "sourceType-1",
		"workspaceId": "workspaceID-1",
		"status":      "ok",
	}).LastValue())
	require.Equal(t, float64(3), ms.Get("processor.delayed_events", stats.Tags{
		"sourceId":    "sourceID-1",
		"sourceType":  "sourceType-1",
		"workspaceId": "workspaceID-1",
		"status":      "late",
	}).LastValue())
	require.Equal(t, float64(1), ms.Get("processor.delayed_events", stats.Tags{
		"sourceId":    "sourceID-1",
		"sourceType":  "sourceType-1",
		"workspaceId": "workspaceID-1",
		"status":      "missing_original_timestamp",
	}).LastValue())
	require.Equal(t, float64(1), ms.Get("processor.delayed_events", stats.Tags{
		"sourceId":    "sourceID-1",
		"sourceType":  "sourceType-1",
		"workspaceId": "workspaceID-1",
		"status":      "missing_sent_at",
	}).LastValue())
}
