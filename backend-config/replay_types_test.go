package backendconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyReplayConfig(t *testing.T) {
	t.Run("Valid Replay Config", func(t *testing.T) {
		c := &ConfigT{
			Sources: []SourceT{
				{
					ID:     "s-1",
					Config: map[string]interface{}{"eventUpload": true},
					SourceDefinition: SourceDefinitionT{
						ID:       "sd-1",
						Type:     "type-1",
						Category: "category-1",
					},
					Destinations: []DestinationT{
						{
							ID:                 "d-1",
							RevisionID:         "rev-1",
							IsProcessorEnabled: false,
						},
						{
							ID:         "d-2",
							RevisionID: "rev-2",
						},
					},
				},
			},
			EventReplays: map[string]EventReplayConfig{
				"er-1": {
					Sources: map[string]EventReplaySource{
						"er-s-1": {
							OriginalSourceID: "s-1",
						},
					},
					Destinations: map[string]EventReplayDestination{
						"er-d-1": {
							OriginalDestinationID: "d-1",
						},
					},
					Connections: []EventReplayConnection{
						{
							SourceID:      "er-s-1",
							DestinationID: "er-d-1",
						},
					},
				},
			},
		}
		c.ApplyReplaySources()

		require.Len(t, c.Sources, 2)
		require.Equal(t, "s-1", c.Sources[0].ID)
		require.Equal(t, "er-s-1", c.Sources[1].ID)
		require.Equal(t, "s-1", c.Sources[1].OriginalID)
		require.Equal(t, "er-s-1", c.Sources[1].WriteKey)
		require.Equal(t, map[string]interface{}{}, c.Sources[1].Config)
		require.Len(t, c.Sources[1].Destinations, 1)
		require.Equal(t, "er-d-1", c.Sources[1].Destinations[0].ID)
		require.Equal(t, true, c.Sources[1].Destinations[0].IsProcessorEnabled)
		require.Equal(t, "rev-1", c.Sources[1].Destinations[0].RevisionID)
	})

	t.Run("Invalid Replay Config", func(t *testing.T) {
		c := &ConfigT{
			Sources: []SourceT{
				{
					ID:     "s-1",
					Config: map[string]interface{}{"eventUpload": true},
					SourceDefinition: SourceDefinitionT{
						ID:       "sd-1",
						Type:     "type-1",
						Category: "category-1",
					},
					Destinations: []DestinationT{
						{
							ID: "d-1",
						},
					},
				},
			},
			EventReplays: map[string]EventReplayConfig{
				"er-1": {
					Sources: map[string]EventReplaySource{
						"er-s-1": {
							OriginalSourceID: "s-1",
						},
						"er-s-2": {
							OriginalSourceID: "s-2",
						},
					},
					Destinations: map[string]EventReplayDestination{
						"er-d-1": {
							OriginalDestinationID: "d-1",
						},
						"er-d-2": {
							OriginalDestinationID: "d-2",
						},
					},
					Connections: []EventReplayConnection{
						{
							SourceID:      "er-s-1",
							DestinationID: "er-d-1",
						},
						{
							SourceID:      "er-s-1",
							DestinationID: "er-d-2",
						},
						{
							SourceID:      "er-s-2",
							DestinationID: "er-d-1",
						},
						{
							SourceID:      "er-s-2",
							DestinationID: "er-d-2",
						},
						{
							SourceID:      "er-s-3",
							DestinationID: "er-d-3",
						},
					},
				},
			},
		}

		c.ApplyReplaySources()

		require.Len(t, c.Sources, 2)
		require.Equal(t, "s-1", c.Sources[0].ID)
		require.Equal(t, "er-s-1", c.Sources[1].ID)
		require.Equal(t, "s-1", c.Sources[1].OriginalID)
		require.Equal(t, "er-s-1", c.Sources[1].WriteKey)
		require.Equal(t, map[string]interface{}{}, c.Sources[1].Config)
		require.Len(t, c.Sources[1].Destinations, 1)
		require.Equal(t, "er-d-1", c.Sources[1].Destinations[0].ID)
	})
}
