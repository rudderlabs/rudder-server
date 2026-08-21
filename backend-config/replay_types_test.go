package backendconfig

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyReplayConfig(t *testing.T) {
	t.Run("Valid Replay Config", func(t *testing.T) {
		c := &ConfigT{
			Sources: []SourceT{
				{
					ID:     "s-1",
					Config: json.RawMessage(`{"eventUpload": true}`),
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
		require.JSONEq(t, "{}", string(c.Sources[1].Config))
		require.JSONEq(t, `{"eventUpload": true}`, string(c.Sources[0].Config),
			"the replay source is a copy: dropping eventUpload must not touch the original")
		require.Len(t, c.Sources[1].Destinations, 1)
		require.Equal(t, "er-d-1", c.Sources[1].Destinations[0].ID)
		require.Equal(t, "d-1", c.Sources[1].Destinations[0].OriginalID)
		require.Equal(t, true, c.Sources[1].Destinations[0].IsProcessorEnabled)
		require.Equal(t, "rev-1", c.Sources[1].Destinations[0].RevisionID)
	})

	t.Run("Source configs the key cannot be dropped from", func(t *testing.T) {
		for _, tc := range []struct {
			name, config, want string
		}{
			{"without the key", `{"foo": "bar"}`, `{"foo": "bar"}`},
			{"empty object", `{}`, `{}`},
			{"empty", ``, ``},
		} {
			t.Run(tc.name, func(t *testing.T) {
				c := &ConfigT{
					Sources: []SourceT{{
						ID:           "s-1",
						Config:       json.RawMessage(tc.config),
						Destinations: []DestinationT{{ID: "d-1"}},
					}},
					EventReplays: map[string]EventReplayConfig{
						"er-1": {
							Sources:      map[string]EventReplaySource{"er-s-1": {OriginalSourceID: "s-1"}},
							Destinations: map[string]EventReplayDestination{"er-d-1": {OriginalDestinationID: "d-1"}},
							Connections:  []EventReplayConnection{{SourceID: "er-s-1", DestinationID: "er-d-1"}},
						},
					},
				}
				c.ApplyReplaySources()

				require.Len(t, c.Sources, 2)
				require.Equal(t, "er-s-1", c.Sources[1].ID)
				require.Equal(t, tc.want, string(c.Sources[1].Config))
			})
		}
	})

	t.Run("Invalid Replay Config", func(t *testing.T) {
		c := &ConfigT{
			Sources: []SourceT{
				{
					ID:     "s-1",
					Config: json.RawMessage(`{"eventUpload": true}`),
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
		require.JSONEq(t, "{}", string(c.Sources[1].Config))
		require.Len(t, c.Sources[1].Destinations, 1)
		require.Equal(t, "er-d-1", c.Sources[1].Destinations[0].ID)
	})
}

// A destination connected to several sources appears once per source, and the control plane gives
// each copy a different config: it filters against the keys the destination's definition declares
// for that source's type. A replay destination has to copy the instance belonging to its own
// source's type, and has to keep copying the same one on every poll.
func TestApplyReplaySourcesDestinationConfig(t *testing.T) {
	// "Javascript" resolves to the web source type, an empty definition name to cloud
	config := func(sourceTypes ...string) *ConfigT {
		c := &ConfigT{
			EventReplays: map[string]EventReplayConfig{
				"er-1": {
					Sources:      map[string]EventReplaySource{"er-s-1": {OriginalSourceID: "s-replayed"}},
					Destinations: map[string]EventReplayDestination{"er-d-1": {OriginalDestinationID: "d-1"}},
					Connections:  []EventReplayConnection{{SourceID: "er-s-1", DestinationID: "er-d-1"}},
				},
			},
		}
		for i, definitionName := range sourceTypes {
			c.Sources = append(c.Sources, SourceT{
				ID:               fmt.Sprintf("s-%d", i),
				SourceDefinition: SourceDefinitionT{Name: definitionName},
				Destinations: []DestinationT{{
					ID:     "d-1",
					Config: map[string]any{"filteredFor": sourceTypeOf(definitionName, "")},
				}},
			})
		}
		return c
	}

	replayed := func(t *testing.T, c *ConfigT) DestinationT {
		t.Helper()
		c.ApplyReplaySources()
		source, ok := c.SourcesMap()["er-s-1"]
		require.True(t, ok, "the replay source was not added")
		require.Len(t, source.Destinations, 1)
		return source.Destinations[0]
	}

	t.Run("copies the instance of its own source type", func(t *testing.T) {
		c := config("Javascript", "")
		c.Sources[0].ID = "s-replayed" // the replay is of the web source
		destination := replayed(t, c)

		require.Equal(t, "er-d-1", destination.ID)
		require.Equal(t, "d-1", destination.OriginalID)
		require.True(t, destination.IsProcessorEnabled)
		require.Equal(t, "web", destination.Config["filteredFor"])
	})

	t.Run("and does so whichever source the destination is listed under first", func(t *testing.T) {
		c := config("", "Javascript")
		c.Sources[1].ID = "s-replayed"
		require.Equal(t, "web", replayed(t, c).Config["filteredFor"])
	})

	t.Run("the replayed pair need never have been connected", func(t *testing.T) {
		// s-replayed is a web source carrying no destinations at all; d-1 lives under two other
		// sources, one of which is also web
		c := config("Javascript", "")
		c.Sources = append(c.Sources, SourceT{
			ID:               "s-replayed",
			SourceDefinition: SourceDefinitionT{Name: "Javascript"},
		})
		require.Equal(t, "web", replayed(t, c).Config["filteredFor"])
	})

	t.Run("falls back to the first source type in order, and keeps falling back to the same one", func(t *testing.T) {
		for range 20 { // the failure this guards against is a map iteration order flake
			// d-1 is on a web and a cloud source; the replayed source is neither
			c := config("Javascript", "")
			c.Sources = append(c.Sources, SourceT{
				ID:               "s-replayed",
				SourceDefinition: SourceDefinitionT{Name: "snowflake", Category: "warehouse"},
			})
			require.Equal(t, "cloud", replayed(t, c).Config["filteredFor"],
				`"cloud" sorts before "web"`)
		}
	})
}
