package transformer

import (
	"testing"

	"github.com/stretchr/testify/require"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestIntegrationOptions(t *testing.T) {
	t.Run("AllOptionsSet", func(t *testing.T) {
		event := ptrans.TransformerEvent{
			Message: map[string]any{
				"integrations": map[string]any{
					"POSTGRES": map[string]any{
						"options": map[string]any{
							"skipReservedKeywordsEscaping": true,
							"useBlendoCasing":              false,
							"skipTracksTable":              true,
							"skipUsersTable":               false,
							"jsonPaths":                    []any{"path1", "path2", "path3"},
						},
					},
				},
			},
			Metadata: ptrans.Metadata{
				DestinationType: "POSTGRES",
			},
		}

		opts := extractIntrOpts(event.Metadata.DestinationType, event.Message)

		require.True(t, opts.skipReservedKeywordsEscaping)
		require.False(t, opts.useBlendoCasing)
		require.True(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.Equal(t, []string{"path1", "path2", "path3"}, opts.jsonPaths)
	})
	t.Run("MissingOptions", func(t *testing.T) {
		event := ptrans.TransformerEvent{
			Message: map[string]any{
				"integrations": map[string]any{
					"POSTGRES": map[string]any{
						"options": map[string]any{},
					},
				},
			},
			Metadata: ptrans.Metadata{
				DestinationType: "POSTGRES",
			},
		}
		opts := extractIntrOpts(event.Metadata.DestinationType, event.Message)

		require.False(t, opts.skipReservedKeywordsEscaping)
		require.False(t, opts.useBlendoCasing)
		require.False(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.Empty(t, opts.jsonPaths)
	})
	t.Run("NilIntegrationOptions", func(t *testing.T) {
		event := ptrans.TransformerEvent{
			Message: map[string]any{
				"integrations": map[string]any{
					"POSTGRES": map[string]any{
						"options": nil,
					},
				},
			},
			Metadata: ptrans.Metadata{
				DestinationType: "POSTGRES",
			},
		}
		opts := extractIntrOpts(event.Metadata.DestinationType, event.Message)

		require.False(t, opts.skipReservedKeywordsEscaping)
		require.False(t, opts.useBlendoCasing)
		require.False(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.Empty(t, opts.jsonPaths)
	})
	t.Run("PartialOptionsSet", func(t *testing.T) {
		event := ptrans.TransformerEvent{
			Message: map[string]any{
				"integrations": map[string]any{
					"POSTGRES": map[string]any{
						"options": map[string]any{
							"skipUsersTable": true,
							"jsonPaths":      []any{"path1"},
						},
					},
				},
			},
			Metadata: ptrans.Metadata{
				DestinationType: "POSTGRES",
			},
		}

		opts := extractIntrOpts(event.Metadata.DestinationType, event.Message)

		require.True(t, opts.skipUsersTable)
		require.False(t, opts.skipReservedKeywordsEscaping)
		require.False(t, opts.useBlendoCasing)
		require.False(t, opts.skipTracksTable)
		require.Equal(t, []string{"path1"}, opts.jsonPaths)
	})
	t.Run("DataWarehouseOptions", func(t *testing.T) {
		event := ptrans.TransformerEvent{
			Message: map[string]any{
				"integrations": map[string]any{
					"POSTGRES": map[string]any{
						"options": map[string]any{
							"skipReservedKeywordsEscaping": true,
							"useBlendoCasing":              false,
							"skipTracksTable":              true,
							"skipUsersTable":               false,
							"jsonPaths":                    []any{"path1", "path2", "path3"},
						},
					},
					"DATA_WAREHOUSE": map[string]any{
						"options": map[string]any{
							"jsonPaths": []any{"path4", "path5"},
						},
					},
				},
			},
			Metadata: ptrans.Metadata{
				DestinationType: "POSTGRES",
			},
		}

		opts := extractIntrOpts(event.Metadata.DestinationType, event.Message)

		require.True(t, opts.skipReservedKeywordsEscaping)
		require.False(t, opts.useBlendoCasing)
		require.True(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.Equal(t, []string{"path1", "path2", "path3", "path4", "path5"}, opts.jsonPaths)
	})
}

func TestDestinationOptions(t *testing.T) {
	t.Run("AllOptionsSet", func(t *testing.T) {
		destConfig := map[string]any{
			"skipTracksTable":         true,
			"skipUsersTable":          false,
			"underscoreDivideNumbers": true,
			"allowUsersContextTraits": false,
			"storeFullEvent":          true,
			"jsonPaths":               "path1,path2",
		}

		opts := extractDestOpts(whutils.POSTGRES, destConfig)

		require.True(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.True(t, opts.underscoreDivideNumbers)
		require.False(t, opts.allowUsersContextTraits)
		require.True(t, opts.storeFullEvent)
		require.Equal(t, []string{"path1", "path2"}, opts.jsonPaths)
	})
	t.Run("MissingOptions", func(t *testing.T) {
		destConfig := map[string]any{}

		opts := extractDestOpts(whutils.POSTGRES, destConfig)

		require.False(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.False(t, opts.underscoreDivideNumbers)
		require.False(t, opts.allowUsersContextTraits)
		require.False(t, opts.storeFullEvent)
		require.Empty(t, opts.jsonPaths)
	})
	t.Run("NilDestinationConfig", func(t *testing.T) {
		opts := extractDestOpts(whutils.POSTGRES, nil)

		require.False(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.False(t, opts.underscoreDivideNumbers)
		require.False(t, opts.allowUsersContextTraits)
		require.False(t, opts.storeFullEvent)
		require.Empty(t, opts.jsonPaths)
	})
	t.Run("PartialOptionsSet", func(t *testing.T) {
		destConfig := map[string]any{
			"skipTracksTable":         true,
			"jsonPaths":               "path1,path2",
			"allowUsersContextTraits": true,
		}

		opts := extractDestOpts(whutils.POSTGRES, destConfig)

		require.True(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.False(t, opts.underscoreDivideNumbers)
		require.True(t, opts.allowUsersContextTraits)
		require.False(t, opts.storeFullEvent)
		require.Equal(t, []string{"path1", "path2"}, opts.jsonPaths)
	})
	t.Run("JSONPathSupported", func(t *testing.T) {
		destConfig := map[string]any{
			"jsonPaths": "path1,path2",
		}

		require.Equal(t, []string{"path1", "path2"}, extractDestOpts(whutils.POSTGRES, destConfig).jsonPaths)
		require.Empty(t, extractDestOpts(whutils.CLICKHOUSE, destConfig).jsonPaths)
	})
}
