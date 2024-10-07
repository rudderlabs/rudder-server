package transformer

import (
	"testing"

	"github.com/stretchr/testify/require"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
)

func TestIntegrationOptions(t *testing.T) {
	t.Run("AllOptionsSet", func(t *testing.T) {
		event := ptrans.TransformerEvent{
			Message: map[string]any{
				"integrations": map[string]any{
					"destinationType": map[string]any{
						"options": map[string]any{
							"skipReservedKeywordsEscaping": true,
							"useBlendoCasing":              false,
							"skipTracksTable":              true,
							"skipUsersTable":               false,
							"jsonPaths":                    []string{"path1", "path2", "path3"},
						},
					},
				},
			},
			Metadata: ptrans.Metadata{
				DestinationType: "destinationType",
			},
		}

		opts := prepareIntegrationOptions(event)

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
					"destinationType": map[string]any{
						"options": map[string]any{},
					},
				},
			},
			Metadata: ptrans.Metadata{
				DestinationType: "destinationType",
			},
		}
		opts := prepareIntegrationOptions(event)

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
					"destinationType": map[string]any{
						"options": nil,
					},
				},
			},
			Metadata: ptrans.Metadata{
				DestinationType: "destinationType",
			},
		}
		opts := prepareIntegrationOptions(event)

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
					"destinationType": map[string]any{
						"options": map[string]any{
							"skipUsersTable": true,
							"jsonPaths":      []string{"path1"},
						},
					},
				},
			},
			Metadata: ptrans.Metadata{
				DestinationType: "destinationType",
			},
		}

		opts := prepareIntegrationOptions(event)

		require.True(t, opts.skipUsersTable)
		require.False(t, opts.skipReservedKeywordsEscaping)
		require.False(t, opts.useBlendoCasing)
		require.False(t, opts.skipTracksTable)
		require.Equal(t, []string{"path1"}, opts.jsonPaths)
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

		opts := prepareDestinationOptions(destConfig)

		require.True(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.True(t, opts.underscoreDivideNumbers)
		require.False(t, opts.allowUsersContextTraits)
		require.True(t, opts.storeFullEvent)
		require.Equal(t, []string{"path1", "path2"}, opts.jsonPaths)
	})
	t.Run("MissingOptions", func(t *testing.T) {
		destConfig := map[string]any{}

		opts := prepareDestinationOptions(destConfig)

		require.False(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.False(t, opts.underscoreDivideNumbers)
		require.False(t, opts.allowUsersContextTraits)
		require.False(t, opts.storeFullEvent)
		require.Empty(t, opts.jsonPaths)
	})
	t.Run("NilDestinationConfig", func(t *testing.T) {
		opts := prepareDestinationOptions(nil)

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

		opts := prepareDestinationOptions(destConfig)

		require.True(t, opts.skipTracksTable)
		require.False(t, opts.skipUsersTable)
		require.False(t, opts.underscoreDivideNumbers)
		require.True(t, opts.allowUsersContextTraits)
		require.False(t, opts.storeFullEvent)
		require.Equal(t, []string{"path1", "path2"}, opts.jsonPaths)
	})
}
