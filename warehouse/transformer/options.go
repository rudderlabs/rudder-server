package transformer

import (
	"strings"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

type (
	integrationsOptions struct {
		// skipReservedKeywordsEscaping when set to true, will skip the escaping of reserved keywords
		skipReservedKeywordsEscaping bool
		// useBlendoCasing when set to true, will use the casing as per Blendo's requirement
		useBlendoCasing bool
		// jsonPaths is a list of json paths that should be extracted from the event and stored as raw instead of normalizing them
		jsonPaths []string
		// skipTracksTable when set to true, will skip the tracks event
		skipTracksTable bool
		// skipUsersTable when set to true, will skip the users event
		skipUsersTable bool
	}

	destConfigOptions struct {
		// skipTracksTable when set to true, will skip the tracks event
		skipTracksTable bool
		// skipUsersTable when set to true, will skip the users event
		skipUsersTable bool
		// storeFullEvent when set to true, will store the full event as rudder_event
		storeFullEvent bool
		// jsonPaths is a list of json paths that should be extracted from the event and stored as raw instead of normalizing them
		jsonPaths []string
		// underscoreDivideNumbers when set to false, if a column has a format like "_v_3_", it will be formatted to "_v3_"
		// underscoreDivideNumbers when set to true, if a column has a format like "_v_3_", we keep it like that
		// For older destinations, it will come as true and for new destinations this config will not be present which means we will treat it as false.
		underscoreDivideNumbers bool
		// allowUsersContextTraits when set to true, if context.traits.* is present, it will be added as context_traits_* and *,
		// e.g., for context.traits.name, context_traits_name and name will be added to the user's table.
		// allowUsersContextTraits when set to false, if context.traits.* is present, it will be added only as context_traits_*
		// e.g., for context.traits.name, only context_traits_name will be added to the user's table.
		// For older destinations, it will come as true, and for new destinations this config will not be present, which means we will treat it as false.
		allowUsersContextTraits bool
	}
)

func prepareIntegrationOptions(event ptrans.TransformerEvent) (opts integrationsOptions) {
	src := misc.MapLookup(event.Message, "integrations", event.Metadata.DestinationType, "options")
	if src == nil || !utils.IsObject(src) {
		return
	}
	var jsonPaths []any

	srcMap := src.(map[string]any)

	setOption(srcMap, "skipReservedKeywordsEscaping", &opts.skipReservedKeywordsEscaping)
	setOption(srcMap, "useBlendoCasing", &opts.useBlendoCasing)
	setOption(srcMap, "skipTracksTable", &opts.skipTracksTable)
	setOption(srcMap, "skipUsersTable", &opts.skipUsersTable)
	setOption(srcMap, "jsonPaths", &jsonPaths)

	for _, jp := range jsonPaths {
		if jpStr, ok := jp.(string); ok {
			opts.jsonPaths = append(opts.jsonPaths, jpStr)
		}
	}
	return
}

func prepareDestinationOptions(destType string, destConfig map[string]any) (opts destConfigOptions) {
	var jsonPaths string

	setOption(destConfig, "skipTracksTable", &opts.skipTracksTable)
	setOption(destConfig, "skipUsersTable", &opts.skipUsersTable)
	setOption(destConfig, "underscoreDivideNumbers", &opts.underscoreDivideNumbers)
	setOption(destConfig, "allowUsersContextTraits", &opts.allowUsersContextTraits)
	setOption(destConfig, "storeFullEvent", &opts.storeFullEvent)
	setOption(destConfig, "jsonPaths", &jsonPaths)

	if len(jsonPaths) > 0 && utils.IsJSONPathSupportedAsPartOfConfig(destType) {
		opts.jsonPaths = strings.Split(jsonPaths, ",")
	}
	return
}

func setOption[T any](src map[string]any, key string, dest *T) {
	if val, ok := src[key].(T); ok {
		*dest = val
	}
}
