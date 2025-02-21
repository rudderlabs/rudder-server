package transformer

import (
	"strings"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

func extractIntrOpts(destType string, message map[string]any) intrOptions {
	options := misc.MapLookup(message, "integrations", destType, "options")
	if options == nil || !utils.IsObject(options) {
		return mergeDataWarehouseIntrOpts(destType, message, intrOptions{})
	}

	var opts intrOptions
	var jsonPaths []any

	srcMap := options.(map[string]any)

	setOption(srcMap, "skipReservedKeywordsEscaping", &opts.skipReservedKeywordsEscaping)
	setOption(srcMap, "skipTracksTable", &opts.skipTracksTable)
	setOption(srcMap, "skipUsersTable", &opts.skipUsersTable)
	setOption(srcMap, "useBlendoCasing", &opts.useBlendoCasing)
	setOption(srcMap, "jsonPaths", &jsonPaths)

	if len(jsonPaths) > 0 && utils.IsJSONPathSupportedAsPartOfConfig(destType) {
		for _, jp := range jsonPaths {
			if jpStr, ok := jp.(string); ok {
				opts.jsonPaths = append(opts.jsonPaths, jpStr)
			}
		}
	}
	return mergeDataWarehouseIntrOpts(destType, message, opts)
}

func mergeDataWarehouseIntrOpts(destType string, message map[string]any, opts intrOptions) intrOptions {
	options := misc.MapLookup(message, "integrations", "DATA_WAREHOUSE", "options")
	if options == nil || !utils.IsObject(options) {
		return opts
	}

	var jsonPaths []any

	srcMap := options.(map[string]any)

	setOption(srcMap, "jsonPaths", &jsonPaths)
	if len(jsonPaths) > 0 && utils.IsJSONPathSupportedAsPartOfConfig(destType) {
		for _, jp := range jsonPaths {
			if jpStr, ok := jp.(string); ok {
				opts.jsonPaths = append(opts.jsonPaths, jpStr)
			}
		}
	}
	return opts
}

func extractDestOpts(destType string, destConfig map[string]any) destOptions {
	var jsonPaths string
	var opts destOptions

	setOption(destConfig, "skipTracksTable", &opts.skipTracksTable)
	setOption(destConfig, "skipUsersTable", &opts.skipUsersTable)
	setOption(destConfig, "underscoreDivideNumbers", &opts.underscoreDivideNumbers)
	setOption(destConfig, "allowUsersContextTraits", &opts.allowUsersContextTraits)
	setOption(destConfig, "storeFullEvent", &opts.storeFullEvent)
	setOption(destConfig, "jsonPaths", &jsonPaths)

	if len(jsonPaths) > 0 && utils.IsJSONPathSupportedAsPartOfConfig(destType) {
		opts.jsonPaths = strings.Split(jsonPaths, ",")
	}
	return opts
}

func setOption[T any](src map[string]any, key string, dest *T) {
	if val, ok := src[key].(T); ok {
		*dest = val
	}
}
