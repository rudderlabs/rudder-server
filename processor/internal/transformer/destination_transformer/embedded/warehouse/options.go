package warehouse

import (
	"strings"

	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse/internal/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func extractIntrOpts(destType string, message map[string]any, jsonPathsSupported bool) intrOptions {
	options := misc.MapLookup(message, "integrations", destType, "options")
	if options == nil || !utils.IsObject(options) {
		return mergeDataWarehouseIntrOpts(message, intrOptions{}, jsonPathsSupported)
	}

	var opts intrOptions
	var jsonPaths []any

	srcMap := options.(map[string]any)

	setOption(srcMap, "skipReservedKeywordsEscaping", &opts.skipReservedKeywordsEscaping)
	setOption(srcMap, "skipTracksTable", &opts.skipTracksTable)
	setOption(srcMap, "skipUsersTable", &opts.skipUsersTable)
	setOption(srcMap, "useBlendoCasing", &opts.useBlendoCasing)
	setOption(srcMap, "jsonPaths", &jsonPaths)

	if len(jsonPaths) > 0 && jsonPathsSupported {
		for _, jp := range jsonPaths {
			if jpStr, ok := jp.(string); ok {
				opts.jsonPaths = append(opts.jsonPaths, jpStr)
			}
		}
	}
	return mergeDataWarehouseIntrOpts(message, opts, jsonPathsSupported)
}

func mergeDataWarehouseIntrOpts(message map[string]any, opts intrOptions, jsonPathsSupported bool) intrOptions {
	options := misc.MapLookup(message, "integrations", "DATA_WAREHOUSE", "options")
	if options == nil || !utils.IsObject(options) {
		return opts
	}

	var jsonPaths []any

	srcMap := options.(map[string]any)

	setOption(srcMap, "jsonPaths", &jsonPaths)
	if len(jsonPaths) > 0 && jsonPathsSupported {
		mergedJSONPaths := make([]string, 0, len(jsonPaths)+len(opts.jsonPaths))
		for _, jsonPath := range jsonPaths {
			if jsonPathStr, ok := jsonPath.(string); ok {
				mergedJSONPaths = append(mergedJSONPaths, jsonPathStr)
			}
		}
		mergedJSONPaths = append(mergedJSONPaths, opts.jsonPaths...)
		opts.jsonPaths = mergedJSONPaths
	}
	return opts
}

func extractDestOpts(destConfig map[string]any, jsonPathsSupported bool) destOptions {
	var jsonPaths string
	var opts destOptions

	setOption(destConfig, "skipTracksTable", &opts.skipTracksTable)
	setOption(destConfig, "skipUsersTable", &opts.skipUsersTable)
	setOption(destConfig, "underscoreDivideNumbers", &opts.underscoreDivideNumbers)
	setOption(destConfig, "allowUsersContextTraits", &opts.allowUsersContextTraits)
	setOption(destConfig, "storeFullEvent", &opts.storeFullEvent)
	setOption(destConfig, "jsonPaths", &jsonPaths)

	if len(jsonPaths) > 0 && jsonPathsSupported {
		opts.jsonPaths = strings.Split(jsonPaths, ",")
	}
	return opts
}

func setOption[T any](src map[string]any, key string, dest *T) {
	if val, ok := src[key].(T); ok {
		*dest = val
	}
}
