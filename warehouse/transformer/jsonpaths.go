package transformer

import (
	"strings"

	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

func extractJSONPathInfo(jsonPaths []string) jsonPathInfo {
	keysMap, legacyKeysMap := make(map[string]int), make(map[string]int)
	for _, jsonPath := range jsonPaths {
		if trimmedJSONPath := strings.TrimSpace(jsonPath); trimmedJSONPath != "" {
			splitPaths := strings.Split(jsonPath, ".")
			key := strings.Join(splitPaths, "_")
			pos := len(splitPaths) - 1

			if utils.HasJSONPathPrefix(jsonPath) {
				keysMap[key] = pos
				continue
			}
			legacyKeysMap[key] = pos
		}
	}
	return jsonPathInfo{keysMap, legacyKeysMap}
}

func isValidJSONPathKey(key string, level int, jsonKeys map[string]int) bool {
	if val, exists := jsonKeys[key]; exists {
		return val == level
	}
	return false
}

func isValidLegacyJSONPathKey(eventType, key string, level int, jsonKeys map[string]int) bool {
	if eventType == "track" {
		return isValidJSONPathKey(key, level, jsonKeys)
	}
	return false
}
