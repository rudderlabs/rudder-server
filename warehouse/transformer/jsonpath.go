package transformer

import (
	"strings"

	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

type jsonPathInfo struct {
	keysMap       map[string]int
	legacyKeysMap map[string]int
}

func extractJSONPathInfo(jsonPaths []string) jsonPathInfo {
	jp := jsonPathInfo{
		keysMap:       make(map[string]int),
		legacyKeysMap: make(map[string]int),
	}
	for _, jsonPath := range jsonPaths {
		if trimmedJSONPath := strings.TrimSpace(jsonPath); trimmedJSONPath != "" {
			jp.processJSONPath(trimmedJSONPath)
		}
	}
	return jp
}

func (jp *jsonPathInfo) processJSONPath(jsonPath string) {
	splitPaths := strings.Split(jsonPath, ".")
	key := strings.Join(splitPaths, "_")
	pos := len(splitPaths) - 1

	if utils.HasJSONPathPrefix(jsonPath) {
		jp.keysMap[key] = pos
		return
	}
	jp.legacyKeysMap[key] = pos
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
