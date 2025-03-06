package utils

import (
	"strings"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

func GetTopicMap(destination backendconfig.DestinationT, key string, convertKeyToLower bool) map[string]string {
	topicMap := make(map[string]string)
	eventToTopicMap, ok := destination.Config[key]
	if !ok {
		return topicMap
	}

	eventToTopicMapList, ok := eventToTopicMap.([]interface{})
	if !ok {
		return topicMap
	}

	for _, mapping := range eventToTopicMapList {
		if m, ok := mapping.(map[string]interface{}); ok {
			from, fromOk := m["from"].(string)
			to, toOk := m["to"].(string)
			if fromOk && toOk {
				if convertKeyToLower {
					topicMap[strings.ToLower(from)] = to
				} else {
					topicMap[from] = to
				}
			}
		}
	}
	return topicMap
}
