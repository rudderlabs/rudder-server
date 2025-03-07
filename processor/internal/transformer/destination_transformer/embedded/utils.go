package utils

import (
	"strings"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
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

			fromOk = fromOk && strings.TrimSpace(from) != ""
			if !fromOk || !toOk {
				continue
			}

			if convertKeyToLower {
				topicMap[strings.ToLower(from)] = to
			} else {
				topicMap[from] = to
			}
		}
	}
	return topicMap
}

func GetValidationErrorStatTags(destination backendconfig.DestinationT) map[string]string {
	return map[string]string{
		"destinationId":  destination.ID,
		"workspaceId":    destination.WorkspaceID,
		"destType":       destination.DestinationDefinition.Name,
		"module":         "destination",
		"implementation": "native",
		"errorCategory":  "dataValidation",
		"errorType":      "configuration",
		"feature":        "processor",
	}
}

/* TODO: remove this once we stop comparing response from embedded and legacy
 * we need this because response from legacy is a map[string]interface{} and not a types.SingularEventT
 * and we need to convert it to types.SingularEventT to compare with response from embedded
 */
func GetMessageAsMap(message types.SingularEventT) map[string]interface{} {
	return message
}
