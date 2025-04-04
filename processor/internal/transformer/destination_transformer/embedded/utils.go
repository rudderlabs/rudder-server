package utils

import (
	"strings"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	genericTimestampFieldMap = []string{"timestamp", "originalTimestamp"}
	timestampValsMap         = map[string][]string{
		"identify": append([]string{"context.timestamp", "context.traits.timestamp", "traits.timestamp"}, genericTimestampFieldMap...),
		"track":    append([]string{"properties.timestamp"}, genericTimestampFieldMap...),
	}
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

func UpdateTimestampFieldForRETLEvent(eventMessage types.SingularEventT) types.SingularEventT {
	if eventMessage["channel"] != "sources" {
		return eventMessage
	}

	if v := misc.MapLookup(eventMessage, "context", "mappedToDestination"); v != nil && v != "" {
		return eventMessage
	}

	eventType, ok := eventMessage["type"].(string)
	if !ok {
		return eventMessage
	}

	newEventMessage := types.SingularEventT{}
	for k, v := range eventMessage {
		newEventMessage[k] = v
	}

	for _, timestampVal := range timestampValsMap[eventType] {
		timestampValParts := strings.Split(timestampVal, ".")
		if v := misc.MapLookup(eventMessage, timestampValParts...); v != nil && v != "" {
			newEventMessage["timestamp"] = v
			break
		}
	}

	return newEventMessage
}
