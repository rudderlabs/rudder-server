package pubsub

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/stringify"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	utils "github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded"
	types "github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var sourceKeys = []string{"properties", "traits", "context.traits"}

func Transform(_ context.Context, events []types.TransformerEvent) types.Response {
	response := types.Response{}
	topicMap := utils.GetTopicMap(events[0].Destination, "eventToTopicMap", true)
	attributesMap := getAttributesMap(events[0].Destination)

	for _, event := range events {
		event.Metadata.SourceDefinitionType = "" // TODO: Currently, it's getting ignored during JSON marshalling Remove this once we start using it.

		if event.Destination.ID != events[0].Destination.ID {
			panic("all events must have the same destination")
		}

		event.Message = utils.UpdateTimestampFieldForRETLEvent(event.Message)
		topic, err := getTopic(event, topicMap)
		if err != nil {
			response.FailedEvents = append(response.FailedEvents, types.TransformerResponse{
				Error:      err.Error(),
				Metadata:   event.Metadata,
				StatusCode: http.StatusBadRequest,
				StatTags:   utils.GetValidationErrorStatTags(event.Destination),
			})
			continue
		}

		attributes := getAttributesMapFromEvent(event, attributesMap)
		userID := ""
		if id, ok := event.Message["userId"].(string); ok && id != "" {
			userID = id
		} else if id, ok := event.Message["anonymousId"].(string); ok {
			userID = id
		}

		response.Events = append(response.Events, types.TransformerResponse{
			Output: map[string]interface{}{
				"userId":     userID,
				"message":    utils.GetMessageAsMap(event.Message),
				"topicId":    topic,
				"attributes": attributes,
			},
			StatusCode: http.StatusOK,
			Metadata:   event.Metadata,
		})
	}

	return response
}

func getAttributesMap(destination backendconfig.DestinationT) map[string][]string {
	attributesMap := make(map[string][]string)

	eventToAttributesMap, ok := destination.Config["eventToAttributesMap"]
	if !ok {
		return attributesMap
	}
	eventToAttributesMapList, ok := eventToAttributesMap.([]interface{})
	if !ok || len(eventToAttributesMapList) == 0 {
		return attributesMap
	}

	for _, mapping := range eventToAttributesMapList {
		if m, ok := mapping.(map[string]interface{}); ok {
			from, fromOk := m["from"].(string)
			to, toOk := m["to"].(string)

			fromOk = fromOk && strings.TrimSpace(from) != ""
			if fromOk && toOk {
				attributesMap[strings.ToLower(from)] = append(attributesMap[strings.ToLower(from)], to)
			}
		}
	}
	return attributesMap
}

func getTopic(event types.TransformerEvent, topicMap map[string]string) (string, error) {
	var eventType string
	if et, ok := event.Message["type"].(string); ok && et != "" {
		eventType = et
	} else {
		return "", fmt.Errorf("type is required for event")
	}

	if eventName, ok := event.Message["event"].(string); ok && eventName != "" {
		if topic, exists := topicMap[strings.ToLower(eventName)]; exists && topic != "" {
			return topic, nil
		}
	}

	if topic, exists := topicMap[strings.ToLower(eventType)]; exists && topic != "" {
		return topic, nil
	}

	if topic, exists := topicMap["*"]; exists && topic != "" {
		return topic, nil
	}

	return "", fmt.Errorf("no topic set for this event")
}

func getAttributeKeysFromEvent(event types.TransformerEvent, attributesMap map[string][]string) []string {
	if eventName, ok := event.Message["event"].(string); ok && eventName != "" {
		if keys, ok := attributesMap[strings.ToLower(eventName)]; ok {
			return keys
		}
	}

	if eventType, ok := event.Message["type"].(string); ok {
		if keys, ok := attributesMap[strings.ToLower(eventType)]; ok {
			return keys
		}
	}

	return attributesMap["*"]
}

func getAttributesMapFromEvent(event types.TransformerEvent, attributesMap map[string][]string) map[string]interface{} {
	attributes := getAttributeKeysFromEvent(event, attributesMap)
	attributeMetadata := make(map[string]interface{})
	for _, attribute := range attributes {
		if value, found := getAttributeValue(event.Message, attribute); found {
			parts := strings.Split(attribute, ".")
			key := parts[len(parts)-1]
			attributeMetadata[key] = value
		}
	}

	return attributeMetadata
}

// getAttributeValue searches for an attribute in the message and its nested structures
func getAttributeValue(message map[string]interface{}, attribute string) (string, bool) {
	attributeKeys := strings.Split(attribute, ".")
	if v := misc.MapLookup(message, attributeKeys...); v != nil {
		return stringify.Any(v), true
	}

	for _, sourceKey := range sourceKeys {
		keys := append(strings.Split(sourceKey, "."), attributeKeys...)
		if v := misc.MapLookup(message, keys...); v != nil {
			return stringify.Any(v), true
		}
	}

	return "", false
}
