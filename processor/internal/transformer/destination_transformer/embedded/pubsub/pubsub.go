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

		var message map[string]interface{}
		message = event.Message

		response.Events = append(response.Events, types.TransformerResponse{
			Output: map[string]interface{}{
				"userId":     userID,
				"message":    message,
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

			fromOk = fromOk && from != ""
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

	return "", fmt.Errorf("No topic set for this event")
}

func getAttributeKeysFromEvent(event types.TransformerEvent, attributesMap map[string][]string) []string {
	if eventName, ok := event.Message["event"].(string); ok && eventName != "" {
		if keys, ok := attributesMap[strings.ToLower(eventName)]; ok {
			return keys
		}
	}

	if eventType, ok := event.Message["eventType"].(string); ok {
		if keys, ok := attributesMap[strings.ToLower(eventType)]; ok {
			return keys
		}
	}

	return attributesMap["*"]
}

func getAttributesMapFromEvent(event types.TransformerEvent, attributesMap map[string][]string) map[string]any {
	attributes := getAttributeKeysFromEvent(event, attributesMap)
	attributeMetadata := make(map[string]any)
	for _, attribute := range attributes {
		if v, ok := event.Message[attribute]; ok {
			attributeMetadata[attribute] = stringify.Any(v)
			continue
		}
		for _, sourceKey := range sourceKeys {
			keys := append(strings.Split(sourceKey, "."), attribute)
			v := misc.MapLookup(event.Message, keys...)
			if v != nil {
				attributeMetadata[attribute] = stringify.Any(v)
				break
			}
		}
	}

	refinedMetadata := make(map[string]any)
	for key, value := range attributeMetadata {
		parts := strings.Split(key, ".")
		refinedKey := parts[len(parts)-1]
		refinedMetadata[refinedKey] = value
	}
	return refinedMetadata
}
