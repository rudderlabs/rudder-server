package pubsub

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/rudderlabs/rudder-go-kit/stringify"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	types "github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var sourceKeys = []string{"properties", "traits", "context.traits"}

func Transform(_ context.Context, events []types.TransformerEvent) types.Response {
	topicMap, err := getTopicMap(events[0].Destination)
	response := types.Response{}
	if err != nil {
		for _, event := range events {
			response.FailedEvents = append(response.FailedEvents, types.TransformerResponse{
				Error:      fmt.Errorf("failed to get topic map: %w", err).Error(),
				Metadata:   event.Metadata,
				StatusCode: http.StatusInternalServerError,
			})
		}
		return response
	}

	attributesMap, err := getAttributesMap(events[0].Destination)
	if err != nil {
		for _, event := range events {
			response.FailedEvents = append(response.FailedEvents, types.TransformerResponse{
				Error:      fmt.Errorf("failed to get attributes map: %w", err).Error(),
				Metadata:   event.Metadata,
				StatusCode: http.StatusInternalServerError,
			})
		}
		return response
	}

	for _, event := range events {
		topic, err := getTopic(event, topicMap)
		if err != nil {
			response.FailedEvents = append(response.FailedEvents, types.TransformerResponse{
				Error:      fmt.Errorf("failed to get topic: %w", err).Error(),
				Metadata:   event.Metadata,
				StatusCode: http.StatusInternalServerError,
			})
		}
		attributes := getAttributesMapFromEvent(event, attributesMap)
		userID := ""
		if id, ok := event.Message["userId"].(string); ok {
			userID = id
		} else if id, ok := event.Message["anonymousId"].(string); ok {
			userID = id
		}
		response.Events = append(response.Events, types.TransformerResponse{
			Output: map[string]interface{}{
				"userId":     userID,
				"message":    event.Message,
				"topicId":    topic,
				"attributes": attributes,
			},
			StatusCode: http.StatusOK,
			Metadata:   event.Metadata,
		})
	}

	return response
}

func getTopicMap(destination backendconfig.DestinationT) (map[string]string, error) {
	eventToTopicMap, ok := destination.Config["eventToTopicMap"]
	if !ok {
		return nil, fmt.Errorf("eventToTopicMap not found in destination config")
	}
	eventToTopicMapList, ok := eventToTopicMap.([]interface{})
	if !ok {
		return nil, fmt.Errorf("eventToTopicMap is not a list")
	}
	topicMap := make(map[string]string)
	for _, mapping := range eventToTopicMapList {
		if m, ok := mapping.(map[string]interface{}); ok {
			from, fromOk := m["from"].(string)
			to, toOk := m["to"].(string)
			if fromOk && toOk {
				topicMap[strings.ToLower(from)] = to
			}
		}
	}
	return topicMap, nil
}

func getAttributesMap(destination backendconfig.DestinationT) (map[string][]string, error) {
	eventToAttributesMap, ok := destination.Config["eventToAttributesMap"]
	if !ok {
		return nil, fmt.Errorf("eventToAttributesMap not found in destination config")
	}
	eventToAttributesMapList, ok := eventToAttributesMap.([]interface{})
	if !ok || len(eventToAttributesMapList) == 0 {
		return nil, fmt.Errorf("eventToAttributesMap is not a list")
	}

	attributesMap := make(map[string][]string)
	for _, mapping := range eventToAttributesMapList {
		if m, ok := mapping.(map[string]interface{}); ok {
			from, fromOk := m["from"].(string)
			to, toOk := m["to"].(string)
			if fromOk && toOk {
				attributesMap[strings.ToLower(from)] = append(attributesMap[strings.ToLower(from)], to)
			}
		}
	}
	return attributesMap, nil
}

func getTopic(event types.TransformerEvent, topicMap map[string]string) (string, error) {
	if msgType, ok := event.Message["type"].(string); !ok || msgType == "" {
		return "", fmt.Errorf("type is required for event")
	}

	if eventName, ok := event.Message["event"].(string); ok {
		if topic, exists := topicMap[strings.ToLower(eventName)]; exists {
			return topic, nil
		}
	}

	if eventType, ok := event.Message["eventType"].(string); ok {
		if topic, exists := topicMap[strings.ToLower(eventType)]; exists {
			return topic, nil
		}
	}

	if topic, exists := topicMap["*"]; exists {
		return topic, nil
	}

	return "", nil
}

func getAttributeKeysFromEvent(event types.TransformerEvent, attributesMap map[string][]string) []string {
	if eventName, ok := event.Message["event"]; ok {
		if eventName, ok := eventName.(string); ok {
			if keys, ok := attributesMap[strings.ToLower(eventName)]; ok {
				return keys
			}
		}
	}

	if eventType, ok := event.Message["eventType"]; ok {
		if eventType, ok := eventType.(string); ok {
			if keys, ok := attributesMap[strings.ToLower(eventType)]; ok {
				return keys
			}
		}
	}

	return attributesMap["*"]
}

func getAttributesMapFromEvent(event types.TransformerEvent, attributesMap map[string][]string) map[string]string {
	attributes := getAttributeKeysFromEvent(event, attributesMap)
	attributeMetadata := make(map[string]string)
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

	refinedMetadata := make(map[string]string)
	for key, value := range attributeMetadata {
		parts := strings.Split(key, ".")
		refinedKey := parts[len(parts)-1]
		refinedMetadata[refinedKey] = value
	}
	return refinedMetadata
}
