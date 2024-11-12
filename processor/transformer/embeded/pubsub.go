package embeded

import (
	"context"
	"strings"

	"github.com/rudderlabs/rudder-server/processor/transformer"
)

type PubSubTransformer struct{}

func (t *PubSubTransformer) Transform(ctx context.Context, clientEvents []transformer.TransformerEvent, batchSize int) transformer.Response {
	var response transformer.Response

	for _, clientEvent := range clientEvents {
		msg := clientEvent.Message

		// Get topic ID for the event
		topicID := getTopicID(clientEvent)
		if topicID == "" {
			response.FailedEvents = append(response.FailedEvents, transformer.TransformerResponse{
				Output: clientEvent.Message,
			})
			continue
		}

		// Create attributes metadata
		attributes := createAttributesMetadata(clientEvent)

		// Create transformed event
		transformedEvent := transformer.TransformerResponse{
			Output: map[string]interface{}{
				"message":    msg,
				"topic_id":   topicID,
				"attributes": attributes,
				"user_id":    getEventUserID(clientEvent),
			},
		}

		response.Events = append(response.Events, transformedEvent)
	}

	return response
}

func getTopicID(event transformer.TransformerEvent) string {
	// Get topic mapping from config
	topicMap := event.Destination.Config["eventToTopicMap"].(map[string]string)

	if event.Message["type"] == "" {
		return ""
	}

	// Try to match event name first, then type, then fallback to wildcard
	if event.Message["event"] != "" {
		if topic, ok := topicMap[strings.ToLower(event.Message["event"].(string))]; ok {
			return topic
		}
	}

	if topic, ok := topicMap[strings.ToLower(event.Message["type"].(string))]; ok {
		return topic
	}

	return topicMap["*"]
}

func createAttributesMetadata(event transformer.TransformerEvent) map[string]string {
	attributes := make(map[string]string)

	// Get attributes mapping from config
	attrMap := event.Destination.Config["eventToAttributesMap"].(map[string]string)
	if len(attrMap) == 0 {
		return attributes
	}

	// ... rest of attributes logic ...

	return attributes
}

func getEventUserID(event transformer.TransformerEvent) string {
	if event.Message["user_id"] != "" {
		return event.Message["user_id"].(string)
	}
	return event.Message["anonymous_id"].(string)
}
