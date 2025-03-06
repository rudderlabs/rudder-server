package kafka

import (
	"context"
	"fmt"
	"net/http"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	utils "github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded"
	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var canonicalNames = []string{"KAFKA", "kafka", "Kafka"}

func Transform(_ context.Context, events []types.TransformerEvent) types.Response {
	response := types.Response{}
	eventTypeToTopicMap := utils.GetTopicMap(events[0].Destination, "eventTypeToTopicMap", true)
	eventToTopicMap := utils.GetTopicMap(events[0].Destination, "eventToTopicMap", false)

	for _, event := range events {
		var integrationsObj map[string]interface{}
		for _, canonicalName := range canonicalNames {
			if inObj, ok := misc.MapLookup(event.Message, "integrations", canonicalName).(map[string]interface{}); ok {
				integrationsObj = inObj
				break
			}
		}

		var userId string
		if id, ok := event.Message["userId"].(string); ok && id != "" {
			userId = id
		} else {
			userId, _ = event.Message["anonymousId"].(string)
		}

		topic, err := getTopic(event, integrationsObj, &eventTypeToTopicMap, &eventToTopicMap)
		if err != nil {
			response.FailedEvents = append(response.FailedEvents, types.TransformerResponse{
				Error:      fmt.Errorf("failed to get topic map: %w", err).Error(),
				Metadata:   event.Metadata,
				StatusCode: http.StatusInternalServerError,
			})
			continue
		}

		var message map[string]interface{}
		message = event.Message

		outputEvent := map[string]interface{}{
			"message": message,
			"userId":  userId,
			"topic":   topic,
		}

		if schemaId, ok := integrationsObj["schemaId"].(string); ok && schemaId != "" {
			outputEvent["schemaId"] = schemaId
		}

		event.Metadata.RudderID = fmt.Sprintf("%s<<>>%s", event.Metadata.RudderID, topic)
		event.Metadata.SourceDefinitionType = "" // TODO: Currently, it's getting ignored during JSON marshalling Remove this once we start using it.

		response.Events = append(response.Events, types.TransformerResponse{
			Output:     outputEvent,
			Metadata:   event.Metadata,
			StatusCode: http.StatusOK,
		})
	}

	return response
}

func getTopic(event types.TransformerEvent, integrationsObj map[string]interface{}, eventTypeToTopicMap *map[string]string, eventToTopicMap *map[string]string) (string, error) {
	if topic, ok := integrationsObj["topic"].(string); ok && topic != "" {
		return topic, nil
	} else if configTopic := filterConfigTopics(event.Message, event.Destination, eventTypeToTopicMap, eventToTopicMap); configTopic != "" {
		return configTopic, nil
	} else if destTopic, ok := event.Destination.Config["topic"].(string); ok {
		return destTopic, nil
	}

	return "", fmt.Errorf("topic is required for Kafka destination")
}

func filterConfigTopics(message types.SingularEventT, destination backendconfig.DestinationT, eventTypeToTopicMap *map[string]string, eventToTopicMap *map[string]string) string {
	if destination.Config["enableMultiTopic"] == true {
		messageType, ok := message["type"].(string)
		if !ok {
			return ""
		}

		switch messageType {
		case "identify", "screen", "page", "group", "alias":
			{
				return (*eventTypeToTopicMap)[messageType]
			}
		case "track":
			{
				eventName, ok := message["event"].(string)
				if !ok || eventName == "" {
					return ""
				}
				return (*eventToTopicMap)[eventName]
			}
		}
	}
	return ""
}
