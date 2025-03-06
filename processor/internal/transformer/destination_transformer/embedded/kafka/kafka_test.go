package kafka

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
)

func TestTransform(t *testing.T) {
	destinationWithConfigTopic := backendconfig.DestinationT{
		Config: map[string]interface{}{
			"topic": "default-topic",
		},
	}

	destinationWithEventMappingTopic := backendconfig.DestinationT{
		Config: map[string]interface{}{
			"topic":            "default-topic",
			"enableMultiTopic": true,
			"eventTypeToTopicMap": []interface{}{
				map[string]interface{}{
					"from": "identify",
					"to":   "identify-topic",
				},
				map[string]interface{}{
					"from": "group",
					"to":   "group-topic",
				},
				map[string]interface{}{
					"from": "",
					"to":   "empty-topic",
				},
				map[string]interface{}{
					"from": "alias",
					"to":   "",
				},
			},
			"eventToTopicMap": []interface{}{
				map[string]interface{}{
					"from": "",
					"to":   "empty-event-topic",
				},
				map[string]interface{}{
					"from": "event-A",
					"to":   "event-A-topic",
				},
				map[string]interface{}{
					"from": "event-B",
					"to":   "event-B-topic",
				},
				map[string]interface{}{
					"from": "event-C",
					"to":   "",
				},
			},
		},
	}

	metadataWithRudderID := types.Metadata{
		RudderID: "rudder-id-123",
	}

	expectedMetadataWithDefaultTopic := types.Metadata{
		RudderID: "rudder-id-123<<>>default-topic",
	}

	cases := []struct {
		name   string
		events []types.TransformerEvent
		want   types.Response
	}{
		{
			name: "should set correct userId for each event",
			events: []types.TransformerEvent{
				{
					Message: map[string]interface{}{
						"userId": "user-123",
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"anonymousId": "anonymous-123",
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId":      "",
						"anonymousId": "anonymous-123",
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId":      "user-123",
						"anonymousId": "",
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
			},
			want: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
							},
							"topic":  "default-topic",
							"userId": "user-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"anonymousId": "anonymous-123",
							},
							"topic":  "default-topic",
							"userId": "anonymous-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId":      "",
								"anonymousId": "anonymous-123",
							},
							"topic":  "default-topic",
							"userId": "anonymous-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId":      "user-123",
								"anonymousId": "",
							},
							"topic":  "default-topic",
							"userId": "user-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "should set schemaId when present and not empty in integrationsObj",
			events: []types.TransformerEvent{
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"integrations": map[string]interface{}{
							"kafka": map[string]interface{}{
								"schemaId": "schema-id-123",
							},
						},
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"integrations": map[string]interface{}{
							"KAFKA": map[string]interface{}{
								"schemaId": "",
							},
						},
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
			},
			want: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"integrations": map[string]interface{}{
									"kafka": map[string]interface{}{
										"schemaId": "schema-id-123",
									},
								},
							},
							"topic":    "default-topic",
							"userId":   "user-123",
							"schemaId": "schema-id-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"integrations": map[string]interface{}{
									"KAFKA": map[string]interface{}{
										"schemaId": "",
									},
								},
							},
							"topic":  "default-topic",
							"userId": "user-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "should set correct topic from integration config",
			events: []types.TransformerEvent{
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"integrations": map[string]interface{}{
							"kafka": map[string]interface{}{
								"topic": "integrations-topic-kafka",
							},
						},
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"integrations": map[string]interface{}{
							"Kafka": map[string]interface{}{
								"topic": "integrations-topic-Kafka",
							},
						},
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"integrations": map[string]interface{}{
							"KAFKA": map[string]interface{}{
								"topic": "integrations-topic-KAFKA",
							},
						},
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"integrations": map[string]interface{}{
							"KAFKA": map[string]interface{}{
								"topic": "integrations-topic-KAFKA",
							},
							"kafka": map[string]interface{}{
								"topic": "integrations-topic-kafka",
							},
							"Kafka": map[string]interface{}{
								"topic": "integrations-topic-Kafka",
							},
						},
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
			},
			want: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"integrations": map[string]interface{}{
									"kafka": map[string]interface{}{
										"topic": "integrations-topic-kafka",
									},
								},
							},
							"topic":  "integrations-topic-kafka",
							"userId": "user-123",
						},
						Metadata: types.Metadata{
							RudderID: "rudder-id-123<<>>integrations-topic-kafka",
						},
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"integrations": map[string]interface{}{
									"Kafka": map[string]interface{}{
										"topic": "integrations-topic-Kafka",
									},
								},
							},
							"topic":  "integrations-topic-Kafka",
							"userId": "user-123",
						},
						Metadata: types.Metadata{
							RudderID: "rudder-id-123<<>>integrations-topic-Kafka",
						},
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"integrations": map[string]interface{}{
									"KAFKA": map[string]interface{}{
										"topic": "integrations-topic-KAFKA",
									},
								},
							},
							"topic":  "integrations-topic-KAFKA",
							"userId": "user-123",
						},
						Metadata: types.Metadata{
							RudderID: "rudder-id-123<<>>integrations-topic-KAFKA",
						},
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"integrations": map[string]interface{}{
									"KAFKA": map[string]interface{}{
										"topic": "integrations-topic-KAFKA",
									},
									"kafka": map[string]interface{}{
										"topic": "integrations-topic-kafka",
									},
									"Kafka": map[string]interface{}{
										"topic": "integrations-topic-Kafka",
									},
								},
							},
							"topic":  "integrations-topic-KAFKA",
							"userId": "user-123",
						},
						Metadata: types.Metadata{
							RudderID: "rudder-id-123<<>>integrations-topic-KAFKA",
						},
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "should set correct topic from destination config",
			events: []types.TransformerEvent{
				{
					Message: map[string]interface{}{
						"userId": "123",
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "456",
						"integrations": map[string]interface{}{
							"unknown": map[string]interface{}{
								"topic": "integrations-topic",
							},
						},
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "456",
						"integrations": map[string]interface{}{
							"kafka": map[string]interface{}{
								"topic": "",
							},
						},
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
			},
			want: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "123",
							},
							"topic":  "default-topic",
							"userId": "123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "456",
								"integrations": map[string]interface{}{
									"unknown": map[string]interface{}{
										"topic": "integrations-topic",
									},
								},
							},
							"topic":  "default-topic",
							"userId": "456",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "456",
								"integrations": map[string]interface{}{
									"kafka": map[string]interface{}{
										"topic": "",
									},
								},
							},
							"topic":  "default-topic",
							"userId": "456",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "should set correct topic from event type mapping if mapping is present",
			events: []types.TransformerEvent{
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"type":   "identify",
					},
					Destination: destinationWithEventMappingTopic,
					Metadata:    metadataWithRudderID,
				},

				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"type":   "group",
					},
					Destination: destinationWithEventMappingTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"type":   "",
					},
					Destination: destinationWithEventMappingTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"type":   "alias",
					},
					Destination: destinationWithEventMappingTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "user-123",
					},
					Destination: destinationWithEventMappingTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"type":   "unknown-event-type",
					},
					Destination: destinationWithEventMappingTopic,
					Metadata:    metadataWithRudderID,
				},
			},
			want: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"type":   "identify",
							},
							"topic":  "identify-topic",
							"userId": "user-123",
						},
						Metadata: types.Metadata{
							RudderID: "rudder-id-123<<>>identify-topic",
						},
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"type":   "group",
							},
							"topic":  "group-topic",
							"userId": "user-123",
						},
						Metadata: types.Metadata{
							RudderID: "rudder-id-123<<>>group-topic",
						},
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"type":   "",
							},
							"topic":  "default-topic",
							"userId": "user-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"type":   "alias",
							},
							"topic":  "default-topic",
							"userId": "user-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
							},
							"topic":  "default-topic",
							"userId": "user-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"type":   "unknown-event-type",
							},
							"topic":  "default-topic",
							"userId": "user-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "should set correct topic from event mapping if mapping is present",
			events: []types.TransformerEvent{
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"event":  "unknown-event",
						"type":   "track",
					},
					Destination: destinationWithEventMappingTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"event":  "event-A",
						"type":   "track",
					},
					Destination: destinationWithEventMappingTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"event":  "",
						"type":   "track",
					},
					Destination: destinationWithEventMappingTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"type":   "track",
					},
					Destination: destinationWithEventMappingTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"event":  "event-B",
						"type":   "track",
					},
					Destination: destinationWithEventMappingTopic,
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId": "user-123",
						"event":  "event-C",
						"type":   "track",
					},
					Destination: destinationWithEventMappingTopic,
					Metadata:    metadataWithRudderID,
				},
			},
			want: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"event":  "unknown-event",
								"type":   "track",
							},
							"topic":  "default-topic",
							"userId": "user-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},

					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"event":  "event-A",
								"type":   "track",
							},
							"topic":  "event-A-topic",
							"userId": "user-123",
						},
						Metadata: types.Metadata{
							RudderID: "rudder-id-123<<>>event-A-topic",
						},
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"event":  "",
								"type":   "track",
							},
							"topic":  "default-topic",
							"userId": "user-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"type":   "track",
							},
							"topic":  "default-topic",
							"userId": "user-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"event":  "event-B",
								"type":   "track",
							},
							"topic":  "event-B-topic",
							"userId": "user-123",
						},
						Metadata: types.Metadata{
							RudderID: "rudder-id-123<<>>event-B-topic",
						},
						StatusCode: http.StatusOK,
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId": "user-123",
								"event":  "event-C",
								"type":   "track",
							},
							"topic":  "default-topic",
							"userId": "user-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
				},
			},
		},
		{
			name: "should should throw error if topic is not present",
			events: []types.TransformerEvent{
				{
					Message: map[string]interface{}{
						"userId":    "user-123",
						"messageId": "message-id-1",
					},
					Destination: backendconfig.DestinationT{},
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId":    "user-123",
						"messageId": "message-id-2",
					},
					Destination: backendconfig.DestinationT{},
					Metadata:    metadataWithRudderID,
				},
				{
					Message: map[string]interface{}{
						"userId":    "user-123",
						"messageId": "message-id-3",
					},
					Destination: destinationWithConfigTopic,
					Metadata:    metadataWithRudderID,
				},
			},
			want: types.Response{
				FailedEvents: []types.TransformerResponse{
					{
						Error:      "failed to get topic map: topic is required for Kafka destination",
						Metadata:   metadataWithRudderID,
						StatusCode: http.StatusInternalServerError,
					},
					{
						Error:      "failed to get topic map: topic is required for Kafka destination",
						Metadata:   metadataWithRudderID,
						StatusCode: http.StatusInternalServerError,
					},
				},
				Events: []types.TransformerResponse{
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"userId":    "user-123",
								"messageId": "message-id-3",
							},
							"topic":  "default-topic",
							"userId": "user-123",
						},
						Metadata:   expectedMetadataWithDefaultTopic,
						StatusCode: http.StatusOK,
					},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Transform(context.Background(), c.events)
			require.Equal(t, c.want, got)
		})
	}
}
