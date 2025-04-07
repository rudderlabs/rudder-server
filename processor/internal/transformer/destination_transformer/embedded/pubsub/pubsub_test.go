package pubsub

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	types "github.com/rudderlabs/rudder-server/processor/types"
)

func TestTransform(t *testing.T) {
	destinationWithConfigTopic := backendconfig.DestinationT{
		ID: "destination-id-123",
		Config: map[string]interface{}{
			"eventToTopicMap": []interface{}{
				map[string]interface{}{
					"from": "event-A",
					"to":   "topic-A",
				},
				map[string]interface{}{
					"from": "",
					"to":   "topic-empty-event-name",
				},
				map[string]interface{}{
					"from": "event-with-empty-topic",
					"to":   "",
				},
				map[string]interface{}{
					"from": "page",
					"to":   "",
				},
				map[string]interface{}{
					"from": "group",
					"to":   "topic-group",
				},
				map[string]interface{}{
					"from": "*",
					"to":   "topic-default",
				},
			},
		},
	}

	destinationWithConfigAttributes := backendconfig.DestinationT{
		ID: "destination-id-123",
		Config: map[string]interface{}{
			"eventToAttributesMap": []interface{}{
				map[string]interface{}{
					"from": "event-A",
					"to":   "attr-key-1",
				},
				map[string]interface{}{
					"from": "event-A",
					"to":   "attr-key-2",
				},
				map[string]interface{}{
					"from": "event-A",
					"to":   "attr-key-3",
				},
				map[string]interface{}{
					"from": "event-A",
					"to":   "A.B.attr-key-4",
				},
				map[string]interface{}{
					"from": "event-A",
					"to":   "A.B.C.attr-key-5",
				},
				map[string]interface{}{
					"from": "event-A",
					"to":   "A.B.attr-key-6",
				},
				map[string]interface{}{
					"from": "event-B",
					"to":   "attr-key-1",
				},
				map[string]interface{}{
					"from": "event-B",
					"to":   "",
				},
				map[string]interface{}{
					"from": "event-C",
				},
				map[string]interface{}{
					"from": "identify",
					"to":   "attr-key-1",
				},
				map[string]interface{}{
					"from": "identify",
					"to":   "attr-key-2",
				},
				map[string]interface{}{
					"from": "identify",
					"to":   "attr-key-3",
				},
				map[string]interface{}{
					"from": "*",
					"to":   "attr-key-default",
				},
			},
			"eventToTopicMap": []interface{}{
				map[string]interface{}{
					"from": "*",
					"to":   "topic-default",
				},
			},
		},
	}
	destinationStatTags := map[string]string{
		"destinationId":  "",
		"workspaceId":    "",
		"destType":       "",
		"module":         "destination",
		"implementation": "native",
		"errorCategory":  "dataValidation",
		"errorType":      "configuration",
		"feature":        "processor",
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
					Destination: destinationWithConfigTopic,
					Message: map[string]interface{}{
						"type":        "identify",
						"userId":      "user-id-123",
						"anonymousId": "anonymous-id-123",
					},
				},
				{
					Destination: destinationWithConfigTopic,
					Message: map[string]interface{}{
						"type":   "identify",
						"userId": "user-id-456",
					},
				},
				{
					Destination: destinationWithConfigTopic,
					Message: map[string]interface{}{
						"type":        "identify",
						"anonymousId": "anonymous-id-789",
					},
				},
				{
					Destination: destinationWithConfigTopic,
					Message: map[string]interface{}{
						"type":   "identify",
						"userId": "",
					},
				},
				{
					Destination: destinationWithConfigTopic,
					Message: map[string]interface{}{
						"type":        "identify",
						"anonymousId": "",
					},
				},
			},
			want: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: map[string]interface{}{
							"userId":  "user-id-123",
							"topicId": "topic-default",
							"message": map[string]interface{}{
								"type":        "identify",
								"userId":      "user-id-123",
								"anonymousId": "anonymous-id-123",
							},
							"attributes": map[string]interface{}{},
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
					{
						Output: map[string]interface{}{
							"userId":  "user-id-456",
							"topicId": "topic-default",
							"message": map[string]interface{}{
								"type":   "identify",
								"userId": "user-id-456",
							},
							"attributes": map[string]interface{}{},
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
					{
						Output: map[string]interface{}{
							"userId":  "anonymous-id-789",
							"topicId": "topic-default",
							"message": map[string]interface{}{
								"type":        "identify",
								"anonymousId": "anonymous-id-789",
							},
							"attributes": map[string]interface{}{},
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
					{
						Output: map[string]interface{}{
							"userId":  "",
							"topicId": "topic-default",
							"message": map[string]interface{}{
								"type":   "identify",
								"userId": "",
							},
							"attributes": map[string]interface{}{},
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
					{
						Output: map[string]interface{}{
							"userId":  "",
							"topicId": "topic-default",
							"message": map[string]interface{}{
								"type":        "identify",
								"anonymousId": "",
							},
							"attributes": map[string]interface{}{},
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
				},
			},
		},
		{
			name: "should set correct topicId for each event",
			events: []types.TransformerEvent{
				{
					Destination: destinationWithConfigTopic,
					Message: map[string]interface{}{
						"type":  "track",
						"event": "event-A",
					},
				},
				{
					Destination: destinationWithConfigTopic,
					Message: map[string]interface{}{
						"type":  "track",
						"event": "event-with-empty-topic",
					},
				},
				{
					Destination: destinationWithConfigTopic,
					Message: map[string]interface{}{
						"type":  "track",
						"event": "",
					},
				},
				{
					Destination: destinationWithConfigTopic,
					Message: map[string]interface{}{
						"type": "group",
					},
				},
				{
					Destination: destinationWithConfigTopic,
					Message: map[string]interface{}{
						"type": "page",
					},
				},
			},
			want: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: map[string]interface{}{
							"topicId": "topic-A",
							"userId":  "",
							"message": map[string]interface{}{
								"type":  "track",
								"event": "event-A",
							},
							"attributes": map[string]interface{}{},
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
					{
						Output: map[string]interface{}{
							"topicId": "topic-default",
							"userId":  "",
							"message": map[string]interface{}{
								"type":  "track",
								"event": "event-with-empty-topic",
							},
							"attributes": map[string]interface{}{},
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
					{
						Output: map[string]interface{}{
							"topicId": "topic-default",
							"userId":  "",
							"message": map[string]interface{}{
								"type":  "track",
								"event": "",
							},
							"attributes": map[string]interface{}{},
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
					{
						Output: map[string]interface{}{
							"topicId": "topic-group",
							"userId":  "",
							"message": map[string]interface{}{
								"type": "group",
							},
							"attributes": map[string]interface{}{},
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
					{
						Output: map[string]interface{}{
							"topicId": "topic-default",
							"userId":  "",
							"message": map[string]interface{}{
								"type": "page",
							},
							"attributes": map[string]interface{}{},
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
				},
			},
		},
		{
			name: "should return error if topic is not set for the event",
			events: []types.TransformerEvent{
				{
					Destination: backendconfig.DestinationT{
						Config: map[string]interface{}{
							"eventToTopicMap": []interface{}{
								map[string]interface{}{
									"from": "event-A",
									"to":   "topic-A",
								},
							},
						},
					},
					Message: map[string]interface{}{
						"type": "identify",
					},
				},
				// this event should be transformed to topic-default
				{
					Destination: backendconfig.DestinationT{},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "event-A",
					},
				},
			},
			want: types.Response{
				FailedEvents: []types.TransformerResponse{
					{
						Error:      "no topic set for this event",
						Metadata:   types.Metadata{},
						StatusCode: http.StatusBadRequest,
						StatTags:   destinationStatTags,
					},
				},
				Events: []types.TransformerResponse{
					{
						Output: map[string]interface{}{
							"topicId": "topic-A",
							"userId":  "",
							"message": map[string]interface{}{
								"type":  "track",
								"event": "event-A",
							},
							"attributes": map[string]interface{}{},
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
				},
			},
		},
		{
			name: "should return error if * is mapped to empty topic with no other mapping",
			events: []types.TransformerEvent{
				{
					Destination: backendconfig.DestinationT{
						Config: map[string]interface{}{
							"eventToTopicMap": []interface{}{
								map[string]interface{}{
									"from": "*",
									"to":   "",
								},
							},
						},
					},
					Message: map[string]interface{}{
						"type":  "track",
						"event": "event-A",
					},
				},
			},
			want: types.Response{
				FailedEvents: []types.TransformerResponse{
					{
						Error:      "no topic set for this event",
						Metadata:   types.Metadata{},
						StatusCode: http.StatusBadRequest,
						StatTags:   destinationStatTags,
					},
				},
			},
		},
		{
			name: "should set correct correct attributes for each event",
			events: []types.TransformerEvent{
				{
					Destination: destinationWithConfigAttributes,
					Message: map[string]interface{}{
						"type":       "track",
						"event":      "event-B",
						"attr-key-1": "value-1",
						"":           "empty-attr-key-value",
						"properties": map[string]interface{}{
							"attr-key-2": "value-2",
							"A": map[string]interface{}{
								"B": map[string]interface{}{
									"attr-key-6": "value-6",
								},
							},
						},
					},
				},
				{
					Destination: destinationWithConfigAttributes,
					Message: map[string]interface{}{
						"type":             "track",
						"event":            "event-unknown",
						"attr-key-1":       "value-1",
						"attr-key-default": "value-default",
						"properties": map[string]interface{}{
							"attr-key-2": "value-2",
							"A": map[string]interface{}{
								"B": map[string]interface{}{
									"attr-key-6": "value-6",
								},
							},
						},
					},
				},
				{
					Destination: destinationWithConfigAttributes,
					Message: map[string]interface{}{
						"type":       "identify",
						"attr-key-1": "value-1",
						"attr-key-3": "value-3",
					},
				},
				{
					Destination: destinationWithConfigAttributes,
					Message: map[string]interface{}{
						"type":       "track",
						"event":      "event-A",
						"attr-key-1": "value-1",
						"properties": map[string]interface{}{
							"attr-key-2": "value-2",
							"A": map[string]interface{}{
								"B": map[string]interface{}{
									"attr-key-6": "value-6",
								},
							},
						},
						"context": map[string]interface{}{
							"traits": map[string]interface{}{
								"attr-key-3": "value-3",
								"A": map[string]interface{}{
									"B": map[string]interface{}{
										"attr-key-4": "value-4",
										"C": map[string]interface{}{
											"attr-key-5": "",
										},
									},
								},
							},
						},
					},
				},
			},
			want: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"type":       "track",
								"event":      "event-B",
								"attr-key-1": "value-1",
								"":           "empty-attr-key-value",
								"properties": map[string]interface{}{
									"attr-key-2": "value-2",
									"A": map[string]interface{}{
										"B": map[string]interface{}{
											"attr-key-6": "value-6",
										},
									},
								},
							},
							"attributes": map[string]interface{}{
								"attr-key-1": "value-1",
								"":           "empty-attr-key-value",
							},
							"topicId": "topic-default",
							"userId":  "",
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"type":             "track",
								"event":            "event-unknown",
								"attr-key-1":       "value-1",
								"attr-key-default": "value-default",
								"properties": map[string]interface{}{
									"attr-key-2": "value-2",
									"A": map[string]interface{}{
										"B": map[string]interface{}{
											"attr-key-6": "value-6",
										},
									},
								},
							},
							"attributes": map[string]interface{}{
								"attr-key-default": "value-default",
							},
							"topicId": "topic-default",
							"userId":  "",
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"type":       "identify",
								"attr-key-1": "value-1",
								"attr-key-3": "value-3",
							},
							"attributes": map[string]interface{}{
								"attr-key-1": "value-1",
								"attr-key-3": "value-3",
							},
							"topicId": "topic-default",
							"userId":  "",
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"type":       "track",
								"event":      "event-A",
								"attr-key-1": "value-1",
								"properties": map[string]interface{}{
									"attr-key-2": "value-2",
									"A": map[string]interface{}{
										"B": map[string]interface{}{
											"attr-key-6": "value-6",
										},
									},
								},
								"context": map[string]interface{}{
									"traits": map[string]interface{}{
										"attr-key-3": "value-3",
										"A": map[string]interface{}{
											"B": map[string]interface{}{
												"attr-key-4": "value-4",
												"C": map[string]interface{}{
													"attr-key-5": "",
												},
											},
										},
									},
								},
							},
							"attributes": map[string]interface{}{
								"attr-key-1": "value-1",
								"attr-key-2": "value-2",
								"attr-key-3": "value-3",
								"attr-key-4": "value-4",
								"attr-key-5": "",
								"attr-key-6": "value-6",
							},
							"topicId": "topic-default",
							"userId":  "",
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
					},
				},
			},
		},
		{
			name: "should update timestamp field for RETL events",
			events: []types.TransformerEvent{
				{
					Destination: destinationWithConfigAttributes,
					Message: map[string]interface{}{
						"type":    "identify",
						"channel": "sources",
						"traits": map[string]interface{}{
							"timestamp": "2020-01-01T00:00:00Z",
						},
					},
				},
			},
			want: types.Response{
				Events: []types.TransformerResponse{
					{
						Output: map[string]interface{}{
							"message": map[string]interface{}{
								"type":      "identify",
								"channel":   "sources",
								"timestamp": "2020-01-01T00:00:00Z",
								"traits": map[string]interface{}{
									"timestamp": "2020-01-01T00:00:00Z",
								},
							},
							"attributes": map[string]interface{}{},
							"topicId":    "topic-default",
							"userId":     "",
						},
						StatusCode: http.StatusOK,
						Metadata:   types.Metadata{},
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

func TestPanicIfDestinationIDIsDifferent(t *testing.T) {
	events := []types.TransformerEvent{
		{
			Destination: backendconfig.DestinationT{ID: "destination-id-123"},
		},
		{
			Destination: backendconfig.DestinationT{ID: "destination-id-456"},
		},
	}

	require.Panics(t, func() {
		Transform(context.Background(), events)
	})
}
