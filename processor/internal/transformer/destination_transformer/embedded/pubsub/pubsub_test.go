package pubsub

import (
	"context"
	"net/http"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	types "github.com/rudderlabs/rudder-server/processor/types"
	"github.com/stretchr/testify/require"
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
						Error:      "No topic set for this event",
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
						Error:      "No topic set for this event",
						Metadata:   types.Metadata{},
						StatusCode: http.StatusBadRequest,
						StatTags:   destinationStatTags,
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
