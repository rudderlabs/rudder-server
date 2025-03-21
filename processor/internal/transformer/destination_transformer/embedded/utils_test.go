package utils

import (
	"reflect"
	"testing"

	"github.com/rudderlabs/rudder-server/processor/types"
)

func TestUpdateTimestampFieldForRETLEvent(t *testing.T) {
	cases := []struct {
		name         string
		eventMessage types.SingularEventT
		expected     types.SingularEventT
	}{
		{
			name: "should not update the timestamp field if the channel is not sources",
			eventMessage: types.SingularEventT{
				"type":    "identify",
				"channel": "destinations",
				"context": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
			expected: types.SingularEventT{
				"type":    "identify",
				"channel": "destinations",
				"context": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
		},
		{
			name: "should not update the timestamp field if channel is not present",
			eventMessage: types.SingularEventT{
				"type": "track",
				"properties": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
			expected: types.SingularEventT{
				"type": "track",
				"properties": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
		},
		{
			name: "should not update the timestamp field if mappedToDestination is present",
			eventMessage: types.SingularEventT{
				"type":    "identify",
				"channel": "sources",
				"context": map[string]interface{}{
					"mappedToDestination": "sources",
					"timestamp":           "2021-01-01T00:00:00Z",
				},
			},
			expected: types.SingularEventT{
				"type":    "identify",
				"channel": "sources",
				"context": map[string]interface{}{
					"mappedToDestination": "sources",
					"timestamp":           "2021-01-01T00:00:00Z",
				},
			},
		},
		{
			name: "should not update the timestamp field if event type is not present",
			eventMessage: types.SingularEventT{
				"channel": "sources",
				"context": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
			expected: types.SingularEventT{
				"channel": "sources",
				"context": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
		},
		{
			name: "should not update the timestamp field if event type is not identify or track",
			eventMessage: types.SingularEventT{
				"type":    "page",
				"channel": "sources",
				"context": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
			expected: types.SingularEventT{
				"type":    "page",
				"channel": "sources",
				"context": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
		},
		{
			name: "should update the timestamp field if event type is identify from context.timestamp",
			eventMessage: types.SingularEventT{
				"type":    "identify",
				"channel": "sources",
				"context": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
					"traits": map[string]interface{}{
						"timestamp": "2022-01-01T00:00:00Z",
					},
				},
			},
			expected: types.SingularEventT{
				"type":      "identify",
				"channel":   "sources",
				"timestamp": "2021-01-01T00:00:00Z",
				"context": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
					"traits": map[string]interface{}{
						"timestamp": "2022-01-01T00:00:00Z",
					},
				},
			},
		},
		{
			name: "should update the timestamp field if event type is identify from context.traits.timestamp",
			eventMessage: types.SingularEventT{
				"type":    "identify",
				"channel": "sources",
				"context": map[string]interface{}{
					"traits": map[string]interface{}{
						"timestamp": "2022-01-01T00:00:00Z",
					},
				},
			},
			expected: types.SingularEventT{
				"type":      "identify",
				"channel":   "sources",
				"timestamp": "2022-01-01T00:00:00Z",
				"context": map[string]interface{}{
					"traits": map[string]interface{}{
						"timestamp": "2022-01-01T00:00:00Z",
					},
				},
			},
		},
		{
			name: "should update the timestamp field if event type is identify from traits.timestamp",
			eventMessage: types.SingularEventT{
				"type":              "identify",
				"channel":           "sources",
				"timestamp":         "2020-01-01T00:00:00Z",
				"originalTimestamp": "2021-01-01T00:00:00Z",
				"traits": map[string]interface{}{
					"timestamp": "2022-01-01T00:00:00Z",
				},
			},
			expected: types.SingularEventT{
				"type":              "identify",
				"channel":           "sources",
				"timestamp":         "2022-01-01T00:00:00Z",
				"originalTimestamp": "2021-01-01T00:00:00Z",
				"traits": map[string]interface{}{
					"timestamp": "2022-01-01T00:00:00Z",
				},
			},
		},
		{
			name: "should update the timestamp field if event type is identify from timestamp field",
			eventMessage: types.SingularEventT{
				"type":              "identify",
				"channel":           "sources",
				"timestamp":         "2022-01-01T00:00:00Z",
				"originalTimestamp": "2021-01-01T00:00:00Z",
			},
			expected: types.SingularEventT{
				"type":              "identify",
				"channel":           "sources",
				"timestamp":         "2022-01-01T00:00:00Z",
				"originalTimestamp": "2021-01-01T00:00:00Z",
			},
		},
		{
			name: "should update the timestamp field if event type is identify from originalTimestamp field",
			eventMessage: types.SingularEventT{
				"type":              "identify",
				"channel":           "sources",
				"originalTimestamp": "2021-01-01T00:00:00Z",
			},
			expected: types.SingularEventT{
				"type":              "identify",
				"channel":           "sources",
				"timestamp":         "2021-01-01T00:00:00Z",
				"originalTimestamp": "2021-01-01T00:00:00Z",
			},
		},
		{
			name: "should update the timestamp field if event type is track from properties.timestamp field",
			eventMessage: types.SingularEventT{
				"type":              "track",
				"channel":           "sources",
				"originalTimestamp": "2020-01-01T00:00:00Z",
				"properties": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
			expected: types.SingularEventT{
				"type":              "track",
				"channel":           "sources",
				"timestamp":         "2021-01-01T00:00:00Z",
				"originalTimestamp": "2020-01-01T00:00:00Z",
				"properties": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
		},
		{
			name: "should update the timestamp field if event type is track from timestamp field",
			eventMessage: types.SingularEventT{
				"type":              "track",
				"channel":           "sources",
				"timestamp":         "2021-01-01T00:00:00Z",
				"originalTimestamp": "2020-01-01T00:00:00Z",
				"properties": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
			expected: types.SingularEventT{
				"type":              "track",
				"channel":           "sources",
				"timestamp":         "2021-01-01T00:00:00Z",
				"originalTimestamp": "2020-01-01T00:00:00Z",
				"properties": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
		},
		{
			name: "should update the timestamp field if event type is track from originalTimestamp field",
			eventMessage: types.SingularEventT{
				"type":              "track",
				"channel":           "sources",
				"originalTimestamp": "2021-01-01T00:00:00Z",
			},
			expected: types.SingularEventT{
				"type":              "track",
				"channel":           "sources",
				"timestamp":         "2021-01-01T00:00:00Z",
				"originalTimestamp": "2021-01-01T00:00:00Z",
			},
		},
		{
			name: "should update the timestamp field even if mappedToDestination is empty string",
			eventMessage: types.SingularEventT{
				"type":    "identify",
				"channel": "sources",
				"context": map[string]interface{}{
					"mappedToDestination": "",
					"timestamp":           "2021-01-01T00:00:00Z",
				},
			},
			expected: types.SingularEventT{
				"type":      "identify",
				"channel":   "sources",
				"timestamp": "2021-01-01T00:00:00Z",
				"context": map[string]interface{}{
					"mappedToDestination": "",
					"timestamp":           "2021-01-01T00:00:00Z",
				},
			},
		},
		{
			name: "should update the timestamp field with the first non empty timestamp field",
			eventMessage: types.SingularEventT{
				"type":    "identify",
				"channel": "sources",
				"context": map[string]interface{}{
					"timestamp": "",
					"traits": map[string]interface{}{
						"timestamp": "",
					},
				},
				"traits": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
			expected: types.SingularEventT{
				"type":      "identify",
				"channel":   "sources",
				"timestamp": "2021-01-01T00:00:00Z",
				"context": map[string]interface{}{
					"timestamp": "",
					"traits": map[string]interface{}{
						"timestamp": "",
					},
				},
				"traits": map[string]interface{}{
					"timestamp": "2021-01-01T00:00:00Z",
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := UpdateTimestampFieldForRETLEvent(c.eventMessage)
			if !reflect.DeepEqual(result, c.expected) {
				t.Errorf("expected %v, got %v", c.expected, result)
			}
		})
	}
}
