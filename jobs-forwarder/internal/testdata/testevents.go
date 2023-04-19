package testdata

import (
	proto "github.com/rudderlabs/rudder-server/proto/event-schema"
)

var TrackEvent = map[string]interface{}{
	"messageId": "message-id",
	"type":      "track",
	"event":     "event-name",
}

var TrackSchema = map[string]string{
	"event":     "string",
	"messageId": "string",
	"type":      "string",
}

var IdentifyEvent = map[string]interface{}{
	"messageId": "message-id-identify",
	"type":      "identify",
}

var CompositeEvent = map[string]interface{}{
	"messageId": "message-id-composite",
	"type":      "identify",
	"properties": map[string]interface{}{
		"property1": "value1",
		"property2": 1,
	},
}

var CompositeFlattenedEvent = map[string]interface{}{
	"messageId":            "message-id-composite",
	"type":                 "identify",
	"properties.property1": "value1",
	"properties.property2": 1,
}

var CompositeSchema = map[string]string{
	"messageId":            "string",
	"type":                 "string",
	"properties.property1": "string",
	"properties.property2": "int",
}

var IdentifyFlattenedEvent = map[string]interface{}{
	"messageId": "message-id-identify",
	"type":      "identify",
}

var TestEventSchemaKey = proto.EventSchemaKey{
	WriteKey:        WriteKeyEnabled,
	EventType:       "track",
	EventIdentifier: "event-name",
}

var TestParams = []byte(`{"source_id": "enabled-source"}`)
