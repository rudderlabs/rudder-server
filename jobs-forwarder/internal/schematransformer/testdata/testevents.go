package testdata

var TrackEvent = map[string]interface{}{
	"messageId": "message-id",
	"type":      "track",
	"event":     "event-name",
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
		"property2": "value2",
	},
}

var CompositeFlattenedEvent = map[string]interface{}{
	"messageId":            "message-id-composite",
	"type":                 "identify",
	"properties.property1": "value1",
	"properties.property2": "value2",
}

var IdentifyFlattenedEvent = map[string]interface{}{
	"messageId": "message-id-identify",
	"type":      "identify",
}
