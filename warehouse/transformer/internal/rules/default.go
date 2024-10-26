package rules

import (
	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
)

var (
	DefaultRules = map[string]string{
		"id":                 "messageId",
		"anonymous_id":       "anonymousId",
		"user_id":            "userId",
		"sent_at":            "sentAt",
		"timestamp":          "timestamp",
		"received_at":        "receivedAt",
		"original_timestamp": "originalTimestamp",
		"channel":            "channel",
		"context_request_ip": "request_ip",
		"context_passed_ip":  "context.ip",
	}
	DefaultFunctionalRules = map[string]FunctionalRules{
		"context_ip": func(event ptrans.TransformerEvent) (any, error) {
			return firstValidValue(event.Message, []string{"context.ip", "request_ip"}), nil
		},
	}
)
