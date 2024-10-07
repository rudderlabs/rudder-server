package rules

import (
	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
)

var (
	IdentifyDataLakeRules = map[string]string{
		"context_request_ip": "request_ip",
		"context_passed_ip":  "context.ip",
	}
	IdentifyNonDataLakeRules = map[string]string{
		"context_request_ip": "request_ip",
		"context_passed_ip":  "context.ip",
		"sent_at":            "sentAt",
		"timestamp":          "timestamp",
		"original_timestamp": "originalTimestamp",
	}
	IdentifyFunctionalRules = map[string]FunctionalRules{
		"context_ip": func(event ptrans.TransformerEvent) (any, error) {
			return firstValidValue(event.Message, []string{"context.ip", "request_ip"}), nil
		},
	}
)
