package rules

import (
	"fmt"
	"strings"

	"github.com/samber/lo"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

type FunctionalRules func(event *ptrans.TransformerEvent) (any, error)

var (
	DefaultRules = map[string]FunctionalRules{
		"id":                 staticRule("messageId"),
		"anonymous_id":       staticRule("anonymousId"),
		"user_id":            staticRule("userId"),
		"sent_at":            staticRule("sentAt"),
		"timestamp":          staticRule("timestamp"),
		"received_at":        staticRule("receivedAt"),
		"original_timestamp": staticRule("originalTimestamp"),
		"channel":            staticRule("channel"),
		"context_ip": func(event *ptrans.TransformerEvent) (any, error) {
			return firstValidValue(event.Message, []string{"context.ip", "request_ip"}), nil
		},
		"context_request_ip": staticRule("request_ip"),
		"context_passed_ip":  staticRule("context.ip"),
	}

	TrackRules = map[string]FunctionalRules{
		"event_text": staticRule("event"),
	}
	TrackEventTableRules = map[string]FunctionalRules{
		"id": func(event *ptrans.TransformerEvent) (any, error) {
			eventType := event.Metadata.EventType
			canUseRecordID := utils.CanUseRecordID(event.Metadata.SourceCategory)
			if eventType == "track" && canUseRecordID {
				return extractCloudRecordID(event.Message, &event.Metadata, event.Metadata.MessageID)
			}
			return event.Metadata.MessageID, nil
		},
	}
	TrackTableRules = map[string]FunctionalRules{
		"record_id": func(event *ptrans.TransformerEvent) (any, error) {
			eventType := event.Metadata.EventType
			canUseRecordID := utils.CanUseRecordID(event.Metadata.SourceCategory)
			if eventType == "track" && canUseRecordID {
				cr, err := extractCloudRecordID(event.Message, &event.Metadata, nil)
				if err != nil {
					return nil, fmt.Errorf("extracting cloud record id: %w", err)
				}
				return utils.ToString(cr), nil
			}
			return nil, nil // nolint: nilnil
		},
	}

	IdentifyRules = map[string]FunctionalRules{
		"context_ip": func(event *ptrans.TransformerEvent) (any, error) {
			return firstValidValue(event.Message, []string{"context.ip", "request_ip"}), nil
		},
		"context_request_ip": staticRule("request_ip"),
		"context_passed_ip":  staticRule("context.ip"),
	}
	IdentifyRulesNonDataLake = map[string]FunctionalRules{
		"context_ip": func(event *ptrans.TransformerEvent) (any, error) {
			return firstValidValue(event.Message, []string{"context.ip", "request_ip"}), nil
		},
		"context_request_ip": staticRule("request_ip"),
		"context_passed_ip":  staticRule("context.ip"),
		"sent_at":            staticRule("sentAt"),
		"timestamp":          staticRule("timestamp"),
		"original_timestamp": staticRule("originalTimestamp"),
	}

	PageRules = map[string]FunctionalRules{
		"name": func(event *ptrans.TransformerEvent) (any, error) {
			return firstValidValue(event.Message, []string{"name", "properties.name"}), nil
		},
	}

	ScreenRules = map[string]FunctionalRules{
		"name": func(event *ptrans.TransformerEvent) (any, error) {
			return firstValidValue(event.Message, []string{"name", "properties.name"}), nil
		},
	}

	AliasRules = map[string]FunctionalRules{
		"previous_id": staticRule("previousId"),
	}

	GroupRules = map[string]FunctionalRules{
		"group_id": staticRule("groupId"),
	}

	ExtractRules = map[string]FunctionalRules{
		"id": func(event *ptrans.TransformerEvent) (any, error) {
			return extractRecordID(&event.Metadata)
		},
		"received_at": staticRule("receivedAt"),
		"event":       staticRule("event"),
	}
)

func staticRule(value string) FunctionalRules {
	return func(*ptrans.TransformerEvent) (any, error) {
		return value, nil
	}
}

var rudderReservedColumns = map[string]map[string]struct{}{
	"track":    createReservedColumns(DefaultRules, TrackRules, TrackTableRules, TrackEventTableRules),
	"identify": createReservedColumns(DefaultRules, IdentifyRules),
	"page":     createReservedColumns(DefaultRules, PageRules),
	"screen":   createReservedColumns(DefaultRules, ScreenRules),
	"group":    createReservedColumns(DefaultRules, GroupRules),
	"alias":    createReservedColumns(DefaultRules, AliasRules),
	"extract":  createReservedColumns(ExtractRules),
}

func createReservedColumns(rules ...map[string]FunctionalRules) map[string]struct{} {
	return lo.MapEntries(lo.Assign(rules...), func(key string, _ FunctionalRules) (string, struct{}) {
		return key, struct{}{}
	})
}

func firstValidValue(message map[string]any, props []string) any {
	for _, prop := range props {
		propKeys := strings.Split(prop, ".")
		if val := misc.MapLookup(message, propKeys...); val != nil && !utils.IsBlank(val) {
			return val
		}
	}
	return nil
}

func extractRecordID(metadata *ptrans.Metadata) (any, error) {
	if metadata.RecordID == nil || utils.IsBlank(metadata.RecordID) {
		return nil, response.ErrRecordIDEmpty
	}
	if utils.IsObject(metadata.RecordID) {
		return nil, response.ErrRecordIDObject
	}
	return metadata.RecordID, nil
}

func extractCloudRecordID(message types.SingularEventT, metadata *ptrans.Metadata, fallbackValue any) (any, error) {
	if sv := misc.MapLookup(message, "context", "sources", "version"); sv != nil && !utils.IsBlank(sv) {
		return extractRecordID(metadata)
	}
	return fallbackValue, nil
}

func IsRudderReservedColumn(eventType, columnName string) bool {
	lowerEventType := strings.ToLower(eventType)
	if _, ok := rudderReservedColumns[lowerEventType]; !ok {
		return false
	}
	lowerColumnName := strings.ToLower(columnName)
	if _, ok := rudderReservedColumns[lowerEventType][lowerColumnName]; ok {
		return true
	}
	return false
}
