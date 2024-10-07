package rules

import (
	"fmt"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

var (
	TrackRules = map[string]string{
		"event_text": "event",
	}
	TrackTableFunctionalRules = map[string]FunctionalRules{
		"record_id": func(event ptrans.TransformerEvent) (any, error) {
			eventType := event.Metadata.EventType
			canUseRecordID := utils.CanUseRecordID(event.Metadata.SourceCategory)
			if eventType == "track" && canUseRecordID {
				cr, err := extractCloudRecordID(event.Message, event.Metadata, nil)
				if err != nil {
					return nil, fmt.Errorf("extracting cloud record id: %w", err)
				}
				return utils.ToString(cr), nil
			}
			return nil, nil // nolint: nilnil
		},
	}
	TrackEventTableFunctionalRules = map[string]FunctionalRules{
		"id": func(event ptrans.TransformerEvent) (any, error) {
			eventType := event.Metadata.EventType
			canUseRecordID := utils.CanUseRecordID(event.Metadata.SourceCategory)
			if eventType == "track" && canUseRecordID {
				return extractCloudRecordID(event.Message, event.Metadata, event.Metadata.MessageID)
			}
			return event.Metadata.MessageID, nil
		},
	}
)

func extractCloudRecordID(message types.SingularEventT, metadata ptrans.Metadata, fallbackValue any) (any, error) {
	sourcesVersion := misc.MapLookup(message, "context", "sources", "version")
	if sourcesVersion == nil || utils.IsBlank(sourcesVersion) {
		return fallbackValue, nil
	}
	return extractRecordID(metadata)
}
