package rules

import (
	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/response"
	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/utils"
)

var (
	ExtractRules = map[string]string{
		"received_at": "receivedAt",
		"event":       "event",
	}
	ExtractFunctionalRules = map[string]FunctionalRules{
		"id": func(event ptrans.TransformerEvent) (any, error) {
			return extractRecordID(event.Metadata)
		},
	}
)

func extractRecordID(metadata ptrans.Metadata) (any, error) {
	if metadata.RecordID == nil || utils.IsBlank(metadata.RecordID) {
		return nil, response.ErrRecordIDEmpty
	}
	if utils.IsObject(metadata.RecordID) {
		return nil, response.ErrRecordIDObject
	}
	return metadata.RecordID, nil
}
