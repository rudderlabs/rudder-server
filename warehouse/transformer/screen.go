package transformer

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/warehouse/transformer/internal/rules"
)

func (t *transformer) handleScreenEvent(pi *processingInfo) ([]map[string]any, error) {
	event := make(map[string]any)
	columnTypes := make(map[string]string)

	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["properties"], event, columnTypes, &prefixInfo{
		completePrefix: "screen_properties_",
		completeLevel:  2,
	}); err != nil {
		return nil, fmt.Errorf("screen: setting data and column types from input: %w", err)
	}
	if err := t.setDataAndColumnTypeFromInput(pi, pi.event.Message["context"], event, columnTypes, &prefixInfo{
		completePrefix: "screen_context_",
		completeLevel:  2,
		prefix:         "context_",
	}); err != nil {
		return nil, fmt.Errorf("screen: setting data and column types from input: %w", err)
	}
	if err := t.setDataAndColumnTypeFromRules(pi, event, columnTypes,
		rules.DefaultRules, lo.Assign(rules.DefaultFunctionalRules, rules.ScreenFunctionalRules),
	); err != nil {
		return nil, fmt.Errorf("screen: setting data and column types from rules: %w", err)
	}

	if err := storeRudderEvent(pi, event, columnTypes); err != nil {
		return nil, fmt.Errorf("screen: storing rudder event: %w", err)
	}

	tableName, err := SafeTableName(pi.event.Metadata.DestinationType, pi.itrOpts, "screens")
	if err != nil {
		return nil, fmt.Errorf("screen: safe table name: %w", err)
	}
	columns, err := t.getColumns(pi.event.Metadata.DestinationType, event, columnTypes)
	if err != nil {
		return nil, fmt.Errorf("screen: getting columns: %w", err)
	}

	mergeEvents, err := t.handleMergeEvent(pi)
	if err != nil {
		return nil, fmt.Errorf("screen: merge event: %w", err)
	}

	screenOutput := map[string]any{
		"data": event,
		"metadata": map[string]any{
			"table":      tableName,
			"columns":    columns,
			"receivedAt": pi.event.Metadata.ReceivedAt,
		},
		"userId": "",
	}
	return append([]map[string]any{screenOutput}, mergeEvents...), nil
}
